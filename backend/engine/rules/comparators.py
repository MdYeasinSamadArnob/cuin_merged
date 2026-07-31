"""
CUIN v2 - Portable Comparator Registry

A closed set of field-matching comparators, each guaranteed to behave
identically on DuckDB and Doris -- the decision made after auditing
what's actually available on both engines (see docs/doris_compatibility.md):
Doris 4.0.3 has `ngram_search`/`soundex`; DuckDB has `levenshtein`/
`jaro_winkler_similarity`/`jaro_similarity`/`damerau_levenshtein`/
`hamming`/`jaccard` but NOT `soundex`. Neither engine has the other's
functions, so nothing built on either is portable -- verified live,
not assumed from documentation (`tools/doris_probe.py` and an
equivalent DuckDB check, both re-run while building this module).

This is exactly why Levenshtein/Jaro-Winkler are excluded here even
though DuckDB has them: a comparator that only works on one engine
would make the SAME saved rule catalog produce different match
decisions depending which engine happens to run it, silently, which
is worse than not offering it. Jaro-Winkler is additionally a poor
fit for this data even where available -- it scores `MD SHARIFUL
ISLAM` vs `MD SAIFUL ISLAM` (different people) at 0.9765, above any
sane match threshold.

Two tiers, per the migration plan:
  Tier A -- no precompute needed. The comparator's SQL reads the raw
    (normalized) column values directly at pair-comparison time via
    portable dialect primitives (engine.ports.dialect.SqlDialect).
  Tier B -- needs a value precomputed ONCE PER RECORD at normalize
    time (a vectorized batch pass over one column, NOT a per-pair SQL
    UDF -- see the note on engine.normalize.explode.py's UDF warning
    below) so that pairwise comparison reduces to a Tier-A op over the
    precomputed column. Declared here as registry metadata for the
    Settings UI to show ("phonetic matching: available once wired into
    normalize") -- NOT yet wired into engine.normalize.explode_dialect,
    which is intentionally out of scope for this pass (see the
    migration plan's Phase 4 note: making the pipeline mapping-driven
    is a separate, larger change from adding the comparator registry
    that will eventually declare its inputs).

engine.normalize.explode.py's docstring warns against Python UDFs the
SQL engine invokes millions of times (once per ROW of a multi-million-
row table, at query time). Tier B's precompute is a different
mechanism: a vectorized pass over one column's distinct-ish values
ONCE at normalize time (~seconds at 1.5M rows), whose OUTPUT is then
read by ordinary portable SQL at pair-comparison time. Said explicitly
here so this registry isn't read as contradicting that warning.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional

from engine.ports.dialect import SqlDialect


@dataclass(frozen=True)
class Comparator:
    id: str
    label: str
    description: str
    tier: str  # "A" (no precompute) | "B" (needs normalize-time precompute)
    applicable_semantic_types: List[str]
    implemented: bool  # False for Tier B until explode_dialect grows a precompute hook
    params_schema: dict = field(default_factory=dict)
    # (a_expr, b_expr, params, dialect) -> SQL boolean/numeric expression.
    # None for comparators not yet implemented.
    sql: Optional[Callable[[str, str, dict, SqlDialect], str]] = None
    # (value_a, value_b, params) -> bool. Python twin of `sql`, used by
    # engine.scoring.confidence.score_pair() when scoring a RAW_COLUMN
    # match rule (any bank-added field, not one of the 6 built-ins) --
    # the live pipeline decides through this Python path, not SQL (see
    # confidence_compiler.py's docstring), so a comparator usable on a
    # custom field needs BOTH or the SQL-only preview would silently
    # diverge from what a real run actually decides. Proven identical
    # to `sql` by tests/unit/test_comparator_python_sql_parity.py.
    python_eval: Optional[Callable[[Any, Any, dict], bool]] = None
    unavailable_reason: Optional[str] = None


def _exact_sql(a: str, b: str, params: dict, dialect: SqlDialect) -> str:
    return f"({a} = {b})"


def _set_intersect_sql(a: str, b: str, params: dict, dialect: SqlDialect) -> str:
    """Boolean: do the two (array-valued) fields share at least one value."""
    return f"({dialect.array_size(dialect.array_intersect(a, b))} > 0)"


def _token_jaccard_sql(a: str, b: str, params: dict, dialect: SqlDialect) -> str:
    """Jaccard similarity of two token arrays: |intersection| / |union|, as a float in [0,1]."""
    inter = dialect.array_size(dialect.array_intersect(a, b))
    union = dialect.array_size(dialect.array_union_distinct(a, b))
    return f"(CASE WHEN {union} = 0 THEN 0.0 ELSE CAST({inter} AS DOUBLE) / {union} END)"


def _token_containment_sql(a: str, b: str, params: dict, dialect: SqlDialect) -> str:
    """Boolean: every token of the SHORTER array appears in the LONGER one (order-independent substring-of-words)."""
    inter = dialect.array_size(dialect.array_intersect(a, b))
    size_a, size_b = dialect.array_size(a), dialect.array_size(b)
    smaller = f"LEAST({size_a}, {size_b})"
    return f"({smaller} > 0 AND {inter} = {smaller})"


def _prefix_sql(a: str, b: str, params: dict, dialect: SqlDialect) -> str:
    n = int(params.get("prefix_len", 4))
    return f"({dialect.substr(a, 1, n)} = {dialect.substr(b, 1, n)})"


def _numeric_tolerance_sql(a: str, b: str, params: dict, dialect: SqlDialect) -> str:
    tol = params.get("tolerance", 0)
    return f"(ABS(CAST({a} AS DOUBLE) - CAST({b} AS DOUBLE)) <= {float(tol)})"


def _date_tolerance_sql(a: str, b: str, params: dict, dialect: SqlDialect) -> str:
    tol_days = int(params.get("tolerance_days", 0))
    diff = dialect.date_diff_days(f"CAST({a} AS DATE)", f"CAST({b} AS DATE)")
    return f"(ABS({diff}) <= {tol_days})"


# ----------------------------------------------------------------------
# Python evaluators -- twins of the _*_sql functions above, used by
# engine.scoring.confidence.score_pair() to score a RAW_COLUMN match
# rule (any bank-added field) in the live pipeline's Python decision
# path. Each takes already-normalized values (see
# engine.scoring.confidence._prep_raw_value, which mirrors the SAME
# UPPER(TRIM(...))-then-split-on-single-space normalization the SQL
# compiler applies) and returns whether the rule MATCHED -- for
# token_jaccard this means "similarity >= params['threshold']", not
# the raw similarity, so every evaluator has the same bool contract.
# ----------------------------------------------------------------------

def _exact_eval(a, b, params: dict) -> bool:
    return a is not None and b is not None and a == b


def _set_intersect_eval(a, b, params: dict) -> bool:
    return bool(set(a or []) & set(b or []))


def _token_jaccard_eval(a, b, params: dict) -> bool:
    threshold = params.get("threshold", 0.5)
    sa, sb = set(a or []), set(b or [])
    union = sa | sb
    if not union:
        return False
    return (len(sa & sb) / len(union)) >= threshold


def _token_containment_eval(a, b, params: dict) -> bool:
    sa, sb = set(a or []), set(b or [])
    if not sa or not sb:
        return False
    smaller, larger = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
    return smaller.issubset(larger)


def _prefix_eval(a, b, params: dict) -> bool:
    n = int(params.get("prefix_len", 4))
    if a is None or b is None:
        return False
    return a[:n] == b[:n]


def _numeric_tolerance_eval(a, b, params: dict) -> bool:
    tol = float(params.get("tolerance", 0))
    try:
        return abs(float(a) - float(b)) <= tol
    except (TypeError, ValueError):
        return False


def _date_tolerance_eval(a, b, params: dict) -> bool:
    from datetime import date, datetime

    def _to_date(x):
        if isinstance(x, datetime):
            return x.date()
        if isinstance(x, date):
            return x
        if isinstance(x, str):
            try:
                return datetime.fromisoformat(x[:10]).date()
            except ValueError:
                return None
        return None

    da, db = _to_date(a), _to_date(b)
    if da is None or db is None:
        return False
    tol_days = int(params.get("tolerance_days", 0))
    return abs((da - db).days) <= tol_days


COMPARATORS: List[Comparator] = [
    Comparator(
        id="exact", label="Must be identical", tier="A", implemented=True,
        description="Values must match exactly, character for character.",
        applicable_semantic_types=["customer_id", "phone", "email", "national_id", "code", "date", "numeric"],
        sql=_exact_sql, python_eval=_exact_eval,
    ),
    Comparator(
        id="set_intersect", label="Shares any value", tier="A", implemented=True,
        description="For multi-value fields (a customer can have several phone numbers/emails/IDs) -- matches "
                     "if the two records share at least one common value.",
        applicable_semantic_types=["phone", "email", "national_id", "address"],
        sql=_set_intersect_sql, python_eval=_set_intersect_eval,
    ),
    Comparator(
        id="token_jaccard", label="Same words, any order", tier="A", implemented=True,
        description="Splits text into words and scores by the overlap -- 'JOHN A SMITH' and 'SMITH JOHN A' "
                     "score identically. Returns a similarity between 0 and 1.",
        applicable_semantic_types=["name", "address", "free_text"],
        sql=_token_jaccard_sql, python_eval=_token_jaccard_eval,
        params_schema={"threshold": {"type": "number", "default": 0.5, "min": 0, "max": 1}},
    ),
    Comparator(
        id="token_containment", label="All words of one appear in the other", tier="A", implemented=True,
        description="Matches when the shorter name/address's words are all present in the longer one -- "
                     "'MD RAHIM' inside 'MD RAHIM UDDIN'.",
        applicable_semantic_types=["name", "address"],
        sql=_token_containment_sql, python_eval=_token_containment_eval,
    ),
    Comparator(
        id="prefix", label="Starts the same", tier="A", implemented=True,
        description="Matches on the first N characters -- useful for truncated/abbreviated names.",
        applicable_semantic_types=["name", "code"],
        sql=_prefix_sql, python_eval=_prefix_eval,
        params_schema={"prefix_len": {"type": "integer", "default": 4, "min": 1, "max": 32}},
    ),
    Comparator(
        id="numeric_tolerance", label="Within a range", tier="A", implemented=True,
        description="Matches if two numbers are within a tolerance of each other -- e.g. amounts, ages.",
        applicable_semantic_types=["numeric"],
        sql=_numeric_tolerance_sql, python_eval=_numeric_tolerance_eval,
        params_schema={"tolerance": {"type": "number", "default": 0}},
    ),
    Comparator(
        id="date_tolerance", label="Within N days", tier="A", implemented=True,
        description="Matches if two dates are within a number of days of each other -- catches DOB typos "
                     "like a swapped day/month.",
        applicable_semantic_types=["date"],
        sql=_date_tolerance_sql, python_eval=_date_tolerance_eval,
        params_schema={"tolerance_days": {"type": "integer", "default": 0, "min": 0, "max": 365}},
    ),
    Comparator(
        id="phonetic", label="Sounds the same", tier="B", implemented=False,
        description="Matches names that sound alike even when spelled differently -- 'MOHAMMAD' and "
                     "'MOHAMMED'. Requires a one-time precomputed phonetic code per record at normalize time.",
        applicable_semantic_types=["name"],
        unavailable_reason="Not yet wired into the normalize stage -- engine.normalize.explode_dialect would "
                            "need to compute and store a phonetic code per record first.",
    ),
    Comparator(
        id="trigram_jaccard", label="Mostly the same letters", tier="B", implemented=False,
        description="Catches typos and transposed letters by comparing overlapping 3-letter chunks of text. "
                     "Requires a one-time precomputed trigram set per record at normalize time.",
        applicable_semantic_types=["name", "address"],
        unavailable_reason="Not yet wired into the normalize stage -- engine.normalize.explode_dialect would "
                            "need to compute and store a trigram set per record first.",
    ),
]

_BY_ID = {c.id: c for c in COMPARATORS}

# Excluded, not merely unlisted -- shown greyed out WITH the reason in
# the UI, per the migration plan ("answering *why* is part of the
# ask"), rather than silently absent.
EXCLUDED_COMPARATORS = [
    {
        "id": "levenshtein", "label": "Edit distance",
        "reason": "Available on DuckDB (levenshtein()) but Doris 4.0.3 has no equivalent function -- "
                   "using it would make the same saved rule behave differently depending which engine runs it.",
    },
    {
        "id": "jaro_winkler", "label": "Jaro-Winkler similarity",
        "reason": "Available on DuckDB (jaro_winkler_similarity()) but not on Doris 4.0.3. Also a poor fit for "
                   "this kind of data even where available -- it scores 'MD SHARIFUL ISLAM' vs 'MD SAIFUL "
                   "ISLAM' (different people) at 0.9765, above any workable match threshold.",
    },
]


def get_comparator(comparator_id: str) -> Optional[Comparator]:
    return _BY_ID.get(comparator_id)


_TOKEN_COMPARATORS = frozenset({"token_jaccard", "token_containment"})


def prep_raw_value(value, comparator_id: str, is_array: bool):
    """
    Normalizes one raw column value the SAME way the SQL compiler's
    RAW_COLUMN path does (engine.rules.compiler._compile_raw_column:
    `UPPER(TRIM(CAST(v AS VARCHAR)))`), so a Python-scored RAW_COLUMN
    match rule agrees with what a real run's SQL-compiled blocking/
    confidence baseline would compute. For array-typed columns, each
    element is normalized independently and empties dropped -- for
    scalar columns feeding a token-based comparator, the normalized
    string is split on a literal single space (`str.split(" ")`, NOT
    the no-argument `.split()`, which collapses runs of whitespace)
    to match DuckDB `str_split(s, ' ')` / Doris `split_by_string(s, ' ')`
    exactly, including the empty-string element a double space
    produces on both engines.
    """
    if is_array:
        return [str(v).strip().upper() for v in (value or []) if v is not None and str(v).strip() != ""]
    if value is None:
        return None
    s = str(value).strip().upper()
    if s == "":
        return None
    if comparator_id in _TOKEN_COMPARATORS:
        return s.split(" ")
    return s


def evaluate_python(comparator_id: str, value_a, value_b, params: dict) -> bool:
    """Dispatches to a Comparator's python_eval. Raises if unimplemented -- callers should check first via get_comparator()."""
    c = get_comparator(comparator_id)
    if c is None or c.python_eval is None:
        raise ValueError(f"Comparator {comparator_id!r} has no Python evaluator")
    return c.python_eval(value_a, value_b, params or {})


def comparators_for_semantic_type(semantic_type: str) -> List[Comparator]:
    return [c for c in COMPARATORS if semantic_type in c.applicable_semantic_types]


def comparator_to_dict(c: Comparator) -> dict:
    return {
        "id": c.id,
        "label": c.label,
        "description": c.description,
        "tier": c.tier,
        "implemented": c.implemented,
        "applicable_semantic_types": c.applicable_semantic_types,
        "params_schema": c.params_schema,
        "unavailable_reason": c.unavailable_reason,
    }


def registry_to_dict() -> dict:
    return {
        "comparators": [comparator_to_dict(c) for c in COMPARATORS],
        "excluded": EXCLUDED_COMPARATORS,
    }
