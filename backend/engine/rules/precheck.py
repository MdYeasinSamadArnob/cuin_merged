"""
CUIN v2 - Exact Blocking-Rule Fan-Out Precheck

Estimates how many candidate pairs a blocking rule would generate --
EXACTLY, not approximately -- before the rule is saved or run. Because
the rule catalog (engine.rules.catalog) makes the blocking key
expression known ahead of time, this is a plain `GROUP BY key, COUNT`
followed by SUM(n*(n-1)/2) over groups with n > 1: the closed-form
count of unordered pairs a block of size n produces. This is exactly
the workload a distributed OLAP engine is fastest at, and it stays
cheap even for a rule that WOULD explode -- the whole point is to
answer "how many pairs would this make" without ever materializing
them.

Critically, this reuses engine.rules.compiler's own setup_statements
and KeySource (not a re-derived approximation of them) -- the staging
table this measures is the exact same one the real self-join would
read from, so `estimate_fanout().n_pairs` is provably equal to what
compile_and_build() would actually produce for that rule alone. See
tests/unit/test_precheck_accuracy.py.
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple

from engine.ports.dialect import SqlDialect
from engine.rules.catalog import BlockingRule, BlockingRuleType
from engine.rules.compiler import compile_rule, _execute_multi


class FanoutSeverity:
    SAFE = "SAFE"
    WARN = "WARN"
    BLOCK = "BLOCK"


@dataclass
class HeavyKey:
    key_repr: str
    n_records: int
    n_pairs: int


@dataclass
class FanoutEstimate:
    rule_id: str
    n_keys: int          # distinct key values with >= 2 records (i.e. that produce >= 1 pair)
    n_pairs: int          # exact SUM(n*(n-1)/2) over those keys
    largest_block: int    # max n_records in any single key
    heaviest_keys: List[HeavyKey]
    severity: str
    suggested_max_block_size: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "n_keys": self.n_keys,
            "n_pairs": self.n_pairs,
            "largest_block": self.largest_block,
            "heaviest_keys": [
                {"key": h.key_repr, "n_records": h.n_records, "n_pairs": h.n_pairs}
                for h in self.heaviest_keys
            ],
            "severity": self.severity,
            "suggested_max_block_size": self.suggested_max_block_size,
        }


# Default budgets -- overridable per call. A rule generating more than
# WARN_PAIRS candidate pairs is flagged but savable; more than
# BLOCK_PAIRS refuses to save until guarded down (see routes_rules.py).
DEFAULT_WARN_PAIRS = 2_000_000
DEFAULT_BLOCK_PAIRS = 20_000_000
DEFAULT_WARN_BLOCK_SIZE = 5_000
DEFAULT_BLOCK_BLOCK_SIZE = 50_000


def _severity(n_pairs: int, largest_block: int, warn_pairs: int, block_pairs: int,
              warn_block: int, block_block: int) -> str:
    if n_pairs > block_pairs or largest_block > block_block:
        return FanoutSeverity.BLOCK
    if n_pairs > warn_pairs or largest_block > warn_block:
        return FanoutSeverity.WARN
    return FanoutSeverity.SAFE


def _suggest_max_block_size(rows: List[Tuple[str, int]], budget_pairs: int) -> Optional[int]:
    """
    Given [(key, n_records), ...] sorted descending by n_records, find
    the smallest max_block_size guard (a cap on n_records per key)
    that would bring total pairs under `budget_pairs`. Returns None if
    even excluding every multi-record key still exceeds budget (the
    rule needs a different key, not just a size cap) or if no cap is
    needed at all.
    """
    sizes = sorted((n for _, n in rows if n >= 2), reverse=True)
    if not sizes:
        return None

    def total_pairs_with_cap(cap: int) -> int:
        return sum((n * (n - 1)) // 2 for n in sizes if n <= cap)

    if total_pairs_with_cap(sizes[0]) <= budget_pairs:
        return None  # no cap needed

    lo, hi = 1, sizes[0]
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if total_pairs_with_cap(mid) <= budget_pairs:
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def _exact_identifier_group_sizes(rule: BlockingRule, dialect: SqlDialect, con) -> List[Tuple[str, int]]:
    """
    identifier_frequency (engine.blocking.suppression) already holds
    the post-validation, per-(id_type, value_norm) record count and
    is_suppressed flag -- exactly the group sizes EXACT_IDENTIFIER's
    self-join would see. No new table needs to be built.
    """
    id_types_sql = ", ".join(dialect.quote_str(t) for t in rule.fields)
    caps = [v for v in (rule.guards.max_block_size, rule.guards.max_key_frequency) if v]
    cap_where = f"AND n_records <= {min(caps)}" if caps else ""
    rows = con.execute(f"""
        SELECT value_norm, n_records
        FROM identifier_frequency
        WHERE id_type IN ({id_types_sql}) AND NOT is_suppressed {cap_where}
    """).fetchall()
    return [(str(v), int(n)) for v, n in rows]


def estimate_fanout(
    rule: BlockingRule,
    dialect: SqlDialect,
    con,
    warn_pairs: int = DEFAULT_WARN_PAIRS,
    block_pairs: int = DEFAULT_BLOCK_PAIRS,
    warn_block_size: int = DEFAULT_WARN_BLOCK_SIZE,
    block_block_size: int = DEFAULT_BLOCK_BLOCK_SIZE,
    top_n_heaviest: int = 20,
) -> FanoutEstimate:
    """
    Requires `identifiers`/`customer_scalars`/`identifier_frequency`
    already built on `con` (i.e. normalize + suppression stages have
    run). Executes the rule's setup_statements (cheap: aggregation
    only, never the actual self-join) then computes exact fan-out.
    """
    rule.validate()

    if rule.type == BlockingRuleType.EXACT_IDENTIFIER:
        rows = _exact_identifier_group_sizes(rule, dialect, con)
    else:
        setup_statements, _select_sql, key_source = compile_rule(rule, dialect)
        for stmt in setup_statements:
            _execute_multi(con, stmt)
        if key_source is None:
            raise ValueError(f"{rule.rule_id}: no key source available for precheck")
        key_cols_sql = ", ".join(key_source.key_columns)
        rows = con.execute(f"""
            SELECT {key_cols_sql}, COUNT(DISTINCT customer_code) AS n
            FROM {key_source.table}
            GROUP BY {key_cols_sql}
        """).fetchall()
        # Collapse (possibly multi-column) key tuples into one display
        # string; count column is always last.
        rows = [("|".join(str(v) for v in r[:-1]), int(r[-1])) for r in rows]

    multi = [(k, n) for k, n in rows if n >= 2]
    n_pairs = sum((n * (n - 1)) // 2 for _, n in multi)
    largest_block = max((n for _, n in rows), default=0)

    heaviest = sorted(multi, key=lambda kn: kn[1], reverse=True)[:top_n_heaviest]
    heaviest_keys = [
        HeavyKey(key_repr=k, n_records=n, n_pairs=(n * (n - 1)) // 2) for k, n in heaviest
    ]

    severity = _severity(n_pairs, largest_block, warn_pairs, block_pairs, warn_block_size, block_block_size)
    suggestion = None
    if severity != FanoutSeverity.SAFE:
        suggestion = _suggest_max_block_size(rows, warn_pairs)

    return FanoutEstimate(
        rule_id=rule.rule_id,
        n_keys=len(multi),
        n_pairs=n_pairs,
        largest_block=largest_block,
        heaviest_keys=heaviest_keys,
        severity=severity,
        suggested_max_block_size=suggestion,
    )


def estimate_catalog_fanout(rules: List[BlockingRule], dialect: SqlDialect, con, **kwargs) -> List[FanoutEstimate]:
    """Runs estimate_fanout for every ENABLED rule in the catalog. Order matches rule.order."""
    enabled = sorted([r for r in rules if r.enabled], key=lambda r: r.order)
    return [estimate_fanout(r, dialect, con, **kwargs) for r in enabled]
