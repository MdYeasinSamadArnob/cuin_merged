"""
CUIN v2 - Apache Doris SQL Dialect

Every mapping here is verified against a live Doris 4.0.3 instance by
backend/tools/doris_probe.py -- see docs/doris_compatibility.md for
the pass/fail matrix this dialect was built from. Do not add a mapping
here without a corresponding probe assertion: silent function-name
guesses are exactly the failure mode Phase 0 of the migration plan
was written to prevent.

Known gap: `strip_accents` has no native Doris equivalent (confirmed
by probe). This falls back to a `translate()`-based ASCII-folding
chain covering common Latin-1/Latin Extended-A accented characters --
adequate for this codebase's Bangladeshi-banking Latin-script name/
address data, but not a general Unicode NFKD fold. If `translate` is
also unavailable, it degrades to a no-op (identity) so the pipeline
still runs, at the cost of not stripping accents -- logged once, not
silently.
"""

import logging

from engine.ports.dialect import SqlDialect

logger = logging.getLogger(__name__)

# Common accented Latin characters -> ASCII fold. Doris's translate()
# is a single-character-for-single-character map (like Postgres'
# translate, not a regex), so multi-char folds (e.g. ae ligature) are
# out of scope -- this covers the accents actually reachable from
# Latin-1 Supplement + Latin Extended-A, which is what a bank's Latin-
# script transliterated name data will contain.
_ACCENT_FOLD_FROM = (
    "ÀÁÂÃÄÅÈÉÊË"
    "ÌÍÎÏÑÒÓÔÕÖ"
    "ÙÚÛÜÝ"
    "àáâãäåèéêë"
    "ìíîïñòóôõö"
    "ùúûüýÿ"
)
_ACCENT_FOLD_TO = (
    "AAAAAAEEEE"
    "IIIINOOOOO"
    "UUUUY"
    "aaaaaaeeee"
    "iiiinooooo"
    "uuuuyy"
)


class DorisDialect(SqlDialect):
    name = "doris"

    def __init__(self, strip_accents_supported: bool = True, translate_supported: bool = True):
        # Set from backend/tools/doris_probe.py's live capability check
        # rather than assumed -- see module docstring.
        self._translate_supported = translate_supported
        self._strip_accents_warned = False

    def quote_str(self, s: str) -> str:
        # CRITICAL, verified live: Doris (MySQL-style) string literals
        # process backslash escapes -- '\s' in a Doris string literal
        # parses to the single character 's' (backslash silently
        # dropped, since \s isn't a recognized MySQL escape), whereas
        # DuckDB string literals do NOT process backslash escapes at
        # all ('\s' stays as the two characters \s). This means every
        # regex pattern containing \s, \d, \b, \., \- etc. -- which is
        # most of them -- silently loses its backslashes on Doris
        # unless they're doubled here first. Confirmed live:
        # regexp_replace('a b', '\s', 'X') is a no-op on Doris;
        # regexp_replace('a b', '\\s', 'X') correctly replaces the
        # space. Do not remove this escaping to "simplify" quoting --
        # every regexp_* method in this dialect depends on it.
        return "'" + s.replace("\\", "\\\\").replace("'", "''") + "'"

    def concat(self, *parts: str) -> str:
        return f"CONCAT({', '.join(parts)})"

    def array_intersect(self, a: str, b: str) -> str:
        return f"array_intersect({a}, {b})"

    def array_union_distinct(self, a: str, b: str) -> str:
        # array_union's dedup guarantee varies by version -- wrap in
        # array_distinct explicitly rather than rely on it.
        return f"array_distinct(array_union({a}, {b}))"

    def array_distinct(self, a: str) -> str:
        return f"array_distinct({a})"

    def array_sort(self, a: str) -> str:
        return f"array_sort({a})"

    def array_contains(self, a: str, x: str) -> str:
        return f"array_contains({a}, {x})"

    def array_size(self, a: str) -> str:
        return f"array_size({a})"

    def array_to_string(self, a: str, sep: str) -> str:
        return f"array_join({a}, {self.quote_str(sep)})"

    def array_filter_nonempty(self, a: str) -> str:
        # Doris's higher-order array functions take the lambda FIRST,
        # the array SECOND -- opposite of what the name suggests and
        # opposite of DuckDB's list_filter(array, lambda). Verified
        # live: array_filter(array, x -> ...) fails to resolve `x`.
        return f"array_filter(x -> x != '', {a})"

    def array_element(self, a: str, one_based_index: int) -> str:
        return f"element_at({a}, {one_based_index})"

    def array_slice_from(self, a: str, one_based_start: int) -> str:
        return f"array_slice({a}, {one_based_start})"

    def collect_distinct_sorted(self, expr: str) -> str:
        # collect_set() is an UNORDERED distinct-array aggregate in
        # Doris (unlike DuckDB's list(DISTINCT x), which is also
        # unordered per se but here explicitly wrapped for
        # determinism either way) -- always wrap in array_sort so
        # cross-run/cross-engine fingerprints agree.
        return f"array_sort(collect_set({expr}))"

    def str_split(self, s: str, sep: str) -> str:
        return f"split_by_string({s}, {self.quote_str(sep)})"

    def str_len(self, s: str) -> str:
        return f"char_length({s})"

    def regexp_matches(self, s: str, pattern: str) -> str:
        return f"({s} REGEXP {self.quote_str(pattern)})"

    def regexp_replace_all(self, s: str, pattern: str, replacement: str) -> str:
        # Doris's regexp_replace already replaces every match (no 'g'
        # flag concept) -- see docs/doris_compatibility.md.
        return f"regexp_replace({s}, {self.quote_str(pattern)}, {self.quote_str(replacement)})"

    def regexp_extract(self, s: str, pattern: str, group: int) -> str:
        return f"regexp_extract({s}, {self.quote_str(pattern)}, {group})"

    def strip_accents(self, s: str) -> str:
        if not self._translate_supported:
            if not self._strip_accents_warned:
                logger.warning(
                    "Doris dialect: translate() unsupported on this instance -- "
                    "strip_accents() is a no-op. Name/address matching on "
                    "accented input will not fold to ASCII. See "
                    "docs/doris_compatibility.md."
                )
                self._strip_accents_warned = True
            return s
        return f"translate({s}, {self.quote_str(_ACCENT_FOLD_FROM)}, {self.quote_str(_ACCENT_FOLD_TO)})"

    def md5(self, s: str) -> str:
        return f"md5({s})"

    def substr(self, s: str, start_1based: int, length: int) -> str:
        return f"substr({s}, {start_1based}, {length})"

    def substr_from(self, s: str, start_1based: int) -> str:
        return f"substr({s}, {start_1based})"

    def least(self, a: str, b: str) -> str:
        return f"least({a}, {b})"

    def greatest(self, a: str, b: str) -> str:
        return f"greatest({a}, {b})"

    def date_part(self, part: str, date_expr: str) -> str:
        # Doris exposes year()/month()/day() scalar functions with the
        # same names as DuckDB.
        return f"{part}({date_expr})"

    @staticmethod
    def _to_mysql_format(fmt: str) -> str:
        """DuckDB strptime/strftime tokens -> Doris (MySQL-style) tokens."""
        return fmt.replace("%M", "%i").replace("%S", "%s")

    def try_parse_datetime(self, s: str, fmt: str) -> str:
        # str_to_date returns NULL on parse failure -- already "try_" semantics.
        return f"str_to_date({s}, {self.quote_str(self._to_mysql_format(fmt))})"

    def format_date(self, date_expr: str, fmt: str) -> str:
        return f"date_format({date_expr}, {self.quote_str(self._to_mysql_format(fmt))})"

    def date_diff_days(self, a_date_expr: str, b_date_expr: str) -> str:
        # DATEDIFF(a, b) is already `a - b` in whole days on Doris/MySQL
        # (verified live: DATEDIFF('2020-01-10','2020-01-01') = 9) --
        # unlike DuckDB's date_diff('day', start, end), which is
        # `end - start` and so needs its arguments swapped instead.
        return f"DATEDIFF({a_date_expr}, {b_date_expr})"

    def unnest_lateral(self, source_relation: str, array_expr: str, elem_alias: str) -> str:
        # EXPLODE (not EXPLODE_OUTER): a NULL/empty array drops the
        # source row entirely, matching DuckDB's `FROM t, UNNEST(arr)`
        # comma-join semantics exactly (an inner-join-like explode).
        return f"{source_relation} LATERAL VIEW EXPLODE({array_expr}) _lv_{elem_alias} AS {elem_alias}"

    def unnest_column_ref(self, elem_alias: str) -> str:
        # Verified live: Doris requires the exploded column to be
        # qualified by its LATERAL VIEW alias -- a bare reference
        # fails to resolve even with no ambiguity in scope.
        return f"_lv_{elem_alias}.{elem_alias}"

    def select_star_except(self, relation_alias: str, excluded_cols: list) -> str:
        cols = ", ".join(excluded_cols)
        return f"{relation_alias}.* EXCEPT ({cols})"

    def create_or_replace_table(self, table_name: str, select_sql: str) -> str:
        # Doris CTAS with no explicit DISTRIBUTED BY auto-buckets in
        # 2.1+ -- fine for the small, ephemeral per-rule staging
        # tables the blocking compiler creates. The persistent,
        # colocated fact tables (identifiers/customer_scalars/
        # candidate_pairs) are defined explicitly in
        # engine.ports.doris_ddl, not through this generic path.
        return (
            f"DROP TABLE IF EXISTS {table_name};\n"
            f"CREATE TABLE {table_name} AS\n{select_sql}"
        )

    def read_source(self, path: str) -> str:
        raise NotImplementedError(
            "Doris has no direct SQL equivalent of DuckDB's read_parquet() FROM "
            "clause -- ingestion goes through STREAM LOAD / broker load, or the "
            "S3()/LOCAL() table-value functions once enabled. This repo's Doris "
            "adapter operates on already-loaded identifiers/customer_scalars "
            "tables; see engine.ports.doris_ddl and the migration plan's Phase 8."
        )
