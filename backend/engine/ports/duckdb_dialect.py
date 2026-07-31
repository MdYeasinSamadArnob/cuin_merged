"""
CUIN v2 - DuckDB SQL Dialect

Every method mirrors an expression already used verbatim in
engine.normalize.explode / engine.blocking.deterministic_blocker /
engine.scoring.evidence -- this dialect exists so the rule compiler
produces byte-identical SQL to those hand-written modules for the
default (seeded-from-YAML) rule catalog. See
tests/unit/test_rules_zero_change.py.
"""

from engine.ports.dialect import SqlDialect


class DuckDbDialect(SqlDialect):
    name = "duckdb"

    def quote_str(self, s: str) -> str:
        return "'" + s.replace("'", "''") + "'"

    def concat(self, *parts: str) -> str:
        return " || ".join(parts)

    def array_intersect(self, a: str, b: str) -> str:
        return f"list_intersect({a}, {b})"

    def array_union_distinct(self, a: str, b: str) -> str:
        return f"list_distinct(list_concat({a}, {b}))"

    def array_distinct(self, a: str) -> str:
        return f"list_distinct({a})"

    def array_sort(self, a: str) -> str:
        return f"list_sort({a})"

    def array_contains(self, a: str, x: str) -> str:
        return f"list_contains({a}, {x})"

    def array_size(self, a: str) -> str:
        return f"len({a})"

    def array_to_string(self, a: str, sep: str) -> str:
        return f"array_to_string({a}, {self.quote_str(sep)})"

    def array_filter_nonempty(self, a: str) -> str:
        return f"list_filter({a}, x -> x != '')"

    def array_element(self, a: str, one_based_index: int) -> str:
        return f"{a}[{one_based_index}]"

    def array_slice_from(self, a: str, one_based_start: int) -> str:
        return f"{a}[{one_based_start}:]"

    def collect_distinct_sorted(self, expr: str) -> str:
        return f"list_sort(list(DISTINCT {expr}))"

    def str_split(self, s: str, sep: str) -> str:
        return f"str_split({s}, {self.quote_str(sep)})"

    def str_len(self, s: str) -> str:
        return f"len({s})"

    def regexp_matches(self, s: str, pattern: str) -> str:
        return f"regexp_matches({s}, {self.quote_str(pattern)})"

    def regexp_replace_all(self, s: str, pattern: str, replacement: str) -> str:
        return f"regexp_replace({s}, {self.quote_str(pattern)}, {self.quote_str(replacement)}, 'g')"

    def regexp_extract(self, s: str, pattern: str, group: int) -> str:
        return f"regexp_extract({s}, {self.quote_str(pattern)}, {group})"

    def strip_accents(self, s: str) -> str:
        return f"strip_accents({s})"

    def md5(self, s: str) -> str:
        return f"md5({s})"

    def substr(self, s: str, start_1based: int, length: int) -> str:
        return f"substr({s}, {start_1based}, {length})"

    def substr_from(self, s: str, start_1based: int) -> str:
        return f"substr({s}, {start_1based})"

    def least(self, a: str, b: str) -> str:
        return f"LEAST({a}, {b})"

    def greatest(self, a: str, b: str) -> str:
        return f"GREATEST({a}, {b})"

    def date_part(self, part: str, date_expr: str) -> str:
        return f"{part}({date_expr})"

    def try_parse_datetime(self, s: str, fmt: str) -> str:
        return f"try_strptime({s}, {self.quote_str(fmt)})"

    def format_date(self, date_expr: str, fmt: str) -> str:
        return f"strftime({date_expr}, {self.quote_str(fmt)})"

    def date_diff_days(self, a_date_expr: str, b_date_expr: str) -> str:
        return f"date_diff('day', {b_date_expr}, {a_date_expr})"

    def unnest_lateral(self, source_relation: str, array_expr: str, elem_alias: str) -> str:
        return f"{source_relation}, UNNEST({array_expr}) AS t({elem_alias})"

    def unnest_column_ref(self, elem_alias: str) -> str:
        return elem_alias

    def select_star_except(self, relation_alias: str, excluded_cols: list) -> str:
        cols = ", ".join(excluded_cols)
        return f"{relation_alias}.* EXCLUDE ({cols})"

    def create_or_replace_table(self, table_name: str, select_sql: str) -> str:
        return f"CREATE OR REPLACE TABLE {table_name} AS\n{select_sql}"

    def read_source(self, path: str) -> str:
        return f"read_parquet({self.quote_str(path)})"
