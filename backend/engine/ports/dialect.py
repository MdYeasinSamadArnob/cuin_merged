"""
CUIN v2 - SQL Dialect Port

A small seam between the ER engine's logic (engine.rules.compiler,
engine.rules.decision_compiler, engine.rules.precheck) and the SQL
text sent to whichever engine executes it. Every method here
corresponds to a construct that differs between DuckDB and Doris --
see docs/doris_compatibility.md for the full capability matrix this
was derived from.

Nothing outside this module and its two implementations
(duckdb_dialect.py, doris_dialect.py) should ever hand-write an
engine-specific SQL fragment for the blocking/scoring pipeline -- that
is what makes the same rule compile correctly to both engines.
"""

from abc import ABC, abstractmethod


class SqlDialect(ABC):
    name: str

    # ---- identifiers / literals -------------------------------------
    @abstractmethod
    def quote_str(self, s: str) -> str:
        """Single-quoted SQL string literal with embedded quotes escaped."""

    # ---- array functions ----------------------------------------------
    @abstractmethod
    def array_intersect(self, a: str, b: str) -> str: ...

    @abstractmethod
    def array_union_distinct(self, a: str, b: str) -> str: ...

    @abstractmethod
    def array_distinct(self, a: str) -> str: ...

    @abstractmethod
    def array_sort(self, a: str) -> str: ...

    @abstractmethod
    def array_contains(self, a: str, x: str) -> str: ...

    @abstractmethod
    def array_size(self, a: str) -> str:
        """Number of elements in an array expression."""

    @abstractmethod
    def array_to_string(self, a: str, sep: str) -> str: ...

    @abstractmethod
    def array_filter_nonempty(self, a: str) -> str:
        """Filters out NULL/empty-string elements from a VARCHAR[] expression."""

    @abstractmethod
    def array_element(self, a: str, one_based_index: int) -> str: ...

    @abstractmethod
    def array_slice_from(self, a: str, one_based_start: int) -> str:
        """Elements from `one_based_start` to the end (inclusive, 1-based)."""

    @abstractmethod
    def collect_distinct_sorted(self, expr: str) -> str:
        """Aggregate: sorted array of DISTINCT `expr` values within the current GROUP BY."""

    # ---- string / regex functions --------------------------------------
    @abstractmethod
    def concat(self, *parts: str) -> str:
        """
        String concatenation. NOT the same as writing `||` inline --
        Doris follows MySQL's default sql_mode, where `||` is LOGICAL
        OR, not concatenation (verified live: `'a' || 'b'` returns
        NULL on Doris, not 'ab'). Every reason/key string built by
        engine.rules.compiler goes through this method instead of a
        literal `||` for exactly that reason.
        """

    @abstractmethod
    def str_split(self, s: str, sep: str) -> str: ...

    @abstractmethod
    def str_len(self, s: str) -> str: ...

    @abstractmethod
    def regexp_matches(self, s: str, pattern: str) -> str:
        """Boolean: does `s` match `pattern` anywhere."""

    @abstractmethod
    def regexp_replace_all(self, s: str, pattern: str, replacement: str) -> str: ...

    @abstractmethod
    def regexp_extract(self, s: str, pattern: str, group: int) -> str: ...

    @abstractmethod
    def strip_accents(self, s: str) -> str: ...

    @abstractmethod
    def md5(self, s: str) -> str: ...

    @abstractmethod
    def substr(self, s: str, start_1based: int, length: int) -> str: ...

    @abstractmethod
    def substr_from(self, s: str, start_1based: int) -> str:
        """substr(s, start) with no length -- everything from `start` to the end of the string."""

    # ---- scalar helpers -------------------------------------------------
    @abstractmethod
    def least(self, a: str, b: str) -> str: ...

    @abstractmethod
    def greatest(self, a: str, b: str) -> str: ...

    @abstractmethod
    def date_part(self, part: str, date_expr: str) -> str: ...

    @abstractmethod
    def try_parse_datetime(self, s: str, fmt: str) -> str: ...

    @abstractmethod
    def format_date(self, date_expr: str, fmt: str) -> str: ...

    @abstractmethod
    def date_diff_days(self, a_date_expr: str, b_date_expr: str) -> str:
        """Whole days between two DATE expressions (a - b), signed. Both args pre-cast to DATE by the caller."""

    # ---- relational fragments -------------------------------------------
    @abstractmethod
    def unnest_lateral(self, source_relation: str, array_expr: str, elem_alias: str) -> str:
        """Returns the FROM-clause fragment for `source_relation` joined against one exploded element of `array_expr`, bound to `elem_alias`."""

    @abstractmethod
    def unnest_column_ref(self, elem_alias: str) -> str:
        """
        How to REFER to the exploded column elsewhere in the query
        (SELECT list, WHERE) after `unnest_lateral` -- DuckDB's comma-
        join UNNEST binds a bare column name, but Doris's `LATERAL
        VIEW EXPLODE ... AS x` requires the reference to be qualified
        by the view's own alias (verified live -- bare `x` fails to
        resolve even with no ambiguity).
        """

    @abstractmethod
    def select_star_except(self, relation_alias: str, excluded_cols: list) -> str: ...

    @abstractmethod
    def create_or_replace_table(self, table_name: str, select_sql: str) -> str: ...

    @abstractmethod
    def read_source(self, path: str) -> str:
        """FROM-clause fragment to read the raw source dataset."""
