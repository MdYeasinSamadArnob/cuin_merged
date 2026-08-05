"""
CUIN v2 - Raw Source Data Viewer (Doris-backed)

Lets a bank officer browse the raw, un-normalized source Parquet file
directly -- before running any pipeline -- with pagination, filters,
and Doris inverted-index-backed text search across every column.

Materializes the source into a real, standing Doris table (NOT a
per-run database -- this exists independent of any pipeline run) via
`CREATE TABLE ... AS SELECT * FROM LOCAL(...)`, the same read-in-place
mechanism pipeline/doris_orchestrator.py uses for its own per-run
ingest (see engine.ports.doris_ingest.local_parquet_view_sql). CTAS
infers Doris-native column types straight from the Parquet file's own
schema -- no manual pyarrow-to-Doris type mapping needed.

Verified live against Doris 4.0.3 before writing this (nothing in this
codebase had used INVERTED indexes before):
- A scalar TEXT/VARCHAR column supports
  `USING INVERTED PROPERTIES('parser'='english','lower_case'='true')`
  for real word-tokenized, case-insensitive full-text search
  (MATCH_ANY/MATCH_ALL). Without an explicit parser, the default
  tokenizer badly under-matches on this dataset's messy real-world
  name strings (mixed case, periods, honorifics) -- empirically 18
  matches for a term that a case-insensitive LIKE finds 55,473 of.
- An ARRAY<TEXT> column (MOBILE/EMAIL/DOCUMENT/FULL_ADDRESS/TELEPHONE
  in this dataset) does NOT support the 'parser' property at all
  (`INVERTED index with parser: english is not supported for array
  column`) -- only the default (no-parser) form works, and it does
  work for MATCH_ANY against array elements.
- CTAS over the full 1.57M-row dataset completes in under 2 seconds;
  a plain (non-indexed) LIKE filter over 1.57M rows runs in ~130ms.
  Both numbers make even a from-scratch materialize-and-scan cheap
  enough to not need caching beyond "don't re-materialize on every
  request" -- the standing table itself IS the cache.
"""

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from engine.ports.doris_conn import DorisConnection
from engine.ports.doris_ingest import get_single_backend_id

logger = logging.getLogger(__name__)

RAW_PREVIEW_DB = "cuin_raw_preview"
RAW_PREVIEW_TABLE = "raw_preview"
PARQUET_PATH = "data_source/oracle_data.parquet"


@dataclass
class ColumnInfo:
    name: str
    doris_type: str
    is_array: bool
    searchable: bool  # True if a usable inverted index exists on this column
    filterable: bool  # True for any column a WHERE clause can reference


def _is_array_type(doris_type: str) -> bool:
    return doris_type.lower().startswith("array<")


def _classify_columns(con: DorisConnection) -> List[ColumnInfo]:
    rows = con.execute(f"DESCRIBE {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}").fetchall()
    indexed = _indexed_columns(con)
    columns = []
    for row in rows:
        name, doris_type = row[0], row[1]
        columns.append(ColumnInfo(
            name=name,
            doris_type=doris_type,
            is_array=_is_array_type(doris_type),
            searchable=name in indexed,
            filterable=True,
        ))
    return columns


def _indexed_columns(con: DorisConnection) -> set:
    try:
        rows = con.execute(f"SHOW INDEX FROM {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}").fetchall()
    except Exception:
        return set()
    # SHOW INDEX FROM's column layout (MySQL-compatible): ... Column_name is index 4.
    return {row[4] for row in rows}


def table_exists(con: DorisConnection) -> bool:
    try:
        rows = con.execute(f"SHOW TABLES FROM {RAW_PREVIEW_DB} LIKE '{RAW_PREVIEW_TABLE}'").fetchall()
    except Exception:
        # RAW_PREVIEW_DB itself doesn't exist yet -- the normal state
        # before the very first /refresh, not an error worth surfacing.
        return False
    return len(rows) > 0


def materialize(con: DorisConnection) -> Dict[str, Any]:
    """
    (Re)builds RAW_PREVIEW_DB.RAW_PREVIEW_TABLE from the current source
    Parquet file and best-effort indexes every column. Safe to call
    repeatedly -- e.g. a bank officer clicking "Refresh" after
    replacing the source file with a new upload.
    """
    con.execute(f"CREATE DATABASE IF NOT EXISTS {RAW_PREVIEW_DB}")
    backend_id = get_single_backend_id(con)

    con.execute(f"DROP TABLE IF EXISTS {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}")
    con.execute(f"""
        CREATE TABLE {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE} AS
        SELECT * FROM LOCAL(
            'file_path' = '{PARQUET_PATH}/*.parquet',
            'backend_id' = '{backend_id}',
            'format' = 'parquet'
        )
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}").fetchone()[0]

    rows = con.execute(f"DESCRIBE {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}").fetchall()
    indexed_columns = []
    failed_columns = []
    for row in rows:
        name, doris_type = row[0], row[1]
        is_array = _is_array_type(doris_type)
        properties = "" if is_array else " PROPERTIES('parser'='english','lower_case'='true')"
        try:
            con.execute(
                f"CREATE INDEX idx_{name}_inv ON {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}({name}) "
                f"USING INVERTED{properties}"
            )
            indexed_columns.append(name)
        except Exception as e:
            # Best-effort: a column type INVERTED genuinely can't index
            # (e.g. some numeric/date types Doris rejects for this
            # index kind) just stays filterable via plain WHERE/LIKE
            # instead of MATCH_ANY -- never fails the whole refresh.
            logger.warning(f"Could not create inverted index on {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}.{name}: {e}")
            failed_columns.append(name)

    logger.info(
        f"Materialized {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}: {row_count} rows, "
        f"{len(indexed_columns)} indexed columns, {len(failed_columns)} not indexed"
    )

    return {
        "row_count": row_count,
        "indexed_columns": indexed_columns,
        "failed_columns": failed_columns,
        "columns": [c.__dict__ for c in _classify_columns(con)],
    }


def get_status(con: DorisConnection) -> Optional[Dict[str, Any]]:
    """Returns None if the preview table has never been materialized."""
    if not table_exists(con):
        return None
    row_count = con.execute(f"SELECT COUNT(*) FROM {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE}").fetchone()[0]
    return {
        "row_count": row_count,
        "columns": [c.__dict__ for c in _classify_columns(con)],
    }


def _parse_array_value(value):
    """
    Array-typed columns come back from pymysql as a JSON-encoded
    string (e.g. '["DHAKA", "DHAKA"]'), not a native Python list --
    DorisConnection's execute()/fetchall() surface doesn't deserialize
    array-typed columns (same behavior relied on/documented in
    api/routes_public_identity_api.py's _parse_token_array). Parsed
    here so the API returns real JSON arrays, not double-encoded
    strings the frontend would have to re-parse itself.
    """
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except (ValueError, TypeError):
        return []


def _validate_column(columns: List[ColumnInfo], name: str) -> ColumnInfo:
    for c in columns:
        if c.name == name:
            return c
    raise ValueError(f"Unknown column: {name!r}")


# Operators exposed to the advanced filter builder. `sql` is a format
# string with `{col}` for the (already-validated, allowlisted) column
# name; the operand itself is always passed as a bound parameter, never
# interpolated, so filter VALUES can never be a SQL-injection vector
# even though column NAMES are (necessarily) interpolated after an
# allowlist check against the table's real DESCRIBE output.
_OPERATORS = {
    "eq": "{col} = %s",
    "neq": "{col} != %s",
    "contains": "{col} LIKE %s",
    "starts_with": "{col} LIKE %s",
    "gt": "{col} > %s",
    "gte": "{col} >= %s",
    "lt": "{col} < %s",
    "lte": "{col} <= %s",
    "is_null": "{col} IS NULL",
    "is_not_null": "{col} IS NOT NULL",
}
_ARRAY_OPERATORS = {
    "array_contains": "array_contains({col}, %s)",
}


def build_where_clause(
    columns: List[ColumnInfo],
    q: Optional[str],
    filters: Optional[List[Dict[str, Any]]],
) -> Tuple[str, List[Any]]:
    """
    Builds a WHERE clause (empty string if no conditions) plus its
    bound parameter list, from a quick-search term (`q`, OR'd across
    every searchable/indexed column via MATCH_ANY) and/or a list of
    per-column advanced filters (AND'd together).
    """
    clauses: List[str] = []
    params: List[Any] = []

    if q:
        searchable = [c for c in columns if c.searchable]
        if searchable:
            match_clauses = [f"{c.name} MATCH_ANY %s" for c in searchable]
            clauses.append("(" + " OR ".join(match_clauses) + ")")
            params.extend([q] * len(searchable))

    for f in (filters or []):
        col = _validate_column(columns, f["column"])
        op = f["op"]
        if col.is_array:
            if op not in _ARRAY_OPERATORS:
                raise ValueError(f"Operator {op!r} not valid for array column {col.name!r}")
            clauses.append(_ARRAY_OPERATORS[op].format(col=col.name))
            params.append(f["value"])
            continue
        if op not in _OPERATORS:
            raise ValueError(f"Unknown filter operator: {op!r}")
        template = _OPERATORS[op]
        clauses.append(template.format(col=col.name))
        if op == "contains":
            params.append(f"%{f['value']}%")
        elif op == "starts_with":
            params.append(f"{f['value']}%")
        elif op not in ("is_null", "is_not_null"):
            params.append(f["value"])

    if not clauses:
        return "", []
    return "WHERE " + " AND ".join(clauses), params


def query_rows(
    con: DorisConnection,
    page: int,
    page_size: int,
    q: Optional[str] = None,
    filters: Optional[List[Dict[str, Any]]] = None,
    sort_col: Optional[str] = None,
    sort_dir: str = "asc",
) -> Dict[str, Any]:
    columns = _classify_columns(con)
    where_sql, params = build_where_clause(columns, q, filters)

    order_sql = ""
    if sort_col:
        sort_column = _validate_column(columns, sort_col)
        if sort_column.is_array:
            # Doris rejects ORDER BY on an ARRAY<TEXT> column outright
            # (INTERNAL_ERROR "meet invalid type") -- reject cleanly
            # here instead of letting that opaque engine error reach
            # the API response.
            raise ValueError(f"Cannot sort by array column {sort_column.name!r}")
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
        order_sql = f"ORDER BY {sort_column.name} {direction}"

    count_sql = f"SELECT COUNT(*) FROM {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE} {where_sql}"
    total = con.execute(count_sql, params or None).fetchone()[0]

    offset = (page - 1) * page_size
    rows_sql = (
        f"SELECT * FROM {RAW_PREVIEW_DB}.{RAW_PREVIEW_TABLE} {where_sql} "
        f"{order_sql} LIMIT {int(page_size)} OFFSET {int(offset)}"
    )
    cursor_result = con.execute(rows_sql, params or None)
    array_cols = {c.name for c in columns if c.is_array}
    col_names = [c.name for c in columns]
    rows = []
    for row in cursor_result.fetchall():
        record = dict(zip(col_names, row))
        for name in array_cols:
            record[name] = _parse_array_value(record[name])
        rows.append(record)

    return {
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "columns": [c.__dict__ for c in columns],
    }
