"""
CUIN v2 - Parquet -> Doris Ingestion

Doris has no SQL-level equivalent of DuckDB's `read_parquet()` FROM
clause (see DorisDialect.read_source). This module uses pyarrow --
already a dependency, and reads Parquet natively -- purely as a
Parquet -> newline-delimited-JSON converter, then bulk-loads that
NDJSON into a Doris table over Stream Load (Doris's HTTP PUT bulk-load
API). One JSON object per line, no enclosing array or commas -- Stream
Load's `read_json_by_line` format, no reshaping needed in between.

pyarrow.parquet.read_table() on a Spark-style multi-part output
directory (this dataset's `data_source/oracle_data.parquet/` is
part-00000..part-NNNNN + .crc sidecars) was verified empirically to
produce the identical row count, row order, and content as the
previous DuckDB read_parquet() implementation on the real dataset
before this rewrite replaced it -- same lexicographic part-file
ordering, .crc/_SUCCESS sidecars correctly ignored by both readers.

This is the standard high-throughput path for getting data into Doris
(the alternative, row-by-row INSERT, does not scale to millions of
rows); a real deployment would more likely run CDC/Kafka -> Stream
Load continuously, but for a one-shot Parquet load this is the
direct, correct mechanism.
"""

import base64
import json
import logging
import os
import re

import httpx
import pyarrow.parquet as pq

_CONTROL_CHARS_RE = re.compile(r"[\r\n\t]")

logger = logging.getLogger(__name__)

RAW_TABLE_COLUMNS = ("CUSTOMER_CODE", "NAME", "BIRTH_DATE", "MOBILE", "EMAIL", "DOCUMENT", "FULL_ADDRESS")


def create_raw_table_sql(buckets: int = 32) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS raw (
    CUSTOMER_CODE VARCHAR(64) NOT NULL,
    NAME          VARCHAR(500),
    BIRTH_DATE    VARCHAR(32),
    MOBILE        ARRAY<TEXT>,
    EMAIL         ARRAY<TEXT>,
    DOCUMENT      ARRAY<TEXT>,
    FULL_ADDRESS  ARRAY<TEXT>
)
DUPLICATE KEY(CUSTOMER_CODE)
DISTRIBUTED BY HASH(CUSTOMER_CODE) BUCKETS {buckets}
""".strip()


def _clean_str(s):
    return _CONTROL_CHARS_RE.sub("", s) if s is not None else None


def _clean_array(arr):
    return None if arr is None else [_clean_str(x) for x in arr]


def export_parquet_to_ndjson(parquet_path: str, out_path: str, limit: int = None) -> int:
    """
    Returns the number of rows written.

    Strips embedded control characters (\\r, \\n, \\t) from every
    string/array-of-string field before export. Found live, on the
    full 1.5M-row dataset specifically (never surfaced on the 5k
    sample): a source DOCUMENT value containing a stray trailing \\r
    (real-world data noise) round-trips through the old DuckDB-based
    JSON export correctly (properly escaped as the 2-byte sequence
    \\r), but Doris's Stream Load JSON parser does not correctly
    unescape it -- it comes out the other side as a literal lowercase
    'r' character appended to the value, which then fails TIN
    validation (regexp '^[0-9]{12}$'). One pair out of 549,879 differed
    because of exactly this. Control characters are never meaningful
    content in any of these fields, so stripping them at the ingestion
    boundary is a safe, narrow fix -- NULL array elements are preserved
    as JSON null (not stripped/dropped), matching the prior
    implementation's list_transform-over-NULL semantics.
    """
    table = pq.read_table(parquet_path, columns=list(RAW_TABLE_COLUMNS))
    if limit:
        table = table.slice(0, limit)

    scalar_cols = ["CUSTOMER_CODE", "NAME", "BIRTH_DATE"]
    array_cols = ["MOBILE", "EMAIL", "DOCUMENT", "FULL_ADDRESS"]
    columns = {c: table.column(c).to_pylist() for c in RAW_TABLE_COLUMNS}
    n_rows = table.num_rows

    with open(out_path, "w", encoding="utf-8") as f:
        for i in range(n_rows):
            row = {c: _clean_str(columns[c][i]) for c in scalar_cols}
            row.update({c: _clean_array(columns[c][i]) for c in array_cols})
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")
    return n_rows


def stream_load_ndjson(
    fe_host: str,
    fe_http_port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    ndjson_path: str,
    label: str = None,
    timeout: int = 600,
) -> dict:
    """
    One Stream Load HTTP PUT for the whole file. Doris recommends
    batching large loads (hundreds of MB to a few GB per request) --
    for the 1.5M-row / ~800MB dataset this is one request; a truly
    10B-row load would chunk this call, which callers can do by
    passing pre-split NDJSON files through this same function.
    """
    url = f"http://{fe_host}:{fe_http_port}/api/{database}/{table}/_stream_load"
    # Stream Load's normal flow is: PUT to the FE, FE responds 307
    # redirecting to the target BE's own HTTP port to actually receive
    # the data. httpx strips Authorization on any redirect that
    # changes host OR port (a deliberate anti-credential-leak
    # behavior, not a bug) -- verified live, this happens even when
    # the header is set manually rather than via the `auth=` kwarg.
    # Doing the redirect ourselves as two independent requests (each
    # with our own headers, never handed to httpx's redirect handling)
    # sidesteps that entirely.
    basic = base64.b64encode(f"{user}:{password}".encode()).decode()
    headers = {
        "Expect": "100-continue",
        "format": "json",
        "read_json_by_line": "true",
        "strip_outer_array": "false",
        "Authorization": f"Basic {basic}",
    }
    if label:
        headers["label"] = label

    with open(ndjson_path, "rb") as f:
        data = f.read()

    with httpx.Client(follow_redirects=False, timeout=timeout) as client:
        first = client.put(url, content=data, headers=headers)
        if first.status_code in (307, 302, 301) and "location" in first.headers:
            resp = client.put(first.headers["location"], content=data, headers=headers)
        else:
            resp = first
    resp.raise_for_status()
    result = resp.json()
    if result.get("Status") not in ("Success", "Publish Timeout"):
        raise RuntimeError(f"Doris Stream Load failed: {result}")
    logger.info(
        f"Stream loaded {result.get('NumberLoadedRows')} rows into {database}.{table} "
        f"in {result.get('LoadTimeMs')}ms (label={result.get('Label')})"
    )
    return result


def get_single_backend_id(con) -> str:
    """
    Doris's LOCAL() table-value function reads files local to ONE
    specific BE, so it needs that BE's id. Only safe to auto-resolve
    for a single-BE cluster -- exactly the low-resource deployment this
    read-in-place path targets. A multi-BE cluster can't guarantee the
    lake files are on any particular BE's local disk; that case needs
    the lake on shared storage, read via the S3() TVF or an external
    Hive/Iceberg catalog instead (both confirmed present in Doris
    4.0.3) -- not this function.
    """
    rows = con.execute("SHOW BACKENDS").fetchall()
    if len(rows) != 1:
        raise RuntimeError(
            f"LOCAL()-based read-in-place requires exactly one Doris BE "
            f"(found {len(rows)}). A multi-BE cluster needs the lake on "
            f"shared storage (S3()/catalog), not LOCAL()."
        )
    return str(rows[0][0])  # BackendId is SHOW BACKENDS' first column


def local_parquet_view_sql(con, view_name: str, be_relative_glob: str) -> str:
    """
    `CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM LOCAL(...)` --
    reads Parquet directly off the BE's local disk, no copy into a
    Doris table, no Stream Load, no NDJSON round-trip (and so none of
    export_parquet_to_ndjson's control-character sanitizing is needed
    either -- Parquet's binary encoding has no JSON-escaping step to
    corrupt).

    `be_relative_glob` is resolved relative to the BE process's OWN
    working directory -- verified live against Doris 4.0.3: LOCAL()
    prefixes `file_path` with the BE's home dir even when it starts
    with '/', it is NOT the container filesystem root. A directory
    (rather than an explicit `*.parquet` glob) resolves zero files --
    "No matches found" -- even when it contains valid Parquet parts, so
    callers must pass the glob, not the bare directory.  See the
    `doris` service's volume mounts in infra/docker-compose.yml for
    where `be_relative_glob` must physically live.
    """
    backend_id = get_single_backend_id(con)
    return f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * FROM LOCAL(
            'file_path' = '{be_relative_glob}',
            'backend_id' = '{backend_id}',
            'format' = 'parquet'
        )
    """


def load_parquet_to_doris(
    parquet_path: str,
    fe_host: str,
    fe_http_port: int,
    user: str,
    password: str,
    database: str,
    table: str = "raw",
    ndjson_scratch_path: str = "/tmp/cuin_doris_ingest.json",
    limit: int = None,
) -> dict:
    """End-to-end: export -> stream load -> cleanup. Returns the Stream Load result dict."""
    n_rows = export_parquet_to_ndjson(parquet_path, ndjson_scratch_path, limit=limit)
    logger.info(f"Exported {n_rows} rows from {parquet_path} to {ndjson_scratch_path}")
    try:
        return stream_load_ndjson(fe_host, fe_http_port, user, password, database, table, ndjson_scratch_path)
    finally:
        if os.path.exists(ndjson_scratch_path):
            os.remove(ndjson_scratch_path)
