"""
Shared live-Doris test fixture helpers.

Extracted from the (now-removed) DuckDB-vs-Doris cross-engine parity
test's setup boilerplate, which every unit test migrated off DuckDB
during the Doris-only migration also needs: a scratch Doris database,
the source Parquet ingested into it, and identifiers/frequency/
candidate_pairs/evidence built through the dialect-portable SQL
builders. Centralized here rather than duplicated per test file.
"""

import os

import pytest

DORIS_HOST = os.environ.get("DORIS_HOST", "127.0.0.1")
DORIS_MYSQL_PORT = int(os.environ.get("DORIS_MYSQL_PORT", "9130"))
DORIS_HTTP_PORT = int(os.environ.get("DORIS_HTTP_PORT", "8130"))
DORIS_USER = os.environ.get("DORIS_USER", "root")
DORIS_PASSWORD = os.environ.get("DORIS_PASSWORD", "")

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")


def execute_multi(con, sql_maybe_multi_statement: str) -> None:
    """
    Executes a `;\\n`-joined batch one statement at a time -- pymysql
    (Doris) cursors, unlike DuckDB's, only run one statement per
    .execute() call. Same splitting convention as
    engine.rules.compiler._execute_multi.
    """
    for part in sql_maybe_multi_statement.split(";\n"):
        part = part.strip()
        if part:
            con.execute(part)


def parse_doris_array(value) -> list:
    """
    pymysql returns Doris ARRAY<...> columns as JSON-text strings, not
    native Python lists -- reuses pipeline.doris_orchestrator's
    production helper rather than re-implementing the same parsing
    (and re-risking the same bug it was written to fix: naively
    calling list() on the string silently chops it into characters).
    """
    from pipeline.doris_orchestrator import _parse_array
    return _parse_array(value)


def bare_connection():
    """A DorisConnection with no scratch database -- for tests that only need to evaluate SQL expressions, not query tables."""
    from engine.ports.doris_conn import DorisConnection
    return DorisConnection(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD)


def skip_unless_doris_reachable():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")
    try:
        import pymysql
        pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD, connect_timeout=3).close()
    except Exception as e:
        pytest.skip(f"No live Doris instance reachable at {DORIS_HOST}:{DORIS_MYSQL_PORT}: {e}")


class _DorisScratchDbFixture:
    """
    Base: a fresh scratch Doris database with SAMPLE_PARQUET ingested
    into a `raw` table (via the same Stream Load path
    engine.ports.doris_ingest uses in production). `.con`/`.dialect`
    are then usable exactly like the old DuckDbDialect-driven tests
    used their in-memory duckdb connection. Call `.close()` (or use as
    a context manager) to drop the scratch DB.
    """

    #: subclasses/callers set this False to skip ingesting the sample
    #: dataset -- e.g. tests that only need an empty database to create
    #: their own ad hoc tables in.
    ingest_raw = True

    def __init__(self, db_name: str):
        import pymysql
        from engine.ports.doris_conn import DorisConnection
        from engine.ports.doris_dialect import DorisDialect

        self.db_name = db_name
        self.dialect = DorisDialect()

        admin = pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD, autocommit=True)
        try:
            with admin.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
                cur.execute(f"CREATE DATABASE {db_name}")
        finally:
            admin.close()

        self.con = DorisConnection(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD, database=db_name)

        if self.ingest_raw:
            from engine.ports.doris_ingest import create_raw_table_sql, load_parquet_to_doris
            self.con.execute(create_raw_table_sql(buckets=8))
            load_parquet_to_doris(
                parquet_path=SAMPLE_PARQUET,
                fe_host=DORIS_HOST, fe_http_port=DORIS_HTTP_PORT,
                user=DORIS_USER, password=DORIS_PASSWORD, database=db_name, table="raw",
            )

    def close(self):
        import pymysql
        try:
            self.con.close()
        except Exception:
            pass
        admin = pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD, autocommit=True)
        try:
            with admin.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
        finally:
            admin.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class DorisScratchDb(_DorisScratchDbFixture):
    """An empty scratch database -- for tests that create their own ad hoc tables, no sample dataset needed."""
    ingest_raw = False


class DorisRawFixture(_DorisScratchDbFixture):
    """Just the ingested `raw` table -- for tests that block/query it directly without needing identifiers/evidence."""


class DorisEvidenceFixture(_DorisScratchDbFixture):
    """
    Adds identifiers/frequency/candidate_pairs/pair_evidence on top of
    `raw`, built through the dialect-portable SQL builders (parameterized
    by DorisDialect) -- for tests needing the full evidence pipeline.
    """

    def __init__(self, db_name: str, blocking_rules=None):
        from engine.normalize import explode_dialect
        from engine.blocking import suppression_dialect
        from engine.scoring import evidence_dialect
        from engine.rules.compiler import compile_and_build
        from engine.rules.catalog import DEFAULT_BLOCKING_RULES

        super().__init__(db_name)
        explode_dialect.build_identifiers_table(self.con, self.dialect, source_relation="raw")
        suppression_dialect.build_frequency_table(self.con, self.dialect)
        compile_and_build(self.con, list(blocking_rules or DEFAULT_BLOCKING_RULES), self.dialect)
        evidence_dialect.build_pair_evidence(self.con, self.dialect)
