"""
Proves the migration plan's central swappability claim end-to-end:
running the FULL pipeline (ingest -> normalize -> suppress -> block ->
evidence -> decide) through the Doris dialect produces pair-for-pair
identical decisions to the same pipeline through DuckDB, on the same
input data.

Requires a live Doris instance reachable over MySQL (port 9130) and
HTTP Stream Load (port 8130) -- see docs/doris_compatibility.md
"Environment notes" for how to stand one up. Skips gracefully if
unreachable, since this exercises real infrastructure, not just code.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

from engine.rules.catalog import DEFAULT_BLOCKING_RULES
from engine.rules.compiler import compile_and_build
from engine.rules.decision_compiler import compile_decision_sql
from engine.rules.scoring_rules import DEFAULT_SCORING_RULES
from engine.ports.duckdb_dialect import DuckDbDialect

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")

DORIS_HOST = os.environ.get("DORIS_HOST", "127.0.0.1")
DORIS_MYSQL_PORT = int(os.environ.get("DORIS_MYSQL_PORT", "9130"))
DORIS_HTTP_PORT = int(os.environ.get("DORIS_HTTP_PORT", "8130"))
DORIS_USER = os.environ.get("DORIS_USER", "root")
DORIS_PASSWORD = os.environ.get("DORIS_PASSWORD", "")
TEST_DB = "cuin_parity_test"


def _duckdb_decisions():
    import duckdb
    from engine.normalize.explode import build_identifiers_table
    from engine.blocking.suppression import build_frequency_table
    from engine.blocking.deterministic_blocker import build_candidate_pairs, build_name_dob_keys
    from engine.scoring.evidence import build_pair_evidence

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_identifiers_table(con)
    build_frequency_table(con)
    build_name_dob_keys(con)
    build_candidate_pairs(con)
    build_pair_evidence(con)
    con.execute(compile_decision_sql(DEFAULT_SCORING_RULES, DuckDbDialect(), table_name="pair_decisions"))
    rows = con.execute("SELECT a_key, b_key, decision FROM pair_decisions ORDER BY a_key, b_key").fetchall()
    con.close()
    return rows


def _doris_decisions():
    import pymysql
    from engine.ports.doris_conn import DorisConnection
    from engine.ports.doris_ingest import create_raw_table_sql, load_parquet_to_doris
    from engine.normalize import explode_dialect
    from engine.blocking import suppression_dialect
    from engine.scoring import evidence_dialect
    from engine.ports.doris_dialect import DorisDialect

    admin = pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD, autocommit=True)
    cur = admin.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
    cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin.close()

    con = DorisConnection(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD, database=TEST_DB)
    try:
        con.execute(create_raw_table_sql(buckets=8))
        load_parquet_to_doris(
            parquet_path=SAMPLE_PARQUET,
            fe_host=DORIS_HOST, fe_http_port=DORIS_HTTP_PORT,
            user=DORIS_USER, password=DORIS_PASSWORD, database=TEST_DB, table="raw",
        )
        dialect = DorisDialect()
        explode_dialect.build_identifiers_table(con, dialect, source_relation="raw")
        suppression_dialect.build_frequency_table(con, dialect)
        compile_and_build(con, list(DEFAULT_BLOCKING_RULES), dialect)
        evidence_dialect.build_pair_evidence(con, dialect)
        con.execute(compile_decision_sql(DEFAULT_SCORING_RULES, dialect, table_name="pair_decisions"))
        rows = con.execute("SELECT a_key, b_key, decision FROM pair_decisions ORDER BY a_key, b_key").fetchall()
    finally:
        con.close()
        admin = pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD, autocommit=True)
        admin.cursor().execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        admin.close()
    # pymysql returns a tuple-of-tuples; normalize to list-of-tuples to
    # match DuckDB's return shape so the comparison is content-only.
    return [tuple(r) for r in rows]


def test_doris_and_duckdb_produce_identical_decisions():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    try:
        import pymysql
        pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user=DORIS_USER, password=DORIS_PASSWORD, connect_timeout=3).close()
    except Exception as e:
        pytest.skip(f"No live Doris instance reachable at {DORIS_HOST}:{DORIS_MYSQL_PORT}: {e}")

    duckdb_rows = _duckdb_decisions()
    doris_rows = _doris_decisions()

    assert len(duckdb_rows) > 0, "no candidate pairs from DuckDB path -- fixture/config problem, not a real parity result"
    assert duckdb_rows == doris_rows, (
        f"DuckDB and Doris produced different decisions on identical input.\n"
        f"DuckDB ({len(duckdb_rows)} pairs): {duckdb_rows}\n"
        f"Doris  ({len(doris_rows)} pairs): {doris_rows}"
    )


if __name__ == "__main__":
    test_doris_and_duckdb_produce_identical_decisions()
    print("Cross-engine parity test passed.")
