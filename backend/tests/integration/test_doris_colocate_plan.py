"""
Proves the migration plan's central Doris performance claim: with
engine.ports.doris_ddl's explicit DISTRIBUTED BY HASH(id_type,
value_norm) on `identifiers`, the EXACT_IDENTIFIER blocking self-join
executes as a local COLOCATE join with zero network shuffle -- not a
SHUFFLE/broadcast join, which is what a naive (default-distribution)
port would produce.

Requires a live Doris instance reachable over the MySQL protocol
(DORIS_HOST/DORIS_PORT/DORIS_USER/DORIS_PASSWORD env vars, defaults
match the docker-compose dev setup). Skips if unreachable -- this is
an integration test against real infrastructure, not a unit test.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

from engine.ports.doris_ddl import all_ddl_statements, session_variable_statements

DORIS_HOST = os.environ.get("DORIS_HOST", "127.0.0.1")
DORIS_PORT = int(os.environ.get("DORIS_PORT", "9130"))
DORIS_USER = os.environ.get("DORIS_USER", "root")
DORIS_PASSWORD = os.environ.get("DORIS_PASSWORD", "")

TEST_DB = "cuin_colocate_plan_test"


def _connect():
    import pymysql
    return pymysql.connect(
        host=DORIS_HOST, port=DORIS_PORT, user=DORIS_USER, password=DORIS_PASSWORD,
        autocommit=True, connect_timeout=3,
    )


@pytest.fixture(scope="module")
def doris_cursor():
    try:
        conn = _connect()
    except Exception as e:
        pytest.skip(f"No live Doris instance reachable at {DORIS_HOST}:{DORIS_PORT}: {e}")

    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
    cur.execute(f"CREATE DATABASE {TEST_DB}")
    cur.execute(f"USE {TEST_DB}")
    for stmt in session_variable_statements():
        cur.execute(stmt)
    for ddl in all_ddl_statements(buckets=8):
        cur.execute(ddl)

    rows = [(f"C{i}", "mobile", "01712345678", "01712345678", None, 1, 0) for i in range(500)]
    placeholders = ",".join(["(%s,%s,%s,%s,%s,%s,%s)"] * len(rows))
    flat = [v for r in rows for v in r]
    cur.execute(f"INSERT INTO identifiers VALUES {placeholders}", flat)

    yield cur

    cur.execute(f"DROP DATABASE {TEST_DB}")
    conn.close()


def test_identifiers_self_join_is_colocated_not_shuffled(doris_cursor):
    doris_cursor.execute("""
        EXPLAIN SELECT a.customer_code, b.customer_code
        FROM identifiers a JOIN identifiers b
          ON a.id_type = b.id_type AND a.value_norm = b.value_norm AND a.customer_code < b.customer_code
        WHERE a.is_valid AND b.is_valid AND NOT a.is_suppressed AND NOT b.is_suppressed
    """)
    plan = "\n".join(r[0] for r in doris_cursor.fetchall())

    assert "COLOCATE" in plan.upper(), f"expected a COLOCATE join in the plan, got:\n{plan}"
    assert "HAS_COLO_PLAN_NODE: TRUE" in plan.upper(), f"expected HAS_COLO_PLAN_NODE: true, got:\n{plan}"
    # A colocate-eligible join falling back to SHUFFLE would mean the
    # DISTRIBUTED BY / colocate_with properties in doris_ddl.py aren't
    # actually taking effect -- this is the regression this test guards.
    assert "SHUFFLE" not in plan.upper(), f"expected no shuffle join, got:\n{plan}"


if __name__ == "__main__":
    print("Run via pytest (uses a module-scoped fixture): pytest tests/integration/test_doris_colocate_plan.py -v")
