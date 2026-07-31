"""
Parity test for engine.normalize.explode vs explode_dialect: both
docstrings have claimed since they were written that this test exists
and proves them byte-identical on DuckDB. It didn't -- this is that
test, written before either module is relied upon for anything new.

engine.normalize.explode is the DuckDB-only oracle pipeline.
duckdb_orchestrator actually runs; explode_dialect is the
dialect-portable twin pipeline.doris_orchestrator runs. If this test
is red, the two engines are normalizing data differently and every
downstream parity claim (candidate pairs, decisions, fingerprints) is
unfounded.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb
import pytest

from engine.normalize.explode import build_identifiers_table as build_identifiers_orig
from engine.normalize.explode_dialect import build_identifiers_table as build_identifiers_dialect
from engine.ports.duckdb_dialect import DuckDbDialect

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")


def _con_with_raw():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    return con


def test_identifiers_table_byte_identical():
    con_orig = _con_with_raw()
    build_identifiers_orig(con_orig)
    orig = con_orig.execute(
        "SELECT customer_code, id_type, value_raw, value_norm, doc_type, is_valid "
        "FROM identifiers ORDER BY customer_code, id_type, value_raw"
    ).fetchall()
    con_orig.close()

    con_new = _con_with_raw()
    build_identifiers_dialect(con_new, DuckDbDialect())
    new = con_new.execute(
        "SELECT customer_code, id_type, value_raw, value_norm, doc_type, is_valid "
        "FROM identifiers ORDER BY customer_code, id_type, value_raw"
    ).fetchall()
    con_new.close()

    assert len(orig) > 0, "no identifier rows in sample fixture -- fixture missing/empty?"
    assert orig == new, (
        f"explode vs explode_dialect diverge on `identifiers`: {len(orig)} vs {len(new)} rows, "
        f"first mismatch: {next(((o, n) for o, n in zip(orig, new) if o != n), None)}"
    )


def test_customer_scalars_byte_identical():
    con_orig = _con_with_raw()
    build_identifiers_orig(con_orig)
    orig = con_orig.execute(
        "SELECT customer_code, name_norm, name_tokens, dob_iso, dob_precision "
        "FROM customer_scalars ORDER BY customer_code"
    ).fetchall()
    con_orig.close()

    con_new = _con_with_raw()
    build_identifiers_dialect(con_new, DuckDbDialect())
    new = con_new.execute(
        "SELECT customer_code, name_norm, name_tokens, dob_iso, dob_precision "
        "FROM customer_scalars ORDER BY customer_code"
    ).fetchall()
    con_new.close()

    assert len(orig) > 0
    assert orig == new, (
        f"explode vs explode_dialect diverge on `customer_scalars`: {len(orig)} vs {len(new)} rows, "
        f"first mismatch: {next(((o, n) for o, n in zip(orig, new) if o != n), None)}"
    )


if __name__ == "__main__":
    test_identifiers_table_byte_identical()
    test_customer_scalars_byte_identical()
    print("explode/explode_dialect parity: OK")
