"""
Parity test for engine.blocking.suppression vs suppression_dialect --
same purpose as test_explode_dialect_parity.py. Neither module's
docstring claims a test file exists for this pair, but they must still
agree: the DuckDB orchestrator runs `suppression`, the Doris
orchestrator runs `suppression_dialect`, and `identifier_frequency`
(incl. `is_suppressed`) feeds directly into blocking guards and the
fan-out precheck on both.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb
import pytest

from engine.normalize.explode import build_identifiers_table
from engine.blocking.suppression import build_frequency_table as build_freq_orig
from engine.blocking.suppression_dialect import build_frequency_table as build_freq_dialect
from engine.ports.duckdb_dialect import DuckDbDialect

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")


def _con_with_identifiers():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_identifiers_table(con)
    return con


def test_identifier_frequency_byte_identical():
    con_orig = _con_with_identifiers()
    build_freq_orig(con_orig)
    orig_freq = con_orig.execute(
        "SELECT id_type, value_norm, n_records, is_suppressed FROM identifier_frequency "
        "ORDER BY id_type, value_norm"
    ).fetchall()
    orig_ids = con_orig.execute(
        "SELECT customer_code, id_type, value_raw, value_norm, doc_type, is_valid, is_suppressed "
        "FROM identifiers ORDER BY customer_code, id_type, value_raw"
    ).fetchall()
    con_orig.close()

    con_new = _con_with_identifiers()
    build_freq_dialect(con_new, DuckDbDialect())
    new_freq = con_new.execute(
        "SELECT id_type, value_norm, n_records, is_suppressed FROM identifier_frequency "
        "ORDER BY id_type, value_norm"
    ).fetchall()
    new_ids = con_new.execute(
        "SELECT customer_code, id_type, value_raw, value_norm, doc_type, is_valid, is_suppressed "
        "FROM identifiers ORDER BY customer_code, id_type, value_raw"
    ).fetchall()
    con_new.close()

    assert len(orig_freq) > 0, "no frequency rows in sample fixture -- fixture missing/empty?"
    assert orig_freq == new_freq, (
        f"suppression vs suppression_dialect diverge on `identifier_frequency`: "
        f"{len(orig_freq)} vs {len(new_freq)} rows, "
        f"first mismatch: {next(((o, n) for o, n in zip(orig_freq, new_freq) if o != n), None)}"
    )
    assert orig_ids == new_ids, (
        f"suppression vs suppression_dialect diverge on `identifiers.is_suppressed`: "
        f"first mismatch: {next(((o, n) for o, n in zip(orig_ids, new_ids) if o != n), None)}"
    )


def test_no_name_token_rows_suppressed():
    """
    DEFAULT_THRESHOLDS in both modules maps 'name_token' -> 1500, but
    `identifiers` never contains id_type='name_token' -- that branch is
    dead code (name tokens are never individually frequency-suppressed,
    only the whole name_key is capped, elsewhere, via
    suppression_name_token_max). Pinned here so a well-meaning
    "generalization" that starts actually suppressing individual name
    tokens is caught immediately, not discovered as a silent decision
    change later.
    """
    con = _con_with_identifiers()
    build_freq_dialect(con, DuckDbDialect())
    n = con.execute(
        "SELECT COUNT(*) FROM identifier_frequency WHERE id_type = 'name_token'"
    ).fetchone()[0]
    con.close()
    assert n == 0


if __name__ == "__main__":
    test_identifier_frequency_byte_identical()
    test_no_name_token_rows_suppressed()
    print("suppression/suppression_dialect parity: OK")
