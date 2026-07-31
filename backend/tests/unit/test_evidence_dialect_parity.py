"""
Parity test for engine.scoring.evidence vs evidence_dialect.

This is the one of the three "missing" dialect-parity tests expected
to FAIL on first write, by design: evidence.py's `list_intersect`/
`list_distinct` are unsorted (hash-order in DuckDB), while
evidence_dialect.py deliberately wraps both in `array_sort`. The
values agree as SETS but not as ordered arrays -- and the array order
leaks into `engine.scoring.tiers.classify()`'s signal strings
(`f"document:{doc['intersection'][0]}"`,
`f"name_rare_token_jaccard_1.0:{tokens_intersection}"`), which
`engine.determinism.fingerprint_edges` hashes. So today, the DuckDB
and Doris orchestrators can reach the identical AUTO_LINK/REVIEW/
REJECT counts on the identical pair set while still producing a
DIFFERENT `output_fingerprint` -- verified empirically this session
(DuckDB run vs Doris run, same 549,879/80,809/190,182/278,888/53,910,
different fingerprint).

The fix (in evidence.py, not evidence_dialect.py -- the sorted form is
the one that also fixes engine.decisioning.decision_engine's signal
parsing) makes both sides canonical. After that fix this test asserts
byte-identical arrays, not just identical sets.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb
import pytest

from engine.normalize.explode import build_identifiers_table
from engine.blocking.suppression import build_frequency_table
from engine.blocking.deterministic_blocker import build_candidate_pairs, build_name_dob_keys
from engine.scoring.evidence import build_pair_evidence as build_evidence_orig
from engine.scoring.evidence_dialect import build_pair_evidence as build_evidence_dialect
from engine.ports.duckdb_dialect import DuckDbDialect

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")


def _con_with_candidates():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_identifiers_table(con)
    build_frequency_table(con)
    build_name_dob_keys(con)
    build_candidate_pairs(con)
    return con


def test_pair_identifier_evidence_byte_identical():
    con_orig = _con_with_candidates()
    build_evidence_orig(con_orig)
    orig = con_orig.execute(
        "SELECT a_key, b_key, id_type, doc_type, values_a, values_b, intersection "
        "FROM pair_identifier_evidence ORDER BY a_key, b_key, id_type, doc_type"
    ).fetchall()
    con_orig.close()

    con_new = _con_with_candidates()
    build_evidence_dialect(con_new, DuckDbDialect())
    new = con_new.execute(
        "SELECT a_key, b_key, id_type, doc_type, values_a, values_b, intersection "
        "FROM pair_identifier_evidence ORDER BY a_key, b_key, id_type, doc_type"
    ).fetchall()
    con_new.close()

    assert len(orig) > 0
    assert orig == new, (
        f"evidence vs evidence_dialect diverge on `pair_identifier_evidence` "
        f"(check array ordering -- evidence.py must sort to match evidence_dialect.py's array_sort): "
        f"first mismatch: {next(((o, n) for o, n in zip(orig, new) if o != n), None)}"
    )


def test_pair_name_dob_evidence_byte_identical():
    con_orig = _con_with_candidates()
    build_evidence_orig(con_orig)
    orig = con_orig.execute(
        "SELECT a_key, b_key, name_a, name_b, tokens_a, tokens_b, "
        "token_intersection, token_union, dob_a, dob_b, dob_precision_a, dob_precision_b "
        "FROM pair_name_dob_evidence ORDER BY a_key, b_key"
    ).fetchall()
    con_orig.close()

    con_new = _con_with_candidates()
    build_evidence_dialect(con_new, DuckDbDialect())
    new = con_new.execute(
        "SELECT a_key, b_key, name_a, name_b, tokens_a, tokens_b, "
        "token_intersection, token_union, dob_a, dob_b, dob_precision_a, dob_precision_b "
        "FROM pair_name_dob_evidence ORDER BY a_key, b_key"
    ).fetchall()
    con_new.close()

    assert len(orig) > 0
    assert orig == new, (
        f"evidence vs evidence_dialect diverge on `pair_name_dob_evidence` "
        f"(check token_intersection/token_union ordering): "
        f"first mismatch: {next(((o, n) for o, n in zip(orig, new) if o != n), None)}"
    )


if __name__ == "__main__":
    test_pair_identifier_evidence_byte_identical()
    test_pair_name_dob_evidence_byte_identical()
    print("evidence/evidence_dialect parity: OK")
