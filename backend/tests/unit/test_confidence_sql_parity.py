"""
Asserts engine.rules.confidence_compiler's SQL confidence/decision
matches engine.scoring.confidence.score_pair() -- the Python
implementation already proven to match the legacy tier oracle by
tests/unit/test_confidence_enumeration.py -- on every candidate pair
of the 5k fixture. Same role as test_decision_sql_parity.py played for
the legacy tier model; this is its Stage-2 successor.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb

from engine.normalize.explode import build_identifiers_table
from engine.blocking.suppression import build_frequency_table
from engine.blocking.deterministic_blocker import build_candidate_pairs, build_name_dob_keys
from engine.scoring.evidence import build_pair_evidence, load_pair_evidence
from engine.scoring.confidence import score_pair
from engine.rules.confidence_compiler import compile_confidence_sql
from engine.rules.match_rules import DEFAULT_MATCH_RULESET
from engine.ports.duckdb_dialect import DuckDbDialect

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")


def _build_evidence_tables():
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_identifiers_table(con)
    build_frequency_table(con)
    build_name_dob_keys(con)
    build_candidate_pairs(con)
    build_pair_evidence(con)
    return con


def test_sql_confidence_matches_python_on_every_pair():
    if not os.path.exists(SAMPLE_PARQUET):
        import pytest
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    con = _build_evidence_tables()
    pairs = con.execute("SELECT a_key, b_key FROM candidate_pairs").fetchall()
    assert len(pairs) > 0, "no candidate pairs in sample fixture"

    py_results = {}
    for a_key, b_key in pairs:
        evidence = load_pair_evidence(con, a_key, b_key)
        score = score_pair(evidence, DEFAULT_MATCH_RULESET)
        py_results[(a_key, b_key)] = (round(score.confidence_pct, 4), score.decision)

    sql = compile_confidence_sql(DEFAULT_MATCH_RULESET, DuckDbDialect())
    con.execute(sql)
    sql_rows = con.execute("SELECT a_key, b_key, confidence_pct, decision FROM pair_decisions").fetchall()
    sql_results = {(a, b): (round(c, 4), d) for a, b, c, d in sql_rows}

    assert set(sql_results.keys()) == set(py_results.keys()), (
        f"pair set mismatch: sql has {len(sql_results)}, python has {len(py_results)}"
    )

    mismatches = [
        (k, py_results[k], sql_results[k])
        for k in py_results
        if py_results[k] != sql_results[k]
    ]
    assert not mismatches, f"SQL/Python confidence disagree on {len(mismatches)} pairs: {mismatches[:10]}"


def test_sql_confidence_matches_legacy_decision_compiler_at_seed():
    """
    Cross-checks the NEW confidence SQL compiler against the OLD tier
    SQL compiler on the same real evidence -- both should be reading
    the same data and reaching the same decisions at the seed values.
    """
    if not os.path.exists(SAMPLE_PARQUET):
        import pytest
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    from engine.rules.decision_compiler import compile_decision_sql
    from engine.rules.scoring_rules import DEFAULT_SCORING_RULES

    con = _build_evidence_tables()

    con.execute(compile_decision_sql(DEFAULT_SCORING_RULES, DuckDbDialect(), table_name="pair_decisions_legacy"))
    legacy = {
        (a, b): d for a, b, d in
        con.execute("SELECT a_key, b_key, decision FROM pair_decisions_legacy").fetchall()
    }

    con.execute(compile_confidence_sql(DEFAULT_MATCH_RULESET, DuckDbDialect(), table_name="pair_decisions_new"))
    new = {
        (a, b): d for a, b, d in
        con.execute("SELECT a_key, b_key, decision FROM pair_decisions_new").fetchall()
    }

    assert set(legacy.keys()) == set(new.keys())
    mismatches = [(k, legacy[k], new[k]) for k in legacy if legacy[k] != new[k]]
    assert not mismatches, f"legacy vs confidence SQL disagree on {len(mismatches)} pairs: {mismatches[:10]}"


if __name__ == "__main__":
    test_sql_confidence_matches_python_on_every_pair()
    test_sql_confidence_matches_legacy_decision_compiler_at_seed()
    print("Confidence SQL/Python parity tests passed.")
