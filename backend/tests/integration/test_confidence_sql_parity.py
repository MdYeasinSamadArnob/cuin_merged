"""
Asserts engine.rules.confidence_compiler's SQL confidence/decision
matches engine.scoring.confidence.score_pair() -- the Python
implementation already proven to match the legacy tier oracle by
tests/unit/test_confidence_enumeration.py -- on every candidate pair
of the 5k fixture, run through live Doris. Same role as
test_decision_sql_parity.py played for the legacy tier model; this is
its Stage-2 successor.

Requires a live Doris instance (see tests/integration/_doris_fixture.py)
-- moved from tests/unit/ during the Doris-only migration, since it can
no longer run against an in-process DuckDB connection.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.scoring.evidence import load_pair_evidence
from engine.scoring.confidence import score_pair
from engine.rules.confidence_compiler import compile_confidence_sql
from engine.rules.match_rules import DEFAULT_MATCH_RULESET

from _doris_fixture import DorisEvidenceFixture, execute_multi, skip_unless_doris_reachable


def test_sql_confidence_matches_python_on_every_pair():
    skip_unless_doris_reachable()

    with DorisEvidenceFixture("cuin_test_confidence_sql") as fx:
        pairs = fx.con.execute("SELECT a_key, b_key FROM candidate_pairs").fetchall()
        assert len(pairs) > 0, "no candidate pairs in sample fixture"

        py_results = {}
        for a_key, b_key in pairs:
            evidence = load_pair_evidence(fx.con, a_key, b_key)
            score = score_pair(evidence, DEFAULT_MATCH_RULESET)
            py_results[(a_key, b_key)] = (round(score.confidence_pct, 4), score.decision)

        execute_multi(fx.con, compile_confidence_sql(DEFAULT_MATCH_RULESET, fx.dialect))
        sql_rows = fx.con.execute("SELECT a_key, b_key, confidence_pct, decision FROM pair_decisions").fetchall()
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
    skip_unless_doris_reachable()

    from engine.rules.decision_compiler import compile_decision_sql
    from engine.rules.scoring_rules import DEFAULT_SCORING_RULES

    with DorisEvidenceFixture("cuin_test_confidence_sql_legacy") as fx:
        execute_multi(fx.con, compile_decision_sql(DEFAULT_SCORING_RULES, fx.dialect, table_name="pair_decisions_legacy"))
        legacy = {
            (a, b): d for a, b, d in
            fx.con.execute("SELECT a_key, b_key, decision FROM pair_decisions_legacy").fetchall()
        }

        execute_multi(fx.con, compile_confidence_sql(DEFAULT_MATCH_RULESET, fx.dialect, table_name="pair_decisions_new"))
        new = {
            (a, b): d for a, b, d in
            fx.con.execute("SELECT a_key, b_key, decision FROM pair_decisions_new").fetchall()
        }

    assert set(legacy.keys()) == set(new.keys())
    mismatches = [(k, legacy[k], new[k]) for k in legacy if legacy[k] != new[k]]
    assert not mismatches, f"legacy vs confidence SQL disagree on {len(mismatches)} pairs: {mismatches[:10]}"


if __name__ == "__main__":
    test_sql_confidence_matches_python_on_every_pair()
    test_sql_confidence_matches_legacy_decision_compiler_at_seed()
    print("Confidence SQL/Python parity tests passed.")
