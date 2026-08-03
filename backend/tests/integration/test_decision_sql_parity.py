"""
Asserts engine.rules.decision_compiler's SQL decision matches
engine.scoring.tiers.classify()/decide() -- the tested Python oracle
-- on every candidate pair of the 5k fixture, run through live Doris.
This is what makes it safe to replace the pipeline's per-pair Python
loop with the compiled SQL for the instant-redecide path: if this test
passes, the two approaches always agree.

Requires a live Doris instance (see tests/integration/_doris_fixture.py)
-- moved from tests/unit/ during the Doris-only migration, since it can
no longer run against an in-process DuckDB connection.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.scoring.evidence import load_pair_evidence
from engine.scoring.tiers import classify, decide
from engine.rules.decision_compiler import compile_decision_sql
from engine.rules.scoring_rules import DEFAULT_SCORING_RULES
from engine.ruleset.config import get_default_ruleset

from _doris_fixture import DorisEvidenceFixture, execute_multi, skip_unless_doris_reachable


def test_sql_decision_matches_python_oracle_on_every_pair():
    skip_unless_doris_reachable()

    with DorisEvidenceFixture("cuin_test_decision_sql") as fx:
        pairs = fx.con.execute("SELECT a_key, b_key FROM candidate_pairs").fetchall()
        assert len(pairs) > 0, "no candidate pairs in sample fixture"

        ruleset = get_default_ruleset()
        py_decisions = {}
        for a_key, b_key in pairs:
            evidence = load_pair_evidence(fx.con, a_key, b_key)
            tier = classify(evidence)
            py_decisions[(a_key, b_key)] = decide(tier, ruleset).value

        execute_multi(fx.con, compile_decision_sql(DEFAULT_SCORING_RULES, fx.dialect))
        sql_rows = fx.con.execute("SELECT a_key, b_key, decision FROM pair_decisions").fetchall()
        sql_decisions = {(a, b): d for a, b, d in sql_rows}

    assert set(sql_decisions.keys()) == set(py_decisions.keys()), (
        f"pair set mismatch: sql has {len(sql_decisions)}, python has {len(py_decisions)}"
    )

    mismatches = [
        (k, py_decisions[k], sql_decisions[k])
        for k in py_decisions
        if py_decisions[k] != sql_decisions[k]
    ]
    assert not mismatches, f"SQL/Python decision disagree on {len(mismatches)} pairs: {mismatches[:10]}"

    counts = {}
    for d in py_decisions.values():
        counts[d] = counts.get(d, 0) + 1
    assert counts, "no decisions computed"


def test_decision_distribution_is_not_trivially_all_reject():
    """
    Guards against a decision-compiler bug that vetoes/rejects
    everything (which would trivially satisfy an all-REJECT parity
    test if the Python oracle had the same bug -- it doesn't, but this
    keeps the test suite honest about actually exercising AUTO_LINK/
    REVIEW paths).
    """
    skip_unless_doris_reachable()

    with DorisEvidenceFixture("cuin_test_decision_sql_dist") as fx:
        execute_multi(fx.con, compile_decision_sql(DEFAULT_SCORING_RULES, fx.dialect))
        rows = fx.con.execute("SELECT decision, COUNT(*) FROM pair_decisions GROUP BY decision").fetchall()
    decisions = {d: n for d, n in rows}
    assert decisions.get("AUTO_LINK", 0) > 0, f"expected some AUTO_LINK pairs in sample fixture, got {decisions}"


if __name__ == "__main__":
    test_sql_decision_matches_python_oracle_on_every_pair()
    test_decision_distribution_is_not_trivially_all_reject()
    print("Decision SQL/Python parity tests passed.")
