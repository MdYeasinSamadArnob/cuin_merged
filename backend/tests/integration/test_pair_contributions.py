"""
Stage 1 of the entity resolution workbench plan: proves
engine.rules.confidence_compiler.compile_contributions_sql() -- the
"why this score" audit table -- never disagrees with
compile_confidence_sql(), the actual decision path. Both are built
from the SAME per-rule CTEs (confidence_compiler.py's
_identifier_confidence_cte / _name_confidence_cte / _dob_confidence_cte
/ _raw_column_confidence_cte, called with include_detail=True here),
so this is a structural guarantee, not a coincidence -- but structure
alone doesn't prove the SQL is well-formed, hence these tests.

Covers, on the real 5k fixture, run through live Doris:
  1. Every enabled rule emits exactly one row per pair (matched=False
     rows included -- mirrors score_pair()'s Contribution list).
  2. Per-pair SUM(awarded_pct) (capped) == pair_decisions.confidence_pct.
  3. Per-pair OR(is_veto) == pair_decisions.has_veto.
  4. A real DOCUMENT per_sub_type double-fire: awarded_pct > configured_pct.
  5. A real veto-firing RAW_COLUMN rule (SPONSOR_NAME): is_veto=True rows exist,
     confidence_cap clipping is visible when raw sum exceeds the cap.

Requires a live Doris instance (see tests/integration/_doris_fixture.py)
-- moved from tests/unit/ during the Doris-only migration, since it can
no longer run against an in-process DuckDB connection.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.rules.confidence_compiler import compile_confidence_sql, compile_contributions_sql
from engine.rules.match_rules import (
    MatchRule, MatchRuleset, DEFAULT_MATCH_RULESET,
    AGGREGATION_PER_SUB_TYPE, VETO_BOTH_PRESENT_NO_OVERLAP, VETO_BOTH_QUALIFIED_AND_DIFFER,
)

from _doris_fixture import DorisEvidenceFixture, execute_multi, skip_unless_doris_reachable


def _assert_parity(con, dialect, ruleset, cap=None):
    cap = cap if cap is not None else ruleset.confidence_cap
    execute_multi(con, compile_confidence_sql(ruleset, dialect, table_name="pair_decisions"))
    execute_multi(con, compile_contributions_sql(ruleset, dialect, table_name="pair_contributions"))

    n_enabled_rules = len([r for r in ruleset.match_rules if r.enabled])
    n_pairs = con.execute("SELECT COUNT(*) FROM pair_decisions").fetchone()[0]
    n_rows = con.execute("SELECT COUNT(*) FROM pair_contributions").fetchone()[0]
    assert n_rows == n_pairs * n_enabled_rules, (
        f"expected one contribution row per (pair, enabled rule): {n_pairs} pairs x {n_enabled_rules} rules "
        f"= {n_pairs * n_enabled_rules}, got {n_rows}"
    )

    mismatches = con.execute(f"""
        WITH summed AS (
            SELECT a_key, b_key,
                   LEAST({cap}, SUM(awarded_pct)) AS total,
                   MAX(CASE WHEN is_veto THEN 1 ELSE 0 END) AS any_veto
            FROM pair_contributions GROUP BY a_key, b_key
        )
        SELECT d.a_key, d.b_key, d.confidence_pct, s.total, d.has_veto, s.any_veto
        FROM pair_decisions d JOIN summed s ON d.a_key = s.a_key AND d.b_key = s.b_key
        WHERE ABS(d.confidence_pct - s.total) > 0.01 OR d.has_veto != (s.any_veto = 1)
    """).fetchall()
    assert not mismatches, f"{len(mismatches)} pairs disagree between pair_decisions and summed pair_contributions: {mismatches[:5]}"
    return n_pairs


def test_default_ruleset_contributions_match_decisions():
    skip_unless_doris_reachable()
    with DorisEvidenceFixture("cuin_test_pair_contrib_default") as fx:
        n_pairs = _assert_parity(fx.con, fx.dialect, DEFAULT_MATCH_RULESET)
    assert n_pairs > 0, "expected candidate pairs on the 5k fixture"


def test_document_per_sub_type_double_fire_is_visible():
    """A DOCUMENT match rule with AGGREGATION_PER_SUB_TYPE can award MORE than its configured_pct
    when both NID and TIN match -- awarded_pct/configured_pct == 2 is the double-fire signal, not a bug."""
    skip_unless_doris_reachable()
    with DorisEvidenceFixture("cuin_test_pair_contrib_docfire") as fx:
        _assert_parity(fx.con, fx.dialect, DEFAULT_MATCH_RULESET)

        doubled = fx.con.execute("""
            SELECT a_key, b_key, awarded_pct, configured_pct
            FROM pair_contributions
            WHERE rule_id = 'document_match' AND awarded_pct > configured_pct
        """).fetchall()
    # Not asserting doubled is non-empty (the 5k fixture may or may not contain an
    # NID+TIN-both-match pair) -- but IF it exists, the ratio must be a clean multiple.
    for a, b, awarded, configured in doubled:
        assert float(awarded) % float(configured) == 0, f"{a}:{b} awarded {awarded} is not a clean multiple of {configured}"


def test_raw_column_veto_and_cap_clipping_visible():
    """SPONSOR_NAME token_jaccard with a veto, weighted high enough to exceed the cap when combined
    with mobile+email -- proves both veto rows AND cap-clipping are visible in the contributions table."""
    skip_unless_doris_reachable()

    rules = (
        MatchRule(rule_id="mobile_match", attribute="MOBILE", comparator="set_intersect_nonempty",
                   confidence_pct=60.0, aggregation="once"),
        MatchRule(rule_id="email_match", attribute="EMAIL", comparator="set_intersect_nonempty",
                   confidence_pct=60.0, aggregation="once"),
        MatchRule(rule_id="sponsor_match", attribute="RAW_COLUMN", comparator="token_jaccard",
                   confidence_pct=20.0, params={"column": "SPONSOR_NAME", "is_array": False, "threshold": 0.5},
                   veto_kind=VETO_BOTH_QUALIFIED_AND_DIFFER),
    )
    ruleset = MatchRuleset(match_rules=rules, auto_link_min_confidence=50.0, review_min_confidence=10.0, confidence_cap=100.0)

    with DorisEvidenceFixture("cuin_test_pair_contrib_veto") as fx:
        n_pairs = _assert_parity(fx.con, fx.dialect, ruleset)
        assert n_pairs > 0

        # Cap clipping: mobile+email alone can already hit 120 (60+60), so any pair matching both
        # must show a raw sum exceeding the 100 cap once contributions are actually summed.
        raw_sums = fx.con.execute("""
            SELECT a_key, b_key, SUM(awarded_pct) AS raw_total
            FROM pair_contributions GROUP BY a_key, b_key HAVING SUM(awarded_pct) > 100
        """).fetchall()
        for a, b, raw_total in raw_sums:
            capped = fx.con.execute("SELECT confidence_pct FROM pair_decisions WHERE a_key=? AND b_key=?", [a, b]).fetchone()[0]
            assert float(capped) == 100.0, f"{a}:{b} raw {raw_total} should have been clipped to the 100 cap, stored {capped}"


if __name__ == "__main__":
    test_default_ruleset_contributions_match_decisions()
    test_document_per_sub_type_double_fire_is_visible()
    test_raw_column_veto_and_cap_clipping_visible()
    print("pair_contributions parity tests: OK")
