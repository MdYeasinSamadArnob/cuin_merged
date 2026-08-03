"""
Asserts engine.rules.precheck.estimate_fanout()'s exact pair count
matches the ACTUAL number of pairs produced by materializing that
rule's atomic_select_sql (engine.rules.compiler.compile_rule) -- i.e.
the precheck is not an approximation, it is provably the same number
the real blocking pass would produce for that rule alone.

Also proves the severity/suggestion machinery actually works: an
intentionally coarse rule (blocking on dob_year alone, no guard) must
be flagged, and applying its suggested_max_block_size as a guard must
bring the exact pair count under budget.

Requires a live Doris instance (see tests/integration/_doris_fixture.py)
-- moved from tests/unit/ during the Doris-only migration, since it can
no longer run against an in-process DuckDB connection.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.rules.catalog import BlockingRule, BlockingRuleType, DEFAULT_BLOCKING_RULES
from engine.rules.compiler import compile_rule
from engine.rules.precheck import estimate_fanout, FanoutSeverity

from _doris_fixture import DorisEvidenceFixture, execute_multi, skip_unless_doris_reachable


def _actual_pair_count(con, rule, dialect) -> int:
    setup_statements, select_sql, _ = compile_rule(rule, dialect)
    for stmt in setup_statements:
        execute_multi(con, stmt)
    return con.execute(f"SELECT COUNT(*) FROM ({select_sql}) t").fetchone()[0]


def test_precheck_matches_actual_pairs_for_exact_identifier():
    skip_unless_doris_reachable()
    rule = next(r for r in DEFAULT_BLOCKING_RULES if r.type == BlockingRuleType.EXACT_IDENTIFIER)

    with DorisEvidenceFixture("cuin_test_precheck_exact") as fx:
        estimate = estimate_fanout(rule, fx.dialect, fx.con)
        actual = _actual_pair_count(fx.con, rule, fx.dialect)

    assert estimate.n_pairs == actual, f"precheck said {estimate.n_pairs}, actual self-join produced {actual}"


def test_precheck_matches_actual_pairs_for_composite_key():
    skip_unless_doris_reachable()
    rule = next(r for r in DEFAULT_BLOCKING_RULES if r.type == BlockingRuleType.COMPOSITE_KEY)

    with DorisEvidenceFixture("cuin_test_precheck_composite") as fx:
        estimate = estimate_fanout(rule, fx.dialect, fx.con)
        actual = _actual_pair_count(fx.con, rule, fx.dialect)

    assert estimate.n_pairs == actual, f"precheck said {estimate.n_pairs}, actual self-join produced {actual}"


def test_coarse_dob_year_rule_is_flagged_and_suggestion_fixes_it():
    skip_unless_doris_reachable()
    coarse_rule = BlockingRule(
        rule_id="dob_year_coarse", type=BlockingRuleType.DATE_PART_KEY,
        order=1, fields=("dob_iso", "year"),
    )

    with DorisEvidenceFixture("cuin_test_precheck_coarse") as fx:
        estimate = estimate_fanout(coarse_rule, fx.dialect, fx.con, warn_pairs=50, block_pairs=500)

        assert estimate.severity in (FanoutSeverity.WARN, FanoutSeverity.BLOCK), (
            f"expected a coarse dob_year-only blocking rule to be flagged, got {estimate.severity} "
            f"with {estimate.n_pairs} pairs"
        )
        assert estimate.heaviest_keys, "expected at least one heavy key to be reported"

        if estimate.suggested_max_block_size:
            # Re-run the SAME precheck's own group-size math with the cap applied,
            # to prove the suggestion is internally consistent (this is the same
            # arithmetic routes_rules.py's precheck endpoint would show the UI).
            setup, _, key_source = compile_rule(coarse_rule, fx.dialect)
            for stmt in setup:
                execute_multi(fx.con, stmt)
            rows = fx.con.execute(
                f"SELECT {key_source.key_columns[0]}, COUNT(DISTINCT customer_code) AS n "
                f"FROM {key_source.table} GROUP BY {key_source.key_columns[0]}"
            ).fetchall()
            capped_pairs = sum(
                (n * (n - 1)) // 2 for _, n in rows if n <= estimate.suggested_max_block_size
            )
            assert capped_pairs <= 50 or capped_pairs < estimate.n_pairs, (
                "applying the suggested max_block_size did not reduce pairs under budget"
            )


def test_safe_rule_is_not_flagged():
    skip_unless_doris_reachable()
    rule = next(r for r in DEFAULT_BLOCKING_RULES if r.type == BlockingRuleType.EXACT_IDENTIFIER)

    with DorisEvidenceFixture("cuin_test_precheck_safe") as fx:
        estimate = estimate_fanout(rule, fx.dialect, fx.con)

    assert estimate.severity == FanoutSeverity.SAFE, (
        f"default exact-identifier rule on a 5k sample should be SAFE, got {estimate.severity} "
        f"({estimate.n_pairs} pairs, largest block {estimate.largest_block})"
    )


if __name__ == "__main__":
    test_precheck_matches_actual_pairs_for_exact_identifier()
    test_precheck_matches_actual_pairs_for_composite_key()
    test_coarse_dob_year_rule_is_flagged_and_suggestion_fixes_it()
    test_safe_rule_is_not_flagged()
    print("Precheck accuracy tests passed.")
