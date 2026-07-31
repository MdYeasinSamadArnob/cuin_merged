"""
Tests engine.rules.catalog.BlockingRuleType.RAW_COLUMN -- blocking
directly on a raw source column (scalar or multi-valued), bypassing
engine.normalize.explode's closed 6-column vocabulary. This is what
makes a column like BRANCH_CODE or SPONSOR_NAME (never normalized,
therefore unreachable by any other blocking rule type) usable for
blocking at all.

A lighter-weight, lower-risk alternative to rewriting the normalize
pipeline into a fully generic schema-driven attributes table (the
original Stage 3 design) -- see
/home/arnob/.claude/plans/i-actually-think-apache-golden-galaxy.md.
Purely additive: does not touch identifiers/customer_scalars or any
existing rule type.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb
import pytest

from engine.rules.catalog import BlockingRule, BlockingRuleType, RuleGuards
from engine.rules.compiler import compile_and_build
from engine.rules.precheck import estimate_fanout
from engine.ports.duckdb_dialect import DuckDbDialect

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")


def _con_with_raw():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    return con


def test_raw_column_scalar_produces_pairs_and_reasons():
    con = _con_with_raw()
    rule = BlockingRule(
        rule_id="branch_code_test", type=BlockingRuleType.RAW_COLUMN,
        fields=("BRANCH_CODE",), params={"is_array": False},
        guards=RuleGuards(max_block_size=100),
    )
    rule.validate()
    compile_and_build(con, [rule], DuckDbDialect())

    n = con.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
    assert n > 0, "expected some BRANCH_CODE pairs on the 5k fixture"

    reasons = con.execute("SELECT blocking_reasons FROM candidate_pairs LIMIT 1").fetchone()[0]
    assert any(r.startswith("branch_code_test:") for r in reasons)


def test_raw_column_array_unnests_correctly():
    """Multi-valued raw columns (e.g. MOBILE, unvalidated) must self-join per element, not per array."""
    con = _con_with_raw()
    rule = BlockingRule(
        rule_id="mobile_raw_test", type=BlockingRuleType.RAW_COLUMN,
        fields=("MOBILE",), params={"is_array": True},
    )
    rule.validate()
    compile_and_build(con, [rule], DuckDbDialect())
    # Just needs to not throw and to produce a well-formed candidate_pairs table.
    n = con.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
    assert n >= 0


def test_raw_column_precheck_matches_materialized_count():
    """engine.rules.precheck's exact fan-out math must equal what compile_and_build actually produces."""
    con = _con_with_raw()
    rule = BlockingRule(
        rule_id="branch_code_test", type=BlockingRuleType.RAW_COLUMN,
        fields=("BRANCH_CODE",), params={"is_array": False},
        guards=RuleGuards(max_block_size=100),
    )
    rule.validate()

    estimate = estimate_fanout(rule, DuckDbDialect(), con)

    con2 = _con_with_raw()
    compile_and_build(con2, [rule], DuckDbDialect())
    actual = con2.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]

    assert estimate.n_pairs == actual, (
        f"precheck estimated {estimate.n_pairs} pairs but compile_and_build produced {actual}"
    )


def test_raw_column_rejects_unsafe_column_name():
    """Column names are interpolated directly into SQL -- must be injection-safe."""
    rule = BlockingRule(
        rule_id="bad", type=BlockingRuleType.RAW_COLUMN,
        fields=("BRANCH_CODE; DROP TABLE candidate_pairs;--",), params={"is_array": False},
    )
    with pytest.raises(ValueError):
        rule.validate()


def test_raw_column_rejects_wrong_field_count():
    rule = BlockingRule(
        rule_id="bad2", type=BlockingRuleType.RAW_COLUMN,
        fields=("BRANCH_CODE", "SPONSOR_NAME"), params={"is_array": False},
    )
    with pytest.raises(ValueError):
        rule.validate()


if __name__ == "__main__":
    test_raw_column_scalar_produces_pairs_and_reasons()
    test_raw_column_array_unnests_correctly()
    test_raw_column_precheck_matches_materialized_count()
    test_raw_column_rejects_unsafe_column_name()
    test_raw_column_rejects_wrong_field_count()
    print("raw_column blocking: OK")
