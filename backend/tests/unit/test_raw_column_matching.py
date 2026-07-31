"""
Tests the RAW_COLUMN match rule (Stage 5.1): a bank adding ANY schema
column (not one of the 6 built-ins) as a weighted matching field --
"the mother's name is worth 30%", including a hard veto ("a document
mismatch always rejects") generalized past the 2 built-in rules that
originally hardcoded it. See engine.rules.match_rules'
ATTRIBUTE_RAW_COLUMN, engine.scoring.confidence._score_raw_column_rule,
engine.rules.confidence_compiler._raw_column_confidence_cte.

Proves, on the real 5k fixture:
  1. Every Tier-A comparator's Python evaluator (engine.rules.
     comparators.evaluate_python) agrees with its SQL twin.
  2. A RAW_COLUMN match rule scores identically in score_pair() (the
     Python path the live pipeline actually decides through) and
     compile_confidence_sql() (the SQL path redecide/reblock use) --
     scalar comparator, array comparator, and both veto kinds.
"""

import os
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb
import pytest

from engine.normalize.explode import build_identifiers_table
from engine.blocking.suppression import build_frequency_table
from engine.blocking.deterministic_blocker import build_candidate_pairs, build_name_dob_keys
from engine.scoring.evidence import build_pair_evidence
from engine.scoring.confidence import score_pair
from engine.rules.confidence_compiler import compile_confidence_sql
from engine.rules.match_rules import MatchRule, MatchRuleset, VETO_BOTH_PRESENT_NO_OVERLAP, VETO_BOTH_QUALIFIED_AND_DIFFER
from engine.rules.comparators import COMPARATORS, evaluate_python, prep_raw_value
from engine.ports.duckdb_dialect import DuckDbDialect

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")


def _skip_if_missing():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")


def _dialect_literal(value, dialect):
    if value is None:
        return "NULL"
    if isinstance(value, list):
        return "[" + ", ".join(dialect.quote_str(v) for v in value) + "]"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    return dialect.quote_str(str(value))


# ----------------------------------------------------------------------
# 1. Comparator Python <-> SQL parity, synthetic values (DuckDB -- the
#    comparator SQL is dialect-neutral through engine.ports.dialect,
#    already proven portable elsewhere this session for every one of
#    these functions via the blocking/evidence dialect parity suites).
# ----------------------------------------------------------------------

_CASES = {
    "exact": [("ABC", "ABC"), ("ABC", "XYZ"), (None, "ABC")],
    "set_intersect": [(["A", "B"], ["B", "C"]), (["A"], ["B"]), ([], ["A"])],
    "token_jaccard": [(["A", "B", "C"], ["A", "B"]), (["A"], ["B"]), (["A", "B"], ["A", "B"])],
    "token_containment": [(["A", "B"], ["A", "B", "C"]), (["A"], ["B", "C"]), ([], ["A"])],
    "prefix": [("JOHN", "JOHNNY"), ("JOHN", "JANE")],
    "numeric_tolerance": [(10.0, 10.4), (10.0, 20.0)],
    "date_tolerance": [(date(2020, 1, 1), date(2020, 1, 3)), (date(2020, 1, 1), date(2021, 1, 1))],
}


def test_comparator_python_sql_parity():
    con = duckdb.connect()
    dialect = DuckDbDialect()
    for comparator in COMPARATORS:
        if not comparator.implemented:
            continue
        cases = _CASES.get(comparator.id)
        assert cases is not None, f"no test cases declared for comparator {comparator.id!r}"
        params = {k: v["default"] for k, v in comparator.params_schema.items()}
        for value_a, value_b in cases:
            sql_expr = comparator.sql(
                _dialect_literal(value_a, dialect), _dialect_literal(value_b, dialect), params, dialect,
            )
            sql_result = con.execute(f"SELECT {sql_expr}").fetchone()[0]
            if comparator.id == "token_jaccard":
                sql_matched = bool(sql_result) and sql_result >= params.get("threshold", 0.5)
            else:
                sql_matched = bool(sql_result)
            py_matched = evaluate_python(comparator.id, value_a, value_b, params)
            assert py_matched == sql_matched, (
                f"{comparator.id}({value_a!r}, {value_b!r}): python={py_matched}, sql={sql_result!r}"
            )


# ----------------------------------------------------------------------
# 2. RAW_COLUMN match rule: score_pair() (Python, what a live run
#    decides through) vs compile_confidence_sql() (SQL, what redecide/
#    reblock preview through) on the real 5k fixture.
# ----------------------------------------------------------------------

def _build_pairs_and_raw(con):
    build_identifiers_table(con)
    build_frequency_table(con)
    build_name_dob_keys(con)
    build_candidate_pairs(con)
    build_pair_evidence(con)
    pairs = con.execute("SELECT a_key, b_key FROM candidate_pairs").fetchall()
    raw_rows = con.execute("SELECT CUSTOMER_CODE, SPONSOR_NAME, BRANCH_CODE, TELEPHONE FROM raw").fetchall()
    raw_by_code = {code: {"SPONSOR_NAME": sp, "BRANCH_CODE": bc, "TELEPHONE": tel} for code, sp, bc, tel in raw_rows}
    return pairs, raw_by_code


def _score_all(con, pairs, raw_by_code, ruleset, raw_columns):
    from engine.scoring.evidence import load_pair_evidence
    results = {}
    for a_key, b_key in pairs:
        evidence = load_pair_evidence(con, a_key, b_key)
        raw_fields = {}
        for col in raw_columns:
            raw_fields[col] = (raw_by_code.get(a_key, {}).get(col), raw_by_code.get(b_key, {}).get(col))
        evidence["raw_fields"] = raw_fields
        score = score_pair(evidence, ruleset)
        results[(a_key, b_key)] = (round(score.confidence_pct, 4), score.decision)
    return results


def _run_sql(con, ruleset):
    con.execute(compile_confidence_sql(ruleset, DuckDbDialect(), table_name="pair_decisions_rawcol"))
    rows = con.execute("SELECT a_key, b_key, confidence_pct, decision FROM pair_decisions_rawcol").fetchall()
    return {(a, b): (round(c, 4), d) for a, b, c, d in rows}


def test_raw_column_scalar_prefix_rule_parity():
    """BRANCH_CODE exact match, no veto -- the simplest custom field."""
    _skip_if_missing()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    pairs, raw_by_code = _build_pairs_and_raw(con)

    rule = MatchRule(
        rule_id="branch_match", attribute="RAW_COLUMN", comparator="exact",
        confidence_pct=15.0, params={"column": "BRANCH_CODE", "is_array": False},
    )
    ruleset = MatchRuleset(match_rules=(rule,), auto_link_min_confidence=10.0, review_min_confidence=5.0)

    py_results = _score_all(con, pairs, raw_by_code, ruleset, ["BRANCH_CODE"])
    sql_results = _run_sql(con, ruleset)

    assert set(py_results) == set(sql_results)
    mismatches = [(k, py_results[k], sql_results[k]) for k in py_results if py_results[k] != sql_results[k]]
    assert not mismatches, f"BRANCH_CODE exact rule: {len(mismatches)} mismatches: {mismatches[:10]}"


def test_raw_column_scalar_token_jaccard_with_veto_parity():
    """SPONSOR_NAME token_jaccard, WITH the generalized both_qualified_and_differ veto."""
    _skip_if_missing()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    pairs, raw_by_code = _build_pairs_and_raw(con)

    rule = MatchRule(
        rule_id="sponsor_match", attribute="RAW_COLUMN", comparator="token_jaccard",
        confidence_pct=20.0, params={"column": "SPONSOR_NAME", "is_array": False, "threshold": 0.5},
        veto_kind=VETO_BOTH_QUALIFIED_AND_DIFFER,
    )
    ruleset = MatchRuleset(match_rules=(rule,), auto_link_min_confidence=15.0, review_min_confidence=5.0)

    py_results = _score_all(con, pairs, raw_by_code, ruleset, ["SPONSOR_NAME"])
    sql_results = _run_sql(con, ruleset)

    assert set(py_results) == set(sql_results)
    mismatches = [(k, py_results[k], sql_results[k]) for k in py_results if py_results[k] != sql_results[k]]
    assert not mismatches, f"SPONSOR_NAME token_jaccard+veto rule: {len(mismatches)} mismatches: {mismatches[:10]}"

    # And prove the veto actually fires at least once on real data, not just parity-of-absence.
    any_rejected_by_veto = any(d == "REJECT" for _, d in py_results.values())
    assert any_rejected_by_veto, "expected at least one SPONSOR_NAME mismatch to force REJECT on real data"


def test_raw_column_array_set_intersect_with_veto_parity():
    """TELEPHONE (raw, unvalidated array) set_intersect, WITH the generalized both_present_no_overlap veto."""
    _skip_if_missing()
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    pairs, raw_by_code = _build_pairs_and_raw(con)

    rule = MatchRule(
        rule_id="telephone_match", attribute="RAW_COLUMN", comparator="set_intersect",
        confidence_pct=20.0, params={"column": "TELEPHONE", "is_array": True},
        veto_kind=VETO_BOTH_PRESENT_NO_OVERLAP,
    )
    ruleset = MatchRuleset(match_rules=(rule,), auto_link_min_confidence=15.0, review_min_confidence=5.0)

    py_results = _score_all(con, pairs, raw_by_code, ruleset, ["TELEPHONE"])
    sql_results = _run_sql(con, ruleset)

    assert set(py_results) == set(sql_results)
    mismatches = [(k, py_results[k], sql_results[k]) for k in py_results if py_results[k] != sql_results[k]]
    assert not mismatches, f"TELEPHONE set_intersect+veto rule: {len(mismatches)} mismatches: {mismatches[:10]}"


def test_prep_raw_value_split_matches_sql_str_split():
    """Double-space edge case: Python's str.split(' ') must keep the empty element, like SQL str_split(s, ' ') does."""
    con = duckdb.connect()
    dialect = DuckDbDialect()
    s = "JOHN  SMITH"
    sql_tokens = con.execute(f"SELECT {dialect.str_split(dialect.quote_str(s), ' ')}").fetchone()[0]
    py_tokens = prep_raw_value(s, "token_jaccard", is_array=False)
    assert list(sql_tokens) == py_tokens, f"sql={sql_tokens!r} python={py_tokens!r}"


if __name__ == "__main__":
    test_comparator_python_sql_parity()
    test_raw_column_scalar_prefix_rule_parity()
    test_raw_column_scalar_token_jaccard_with_veto_parity()
    test_raw_column_array_set_intersect_with_veto_parity()
    test_prep_raw_value_split_matches_sql_str_split()
    print("RAW_COLUMN matching parity tests passed.")
