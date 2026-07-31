"""
Tests engine.segments.classifier -- the Company/Individual record
segmentation from Stage 5 of the banker-rule-engine migration.

Two things must hold:
1. Disabled (the default) is a byte-identical no-op -- every customer
   is segment "ALL", matching the pre-Stage-5 world exactly.
2. The SQL expression (segment_sql_expr) and the Python oracle
   (classify_segment_python) agree on every case, on both dialects.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb
import pytest

from engine.segments.classifier import (
    SegmentationConfig, DEFAULT_SEGMENTATION_CONFIG, DEFAULT_COMPANY_KEYWORDS,
    classify_segment_python, segment_sql_expr, build_customer_segments, segment_counts,
    SEGMENT_ALL, SEGMENT_COMPANY, SEGMENT_INDIVIDUAL,
)
from engine.ports.duckdb_dialect import DuckDbDialect

_CASES = [
    ("MD RAHIM UDDIN", SEGMENT_INDIVIDUAL),
    ("RANA ENTERPRISE", SEGMENT_COMPANY),
    ("M/S. BASHAR TRADERS", SEGMENT_COMPANY),
    ("DOMINANT ABASHAN LTD.", SEGMENT_COMPANY),
    ("GOLDFISH CORPORATION", SEGMENT_COMPANY),
    ("BN SCHOOL & COLLEGE", SEGMENT_COMPANY),
    (None, SEGMENT_INDIVIDUAL),
    ("", SEGMENT_INDIVIDUAL),
    ("FATEMA BEGUM", SEGMENT_INDIVIDUAL),
]


def test_disabled_is_always_all():
    for name, _expected in _CASES:
        assert classify_segment_python(name, DEFAULT_SEGMENTATION_CONFIG) == SEGMENT_ALL


def test_enabled_python_classification():
    config = SegmentationConfig(enabled=True)
    for name, expected in _CASES:
        got = classify_segment_python(name, config)
        assert got == expected, f"{name!r}: expected {expected}, got {got}"


def test_sql_matches_python_oracle():
    con = duckdb.connect()
    dialect = DuckDbDialect()
    config = SegmentationConfig(enabled=True)

    rows = [(i, name) for i, (name, _) in enumerate(_CASES)]
    con.execute("CREATE TABLE t (id INTEGER, name_norm VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?, ?)", rows)

    expr = segment_sql_expr("name_norm", config, dialect)
    sql_results = {r[0]: r[1] for r in con.execute(f"SELECT id, {expr} FROM t").fetchall()}

    for i, (name, _expected) in enumerate(_CASES):
        py_result = classify_segment_python(name, config)
        assert sql_results[i] == py_result, f"case {i} ({name!r}): sql={sql_results[i]!r} py={py_result!r}"


def test_disabled_sql_is_constant_all():
    """When disabled, the SQL expression should be a bare constant, not a CASE over every keyword."""
    dialect = DuckDbDialect()
    expr = segment_sql_expr("name_norm", DEFAULT_SEGMENTATION_CONFIG, dialect)
    assert expr == dialect.quote_str(SEGMENT_ALL)


def test_build_customer_segments_and_counts():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE customer_scalars (customer_code VARCHAR, name_norm VARCHAR, name_tokens VARCHAR[],
                                        dob_iso VARCHAR, dob_precision VARCHAR)
    """)
    con.executemany(
        "INSERT INTO customer_scalars VALUES (?, ?, [], NULL, NULL)",
        [(f"C{i}", name) for i, (name, _) in enumerate(_CASES)],
    )
    config = SegmentationConfig(enabled=True)
    build_customer_segments(con, DuckDbDialect(), config)

    counts = segment_counts(con)
    expected_company = sum(1 for _, exp in _CASES if exp == SEGMENT_COMPANY)
    expected_individual = len(_CASES) - expected_company
    assert counts.get(SEGMENT_COMPANY, 0) == expected_company
    assert counts.get(SEGMENT_INDIVIDUAL, 0) == expected_individual


if __name__ == "__main__":
    test_disabled_is_always_all()
    test_enabled_python_classification()
    test_sql_matches_python_oracle()
    test_disabled_sql_is_constant_all()
    test_build_customer_segments_and_counts()
    print("segmentation: OK")
