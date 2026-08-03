"""
Tests engine.segments.classifier -- the Company/Individual record
segmentation from Stage 5 of the banker-rule-engine migration.

Two things must hold:
1. Disabled (the default) is a byte-identical no-op -- every customer
   is segment "ALL", matching the pre-Stage-5 world exactly.
2. The SQL expression (segment_sql_expr) and the Python oracle
   (classify_segment_python) agree on every case, run through live
   Doris.

Requires a live Doris instance (see tests/integration/_doris_fixture.py)
for the two SQL-touching tests below -- moved from tests/unit/ during
the Doris-only migration, since they can no longer run against an
in-process DuckDB connection. The three pure-Python tests have no
database dependency at all and are unaffected.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.segments.classifier import (
    SegmentationConfig, DEFAULT_SEGMENTATION_CONFIG, DEFAULT_COMPANY_KEYWORDS,
    classify_segment_python, segment_sql_expr, build_customer_segments, segment_counts,
    SEGMENT_ALL, SEGMENT_COMPANY, SEGMENT_INDIVIDUAL,
)

from _doris_fixture import DorisScratchDb, skip_unless_doris_reachable

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
    skip_unless_doris_reachable()
    config = SegmentationConfig(enabled=True)

    with DorisScratchDb("cuin_test_segmentation_sql") as fx:
        fx.con.execute("""
            CREATE TABLE t (id INT NOT NULL, name_norm VARCHAR(500))
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4
        """)
        rows = [(i, name) for i, (name, _) in enumerate(_CASES)]
        fx.con.executemany("INSERT INTO t VALUES (?, ?)", rows)

        expr = segment_sql_expr("name_norm", config, fx.dialect)
        sql_results = {r[0]: r[1] for r in fx.con.execute(f"SELECT id, {expr} FROM t").fetchall()}

    for i, (name, _expected) in enumerate(_CASES):
        py_result = classify_segment_python(name, config)
        assert sql_results[i] == py_result, f"case {i} ({name!r}): sql={sql_results[i]!r} py={py_result!r}"


def test_disabled_sql_is_constant_all():
    """When disabled, the SQL expression should be a bare constant, not a CASE over every keyword."""
    skip_unless_doris_reachable()
    from engine.ports.doris_dialect import DorisDialect

    dialect = DorisDialect()
    expr = segment_sql_expr("name_norm", DEFAULT_SEGMENTATION_CONFIG, dialect)
    assert expr == dialect.quote_str(SEGMENT_ALL)


def test_build_customer_segments_and_counts():
    skip_unless_doris_reachable()
    config = SegmentationConfig(enabled=True)

    with DorisScratchDb("cuin_test_segmentation_build") as fx:
        fx.con.execute("""
            CREATE TABLE customer_scalars (
                customer_code VARCHAR(64) NOT NULL,
                name_norm VARCHAR(500),
                name_tokens ARRAY<TEXT>,
                dob_iso VARCHAR(32),
                dob_precision VARCHAR(16)
            ) DUPLICATE KEY(customer_code) DISTRIBUTED BY HASH(customer_code) BUCKETS 4
        """)
        fx.con.executemany(
            "INSERT INTO customer_scalars VALUES (?, ?, [], NULL, NULL)",
            [(f"C{i}", name) for i, (name, _) in enumerate(_CASES)],
        )
        build_customer_segments(fx.con, fx.dialect, config)
        counts = segment_counts(fx.con)

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
