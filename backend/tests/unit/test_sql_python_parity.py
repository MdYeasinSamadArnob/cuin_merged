"""
Asserts engine.normalize.explode's native-SQL normalization agrees with
engine.normalize.identity's tested Python "oracle" implementation on a
real data sample. The bulk explosion step was rewritten from Python
UDFs to native SQL for throughput (Python UDFs took minutes on 1.5M+
rows; native SQL takes under a second) -- this test is what keeps that
rewrite honest: if the two implementations drift, this test catches it
rather than silently producing different normalized values.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import duckdb

from engine.normalize.explode import build_identifiers_table
from engine.normalize.identity import (
    norm_mobile_bd, norm_email, parse_document, norm_name, norm_dob, norm_address,
)

SAMPLE_PARQUET = os.path.join(
    os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet"
)


def _load_sql_identifiers():
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=true")
    con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    build_identifiers_table(con)
    return con


def test_mobile_parity():
    con = _load_sql_identifiers()
    rows = con.execute("SELECT value_raw, value_norm, is_valid FROM identifiers WHERE id_type = 'mobile'").fetchall()
    assert len(rows) > 0, "no mobile rows in sample -- fixture missing/empty?"

    mismatches = []
    for raw, sql_norm, sql_valid in rows:
        py_norm, py_reason = norm_mobile_bd(raw)
        py_valid = py_norm is not None
        if bool(sql_valid) != py_valid or (py_valid and sql_norm != py_norm):
            mismatches.append((raw, sql_norm, sql_valid, py_norm, py_reason))

    assert not mismatches, f"SQL/Python mobile normalization disagree on {len(mismatches)} values: {mismatches[:10]}"


def test_document_parity():
    con = _load_sql_identifiers()
    rows = con.execute("SELECT value_raw, value_norm, doc_type, is_valid FROM identifiers WHERE id_type = 'document'").fetchall()
    assert len(rows) > 0

    mismatches = []
    for raw, sql_norm, sql_doc_type, sql_valid in rows:
        py_doc_type, py_norm, py_reason = parse_document(raw)
        py_valid = py_norm is not None
        if bool(sql_valid) != py_valid or (py_valid and (sql_norm != py_norm or sql_doc_type != py_doc_type)):
            mismatches.append((raw, sql_norm, sql_doc_type, sql_valid, py_norm, py_doc_type, py_reason))

    assert not mismatches, f"SQL/Python document normalization disagree on {len(mismatches)} values: {mismatches[:10]}"


def test_email_parity():
    con = _load_sql_identifiers()
    rows = con.execute("SELECT value_raw, value_norm, is_valid FROM identifiers WHERE id_type = 'email'").fetchall()
    assert len(rows) > 0

    mismatches = []
    for raw, sql_norm, sql_valid in rows:
        py_norm, py_reason = norm_email(raw)
        py_valid = py_norm is not None
        if bool(sql_valid) != py_valid or (py_valid and sql_norm != py_norm):
            mismatches.append((raw, sql_norm, sql_valid, py_norm))

    assert not mismatches, f"SQL/Python email normalization disagree on {len(mismatches)} values: {mismatches[:10]}"


def test_name_and_dob_parity():
    con = _load_sql_identifiers()
    con.execute(f"CREATE OR REPLACE VIEW raw2 AS SELECT * FROM read_parquet('{SAMPLE_PARQUET}')")
    rows = con.execute("""
        SELECT r.CUSTOMER_CODE, r.NAME, r.BIRTH_DATE, s.name_norm, s.name_tokens, s.dob_iso, s.dob_precision
        FROM customer_scalars s JOIN raw2 r ON r.CUSTOMER_CODE = s.customer_code
        USING SAMPLE 2000 ROWS
    """).fetchall()
    assert len(rows) > 0

    name_mismatches, dob_mismatches = [], []
    for _, raw_name, raw_dob, sql_name_norm, sql_tokens, sql_dob_iso, sql_dob_prec in rows:
        py_name_norm, py_tokens = norm_name(raw_name)
        if (sql_name_norm or None) != (py_name_norm or None) or sorted(sql_tokens or []) != sorted(py_tokens or []):
            name_mismatches.append((raw_name, sql_name_norm, sql_tokens, py_name_norm, py_tokens))

        py_dob_iso, py_dob_prec = norm_dob(raw_dob)
        if (sql_dob_iso or None) != (py_dob_iso or None) or (sql_dob_prec or None) != (py_dob_prec or None):
            dob_mismatches.append((raw_dob, sql_dob_iso, sql_dob_prec, py_dob_iso, py_dob_prec))

    assert not name_mismatches, f"SQL/Python name normalization disagree on {len(name_mismatches)}: {name_mismatches[:10]}"
    assert not dob_mismatches, f"SQL/Python dob normalization disagree on {len(dob_mismatches)}: {dob_mismatches[:10]}"


if __name__ == "__main__":
    if not os.path.exists(SAMPLE_PARQUET):
        print(f"SKIP: fixture not found at {SAMPLE_PARQUET}")
    else:
        test_mobile_parity()
        test_document_parity()
        test_email_parity()
        test_name_and_dob_parity()
        print("All SQL/Python parity tests passed.")
