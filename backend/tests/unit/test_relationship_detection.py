"""Tests engine.segments.relationships.detect_relationship_evidence."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.segments.relationships import detect_relationship_evidence


def test_shared_mobile_detected():
    evidence = {
        "identifiers": [
            {"id_type": "mobile", "doc_type": None, "values_a": ["01712345678"], "values_b": ["01712345678"], "intersection": ["01712345678"]},
        ],
        "name_dob": {"token_union": [], "token_intersection": []},
    }
    result = detect_relationship_evidence(evidence)
    assert len(result) == 1
    assert result[0].field == "mobile"
    assert result[0].value == "01712345678"


def test_no_evidence_no_relationship():
    evidence = {
        "identifiers": [
            {"id_type": "mobile", "doc_type": None, "values_a": ["01712345678"], "values_b": ["01799999999"], "intersection": []},
        ],
        "name_dob": {"token_union": ["A", "B"], "token_intersection": []},
    }
    assert detect_relationship_evidence(evidence) == []


def test_multiple_fields_all_detected():
    evidence = {
        "identifiers": [
            {"id_type": "mobile", "doc_type": None, "values_a": ["01712345678"], "values_b": ["01712345678"], "intersection": ["01712345678"]},
            {"id_type": "email", "doc_type": None, "values_a": ["a@x.com"], "values_b": ["a@x.com"], "intersection": ["a@x.com"]},
        ],
        "name_dob": {"token_union": [], "token_intersection": []},
    }
    result = detect_relationship_evidence(evidence)
    fields = {r.field for r in result}
    assert fields == {"mobile", "email"}


def test_matching_dob_detected():
    evidence = {
        "identifiers": [],
        "name_dob": {"token_union": [], "token_intersection": [], "dob_a": "1990-01-01", "dob_b": "1990-01-01"},
    }
    result = detect_relationship_evidence(evidence)
    assert len(result) == 1
    assert result[0].field == "dob"


if __name__ == "__main__":
    test_shared_mobile_detected()
    test_no_evidence_no_relationship()
    test_multiple_fields_all_detected()
    test_matching_dob_detected()
    print("relationship detection: OK")
