"""
Unit tests for engine.normalize.identity, asserting the fixes over
engine.normalize.standardize that were measured to be necessary:
  - phone: reject round-number junk structurally (no blacklist)
  - document: reject boilerplate ("EI:Enterprise Info") structurally
  - name: honorific-only names ("MD.", "MD", "MST.") produce an EMPTY
    rare-token list -- this is what prevents 4,054+ records collapsing
    into one giant block on a bare "MD." blocking key.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.normalize.identity import (
    norm_mobile_bd, norm_email, parse_document, norm_name, norm_dob, norm_address,
)


def test_valid_bd_mobile_accepted():
    assert norm_mobile_bd("01712345678")[0] == "01712345678"
    assert norm_mobile_bd("+8801712345678")[0] == "01712345678"
    assert norm_mobile_bd("8801912345678")[0] == "01912345678"
    assert norm_mobile_bd("1712345678")[0] == "01712345678"


def test_junk_mobile_rejected_without_blacklist():
    for junk in ["01700000000", "01711000000", "01800000000", "0000000000", "123"]:
        val, reason = norm_mobile_bd(junk)
        assert val is None, f"{junk!r} should be rejected but got {val!r}"
        assert reason is not None


def test_document_valid_tin_accepted():
    dtype, val, reason = parse_document("TIN:574352752846")
    assert dtype == "TIN"
    assert val == "574352752846"
    assert reason is None


def test_document_boilerplate_rejected():
    for junk in ["TIN:NO", "TIN:NO TIN", "NAI:123", "OTH:123", "EI:Enterprise Info", "EI:Enterprise info"]:
        dtype, val, reason = parse_document(junk)
        assert val is None, f"{junk!r} should be rejected but got {val!r}"


def test_honorific_only_names_produce_empty_token_set():
    # This is the regression that matters most: an empty-string blocking
    # key collapsing thousands of records into one component is exactly
    # how the reported bug would recur in new clothes.
    for stub in ["MD.", "MD", "MST.", "MST"]:
        _, tokens = norm_name(stub)
        assert tokens == [], f"{stub!r} should yield empty rare tokens, got {tokens}"


def test_real_name_retains_rare_tokens_after_honorific_strip():
    _, tokens = norm_name("MD SHARIFUL ISLAM")
    assert "SHARIFUL" in tokens
    assert "ISLAM" in tokens
    assert "MD" not in tokens


def test_dob_year_only_stub_flagged():
    iso, precision = norm_dob("1980-01-01T00:00:00")
    assert iso == "1980-01-01"
    assert precision == "YEAR_ONLY"

    iso2, precision2 = norm_dob("1989-05-01T00:00:00")
    assert iso2 == "1989-05-01"
    assert precision2 == "FULL"


def test_address_boilerplate_rejected():
    for junk in ["DO", "SAME", "NA", "SAME AS PRESENT ADDRESS"]:
        assert norm_address(junk) is None, f"{junk!r} should be rejected"


def test_address_real_value_kept():
    result = norm_address("House 12, Road 5, Dhanmondi")
    assert result is not None
    assert "DHANMONDI" in result


if __name__ == "__main__":
    test_valid_bd_mobile_accepted()
    test_junk_mobile_rejected_without_blacklist()
    test_document_valid_tin_accepted()
    test_document_boilerplate_rejected()
    test_honorific_only_names_produce_empty_token_set()
    test_real_name_retains_rare_tokens_after_honorific_strip()
    test_dob_year_only_stub_flagged()
    test_address_boilerplate_rejected()
    test_address_real_value_kept()
    print("All identity normalization tests passed.")
