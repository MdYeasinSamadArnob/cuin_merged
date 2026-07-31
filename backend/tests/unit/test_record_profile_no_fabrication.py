"""
Regression test for api.routes_graph.get_record_profile()'s deleted
mock-data branch. Any record id starting "0005"/"50"/"DUP" used to get
a randomly-generated fake name/balance/product -- verified live that
9,059 real customers match that pattern (e.g. "00050002" is genuinely
"MDTANVIR RAHMAN"). This test proves the function now only ever
returns data it actually read, never invents it.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from api.routes_graph import get_record_profile, _run_records_disk_cache


def test_no_mock_data_for_0005_prefix_without_records():
    _run_records_disk_cache.clear()
    profile = get_record_profile("00050002", run_records=None, run_id=None)
    assert profile["resolved"] is False
    assert "generated" not in profile.get("metadata", {})
    assert "GOLAM MOHD ZUBAYED" not in profile["name"]
    assert "SK MAHBUBLLAH" not in profile["name"]
    assert "MD MOHI UDDIN" not in profile["name"]
    assert "KAZI MASIHUR RAHMAN" not in profile["name"]


def test_no_mock_data_for_50_prefix():
    _run_records_disk_cache.clear()
    profile = get_record_profile("50012345", run_records=None, run_id=None)
    assert profile["resolved"] is False
    assert profile["name"] == "Unknown (50012345)"


def test_no_mock_data_for_DUP_prefix():
    _run_records_disk_cache.clear()
    profile = get_record_profile("DUP-00012", run_records=None, run_id=None)
    assert profile["resolved"] is False


def test_real_record_from_run_records_dict_is_used():
    real = {"00050002": {"name_norm": "MDTANVIR RAHMAN", "source_customer_id": "00050002"}}
    profile = get_record_profile("00050002", run_records=real, run_id=None)
    assert profile["resolved"] is True
    assert profile["name"] == "MDTANVIR RAHMAN"


def test_disk_fallback_reads_real_records_json_and_caches():
    """Uses a real *_records.json on disk if one exists, proving the
    tier-2 disk fallback (previously a documented no-op) now actually loads it."""
    import glob
    candidates = glob.glob("data/runs/*_records.json")
    if not candidates:
        import pytest
        pytest.skip("no data/runs/*_records.json fixture present in this environment")

    import json
    path = candidates[0]
    run_id = os.path.basename(path).replace("_records.json", "")
    with open(path) as f:
        data = json.load(f)
    if not data:
        import pytest
        pytest.skip("records.json file is empty")
    some_code = next(iter(data))
    expected_name = data[some_code].get("name_norm", data[some_code].get("name"))

    _run_records_disk_cache.clear()
    profile = get_record_profile(some_code, run_records=None, run_id=run_id)
    assert profile["resolved"] is True
    assert profile["name"] == expected_name
    assert run_id in _run_records_disk_cache, "disk load should populate the cache"


if __name__ == "__main__":
    test_no_mock_data_for_0005_prefix_without_records()
    test_no_mock_data_for_50_prefix()
    test_no_mock_data_for_DUP_prefix()
    test_real_record_from_run_records_dict_is_used()
    test_disk_fallback_reads_real_records_json_and_caches()
    print("record profile no-fabrication tests: OK")
