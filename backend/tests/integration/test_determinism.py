"""
Proves the pipeline is bit-reproducible: the same (input, ruleset)
always yields an identical output_fingerprint, run after run, process
after process, regardless of PYTHONHASHSEED.

PYTHONHASHSEED is deliberately DIFFERENT on every trial. That is the
whole point: if any set()/dict() iteration order leaks into the
output -- and before this rework it did, via UnionFind.get_clusters()
returning Set[str] and cluster_manager picking members[0] off an
unordered set -- trial 0 would disagree with trial 1. Pinning the seed
to a fixed value would HIDE that class of bug rather than catch it.
That shared clustering code (engine.clustering) is unchanged by which
pipeline engine runs it, so this property is exactly as meaningful for
Doris as it was for the now-removed DuckDB engine this test originally
ran against.

Each trial runs in a SEPARATE SUBPROCESS, not a loop in this process,
because engine.clustering.cluster_manager._cluster_manager is a
process-global singleton that would accumulate state across
in-process trials and pass falsely.

Requires a live Doris instance reachable over MySQL (port 9130) --
skips gracefully if unreachable, since this exercises real
infrastructure, not just code.
"""

import json
import os
import subprocess
import sys

import pytest

BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SAMPLE_PARQUET = os.environ.get(
    "CUIN_TEST_SAMPLE_PARQUET",
    os.path.join(BACKEND_DIR, "tests", "fixtures", "sample_5k.parquet"),
)

DORIS_HOST = os.environ.get("DORIS_HOST", "127.0.0.1")
DORIS_MYSQL_PORT = int(os.environ.get("DORIS_MYSQL_PORT", "9130"))


def _run_pipeline_subprocess(hash_seed: int, parquet_path: str) -> dict:
    driver = f'''
import asyncio, sys, json
sys.path.insert(0, {BACKEND_DIR!r})
import pipeline.doris_orchestrator as mod
mod.PARQUET_PATH = {parquet_path!r}
from pipeline.doris_orchestrator import DorisPipelineOrchestrator

async def main():
    orch = DorisPipelineOrchestrator(run_id="determinism-test")
    result = await orch.run(run_id="determinism-test", mode="FULL")
    print(json.dumps({{
        "success": result.success,
        "error": result.error_message,
        "auto_links": result.auto_links,
        "review": result.review_items,
        "fingerprints": orch.get_fingerprints(),
    }}))

asyncio.run(main())
'''
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = str(hash_seed)
    proc = subprocess.run(
        [sys.executable, "-c", driver],
        env=env, capture_output=True, text=True, cwd=BACKEND_DIR, timeout=300,
    )
    assert proc.returncode == 0, f"subprocess failed:\n{proc.stderr}"
    json_lines = [l for l in proc.stdout.strip().split("\n") if l.startswith("{")]
    assert json_lines, f"no JSON output found:\n{proc.stdout}\n{proc.stderr}"
    data = json.loads(json_lines[-1])
    assert data["success"], f"pipeline run failed: {data.get('error')}"
    return data


def test_pipeline_is_bit_reproducible():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET} (see tests/fixtures/README.md)")

    try:
        import pymysql
        pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user="root", password="", connect_timeout=3).close()
    except Exception as e:
        pytest.skip(f"No live Doris instance reachable at {DORIS_HOST}:{DORIS_MYSQL_PORT}: {e}")

    trials = [_run_pipeline_subprocess(seed, SAMPLE_PARQUET) for seed in (0, 7919, 15838)]

    fps = [t["fingerprints"]["output_fingerprint"] for t in trials]
    assert fps[0] == fps[1] == fps[2], f"fingerprints diverged across hash seeds: {fps}"

    auto_links = [t["auto_links"] for t in trials]
    assert auto_links[0] == auto_links[1] == auto_links[2], (
        f"auto_link counts diverged across hash seeds: {auto_links}"
    )


def test_ruleset_change_changes_fingerprint(tmp_path):
    """
    Without this test, a broken fingerprint function (e.g. one that
    accidentally ignores the ruleset entirely) would trivially pass
    test_pipeline_is_bit_reproducible.
    """
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    sys.path.insert(0, BACKEND_DIR)
    from engine.ruleset.version import ruleset_fingerprint

    fp_before = ruleset_fingerprint(base_dir=BACKEND_DIR)

    ruleset_path = os.path.join(BACKEND_DIR, "policies", "ruleset_v2.yaml")
    with open(ruleset_path) as f:
        original = f.read()
    try:
        with open(ruleset_path, "w") as f:
            f.write(original.replace("mobile_max_records: 20", "mobile_max_records: 21"))
        fp_after = ruleset_fingerprint(base_dir=BACKEND_DIR)
        assert fp_before != fp_after, "ruleset_fingerprint did not change after editing the ruleset file"
    finally:
        with open(ruleset_path, "w") as f:
            f.write(original)


if __name__ == "__main__":
    test_pipeline_is_bit_reproducible()
    print("Reproducibility test passed.")
