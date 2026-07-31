#!/usr/bin/env python3
"""
Universal gate for the banker-rule-engine migration
(/home/arnob/.claude/plans/i-actually-think-apache-golden-galaxy.md).

Compares a completed run's counters + output_fingerprint against
tests/golden/baseline_manifest.json. Run after every migration stage,
on a fresh full-dataset run of EACH engine:

    python -m scripts.verify_baseline <run_id>

Exits non-zero (and prints exactly what differs) if the run doesn't
match. Does not run the pipeline itself -- trigger a run via
POST /datasource/demo first (engine=duckdb or engine=doris), wait for
COMPLETED, then pass its run_id here.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

MANIFEST_PATH = os.path.join(os.path.dirname(__file__), "..", "tests", "golden", "baseline_manifest.json")


def load_manifest() -> dict:
    with open(MANIFEST_PATH) as f:
        return json.load(f)


def verify(run_id: str) -> bool:
    from services.run_service import get_run_service

    run = get_run_service().get_run(run_id)
    if not run:
        print(f"FAIL: run {run_id} not found")
        return False
    if run.status.value != "COMPLETED":
        print(f"FAIL: run {run_id} status is {run.status.value}, not COMPLETED")
        return False

    manifest = load_manifest()
    counts = {
        "candidate_pairs": run.counters.candidates_generated,
        "auto_link": run.counters.auto_links,
        "review": run.counters.review_items,
        "reject": run.counters.rejected,
        "clusters": run.counters.clusters_created,
    }

    ok = True
    for key, expected in manifest["counts"].items():
        actual = counts.get(key)
        if actual != expected:
            print(f"FAIL: {key} = {actual}, expected {expected}")
            ok = False

    expected_fp = manifest.get("output_fingerprint")
    if expected_fp and run.output_fingerprint != expected_fp:
        print(f"FAIL: output_fingerprint = {run.output_fingerprint}, expected {expected_fp}")
        ok = False

    if ok:
        print(f"PASS: run {run_id} ({run.engine}) matches baseline manifest")
        print(f"      {counts}")
        print(f"      fingerprint={run.output_fingerprint}")
    return ok


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python -m scripts.verify_baseline <run_id>")
        sys.exit(2)
    sys.exit(0 if verify(sys.argv[1]) else 1)
