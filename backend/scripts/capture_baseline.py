#!/usr/bin/env python3
"""
Writes tests/golden/baseline_manifest.json from a completed run.

Only run this deliberately, as an explicit, reviewed re-baseline
(e.g. Stage 0 of the banker-rule-engine migration, which canonicalizes
evidence array ordering and therefore changes output_fingerprint
exactly once, by design -- see engine/scoring/evidence.py's docstring
and tests/unit/test_evidence_dialect_parity.py). Never run this to
"make a failing verify_baseline.py pass" -- that defeats the point of
the gate.

    python -m scripts.capture_baseline <run_id> [--note "why"]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

MANIFEST_PATH = os.path.join(os.path.dirname(__file__), "..", "tests", "golden", "baseline_manifest.json")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_id")
    parser.add_argument("--note", default="")
    args = parser.parse_args()

    from services.run_service import get_run_service

    run = get_run_service().get_run(args.run_id)
    if not run:
        print(f"run {args.run_id} not found")
        sys.exit(1)
    if run.status.value != "COMPLETED":
        print(f"run {args.run_id} status is {run.status.value}, not COMPLETED")
        sys.exit(1)

    manifest = {
        "captured_from_run_id": run.run_id,
        "captured_from_engine": run.engine,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "note": args.note,
        "counts": {
            "candidate_pairs": run.counters.candidates_generated,
            "auto_link": run.counters.auto_links,
            "review": run.counters.review_items,
            "reject": run.counters.rejected,
            "clusters": run.counters.clusters_created,
        },
        "output_fingerprint": run.output_fingerprint,
        "ruleset_version": run.ruleset_version,
    }

    os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"Wrote {MANIFEST_PATH}:")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
