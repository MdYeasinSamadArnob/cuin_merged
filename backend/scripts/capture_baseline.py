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

The manifest holds multiple NAMED baselines (keyed by rule-catalog
identity, not by stage) because the same pipeline code legitimately
produces different, both-correct results depending on the active
policy_versions row -- e.g. "segmentation_off" (the seeded defaults)
vs "active_policy_v4" (adds a custom-weighted field). The entity
resolution workbench (Stages 0-6 of the plan) is additive to the
engine and must not move EITHER number, so both are pinned and
checked independently.

    python -m scripts.capture_baseline <run_id> <baseline_name> [--note "why"]
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
    parser.add_argument("baseline_name")
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

    entry = {
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

    manifest = {}
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)
        # Migrate the old single-baseline shape (no "baselines" key) forward.
        if "baselines" not in manifest and "output_fingerprint" in manifest:
            manifest = {"baselines": {"segmentation_off": manifest}}
    manifest.setdefault("baselines", {})
    manifest["baselines"][args.baseline_name] = entry

    os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    print(f"Wrote {MANIFEST_PATH} (baseline {args.baseline_name!r}):")
    print(json.dumps(entry, indent=2))


if __name__ == "__main__":
    main()
