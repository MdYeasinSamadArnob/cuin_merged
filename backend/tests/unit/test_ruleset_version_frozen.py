"""
Guards RULESET_VERSION during the banker-rule-engine migration
(stages 0-6 of the plan at
/home/arnob/.claude/plans/i-actually-think-apache-golden-galaxy.md).

engine.clustering.cluster_manager.py:114-117 derives every cluster_id
from `sha256(RULESET_VERSION + "|" + sorted(members))`, and
engine.determinism.fingerprint_clusters hashes cluster_id -- so
bumping this literal changes output_fingerprint with ZERO change to
actual clustering. policies/ruleset_v2.yaml's header comment actively
instructs "bump RULESET_VERSION in the same commit as any change",
which is exactly the wrong instruction to follow while a byte-for-byte
baseline is load-bearing across a multi-stage migration.

This is deliberately a trivial, high-signal test: it exists so that
whoever edits tiers.py/identity.py/deterministic_blocker.py next
(engine.ruleset.version.ruleset_fingerprint's own source list) gets an
immediate, legible failure here instead of a mysterious
output_fingerprint mismatch three files away. Delete or update this
test only as a deliberate part of a new baseline capture
(scripts/capture_baseline.py), never as an incidental fix.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.ruleset.version import RULESET_VERSION


def test_ruleset_version_unchanged_during_migration():
    assert RULESET_VERSION == "er-2026.07.1", (
        f"RULESET_VERSION changed to {RULESET_VERSION!r}. This changes every cluster_id "
        f"(engine.clustering.cluster_manager.py:114-117) and therefore output_fingerprint, "
        f"even if clustering logic itself is unchanged. If this is a deliberate, planned "
        f"re-baseline (see scripts/capture_baseline.py), update this test's expected value "
        f"in the same commit. If not, revert the version bump."
    )


if __name__ == "__main__":
    test_ruleset_version_unchanged_during_migration()
    print("RULESET_VERSION frozen: OK")
