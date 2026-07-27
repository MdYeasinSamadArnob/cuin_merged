"""
CUIN v2 - Ruleset Versioning

Stamps every run/edge/cluster with the exact ruleset that produced it,
so results are traceable and reproducible for audit.
"""

import hashlib
import os

RULESET_VERSION = "er-2026.07.1"

_RULESET_SOURCE_FILES = [
    "policies/ruleset_v2.yaml",
    "engine/normalize/identity.py",
    "engine/scoring/tiers.py",
    "engine/blocking/deterministic_blocker.py",
]


def ruleset_fingerprint(base_dir: str = ".") -> str:
    """
    Deterministic fingerprint of the ruleset: the config file contents
    plus the source of every module whose logic affects match/decision
    outcomes. Changing any of these changes the fingerprint, which is
    the audit signal that "the rules changed" rather than "the data changed".
    """
    h = hashlib.sha256()
    h.update(RULESET_VERSION.encode("utf-8"))

    for rel_path in sorted(_RULESET_SOURCE_FILES):
        path = os.path.join(base_dir, rel_path)
        h.update(rel_path.encode("utf-8"))
        try:
            with open(path, "rb") as f:
                h.update(f.read())
        except FileNotFoundError:
            h.update(b"<missing>")

    return h.hexdigest()
