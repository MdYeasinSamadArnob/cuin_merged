"""
CUIN v2 - Determinism / Fingerprinting Utilities (Ruleset v2, Layer 6)

Provides the fingerprint functions that prove a pipeline run is
reproducible: the same (input, ruleset) always yields the same
output_fingerprint, regardless of PYTHONHASHSEED, process, or run
order. This is the compliance proof requested by management -- every
run stamps these fingerprints so a second identical run can be checked
against the first with a single string comparison.

Reuses the sha256("|".join(...)) idiom from
engine.normalize.standardize.compute_record_hash.
"""

import hashlib
import json
import os
from typing import Iterable, List, Tuple


def canonical_json(obj) -> str:
    """Deterministic JSON serialization: sorted keys, no whitespace."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def sha256_hex(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def input_fingerprint(parquet_dir: str) -> str:
    """
    sha256 over sorted [(part filename, size, file sha256)] of every
    file in the parquet directory. Detects any change to the source
    data, byte-for-byte, regardless of file modification time.
    """
    entries = []
    for name in sorted(os.listdir(parquet_dir)):
        path = os.path.join(parquet_dir, name)
        if not os.path.isfile(path):
            continue
        size = os.path.getsize(path)
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        entries.append((name, size, h.hexdigest()))

    return sha256_hex(canonical_json(sorted(entries)))


def fingerprint_edges(edges: Iterable[Tuple[str, str, str, List[str]]]) -> str:
    """
    edges: iterable of (a_key, b_key, decision, signals_hit).
    Sorted before hashing so ordering never affects the fingerprint.
    """
    lines = sorted(
        f"{a}|{b}|{decision}|{'.'.join(sorted(signals))}"
        for a, b, decision, signals in edges
    )
    return sha256_hex("\n".join(lines))


def fingerprint_clusters(clusters: dict) -> str:
    """clusters: cluster_id -> list of member keys."""
    lines = sorted(
        f"{cluster_id}:{','.join(sorted(members))}"
        for cluster_id, members in clusters.items()
    )
    return sha256_hex("\n".join(lines))


def output_fingerprint(edges_fp: str, clusters_fp: str) -> str:
    return sha256_hex(edges_fp + clusters_fp)
