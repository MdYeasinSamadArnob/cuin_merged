"""
Runs the ACTUAL pipeline.doris_orchestrator.DorisPipelineOrchestrator
end-to-end (not just the underlying SQL modules) twice on the same
input and asserts identical decision counts and per-pair decisions --
the orchestrator-level regression test that originally caught a real
bug (pymysql returns Doris ARRAY<...> columns as JSON-text strings,
not native Python lists; naively calling list() on that string
silently chops it into characters instead of parsing elements,
corrupting every jaccard/array-size computation and producing wrong
AUTO_LINK/REVIEW/REJECT splits with the SAME total pair count -- easy
to miss without a test at this level). See
pipeline.doris_orchestrator._parse_array.

Originally compared against pipeline.duckdb_orchestrator.
DuckDBPipelineOrchestrator as an independent reference; DuckDB has
since been removed (Doris is now the only engine), so this compares
two independent Doris runs of the same input against each other
instead. This catches non-determinism directly; it does NOT catch a
corruption bug that is wrong-but-consistent across runs (the original
ARRAY-as-string bug corrupted the same way every time) the way a
comparison against an independent second engine did -- that tradeoff
is accepted as part of removing the second engine entirely, not
something a same-engine test can fully replace.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")

DORIS_HOST = os.environ.get("DORIS_HOST", "127.0.0.1")
DORIS_MYSQL_PORT = int(os.environ.get("DORIS_MYSQL_PORT", "9130"))


def _run_doris_orchestrator(run_id: str):
    import pipeline.doris_orchestrator as mod
    mod.PARQUET_PATH = SAMPLE_PARQUET
    from pipeline.doris_orchestrator import DorisPipelineOrchestrator

    orch = DorisPipelineOrchestrator(run_id=run_id)
    result = asyncio.run(orch.run(run_id=run_id, mode="FULL"))
    assert result.success, f"Doris orchestrator run failed: {result.error_message}"
    return result, orch


def test_doris_orchestrator_is_internally_consistent_and_reproducible():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    try:
        import pymysql
        pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user="root", password="", connect_timeout=3).close()
    except Exception as e:
        pytest.skip(f"No live Doris instance reachable at {DORIS_HOST}:{DORIS_MYSQL_PORT}: {e}")

    result_a, orch_a = _run_doris_orchestrator("test-doris-orch-parity-a")
    result_b, orch_b = _run_doris_orchestrator("test-doris-orch-parity-b")

    split_a = (result_a.auto_links, result_a.review_items, result_a.rejected)
    split_b = (result_b.auto_links, result_b.review_items, result_b.rejected)
    assert split_a == split_b, (
        f"Decision split diverged across two independent runs of the same input: {split_a} vs {split_b} "
        "-- the pipeline is supposed to be deterministic, see tests/integration/test_determinism.py"
    )

    total = sum(split_a)
    assert total > 0, "Expected at least one scored pair on the sample fixture"

    decisions_a = {pid: d.value for pid, d in orch_a.get_decisions().items()}
    decisions_b = {pid: d.value for pid, d in orch_b.get_decisions().items()}
    assert decisions_a == decisions_b, (
        "Per-pair decisions diverged across two independent runs of the same input: "
        f"{[(k, decisions_a[k], decisions_b.get(k)) for k in decisions_a if decisions_a[k] != decisions_b.get(k)]}"
    )


if __name__ == "__main__":
    test_doris_orchestrator_is_internally_consistent_and_reproducible()
    print("Orchestrator-level internal consistency test passed.")
