"""
Runs the ACTUAL pipeline.doris_orchestrator.DorisPipelineOrchestrator
end-to-end (not just the underlying SQL modules) and asserts its
decision counts match pipeline.duckdb_orchestrator.DuckDBPipelineOrchestrator
on the same input.

This is deliberately a separate, higher-level test from
test_doris_cross_engine_parity.py: that one drives the dialect-ported
SQL modules directly and proves the SQL layer is correct. This one
exercises the orchestrator's Python-side evidence consumption too --
which is exactly where a real bug lived (pymysql returns Doris
ARRAY<...> columns as JSON-text strings, not native Python lists;
naively calling list() on that string silently chops it into
characters instead of parsing elements, corrupting every jaccard/
array-size computation and producing wrong AUTO_LINK/REVIEW/REJECT
splits with the SAME total pair count -- easy to miss without a test
at this level). See pipeline.doris_orchestrator._parse_array.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

SAMPLE_PARQUET = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample_5k.parquet")

DORIS_HOST = os.environ.get("DORIS_HOST", "127.0.0.1")
DORIS_MYSQL_PORT = int(os.environ.get("DORIS_MYSQL_PORT", "9130"))


def _run_duckdb_orchestrator():
    import pipeline.duckdb_orchestrator as mod
    mod.PARQUET_PATH = SAMPLE_PARQUET
    from pipeline.duckdb_orchestrator import DuckDBPipelineOrchestrator

    orch = DuckDBPipelineOrchestrator(run_id="test-duckdb-orch-parity")
    result = asyncio.run(orch.run(run_id="test-duckdb-orch-parity", mode="FULL"))
    assert result.success, f"DuckDB orchestrator run failed: {result.error_message}"
    return result, orch


def _run_doris_orchestrator():
    import pipeline.doris_orchestrator as mod
    mod.PARQUET_PATH = SAMPLE_PARQUET
    from pipeline.doris_orchestrator import DorisPipelineOrchestrator

    orch = DorisPipelineOrchestrator(run_id="test-doris-orch-parity")
    result = asyncio.run(orch.run(run_id="test-doris-orch-parity", mode="FULL"))
    assert result.success, f"Doris orchestrator run failed: {result.error_message}"
    return result, orch


def test_doris_orchestrator_matches_duckdb_orchestrator():
    if not os.path.exists(SAMPLE_PARQUET):
        pytest.skip(f"fixture not found: {SAMPLE_PARQUET}")

    try:
        import pymysql
        pymysql.connect(host=DORIS_HOST, port=DORIS_MYSQL_PORT, user="root", password="", connect_timeout=3).close()
    except Exception as e:
        pytest.skip(f"No live Doris instance reachable at {DORIS_HOST}:{DORIS_MYSQL_PORT}: {e}")

    duckdb_result, duckdb_orch = _run_duckdb_orchestrator()
    doris_result, doris_orch = _run_doris_orchestrator()

    assert (duckdb_result.auto_links, duckdb_result.review_items, duckdb_result.rejected) == (
        doris_result.auto_links, doris_result.review_items, doris_result.rejected
    ), (
        f"Decision split diverged between engines: "
        f"DuckDB={(duckdb_result.auto_links, duckdb_result.review_items, duckdb_result.rejected)} "
        f"Doris={(doris_result.auto_links, doris_result.review_items, doris_result.rejected)}"
    )

    duckdb_decisions = {pid: d.value for pid, d in duckdb_orch.get_decisions().items()}
    doris_decisions = {pid: d.value for pid, d in doris_orch.get_decisions().items()}
    assert duckdb_decisions == doris_decisions, (
        f"Per-pair decisions diverged: "
        f"{[(k, duckdb_decisions[k], doris_decisions.get(k)) for k in duckdb_decisions if duckdb_decisions[k] != doris_decisions.get(k)]}"
    )


if __name__ == "__main__":
    test_doris_orchestrator_matches_duckdb_orchestrator()
    print("Orchestrator-level cross-engine parity test passed.")
