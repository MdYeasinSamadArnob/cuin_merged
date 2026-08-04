"""
CUIN v2 Control Plane - Run Management API Routes

Endpoints for listing, inspecting, cancelling, and deleting ER pipeline
runs. Runs themselves are created by api/routes_datasource.py (the
live Doris pipeline) -- this router no longer creates runs itself.
"""

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.run_service import get_run_service, RunStatus

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================
# Request/Response Models
# ============================================

class RunListResponse(BaseModel):
    """Response for listing runs."""
    runs: list
    total: int
    page: int
    page_size: int


class DeleteRunResponse(BaseModel):
    """Response from deleting a run."""
    success: bool
    message: str
    run_id: str


# ============================================
# Routes
# ============================================

@router.get("", response_model=RunListResponse)
async def list_runs(
    page: int = 1,
    page_size: int = 20
) -> dict:
    """
    List all pipeline runs with pagination.
    """
    run_service = get_run_service()
    runs, total = run_service.list_runs(page=page, page_size=page_size)

    return {
        "runs": [r.to_dict() for r in runs],
        "total": total,
        "page": page,
        "page_size": page_size
    }


@router.get("/{run_id}")
async def get_run(run_id: str) -> dict:
    """
    Get details of a specific run.
    """
    run_service = get_run_service()
    run = run_service.get_run(run_id)

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    return run.to_dict()


@router.post("/{run_id}/cancel")
async def cancel_run(run_id: str) -> dict:
    """
    Cancel a currently-running pipeline. Does not delete anything --
    the run stays in the registry with status CANCELLED. See DELETE
    /runs/{run_id} to actually remove a run.
    """
    run_service = get_run_service()
    success = run_service.cancel_run(run_id)

    if not success:
        raise HTTPException(
            status_code=400,
            detail="Cannot cancel run (not running or not found)"
        )

    return {"message": "Run cancelled", "run_id": run_id}


@router.delete("/{run_id}", response_model=DeleteRunResponse)
async def delete_run(run_id: str) -> DeleteRunResponse:
    """
    Permanently delete a run: its dedicated Doris database
    (cuin_run_<run_id>), its Postgres footprint (candidate_pairs,
    match_scores, match_decisions, review_queue, referee_explanations,
    identifier_frequency, and entity_relationships all cascade from the
    `runs` row; audit_events.run_id is nulled, not deleted, to keep the
    append-only hash-chained audit trail intact), its file artifacts
    under data/runs/, its entries in the review-queue pair index, and
    its entry in the run registry.

    Deliberately does NOT touch entities/entity_members/entity_lineage/
    resolution_overrides/review_decisions/customers_norm -- those are
    global platform state with no single owning run (a later run's
    carry_forward can update an entity an earlier run created), so
    deleting one run must never delete data another run depends on.

    Cannot delete a currently-RUNNING run -- cancel it first via POST
    /runs/{run_id}/cancel.
    """
    run_service = get_run_service()
    run = run_service.get_run(run_id)

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    if run.status == RunStatus.RUNNING:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete a running run -- cancel it first"
        )

    from api.config import settings

    # 1. Drop the run's dedicated Doris database.
    try:
        import pymysql
        from pipeline.doris_orchestrator import _sanitize_db_name

        admin = pymysql.connect(
            host=settings.DORIS_HOST, port=settings.DORIS_MYSQL_PORT,
            user=settings.DORIS_USER, password=settings.DORIS_PASSWORD,
            autocommit=True, connect_timeout=5,
        )
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {_sanitize_db_name(run_id)}")
        admin.close()
        logger.info(f"Dropped Doris database for run {run_id}")
    except Exception as e:
        logger.warning(f"Failed to drop Doris database for run {run_id} (Doris may not be running): {e}")

    # 2. Delete the Postgres footprint (cascades from `runs`).
    try:
        import psycopg2
        from db import repository

        conn = psycopg2.connect(settings.DATABASE_URL)
        repository.delete_run(conn, run_id)
        conn.close()
        logger.info(f"Deleted Postgres footprint for run {run_id}")
    except Exception as e:
        logger.warning(f"Failed to delete Postgres footprint for run {run_id}: {e}")

    # 3. Delete file artifacts under data/runs/ (records/clusters/
    #    singletons/scores/review-queue snapshots -- all named by
    #    run_id per pipeline/doris_orchestrator.py and services/review_service.py).
    runs_dir = Path("data/runs")
    if runs_dir.exists():
        for item in runs_dir.glob(f"{run_id}_*"):
            try:
                item.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete artifact {item}: {e}")

    # 4. Remove this run's entries from the review-queue pair index and
    #    evict its in-memory review-service state, if loaded.
    try:
        from services.review_service import get_review_service

        review_service = get_review_service()
        review_service._loaded_runs.discard(run_id)
        review_service._dirty_runs.discard(run_id)

        stale_review_ids = [
            rid for rid, item in review_service._items.items()
            if item.run_id == run_id
        ]
        for rid in stale_review_ids:
            item = review_service._items.pop(rid, None)
            if item:
                review_service._by_pair.pop(item.pair_id, None)

        stale_pair_ids = [
            pid for pid, r_id in review_service._pair_index.items()
            if r_id == run_id
        ]
        for pid in stale_pair_ids:
            review_service._pair_index.pop(pid, None)
        review_service._save_pair_index()
    except Exception as e:
        logger.warning(f"Failed to clean up review service state for run {run_id}: {e}")

    # 5. Remove from the run registry itself (data/runs_index.json).
    run_service.delete_run(run_id)

    logger.info(f"Deleted run {run_id}")

    return DeleteRunResponse(
        success=True,
        message=f"Run {run_id} deleted",
        run_id=run_id,
    )
