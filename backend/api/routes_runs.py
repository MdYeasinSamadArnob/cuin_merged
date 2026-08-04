"""
CUIN v2 Control Plane - Run Management API Routes

Endpoints for listing, inspecting, and cancelling ER pipeline runs.
Runs themselves are created by api/routes_datasource.py (the live
Doris pipeline) -- this router no longer creates runs itself.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from services.run_service import get_run_service

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


@router.delete("/{run_id}")
async def cancel_run(run_id: str) -> dict:
    """
    Cancel a running pipeline.
    """
    run_service = get_run_service()
    success = run_service.cancel_run(run_id)
    
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Cannot cancel run (not running or not found)"
        )
    
    return {"message": "Run cancelled", "run_id": run_id}
