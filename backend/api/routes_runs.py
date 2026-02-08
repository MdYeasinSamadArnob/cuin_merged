"""
CUIN v2 Control Plane - Run Management API Routes

Endpoints for creating and managing ER pipeline runs.
"""

import asyncio
from datetime import datetime
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from services.run_service import get_run_service, RunStatus, RunMode
from api.ws_events import ws_manager, EventType

router = APIRouter()


# ============================================
# Request/Response Models
# ============================================

class CreateRunRequest(BaseModel):
    """Request to create a new pipeline run."""
    mode: str = Field(default="FULL", pattern="^(FULL|DELTA|AUTO)$")
    description: Optional[str] = Field(default="")
    policy_version: int = Field(default=1, ge=1)


class RunResponse(BaseModel):
    """Response for a single run."""
    run_id: str
    mode: str
    policy_version: int
    status: str
    description: str
    counters: dict
    started_at: str
    ended_at: Optional[str]
    duration_seconds: Optional[float]
    error_message: Optional[str]


class RunListResponse(BaseModel):
    """Response for listing runs."""
    runs: list
    total: int
    page: int
    page_size: int


# ============================================
# Demo Data for Testing
# ============================================

DEMO_RECORDS = [
    {
        "customer_id": "CUST001",
        "first_name": "John",
        "last_name": "Smith",
        "phone": "555-123-4567",
        "email": "john.smith@email.com",
        "dob": "1985-03-15",
        "address": "123 Main Street, Apt 4B",
        "city": "New York",
        "source_system": "CORE_BANKING"
    },
    {
        "customer_id": "CUST002",
        "first_name": "John",
        "last_name": "Smyth",  # Typo
        "phone": "5551234567",  # Same phone, different format
        "email": "jsmith@email.com",
        "dob": "1985-03-15",
        "address": "123 Main St, Apt 4B",  # Abbreviated
        "city": "New York",
        "source_system": "LOANS"
    },
    {
        "customer_id": "CUST003",
        "first_name": "Jane",
        "last_name": "Doe",
        "phone": "555-987-6543",
        "email": "jane.doe@gmail.com",
        "dob": "1990-07-22",
        "address": "456 Oak Avenue",
        "city": "Los Angeles",
        "source_system": "CORE_BANKING"
    },
    {
        "customer_id": "CUST004",
        "first_name": "Jane",
        "last_name": "Doe",
        "phone": "555-987-6543",
        "email": "jane.doe@gmail.com",
        "dob": "1990-07-22",
        "address": "456 Oak Ave",
        "city": "Los Angeles",
        "source_system": "CARDS"
    },
    {
        "customer_id": "CUST005",
        "first_name": "Robert",
        "last_name": "Johnson",
        "phone": "555-456-7890",
        "email": "robert.j@company.com",
        "dob": "1978-11-30",
        "address": "789 Pine Road",
        "city": "Chicago",
        "source_system": "CORE_BANKING"
    },
    {
        "customer_id": "CUST006",
        "first_name": "Bob",
        "last_name": "Johnson",
        "phone": "555-456-7890",
        "email": "bob.johnson@company.com",
        "dob": "1978-11-30",
        "address": "789 Pine Rd",
        "city": "Chicago",
        "source_system": "INVESTMENTS"
    },
    {
        "customer_id": "CUST007",
        "first_name": "Maria",
        "last_name": "Garcia",
        "phone": "555-111-2222",
        "email": "maria.garcia@email.com",
        "dob": "1995-02-14",
        "address": "321 Elm Street",
        "city": "Miami",
        "source_system": "CORE_BANKING"
    },
    {
        "customer_id": "CUST008",
        "first_name": "David",
        "last_name": "Williams",
        "phone": "555-333-4444",
        "email": "david.w@email.com",
        "dob": "1982-09-08",
        "address": "654 Maple Drive",
        "city": "Seattle",
        "source_system": "CORE_BANKING"
    },
    {
        "customer_id": "CUST009",
        "first_name": "D.",
        "last_name": "Williams",
        "phone": "5553334444",
        "email": "dwilliams@email.com",
        "dob": "1982-09-08",
        "address": "654 Maple Dr",
        "city": "Seattle",
        "source_system": "CARDS"
    },
    {
        "customer_id": "CUST010",
        "first_name": "Sarah",
        "last_name": "Brown",
        "phone": "555-555-5555",
        "email": "sarah.brown@email.com",
        "dob": "1988-12-25",
        "address": "987 Cedar Lane",
        "city": "Boston",
        "source_system": "CORE_BANKING"
    },
]


# ============================================
# Routes
# ============================================

@router.post("", response_model=RunResponse)
async def create_run(
    request: CreateRunRequest,
    background_tasks: BackgroundTasks
) -> dict:
    """
    Create a new pipeline run.
    
    The run will start in PENDING status and can be executed
    by triggering the pipeline.
    """
    run_service = get_run_service()
    
    run = run_service.create_run(
        mode=request.mode,
        description=request.description,
        policy_version=request.policy_version
    )
    
    # Execute pipeline in background with demo data
    async def execute_pipeline():
        try:
            await run_service.execute_run(run.run_id, DEMO_RECORDS)
            
            # Broadcast completion
            updated_run = run_service.get_run(run.run_id)
            if updated_run:
                await ws_manager.broadcast(EventType.RUN_COMPLETE, {
                    'run_id': updated_run.run_id,
                    'status': updated_run.status.value,
                    'counters': {
                        'records_in': updated_run.counters.records_in,
                        'auto_links': updated_run.counters.auto_links,
                        'review_items': updated_run.counters.review_items,
                    }
                })
        except Exception as e:
            print(f"Pipeline error: {e}")
    
    background_tasks.add_task(execute_pipeline)
    
    return run.to_dict()


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


@router.post("/{run_id}/retry")
async def retry_run(
    run_id: str,
    background_tasks: BackgroundTasks
) -> dict:
    """
    Retry a failed run.
    """
    run_service = get_run_service()
    run = run_service.get_run(run_id)
    
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    if run.status not in [RunStatus.FAILED, RunStatus.CANCELLED]:
        raise HTTPException(
            status_code=400,
            detail="Can only retry failed or cancelled runs"
        )
    
    # Create a new run as retry
    new_run = run_service.create_run(
        mode=run.mode.value,
        description=f"Retry of {run_id}: {run.description}",
        policy_version=run.policy_version
    )
    
    # Execute in background
    async def execute_retry():
        await run_service.execute_run(new_run.run_id, DEMO_RECORDS)
    
    background_tasks.add_task(execute_retry)
    
    return new_run.to_dict()
