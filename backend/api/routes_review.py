"""
CUIN v2 Control Plane - Review Queue API Routes

Endpoints for human review workflow.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.review_service import get_review_service, ReviewStatus
from agents.referee_agent import get_referee

router = APIRouter()


# ============================================
# Request/Response Models
# ============================================

class ReviewDecisionRequest(BaseModel):
    """Request for approve/reject action."""
    reviewer: str = Field(..., min_length=2)
    reason: str = Field(..., min_length=5)


class ReviewItemResponse(BaseModel):
    """Response for a review item."""
    review_id: str
    pair_id: str
    run_id: str
    a_key: str
    b_key: str
    score: float
    evidence: list
    signals: list
    status: str
    reviewer: Optional[str]
    review_reason: Optional[str]
    reviewed_at: Optional[str]
    created_at: str
    has_ai_explanation: bool


# ============================================
# Routes
# ============================================

@router.get("/queue")
async def get_review_queue(
    status: Optional[str] = None,
    run_id: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    sort_by: str = "score",
    sort_desc: bool = True
) -> dict:
    """
    Get the review queue with optional filtering.
    """
    review_service = get_review_service()
    
    # Convert status string to enum if provided
    status_enum = None
    if status:
        try:
            status_enum = ReviewStatus(status.upper())
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
    
    items, total = review_service.get_queue(
        status=status_enum,
        run_id=run_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_desc=sort_desc
    )
    
    return {
        "items": [item.to_dict() for item in items],
        "total": total,
        "page": page,
        "page_size": page_size
    }


@router.get("/stats")
async def get_review_stats() -> dict:
    """
    Get review queue statistics.
    """
    review_service = get_review_service()
    return review_service.get_stats()


@router.get("/{pair_id}")
async def get_review_item(pair_id: str) -> dict:
    """
    Get a specific review item by pair ID.
    """
    review_service = get_review_service()
    item = review_service.get_by_pair(pair_id)
    
    if not item:
        raise HTTPException(status_code=404, detail="Review item not found")
    
    return item.to_dict()


@router.post("/{pair_id}/approve")
async def approve_review(
    pair_id: str,
    request: ReviewDecisionRequest
) -> dict:
    """
    Approve a review item (confirm match).
    
    Requires a reviewer name and reason.
    """
    review_service = get_review_service()
    item = review_service.get_by_pair(pair_id)
    
    if not item:
        raise HTTPException(status_code=404, detail="Review item not found")
    
    try:
        updated = review_service.approve(
            review_id=item.review_id,
            reviewer=request.reviewer,
            reason=request.reason
        )
        return updated.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{pair_id}/reject")
async def reject_review(
    pair_id: str,
    request: ReviewDecisionRequest
) -> dict:
    """
    Reject a review item (not a match).
    
    Requires a reviewer name and reason.
    """
    review_service = get_review_service()
    item = review_service.get_by_pair(pair_id)
    
    if not item:
        raise HTTPException(status_code=404, detail="Review item not found")
    
    try:
        updated = review_service.reject(
            review_id=item.review_id,
            reviewer=request.reviewer,
            reason=request.reason
        )
        return updated.to_dict()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{pair_id}/explanation")
async def get_explanation(pair_id: str) -> dict:
    """
    Get AI-generated explanation for a review item.
    
    Only available for gray-zone cases where the referee was invoked.
    """
    referee = get_referee()
    explanation = referee.get_explanation(pair_id)
    
    if not explanation:
        # Check if item exists
        review_service = get_review_service()
        item = review_service.get_by_pair(pair_id)
        
        if not item:
            raise HTTPException(status_code=404, detail="Review item not found")
        
        if not item.has_ai_explanation:
            return {
                "available": False,
                "reason": "Score not in gray zone - AI explanation not generated"
            }
        
        return {
            "available": False,
            "reason": "Explanation not yet generated"
        }
    
    return {
        "available": True,
        "explanation_id": explanation.explanation_id,
        "pair_id": explanation.pair_id,
        "explanation_text": explanation.explanation_text,
        "evidence_summary": explanation.evidence_summary,
        "model_name": explanation.model_name,
        "model_version": explanation.model_version,
        "created_at": explanation.created_at.isoformat()
    }
