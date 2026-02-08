"""
CUIN v2 - Review Service

Manages the human review queue for uncertain matches.
Implements maker-checker workflow with audit logging.
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
from dataclasses import dataclass
from enum import Enum

from services.audit import log_audit_event, AuditEventType
from engine.clustering import get_cluster_manager
from agents.referee_agent import get_referee


class ReviewStatus(str, Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


@dataclass
class ReviewItem:
    """A review queue item."""
    review_id: str
    pair_id: str
    run_id: str
    a_key: str
    b_key: str
    score: float
    evidence: List[dict]
    signals: List[str]
    status: ReviewStatus
    reviewer: Optional[str] = None
    review_reason: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    created_at: datetime = None
    has_ai_explanation: bool = False
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
    
    def to_dict(self) -> dict:
        return {
            'review_id': self.review_id,
            'pair_id': self.pair_id,
            'run_id': self.run_id,
            'a_key': self.a_key,
            'b_key': self.b_key,
            'score': self.score,
            'evidence': self.evidence,
            'signals': self.signals,
            'status': self.status.value,
            'reviewer': self.reviewer,
            'review_reason': self.review_reason,
            'reviewed_at': self.reviewed_at.isoformat() if self.reviewed_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'has_ai_explanation': self.has_ai_explanation,
        }


class ReviewService:
    """
    Service for managing the human review queue.
    
    Features:
    - Queue items for review
    - Approve/reject with mandatory reason
    - Request AI explanation for gray-zone cases
    - Audit all decisions
    """
    
    def __init__(self):
        self._items: Dict[str, ReviewItem] = {}
        self._by_pair: Dict[str, str] = {}  # pair_id -> review_id
    
    def queue_for_review(
        self,
        pair_id: str,
        run_id: str,
        a_key: str,
        b_key: str,
        score: float,
        evidence: List[dict],
        signals: List[str] = None
    ) -> ReviewItem:
        """Add a pair to the review queue."""
        review_id = str(uuid4())
        
        # Check if AI explanation should be generated
        referee = get_referee()
        has_explanation = False
        
        if referee.should_invoke(score, []):
            # Generate explanation for gray-zone cases
            # This would be called with full record data in real impl
            has_explanation = True
        
        item = ReviewItem(
            review_id=review_id,
            pair_id=pair_id,
            run_id=run_id,
            a_key=a_key,
            b_key=b_key,
            score=score,
            evidence=evidence,
            signals=signals or [],
            status=ReviewStatus.PENDING,
            has_ai_explanation=has_explanation,
        )
        
        self._items[review_id] = item
        self._by_pair[pair_id] = review_id
        
        # Log audit event
        log_audit_event(
            AuditEventType.DECISION_REVIEW,
            {
                'pair_id': pair_id,
                'review_id': review_id,
                'score': score,
                'signals': signals,
            },
            run_id=run_id
        )
        
        return item
    
    def get_item(self, review_id: str) -> Optional[ReviewItem]:
        """Get a review item by ID."""
        return self._items.get(review_id)
    
    def get_by_pair(self, pair_id: str) -> Optional[ReviewItem]:
        """Get a review item by pair ID."""
        review_id = self._by_pair.get(pair_id)
        if review_id:
            return self._items.get(review_id)
        return None
    
    def get_queue(
        self,
        status: Optional[ReviewStatus] = None,
        run_id: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "score",
        sort_desc: bool = True
    ) -> Tuple[List[ReviewItem], int]:
        """Get review queue with filtering and pagination."""
        items = list(self._items.values())
        
        # Filter by status
        if status:
            items = [i for i in items if i.status == status]
        
        # Filter by run
        if run_id:
            items = [i for i in items if i.run_id == run_id]
        
        # Sort
        if sort_by == "score":
            items.sort(key=lambda x: x.score, reverse=sort_desc)
        elif sort_by == "created_at":
            items.sort(key=lambda x: x.created_at, reverse=sort_desc)
        
        # Paginate
        total = len(items)
        start = (page - 1) * page_size
        end = start + page_size
        
        return items[start:end], total
    
    def approve(
        self,
        review_id: str,
        reviewer: str,
        reason: str
    ) -> ReviewItem:
        """
        Approve a review item (confirm the match).
        
        Args:
            review_id: Review item ID
            reviewer: User who approved
            reason: Mandatory reason for approval
        """
        item = self._items.get(review_id)
        if not item:
            raise ValueError(f"Review item {review_id} not found")
        
        if item.status != ReviewStatus.PENDING:
            raise ValueError(f"Review item {review_id} is not pending")
        
        if not reason or len(reason.strip()) < 5:
            raise ValueError("Reason is required (minimum 5 characters)")
        
        # Update item
        item.status = ReviewStatus.APPROVED
        item.reviewer = reviewer
        item.review_reason = reason
        item.reviewed_at = datetime.utcnow()
        
        # Add to cluster
        cluster_manager = get_cluster_manager()
        cluster_manager.link(item.a_key, item.b_key)
        
        # Log audit event
        log_audit_event(
            AuditEventType.REVIEW_APPROVED,
            {
                'review_id': review_id,
                'pair_id': item.pair_id,
                'reviewer': reviewer,
                'reason': reason,
            },
            actor=reviewer,
            run_id=item.run_id
        )
        
        return item
    
    def reject(
        self,
        review_id: str,
        reviewer: str,
        reason: str
    ) -> ReviewItem:
        """
        Reject a review item (confirm not a match).
        
        Args:
            review_id: Review item ID
            reviewer: User who rejected
            reason: Mandatory reason for rejection
        """
        item = self._items.get(review_id)
        if not item:
            raise ValueError(f"Review item {review_id} not found")
        
        if item.status != ReviewStatus.PENDING:
            raise ValueError(f"Review item {review_id} is not pending")
        
        if not reason or len(reason.strip()) < 5:
            raise ValueError("Reason is required (minimum 5 characters)")
        
        # Update item
        item.status = ReviewStatus.REJECTED
        item.reviewer = reviewer
        item.review_reason = reason
        item.reviewed_at = datetime.utcnow()
        
        # Log audit event
        log_audit_event(
            AuditEventType.REVIEW_REJECTED,
            {
                'review_id': review_id,
                'pair_id': item.pair_id,
                'reviewer': reviewer,
                'reason': reason,
            },
            actor=reviewer,
            run_id=item.run_id
        )
        
        return item
    
    def get_stats(self) -> dict:
        """Get review queue statistics."""
        items = list(self._items.values())
        
        pending = sum(1 for i in items if i.status == ReviewStatus.PENDING)
        approved = sum(1 for i in items if i.status == ReviewStatus.APPROVED)
        rejected = sum(1 for i in items if i.status == ReviewStatus.REJECTED)
        
        # Calculate average time to review
        reviewed = [i for i in items if i.reviewed_at]
        avg_time = 0
        if reviewed:
            times = [(i.reviewed_at - i.created_at).total_seconds() for i in reviewed]
            avg_time = sum(times) / len(times)
        
        return {
            'pending': pending,
            'approved': approved,
            'rejected': rejected,
            'total': len(items),
            'avg_review_time_seconds': avg_time,
            'with_ai_explanation': sum(1 for i in items if i.has_ai_explanation),
        }


# Singleton instance
_review_service = ReviewService()


def get_review_service() -> ReviewService:
    """Get the global review service instance."""
    return _review_service
