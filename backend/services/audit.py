"""
CUIN v2 - Audit Chain Module

Append-only, tamper-evident audit logging with SHA-256 hash chain.
"""

import hashlib
import json
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4


class AuditEventType(str, Enum):
    """Types of audit events."""
    # Run lifecycle
    RUN_STARTED = "RUN_STARTED"
    RUN_COMPLETED = "RUN_COMPLETED"
    RUN_FAILED = "RUN_FAILED"
    
    # Decisions
    DECISION_AUTO_LINK = "DECISION_AUTO_LINK"
    DECISION_REVIEW = "DECISION_REVIEW"
    DECISION_REJECT = "DECISION_REJECT"
    
    # Review workflow
    REVIEW_APPROVED = "REVIEW_APPROVED"
    REVIEW_REJECTED = "REVIEW_REJECTED"
    
    # Cluster operations
    CLUSTER_CREATED = "CLUSTER_CREATED"
    CLUSTER_MERGED = "CLUSTER_MERGED"
    
    # Policy changes
    POLICY_CREATED = "POLICY_CREATED"
    POLICY_APPROVED = "POLICY_APPROVED"
    
    # AI operations
    REFEREE_EXPLANATION = "REFEREE_EXPLANATION"
    PLANNER_DECISION = "PLANNER_DECISION"
    
    # System events
    SYSTEM_GENESIS = "SYSTEM_GENESIS"


@dataclass
class AuditEvent:
    """An immutable audit event."""
    audit_id: str
    event_type: AuditEventType
    payload: Dict[str, Any]
    actor: str
    prev_hash: str
    this_hash: str
    created_at: datetime
    run_id: Optional[str] = None


class AuditChain:
    """
    Tamper-evident audit chain implementation.
    
    Each event includes a hash of the previous event,
    creating an immutable chain that can be verified.
    """
    
    GENESIS_HASH = "0" * 64
    
    def __init__(self):
        self._events: List[AuditEvent] = []
        self._last_hash = self.GENESIS_HASH
    
    def _compute_hash(
        self,
        prev_hash: str,
        event_type: str,
        payload: Dict[str, Any],
        created_at: datetime
    ) -> str:
        """Compute SHA-256 hash for an event."""
        data = f"{prev_hash}|{event_type}|{json.dumps(payload, sort_keys=True)}|{created_at.isoformat()}"
        return hashlib.sha256(data.encode('utf-8')).hexdigest()
    
    def append(
        self,
        event_type: AuditEventType,
        payload: Dict[str, Any],
        actor: str = "SYSTEM",
        run_id: Optional[str] = None
    ) -> AuditEvent:
        """
        Append a new event to the audit chain.
        
        Args:
            event_type: Type of audit event
            payload: Event data (must be JSON-serializable)
            actor: User or system that triggered the event
            run_id: Optional associated run ID
            
        Returns:
            The created AuditEvent
        """
        created_at = datetime.utcnow()
        this_hash = self._compute_hash(
            self._last_hash,
            event_type.value,
            payload,
            created_at
        )
        
        event = AuditEvent(
            audit_id=str(uuid4()),
            event_type=event_type,
            payload=payload,
            actor=actor,
            prev_hash=self._last_hash,
            this_hash=this_hash,
            created_at=created_at,
            run_id=run_id
        )
        
        self._events.append(event)
        self._last_hash = this_hash
        
        return event
    
    def verify(self) -> Tuple[bool, Optional[str]]:
        """
        Verify the integrity of the entire audit chain.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self._events:
            return True, None
        
        prev_hash = self.GENESIS_HASH
        
        for i, event in enumerate(self._events):
            # Verify prev_hash matches
            if event.prev_hash != prev_hash:
                return False, f"Event {i}: prev_hash mismatch"
            
            # Recompute hash
            expected_hash = self._compute_hash(
                event.prev_hash,
                event.event_type.value,
                event.payload,
                event.created_at
            )
            
            if event.this_hash != expected_hash:
                return False, f"Event {i}: hash mismatch (tampering detected)"
            
            prev_hash = event.this_hash
        
        return True, None
    
    def get_events(
        self,
        event_type: Optional[AuditEventType] = None,
        run_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[AuditEvent]:
        """Get events with optional filtering."""
        events = self._events
        
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        if run_id:
            events = [e for e in events if e.run_id == run_id]
        
        return events[offset:offset + limit]
    
    def get_events_for_pair(self, pair_id: str) -> List[AuditEvent]:
        """Get all events related to a specific pair."""
        return [
            e for e in self._events
            if e.payload.get('pair_id') == pair_id
        ]
    
    def to_compliance_report(self) -> Dict[str, Any]:
        """Generate a compliance report."""
        is_valid, error = self.verify()
        
        event_counts = {}
        for event in self._events:
            event_type = event.event_type.value
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
        
        return {
            'chain_valid': is_valid,
            'validation_error': error,
            'total_events': len(self._events),
            'event_counts': event_counts,
            'first_event': self._events[0].created_at.isoformat() if self._events else None,
            'last_event': self._events[-1].created_at.isoformat() if self._events else None,
            'last_hash': self._last_hash,
            'verified_at': datetime.utcnow().isoformat(),
        }


# Singleton instance
_audit_chain = AuditChain()


def get_audit_chain() -> AuditChain:
    """Get the global audit chain instance."""
    return _audit_chain


def log_audit_event(
    event_type: AuditEventType,
    payload: Dict[str, Any],
    actor: str = "SYSTEM",
    run_id: Optional[str] = None
) -> AuditEvent:
    """Convenience function to log an audit event."""
    return _audit_chain.append(event_type, payload, actor, run_id)
