"""
CUIN v2 Control Plane - Audit API Routes

Endpoints for audit events and compliance reporting.
"""

from typing import Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from services.audit import get_audit_chain, AuditEventType

router = APIRouter()


# ============================================
# Routes
# ============================================

@router.get("/events")
async def list_audit_events(
    event_type: Optional[str] = None,
    run_id: Optional[str] = None,
    page: int = 1,
    page_size: int = 50
) -> dict:
    """
    List audit events with optional filtering.
    """
    audit_chain = get_audit_chain()
    
    # Convert event type string to enum if provided
    event_type_enum = None
    if event_type:
        try:
            event_type_enum = AuditEventType(event_type.upper())
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid event type: {event_type}")
    
    offset = (page - 1) * page_size
    events = audit_chain.get_events(
        event_type=event_type_enum,
        run_id=run_id,
        limit=page_size,
        offset=offset
    )
    
    return {
        "events": [
            {
                "audit_id": e.audit_id,
                "event_type": e.event_type.value,
                "payload": e.payload,
                "actor": e.actor,
                "run_id": e.run_id,
                "prev_hash": e.prev_hash[:16] + "...",  # Truncate for display
                "this_hash": e.this_hash[:16] + "...",
                "created_at": e.created_at.isoformat(),
            }
            for e in events
        ],
        "page": page,
        "page_size": page_size
    }


@router.get("/pair/{pair_id}")
async def get_pair_audit_trail(pair_id: str) -> dict:
    """
    Get complete audit trail for a specific pair.
    """
    audit_chain = get_audit_chain()
    events = audit_chain.get_events_for_pair(pair_id)
    
    return {
        "pair_id": pair_id,
        "events": [
            {
                "audit_id": e.audit_id,
                "event_type": e.event_type.value,
                "payload": e.payload,
                "actor": e.actor,
                "created_at": e.created_at.isoformat(),
            }
            for e in events
        ],
        "total": len(events)
    }


@router.get("/verify")
async def verify_audit_chain() -> dict:
    """
    Verify the integrity of the audit chain.
    
    Checks that all hash links are valid and no tampering
    has occurred.
    """
    audit_chain = get_audit_chain()
    is_valid, error = audit_chain.verify()
    
    return {
        "valid": is_valid,
        "error": error,
        "verified_at": datetime.utcnow().isoformat(),
        "chain_length": len(audit_chain._events)
    }


@router.get("/compliance/report")
async def get_compliance_report() -> dict:
    """
    Generate a compliance report.
    
    Includes chain verification and event statistics.
    """
    audit_chain = get_audit_chain()
    report = audit_chain.to_compliance_report()
    
    return report


@router.get("/export")
async def export_audit_events(
    run_id: Optional[str] = None,
    format: str = "json"
) -> JSONResponse:
    """
    Export audit events for external compliance systems.
    """
    audit_chain = get_audit_chain()
    
    events = audit_chain.get_events(
        run_id=run_id,
        limit=10000  # Export limit
    )
    
    export_data = {
        "exported_at": datetime.utcnow().isoformat(),
        "total_events": len(events),
        "run_filter": run_id,
        "events": [
            {
                "audit_id": e.audit_id,
                "event_type": e.event_type.value,
                "payload": e.payload,
                "actor": e.actor,
                "run_id": e.run_id,
                "prev_hash": e.prev_hash,
                "this_hash": e.this_hash,
                "created_at": e.created_at.isoformat(),
            }
            for e in events
        ]
    }
    
    return JSONResponse(
        content=export_data,
        headers={
            "Content-Disposition": f"attachment; filename=audit_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        }
    )
