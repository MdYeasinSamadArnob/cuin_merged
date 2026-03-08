"""
CUIN v2 Control Plane - WebSocket Event Manager

Handles real-time event broadcasting to connected clients.
"""

import asyncio
import json
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """Types of WebSocket events."""
    # Connection events
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    
    # Pipeline stage events
    STAGE_STARTED = "STAGE_STARTED"
    STAGE_PROGRESS = "STAGE_PROGRESS"
    STAGE_COMPLETE = "STAGE_COMPLETE"
    STAGE_ERROR = "STAGE_ERROR"
    
    # Run events
    RUN_STARTED = "RUN_STARTED"
    RUN_PROGRESS = "RUN_PROGRESS"
    RUN_COMPLETE = "RUN_COMPLETE"
    RUN_FAILED = "RUN_FAILED"
    
    # Review events
    REVIEW_ITEM_CREATED = "REVIEW_ITEM_CREATED"
    REVIEW_DECISION_MADE = "REVIEW_DECISION_MADE"
    
    # Cluster events
    CLUSTER_CREATED = "CLUSTER_CREATED"
    CLUSTER_MERGED = "CLUSTER_MERGED"
    
    # System events
    SYSTEM_STATUS = "SYSTEM_STATUS"


@dataclass
class WSEvent:
    """A WebSocket event."""
    type: EventType
    payload: Dict[str, Any]
    timestamp: datetime = None
    run_id: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
    
    def to_json(self) -> str:
        """Convert event to JSON string."""
        return json.dumps({
            "type": self.type.value,
            "data": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "run_id": self.run_id,
        })


class ConnectionManager:
    """
    Manages WebSocket connections and event broadcasting.
    
    Features:
    - Track active connections
    - Subscribe to specific run updates
    - Broadcast events to all or filtered connections
    """
    
    def __init__(self):
        self._connections: Set[WebSocket] = set()
        self._run_subscriptions: Dict[str, Set[WebSocket]] = {}
        self._connection_metadata: Dict[WebSocket, Dict[str, Any]] = {}
    
    async def connect(self, websocket: WebSocket) -> None:
        """Accept a new WebSocket connection."""
        await websocket.accept()
        self._connections.add(websocket)
        self._connection_metadata[websocket] = {
            "connected_at": datetime.utcnow(),
            "subscriptions": set(),
        }
        
        # Send welcome message
        await self._send(websocket, WSEvent(
            type=EventType.CONNECTED,
            payload={"message": "Connected to CUIN v2 Control Plane"}
        ))
        
        logger.info(f"WebSocket client connected. Total: {len(self._connections)}")
    
    def disconnect(self, websocket: WebSocket) -> None:
        """Handle WebSocket disconnection."""
        self._connections.discard(websocket)
        
        # Remove from all subscriptions
        for run_id, subs in list(self._run_subscriptions.items()):
            subs.discard(websocket)
            if not subs:
                del self._run_subscriptions[run_id]
        
        if websocket in self._connection_metadata:
            del self._connection_metadata[websocket]
        
        logger.info(f"WebSocket client disconnected. Total: {len(self._connections)}")
    
    def subscribe_to_run(self, websocket: WebSocket, run_id: str) -> None:
        """Subscribe a connection to run-specific updates."""
        if run_id not in self._run_subscriptions:
            self._run_subscriptions[run_id] = set()
        
        self._run_subscriptions[run_id].add(websocket)
        
        if websocket in self._connection_metadata:
            self._connection_metadata[websocket]["subscriptions"].add(run_id)
    
    def unsubscribe_from_run(self, websocket: WebSocket, run_id: str) -> None:
        """Unsubscribe a connection from run updates."""
        if run_id in self._run_subscriptions:
            self._run_subscriptions[run_id].discard(websocket)
        
        if websocket in self._connection_metadata:
            self._connection_metadata[websocket]["subscriptions"].discard(run_id)
    
    async def _send(self, websocket: WebSocket, event: WSEvent) -> bool:
        """Send an event to a specific connection."""
        try:
            await websocket.send_text(event.to_json())
            return True
        except Exception as e:
            logger.warning(f"Failed to send to WebSocket: {e}")
            self.disconnect(websocket)
            return False
    
    async def broadcast(
        self,
        event_type: EventType,
        payload: Dict[str, Any],
        run_id: Optional[str] = None
    ) -> int:
        """
        Broadcast an event to connected clients.
        
        Args:
            event_type: Type of event
            payload: Event data
            run_id: If provided, only send to clients subscribed to this run
            
        Returns:
            Number of clients that received the event
        """
        event = WSEvent(
            type=event_type,
            payload=payload,
            run_id=run_id
        )
        
        # Determine target connections
        if run_id and run_id in self._run_subscriptions:
            targets = self._run_subscriptions[run_id]
        else:
            targets = self._connections
        
        # Send to all targets
        sent_count = 0
        for websocket in list(targets):
            if await self._send(websocket, event):
                sent_count += 1
        
        return sent_count
    
    async def broadcast_stage_progress(
        self,
        run_id: str,
        stage: str,
        status: str,
        message: str,
        records_in: int = 0,
        records_out: int = 0,
        reduction_pct: float = 0.0,
        duration_ms: int = 0,
        data: Optional[Dict[str, Any]] = None
    ) -> int:
        """Broadcast pipeline stage progress."""
        return await self.broadcast(
            EventType.STAGE_PROGRESS,
            {
                "run_id": run_id,   # included in payload so frontend guard matches
                "stage": stage,
                "status": status,
                "message": message,
                "records_in": records_in,
                "records_out": records_out,
                "reduction_pct": reduction_pct,
                "duration_ms": duration_ms,
                "data": data,
            },
            run_id=run_id
        )
    
    async def broadcast_run_complete(
        self,
        run_id: str,
        success: bool,
        counters: Dict[str, int]
    ) -> int:
        """Broadcast run completion."""
        event_type = EventType.RUN_COMPLETE if success else EventType.RUN_FAILED
        
        return await self.broadcast(
            event_type,
            {
                "run_id": run_id,
                "success": success,
                "counters": counters,
            },
            run_id=run_id
        )
    
    def get_connection_count(self) -> int:
        """Get number of active connections."""
        return len(self._connections)
    
    def get_stats(self) -> dict:
        """Get connection statistics."""
        return {
            "active_connections": len(self._connections),
            "run_subscriptions": {
                run_id: len(subs)
                for run_id, subs in self._run_subscriptions.items()
            },
        }


# Singleton instance
ws_manager = ConnectionManager()


def get_ws_manager() -> ConnectionManager:
    """Get the global WebSocket manager instance."""
    return ws_manager
