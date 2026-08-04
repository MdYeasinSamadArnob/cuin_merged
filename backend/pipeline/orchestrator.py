"""
CUIN v2 - Shared pipeline types

PipelineStage, StageProgress, and PipelineResult are the shared
progress/result contract used by pipeline/doris_orchestrator.py (the
only live pipeline engine). The in-memory PipelineOrchestrator class
that used to live here powered the legacy Excel/CSV upload pipeline
and has been removed along with that feature.
"""

from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from enum import Enum


class PipelineStage(str, Enum):
    """Pipeline stage identifiers."""
    INGEST = "ingest"
    NORMALIZE = "normalize"
    BLOCK = "block"
    CANDIDATES = "candidates"
    SCORE = "score"
    DECIDE = "decide"
    EXPLAIN = "explain"
    CLUSTER = "cluster"
    PERSIST = "persist"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class StageProgress:
    """Progress update for a pipeline stage."""
    stage: PipelineStage
    status: str  # pending, running, complete, error
    records_in: int = 0
    records_out: int = 0
    reduction_pct: float = 0.0
    duration_ms: int = 0
    message: str = ""
    data: Optional[Dict[str, Any]] = None


@dataclass
class PipelineResult:
    """Final result of pipeline execution."""
    run_id: str
    success: bool
    mode: str
    stages: List[StageProgress]

    # Counters
    records_in: int = 0
    records_normalized: int = 0
    blocks_created: int = 0
    candidates_generated: int = 0
    pairs_scored: int = 0
    auto_links: int = 0
    review_items: int = 0
    rejected: int = 0

    # Timing
    started_at: datetime = field(default_factory=datetime.utcnow)
    ended_at: Optional[datetime] = None

    # Error info
    error_message: Optional[str] = None
