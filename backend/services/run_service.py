"""
CUIN v2 - Pipeline Run Service

Central service for executing pipeline runs with WebSocket updates.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any
from uuid import uuid4
from dataclasses import dataclass, field, asdict
from enum import Enum

from pipeline import (
    StageProgress,
)
from services.audit import log_audit_event, AuditEventType
from engine.clustering import get_cluster_manager
from engine.clustering import get_cluster_manager

logger = logging.getLogger(__name__)


class RunMode(str, Enum):
    FULL = "FULL"
    DELTA = "DELTA"
    AUTO = "AUTO"


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


@dataclass
class RunCounters:
    """Counters for pipeline run progress."""
    records_in: int = 0
    records_normalized: int = 0
    blocks_created: int = 0
    candidates_generated: int = 0
    pairs_scored: int = 0
    auto_links: int = 0
    review_items: int = 0
    rejected: int = 0
    clusters_created: int = 0


@dataclass
class Run:
    """A pipeline run record."""
    run_id: str
    mode: RunMode
    policy_version: int
    status: RunStatus
    description: str
    counters: RunCounters
    started_at: datetime
    ended_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    current_stage: Optional[str] = None
    ruleset_version: Optional[str] = None
    output_fingerprint: Optional[str] = None
    engine: str = "doris"
    # Wall-clock ms spent in each completed stage, keyed by
    # PipelineStage.value (e.g. "ingest", "persist"). Populated from
    # StageProgress.duration_ms as "complete" events arrive -- makes the
    # time spent in _persist_run_artifacts/_persist_to_postgres visible
    # instead of disappearing into the gap between the last stage event
    # and COMPLETE.
    stage_timings_ms: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            'run_id': self.run_id,
            'mode': self.mode.value,
            'policy_version': self.policy_version,
            'status': self.status.value,
            'ruleset_version': self.ruleset_version,
            'output_fingerprint': self.output_fingerprint,
            'description': self.description,
            'counters': asdict(self.counters),
            'started_at': self.started_at.isoformat(),
            'ended_at': self.ended_at.isoformat() if self.ended_at else None,
            'duration_seconds': self.duration_seconds,
            'error_message': self.error_message,
            'current_stage': self.current_stage,
            'engine': self.engine,
            'stage_timings_ms': self.stage_timings_ms,
        }


class RunService:
    """
    Service for managing pipeline runs.
    
    Handles:
    - Creating and tracking runs
    - Executing pipeline with progress updates
    - Storing results and routing to review/auto-link
    """
    
    def __init__(self):
        self._runs: Dict[str, Run] = {}
        # Doris pipeline runs (api/routes_datasource.py) register their
        # DorisPipelineOrchestrator instance here so routes_matches.py,
        # routes_candidates.py, routes_graph.py etc. can look it up by
        # run_id while the process is warm. Not type-imported here to
        # avoid a services -> pipeline.doris_orchestrator dependency.
        self._orchestrators: Dict[str, Any] = {}
        self._progress_callback: Optional[Callable[[str, StageProgress], Any]] = None
        self._load_runs()

    def _save_runs(self):
        """Persist runs to disk."""
        try:
            import os
            import json
            os.makedirs('data', exist_ok=True)
            # Convert datetime objects to ISO format strings for JSON serialization
            data = {}
            for rid, r in self._runs.items():
                r_dict = r.to_dict()
                data[rid] = r_dict
            
            with open('data/runs_index.json', 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save runs: {e}")

    def _load_runs(self):
        """Load runs from disk."""
        try:
            import os
            import json
            if not os.path.exists('data/runs_index.json'):
                return
            
            with open('data/runs_index.json', 'r') as f:
                data = json.load(f)
            
            for rid, r_data in data.items():
                # Reconstruct Run objects
                # Convert string dates back to datetime
                if r_data.get('started_at'):
                    r_data['started_at'] = datetime.fromisoformat(r_data['started_at'])
                if r_data.get('ended_at'):
                    r_data['ended_at'] = datetime.fromisoformat(r_data['ended_at'])
                
                # Reconstruct counters
                if 'counters' in r_data:
                    r_data['counters'] = RunCounters(**r_data['counters'])
                
                # Convert mode/status back to Enum
                if 'mode' in r_data:
                    r_data['mode'] = RunMode(r_data['mode'])
                if 'status' in r_data:
                    r_data['status'] = RunStatus(r_data['status'])
                
                self._runs[rid] = Run(**r_data)
            
            logger.info(f"Loaded {len(self._runs)} runs from disk")
        except Exception as e:
            logger.error(f"Failed to load runs: {e}")
    
    def set_progress_callback(
        self,
        callback: Callable[[str, StageProgress], Any]
    ) -> None:
        """Set callback for progress updates (WebSocket broadcast)."""
        self._progress_callback = callback
    
    def create_run(
        self,
        mode: str = "FULL",
        description: str = "",
        policy_version: int = 1,
        engine: str = "doris",
    ) -> Run:
        """Create a new pipeline run."""
        run_id = str(uuid4())

        run = Run(
            run_id=run_id,
            mode=RunMode(mode),
            policy_version=policy_version,
            status=RunStatus.PENDING,
            description=description,
            counters=RunCounters(),
            started_at=datetime.utcnow(),
            engine=engine,
        )
        
        self._runs[run_id] = run
        self._save_runs()
        
        # Log audit event
        log_audit_event(
            AuditEventType.RUN_STARTED,
            {
                'run_id': run_id,
                'mode': mode,
                'policy_version': policy_version,
            },
            run_id=run_id
        )
        
        return run
    
    def get_run(self, run_id: str) -> Optional[Run]:
        """Get a run by ID."""
        return self._runs.get(run_id)
    
    def list_runs(
        self,
        page: int = 1,
        page_size: int = 20
    ) -> tuple:
        """List runs with pagination."""
        runs = sorted(
            self._runs.values(),
            key=lambda r: r.started_at,
            reverse=True
        )
        
        start = (page - 1) * page_size
        end = start + page_size
        
        return runs[start:end], len(runs)
    
    async def _create_progress_handler(self, run_id: str) -> Callable:
        """Create a progress handler that broadcasts updates."""
        async def handler(progress: StageProgress):
            # Update run's current stage
            run = self._runs.get(run_id)
            if run:
                run.current_stage = progress.stage.value
            
            # Broadcast via callback
            if self._progress_callback:
                try:
                    # Check if callback is async
                    if asyncio.iscoroutinefunction(self._progress_callback):
                        await self._progress_callback(run_id, progress)
                    else:
                        self._progress_callback(run_id, progress)
                except Exception as e:
                    logger.error(f"Progress callback error: {e}")
        
        return handler
    
    def cancel_run(self, run_id: str) -> bool:
        """Cancel a running pipeline."""
        run = self._runs.get(run_id)
        if not run:
            return False
        
        if run.status == RunStatus.RUNNING:
            run.status = RunStatus.CANCELLED
            run.ended_at = datetime.utcnow()
            self._save_runs()
            return True
        
        return False
    
    def delete_run(self, run_id: str) -> bool:
        """
        Remove a run from the registry (self._runs / data/runs_index.json)
        and evict its live orchestrator if the process is warm. Only the
        registry piece -- callers (api/routes_runs.py's DELETE endpoint)
        are responsible for the run's Doris database, Postgres footprint,
        and file artifacts, which live outside this service.
        """
        run = self._runs.get(run_id)
        if not run:
            return False

        if run.status == RunStatus.RUNNING:
            return False

        self._runs.pop(run_id, None)
        self._orchestrators.pop(run_id, None)
        self._save_runs()
        return True

    def get_orchestrator(self, run_id: str) -> Optional[Any]:
        """Get the orchestrator for a run."""
        return self._orchestrators.get(run_id)
    
    def get_dashboard_metrics(self) -> dict:
        """Get dashboard KPIs."""
        completed_runs = [
            r for r in self._runs.values()
            if r.status == RunStatus.COMPLETED
        ]
        
        total_records = sum(r.counters.records_in for r in completed_runs)
        total_auto_links = sum(r.counters.auto_links for r in completed_runs)
        total_review = sum(r.counters.review_items for r in completed_runs)
        total_duplicates = total_auto_links + total_review
        
        cluster_manager = get_cluster_manager()
        cluster_stats = cluster_manager.get_stats()
        
        avg_duration = 0
        if completed_runs:
            durations = [r.duration_seconds for r in completed_runs if r.duration_seconds]
            avg_duration = sum(durations) / len(durations) if durations else 0
        
        last_run = max(
            (r.started_at for r in self._runs.values()),
            default=None
        )
        
        return {
            'total_records': total_records,
            'total_clusters': cluster_stats['total_clusters'],
            'duplicates_detected': total_duplicates,
            'duplicate_rate_pct': (total_duplicates / total_records * 100) if total_records else 0,
            'review_backlog': total_review,
            'auto_link_rate_pct': (total_auto_links / total_duplicates * 100) if total_duplicates else 0,
            'avg_run_duration_seconds': avg_duration,
            'last_run_at': last_run.isoformat() if last_run else None,
        }


# Singleton instance
_run_service = RunService()


def get_run_service() -> RunService:
    """Get the global run service instance."""
    return _run_service
