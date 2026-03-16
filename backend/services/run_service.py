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
    PipelineOrchestrator,
    PipelineResult,
    StageProgress,
    BlockingConfig,
    ScoringConfig,
    MatchDecision,
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
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            'run_id': self.run_id,
            'mode': self.mode.value,
            'policy_version': self.policy_version,
            'status': self.status.value,
            'description': self.description,
            'counters': asdict(self.counters),
            'started_at': self.started_at.isoformat(),
            'ended_at': self.ended_at.isoformat() if self.ended_at else None,
            'duration_seconds': self.duration_seconds,
            'error_message': self.error_message,
            'current_stage': self.current_stage,
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
        self._orchestrators: Dict[str, PipelineOrchestrator] = {}
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
        policy_version: int = 1
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
    
    async def execute_run(
        self,
        run_id: str,
        records: List[dict]
    ) -> PipelineResult:
        """
        Execute a pipeline run with the given records.
        
        Args:
            run_id: The run ID to execute
            records: Raw customer records to process
            
        Returns:
            PipelineResult with execution details
        """
        run = self._runs.get(run_id)
        if not run:
            raise ValueError(f"Run {run_id} not found")
        
        # Update status
        run.status = RunStatus.RUNNING
        self._save_runs()
        
        # Use the standard (non-Spark) orchestrator for in-memory records (Excel/CSV uploads).
        # Spark is reserved for large Parquet datasource runs via routes_datasource.py.
        from api.routes_config import get_current_blocking_config, get_current_scoring_config

        orchestrator = PipelineOrchestrator(
            blocking_config=get_current_blocking_config(),
            scoring_config=get_current_scoring_config(),
            progress_callback=await self._create_progress_handler(run_id),
        )
        self._orchestrators[run_id] = orchestrator
        
        try:
            # Execute pipeline
            result = await orchestrator.run(
                run_id=run_id,
                raw_records=records,
                mode=run.mode.value
            )
            
            # Update run with results
            run.counters.records_in = result.records_in
            run.counters.records_normalized = result.records_normalized
            run.counters.blocks_created = result.blocks_created
            run.counters.candidates_generated = result.candidates_generated
            run.counters.pairs_scored = result.pairs_scored
            run.counters.auto_links = result.auto_links
            run.counters.review_items = result.review_items
            run.counters.rejected = result.rejected
            
            run.ended_at = datetime.utcnow()
            run.duration_seconds = (run.ended_at - run.started_at).total_seconds()
            
            if result.success:
                run.status = RunStatus.COMPLETED
                self._save_runs()
                
                # Process auto-links through clustering
                auto_links = orchestrator.get_auto_links()
                if auto_links:
                    cluster_manager = get_cluster_manager()
                    pairs = [(s.a_key, s.b_key) for s in auto_links]
                    cluster_manager.process_auto_links(pairs)
                
                # Log completion
                log_audit_event(
                    AuditEventType.RUN_COMPLETED,
                    {
                        'run_id': run_id,
                        'counters': asdict(run.counters),
                        'duration_seconds': run.duration_seconds,
                    },
                    run_id=run_id
                )
            else:
                run.status = RunStatus.FAILED
                run.error_message = result.error_message
                self._save_runs()
                
                log_audit_event(
                    AuditEventType.RUN_FAILED,
                    {
                        'run_id': run_id,
                        'error': result.error_message,
                    },
                    run_id=run_id
                )
            
            return result
            
        except Exception as e:
            run.status = RunStatus.FAILED
            run.error_message = str(e)
            run.ended_at = datetime.utcnow()
            run.duration_seconds = (run.ended_at - run.started_at).total_seconds()
            self._save_runs()
            
            log_audit_event(
                AuditEventType.RUN_FAILED,
                {
                    'run_id': run_id,
                    'error': str(e),
                },
                run_id=run_id
            )
            
            raise
    
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
    
    def get_orchestrator(self, run_id: str) -> Optional[PipelineOrchestrator]:
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
