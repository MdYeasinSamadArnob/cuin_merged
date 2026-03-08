"""
CUIN v2 - Pipeline Orchestrator

Main pipeline orchestration that coordinates all stages:
1. Load data from staging
2. Normalize records
3. Multi-pass blocking
4. Generate candidate pairs
5. Score pairs with Splink
6. Make decisions
7. Route to auto-link or review
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4
import json
import os
from engine.normalize.standardize import normalize_record
from engine.blocking.multipass_blocker import MultiPassBlocker, BlockingConfig
from engine.blocking.candidate_builder import CandidateBuilder, CandidatePair
from engine.matching.splink_engine import SplinkScorer
from engine.decisioning.decision_engine import DecisionEngine
from engine.structures import ScoringConfig, MatchDecision, MatchScore
from engine.clustering import get_cluster_manager
from engine.graph.neo4j_writer import get_neo4j_writer
from engine.read_staging import read_staging_data
from agents.referee_agent import get_referee
from agents.planner_agent import get_planner_agent
from services.review_service import get_review_service

logger = logging.getLogger(__name__)


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


@dataclass
class PipelineResult:
    """Final result of pipeline execution."""
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


class PipelineOrchestrator:
    """
    Orchestrates the complete ER pipeline.
    
    Supports both FULL and DELTA modes.
    Emits progress events for real-time UI updates.
    """
    
    def __init__(
        self,
        blocking_config: Optional[BlockingConfig] = None,
        scoring_config: Optional[ScoringConfig] = None,
        progress_callback: Optional[Callable[[StageProgress], None]] = None,
        run_id: Optional[str] = None
    ):
        self.blocker = MultiPassBlocker(blocking_config)
        self.candidate_builder = CandidateBuilder(self.blocker)
        self.scorer = SplinkScorer(scoring_config)
        self.decision_engine = DecisionEngine(scoring_config)
        self.progress_callback = progress_callback
        self.run_id = run_id
        
        # In-memory storage for demo (replace with DB in production)
        self._records: Dict[str, dict] = {}
        self._candidates: List[CandidatePair] = []
        self._scores: Dict[str, MatchScore] = {}
        self._decisions: Dict[str, MatchDecision] = {}
    
    async def _emit_progress(self, progress: StageProgress) -> None:
        """Emit progress update."""
        if self.progress_callback:
            try:
                self.progress_callback(progress)
            except Exception as e:
                logger.error(f"Progress callback error: {e}")
    
    async def _stage_ingest(
        self,
        raw_records: List[dict]
    ) -> List[dict]:
        """
        Stage 1: Ingest raw records.
        """
        start = datetime.utcnow()
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.INGEST,
            status="running",
            message="Loading records..."
        ))
        
        records = list(raw_records)
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.INGEST,
            status="complete",
            records_in=len(records),
            records_out=len(records),
            reduction_pct=0.0,
            duration_ms=duration,
            message=f"Loaded {len(records)} records"
        ))
        
        return records
    
    async def _stage_normalize(
        self,
        records: List[dict]
    ) -> List[dict]:
        """
        Stage 2: Normalize all records.
        """
        start = datetime.utcnow()
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.NORMALIZE,
            status="running",
            message="Normalizing records..."
        ))
        
        normalized = []
        for record in records:
            try:
                norm = normalize_record(record)
                # Add unique key if not present
                if not norm.get('customer_key'):
                    norm['customer_key'] = str(uuid4())
                normalized.append(norm)
                self._records[norm['customer_key']] = norm
            except Exception as e:
                logger.warning(f"Failed to normalize record: {e}")
        
        # Persist records to disk for reliability
        if self.run_id:
            try:
                from api.config import settings
                runs_dir = f'{settings.DATA_DIR}/runs'
                os.makedirs(runs_dir, exist_ok=True)
                with open(f'{runs_dir}/{self.run_id}_records.json', 'w') as f:
                    json.dump(self._records, f, default=str)
                logger.info(f"Persisted {len(self._records)} records for run {self.run_id}")
            except Exception as e:
                logger.error(f"Failed to persist records: {e}")

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.NORMALIZE,
            status="complete",
            records_in=len(records),
            records_out=len(normalized),
            reduction_pct=((len(records) - len(normalized)) / len(records) * 100) if records else 0,
            duration_ms=duration,
            message=f"Normalized {len(normalized)} records"
        ))
        
        return normalized
    
    async def _stage_block(
        self,
        records: List[dict]
    ) -> Dict[str, List[str]]:
        """
        Stage 3: Multi-pass blocking.
        """
        start = datetime.utcnow()
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.BLOCK,
            status="running",
            message="Building blocking keys..."
        ))
        
        blocks = self.blocker.build_blocks(records)
        stats = self.blocker.get_stats()
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.BLOCK,
            status="complete",
            records_in=len(records),
            records_out=stats['total_keys'],
            reduction_pct=0,
            duration_ms=duration,
            message=f"Created {stats['total_keys']} blocking keys"
        ))
        
        return blocks
    
    async def _stage_candidates(
        self,
        records: List[dict],
        max_pairs: int = 100000
    ) -> List[CandidatePair]:
        """
        Stage 4: Generate candidate pairs.
        """
        start = datetime.utcnow()
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CANDIDATES,
            status="running",
            message="Generating candidate pairs..."
        ))
        
        
        candidates = self.candidate_builder.generate_candidate_pairs(self.blocker.build_blocks(records), max_pairs)
        self._candidates = candidates
        
        # Calculate theoretical max pairs
        n = len(records)
        max_possible = n * (n - 1) // 2
        reduction = ((max_possible - len(candidates)) / max_possible * 100) if max_possible > 0 else 0
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CANDIDATES,
            status="complete",
            records_in=max_possible,
            records_out=len(candidates),
            reduction_pct=reduction,
            duration_ms=duration,
            message=f"Generated {len(candidates)} candidate pairs ({reduction:.1f}% reduction)"
        ))
        
        return candidates
    
    async def _stage_score(
        self,
        candidates: List[CandidatePair]
    ) -> List[MatchScore]:
        """
        Stage 5: Score all candidate pairs.
        """
        start = datetime.utcnow()
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.SCORE,
            status="running",
            message="Scoring candidate pairs..."
        ))
        
        scores = []
        for i, candidate in enumerate(candidates):
            record_a = self._records.get(candidate.a_key)
            record_b = self._records.get(candidate.b_key)
            
            if not record_a or not record_b:
                continue
            
            pair_id = str(uuid4())
            score = self.scorer.score_pair(pair_id, record_a, record_b)
            scores.append(score)
            self._scores[pair_id] = score
            
            # Emit progress every 100 pairs
            if (i + 1) % 100 == 0:
                await self._emit_progress(StageProgress(
                    stage=PipelineStage.SCORE,
                    status="running",
                    records_in=len(candidates),
                    records_out=len(scores),
                    message=f"Scored {len(scores)}/{len(candidates)} pairs"
                ))
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.SCORE,
            status="complete",
            records_in=len(candidates),
            records_out=len(scores),
            reduction_pct=0,
            duration_ms=duration,
            message=f"Scored {len(scores)} pairs"
        ))
        
        return scores
    
    async def _stage_decide(
        self,
        scores: List[MatchScore],
        run_id: str
    ) -> Dict[str, Dict[str, int]]:
        """
        Stage 6: Make decisions for all scored pairs.
        """
        start = datetime.utcnow()
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.DECIDE,
            status="running",
            message="Making decisions..."
        ))
        
        review_service = get_review_service()
        
        counters = {
            'auto_links': 0,
            'review_items': 0,
            'rejected': 0
        }
        
        for score in scores:
            decision = self.decision_engine.make_decision(score)
            self._decisions[score.pair_id] = decision
            
            if decision == MatchDecision.AUTO_LINK:
                counters['auto_links'] += 1
            elif decision == MatchDecision.REVIEW:
                counters['review_items'] += 1
                
                # Convert evidence to dicts
                evidence_dicts = [
                    {
                        'field': ev.field_name,
                        'value_a': ev.value_a,
                        'value_b': ev.value_b,
                        'type': ev.comparison_type,
                        'similarity': ev.similarity_score,
                        'explanation': ev.explanation
                    }
                    for ev in score.evidence
                ]
                
                # Add to review queue
                review_service.queue_for_review(
                    pair_id=score.pair_id,
                    run_id=run_id,
                    a_key=score.a_key,
                    b_key=score.b_key,
                    score=score.score,
                    evidence=evidence_dicts,
                    signals=score.signals_hit
                )
            else:
                counters['rejected'] += 1
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.DECIDE,
            status="complete",
            records_in=len(scores),
            records_out=counters['auto_links'] + counters['review_items'],
            reduction_pct=0,
            duration_ms=duration,
            message=f"Auto: {counters['auto_links']}, Review: {counters['review_items']}, Reject: {counters['rejected']}"
        ))
        
        return counters
    
    async def _stage_explain(
        self,
        scores: List[MatchScore],
        decisions: Dict[str, MatchDecision],
        records: List[dict],
        run_id: str
    ) -> int:
        """
        Stage 7: Explain gray-zone decisions with Referee Agent.
        """
        start = datetime.utcnow()
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.EXPLAIN,
            status="running",
            message="Generating explanations..."
        ))
        
        referee = get_referee()
        
        # Build record lookup
        record_map = {r.get('customer_key'): r for r in records}
        
        explanation_count = 0
        

        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.EXPLAIN,
            status="complete",
            records_in=len(scores),
            records_out=explanation_count,
            duration_ms=duration,
            message=f"Generated {explanation_count} explanations"
        ))
        
        return explanation_count
    
    async def _stage_cluster(
        self,
        scores: List[MatchScore],
        decisions: Dict[str, MatchDecision],
        records: List[dict]
    ) -> int:
        """
        Stage 8: Cluster records and generate golden records.
        """
        start = datetime.utcnow()
        writer = get_neo4j_writer()
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CLUSTER,
            status="running",
            message="Clustering records..."
        ))
        
        manager = get_cluster_manager()
        
        # 1. Collect auto-link pairs
        auto_links = []
        for score in scores:
            if decisions.get(score.pair_id) == MatchDecision.AUTO_LINK:
                auto_links.append((score.a_key, score.b_key))
        
        # 2. Update clusters with auto-links
        manager.process_auto_links(auto_links)
        
        # 3. Ensure all records have a cluster and generate golden records
        # Build record lookup
        record_map = {r.get('customer_key'): r for r in records}
        
        # Find all involved clusters
        all_cluster_ids = set()
        for record in records:
            key = record.get('customer_key')
            if key:
                # Ensure record is registered in clustering service
                cluster_id = manager.register_record(key)
                all_cluster_ids.add(cluster_id)
        
        # Generate golden records
        generated_count = 0
        for cluster_id in all_cluster_ids:
            members_keys = manager.get_cluster_members(cluster_id)
            # Filter to records we actually have in this batch/context
            # Note: In a real system we might need to fetch other members from DB
            cluster_records = [record_map[k] for k in members_keys if k in record_map]
            
            if cluster_records:
                golden = manager.generate_golden_record(cluster_id, cluster_records)
                writer.project_cluster(
                    cluster_id=cluster_id, 
                    members=cluster_records, 
                    golden_record=golden.payload
                )
                generated_count += 1
                
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CLUSTER,
            status="complete",
            records_in=len(auto_links),
            records_out=generated_count,
            duration_ms=duration,
            message=f"Created/Updated {generated_count} golden records"
        ))
        
        return generated_count
    
    async def run(
        self,
        run_id: str,
        raw_records: List[dict],
        mode: str = "FULL"
    ) -> PipelineResult:
        """
        Execute the complete pipeline.
        
        Args:
            run_id: Unique run identifier
            raw_records: Raw customer records to process
            mode: "FULL" or "DELTA"
            
        Returns:
            PipelineResult with counters and status
        """
        if not raw_records:
            # Try reading from staging CSV first
            raw_records = read_staging_data()
            
            # Fallback to demo data only if still empty
            if not raw_records:
                logger.info("No staging data found, using demo data.")
                from pipeline.demo_data import get_demo_records
                raw_records = get_demo_records()
            
        # 0. Planner Step
        actual_mode = mode
        if mode.upper() == "AUTO":
            planner = get_planner_agent()
            plan = planner.plan_run(
                new_records_count=len(raw_records),
                total_records_count=len(raw_records) # Simulating total
            )
            actual_mode = plan.mode
            logger.info(f"Planner Agent set mode to: {actual_mode} (Reason: {plan.reason})")
        
        # Initialize result
        result = PipelineResult(
            run_id=run_id,
            success=False,
            mode=actual_mode,
            stages=[],
            started_at=datetime.utcnow()
        )
        
        try:
            # Stage 1: Ingest
            records = await self._stage_ingest(raw_records)
            result.records_in = len(records)
            
            # Stage 2: Normalize
            normalized = await self._stage_normalize(records)
            result.records_normalized = len(normalized)
            
            # Stage 3: Block
            blocks = await self._stage_block(normalized)
            result.blocks_created = len(blocks)
            
            # Stage 4: Candidates
            candidates = await self._stage_candidates(normalized)
            result.candidates_generated = len(candidates)
            
            # Stage 5: Score
            scores = await self._stage_score(candidates)
            result.pairs_scored = len(scores)
            
            # Stage 6: Decide
            counters = await self._stage_decide(scores, run_id)
            result.auto_links = counters['auto_links']
            result.review_items = counters['review_items']
            result.rejected = counters['rejected']

            # Stage 7: Explain
            await self._stage_explain(scores, self._decisions, normalized, run_id)

            # Stage 8: Cluster
            # We pass normalized records because they have the customer_key used in matching
            await self._stage_cluster(scores, self._decisions, normalized)
            
            # Persist Cluster Snapshot
            if self.run_id:
                from api.config import settings
                cm = get_cluster_manager()
                cm.save_snapshot(f'{settings.DATA_DIR}/runs/{self.run_id}_clusters.json')
            
            # Complete
            result.success = True
            result.ended_at = datetime.utcnow()
            
            await self._emit_progress(StageProgress(
                stage=PipelineStage.COMPLETE,
                status="complete",
                message="Pipeline completed successfully"
            ))
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            result.success = False
            result.error_message = str(e)
            result.ended_at = datetime.utcnow()
            
            await self._emit_progress(StageProgress(
                stage=PipelineStage.FAILED,
                status="error",
                message=str(e)
            ))
        
        return result
    
    def get_scores(self) -> Dict[str, MatchScore]:
        """Get all scored pairs."""
        return self._scores
    
    def get_decisions(self) -> Dict[str, MatchDecision]:
        """Get all decisions."""
        return self._decisions
    
    def get_review_queue(self) -> List[MatchScore]:
        """Get pairs routed to review."""
        return [
            self._scores[pair_id]
            for pair_id, decision in self._decisions.items()
            if decision == MatchDecision.REVIEW
        ]
    
    def get_auto_links(self) -> List[MatchScore]:
        """Get automatically linked pairs."""
        return [
            self._scores[pair_id]
            for pair_id, decision in self._decisions.items()
            if decision == MatchDecision.AUTO_LINK
        ]

    def get_uniques(self) -> List[dict]:
        """
        Get unique records (singletons) that were not linked to any other record.
        Records involved in pairs deemed REJECT are also returned if they have no other links.
        """
        linked_keys = set()
        
        for pair_id, decision in self._decisions.items():
            if decision in (MatchDecision.AUTO_LINK, MatchDecision.REVIEW):
                score = self._scores.get(pair_id)
                if score:
                    linked_keys.add(score.a_key)
                    linked_keys.add(score.b_key)
        
        uniques = []
        for key, record in self._records.items():
            if key not in linked_keys:
                uniques.append(record)
                
        return uniques

    def get_result_clusters(self) -> List[dict]:
        """
        Get all resolved clusters (entities) for records in this run.
        Returns list of {cluster_id, size, members, representative_record}.
        """
        manager = get_cluster_manager()
        all_clusters = manager.get_clusters()
        
        run_keys = set(self._records.keys())
        result_clusters = []
        
        for cluster_id, members in all_clusters.items():
            # Only include clusters that contain records from this run
            if any(k in run_keys for k in members):
                # Get member details
                cluster_records = [self._records[k] for k in members if k in self._records]
                
                if not cluster_records:
                    continue
                    
                # Pick representative (Golden Record or first member)
                golden = manager.get_golden_record(cluster_id)
                representative = golden.payload if golden else cluster_records[0]
                
                result_clusters.append({
                    "cluster_id": cluster_id,
                    "size": len(members), # Total size including historical
                    "members": cluster_records, # Only members in this run (for display)
                    "representative_record": representative,
                    "is_singleton": len(members) == 1
                })
                
        return result_clusters
