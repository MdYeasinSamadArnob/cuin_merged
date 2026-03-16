"""
CUIN v2 - Pipeline Package
"""

from engine.normalize.standardize import (
    normalize_name,
    normalize_phone,
    normalize_email,
    normalize_dob,
    normalize_address,
    normalize_natid,
    normalize_record,
    compute_record_hash,
)

from engine.blocking.multipass_blocker import (
    BlockingConfig,
    BlockingResult,
    MultiPassBlocker,
)

from engine.blocking.candidate_builder import (
    CandidatePair,
)

from engine.structures import (
    ScoringConfig,
    MatchScore,
    MatchDecision,
    FieldEvidence,
)

from engine.matching.splink_engine import (
    SplinkScorer,
)

from pipeline.orchestrator import (
    PipelineStage,
    StageProgress,
    PipelineResult,
    PipelineOrchestrator,
)

__all__ = [
    # Normalization
    'normalize_name',
    'normalize_phone',
    'normalize_email',
    'normalize_dob',
    'normalize_address',
    'normalize_natid',
    'normalize_record',
    'compute_record_hash',
    # Blocking
    'BlockingConfig',
    'BlockingResult',
    'CandidatePair',
    'MultiPassBlocker',
    # Scoring
    'ScoringConfig',
    'MatchScore',
    'MatchDecision',
    'FieldEvidence',
    'SplinkScorer',
    # Orchestrator
    'PipelineStage',
    'StageProgress',
    'PipelineResult',
    'PipelineOrchestrator',
]
