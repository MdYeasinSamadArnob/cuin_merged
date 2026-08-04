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

from engine.structures import (
    ScoringConfig,
    MatchScore,
    MatchDecision,
    FieldEvidence,
)

from engine.matching.custom_scorer import (
    CustomProbabilisticScorer,
)

from pipeline.orchestrator import (
    PipelineStage,
    StageProgress,
    PipelineResult,
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
    # Scoring
    'ScoringConfig',
    'MatchScore',
    'MatchDecision',
    'FieldEvidence',
    'CustomProbabilisticScorer',
    # Shared pipeline types
    'PipelineStage',
    'StageProgress',
    'PipelineResult',
]
