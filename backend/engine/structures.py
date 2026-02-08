"""
CUIN v2 - Shared Structures

Common data structures and configurations for the engine modules.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

class MatchDecision(str, Enum):
    """Match decision categories."""
    AUTO_LINK = "AUTO_LINK"
    REVIEW = "REVIEW"
    REJECT = "REJECT"


@dataclass
class FieldEvidence:
    """Evidence for a single field comparison."""
    field_name: str
    value_a: Optional[str]
    value_b: Optional[str]
    comparison_type: str  # exact, fuzzy, null, etc.
    similarity_score: float
    match_weight: float
    explanation: str


@dataclass
class MatchScore:
    """Complete match score with evidence."""
    pair_id: str
    a_key: str
    b_key: str
    score: float  # 0.0 to 1.0
    evidence: List[FieldEvidence] = field(default_factory=list)
    hard_conflicts: List[str] = field(default_factory=list)
    signals_hit: List[str] = field(default_factory=list)


@dataclass
class ScoringConfig:
    """Configuration for scoring operations."""
    auto_link_threshold: float = 0.92
    review_threshold: float = 0.55
    
    # Field weights (should sum to ~1.0)
    name_weight: float = 0.25
    phone_weight: float = 0.20
    email_weight: float = 0.20
    dob_weight: float = 0.15
    natid_weight: float = 0.15
    address_weight: float = 0.05


from datetime import datetime
from typing import Dict

@dataclass
class ClusterMember:
    """A member of a cluster with version info."""
    customer_key: str
    cluster_id: str
    version: int
    valid_from: datetime
    valid_to: Optional[datetime] = None


@dataclass
class GoldenRecord:
    """Merged golden record for a cluster."""
    cluster_id: str
    version: int
    payload: Dict[str, any]
    created_at: datetime
    created_by: str
