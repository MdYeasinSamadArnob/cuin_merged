"""
Planner Agent for CUIN v2.
Responsible for determining execution mode (FULL vs DELTA),
setting thresholds, and gating the Referee Agent.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dataclass
class PlannerDecision:
    mode: str = "FULL"
    auto_link_threshold: float = 0.92
    review_threshold: float = 0.55
    referee_enabled: bool = True
    referee_score_range: Tuple[float, float] = (0.45, 0.80)
    hard_conflict_rules: Optional[list] = None
    reason: str = "Default decision"

class PlannerAgent:
    """
    Agent determining strategy for the current run based on staging stats.
    """
    
    def __init__(self):
        self.default_decision = PlannerDecision()
    
    def plan_run(
        self,
        new_records_count: int = 0,
        total_records_count: int = 0,
        last_run_timestamp: Optional[datetime] = None
    ) -> PlannerDecision:
        """
        Determine run parameters based on input stats.
        API mimicking an LLM or Logic based planner.
        """
        # Logic: If small increment in records -> DELTA, else FULL
        # For Demo/MVP, we default to FULL unless explicitly requested or very small update logic added later.
        
        decision = PlannerDecision(
            mode="FULL",
            reason="Standard full run",
            auto_link_threshold=0.92,
            review_threshold=0.55,
            referee_enabled=True,
            referee_score_range=(0.45, 0.80)
        )

        if total_records_count > 0 and new_records_count > 0:
            ratio = new_records_count / total_records_count
            if ratio < 0.2:
                # Less than 20% change -> Suggest DELTA
                decision.mode = "DELTA"
                decision.reason = f"Small update detected ({ratio:.1%}), recommending DELTA processing"
        
        logger.info(f"Planner Agent decision: {decision}")
        return decision

# Singleton
_planner = PlannerAgent()

def get_planner_agent() -> PlannerAgent:
    return _planner
