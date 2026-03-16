"""
CUIN v2 - Decision Engine

Logic for making resolutions based on match scores.
"""

from typing import Optional
from engine.structures import MatchScore, MatchDecision, ScoringConfig


class DecisionEngine:
    """
    Evaluates match scores to determine the resolution action.
    """
    
    def __init__(self, config: Optional[ScoringConfig] = None):
        self.config = config or ScoringConfig()
        
    def make_decision(
        self,
        match_score: MatchScore
    ) -> MatchDecision:
        """
        Make a decision based on score, signals, and conflicts.
        
        Decision logic:
        1. If hard conflicts exist -> REJECT
        2. If score >= auto_link_threshold AND >= 2 signals AND no conflicts -> AUTO_LINK
        3. If score >= review_threshold -> REVIEW
        4. Otherwise -> REJECT
        """
        # Hard conflicts always reject
        if match_score.hard_conflicts:
            return MatchDecision.REJECT
        
        # Auto-link requires high score AND multiple signals
        # OR extremely high score (e.g. perfect match on key field)
        if (match_score.score >= self.config.auto_link_threshold and 
            (len(match_score.signals_hit) >= 2 or match_score.score >= 0.98)):
            return MatchDecision.AUTO_LINK
        
        # Review for uncertain cases
        if match_score.score >= self.config.review_threshold:
            return MatchDecision.REVIEW
        
        # Default to reject
        return MatchDecision.REJECT
