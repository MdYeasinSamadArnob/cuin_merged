"""
CUIN v2 - Decision Engine (Ruleset v2)

Logic for making resolutions based on match scores. Delegates to
engine.scoring.tiers so both the DuckDB pipeline (which builds a
TierResult directly) and any caller holding only a MatchScore (the
wire-format type routes_matches.py serializes) use the identical
auto-link/review/reject rule -- one source of truth, not a numeric
threshold that has drifted from the tier logic.

MatchScore.signals_hit encodes tier membership by string prefix
(see engine.scoring.tiers.classify): "mobile:"/"email:"/"document:"
count as STRONG signals; "name_rare_token_jaccard_1.0:" is MEDIUM
name; "dob_full_exact:" is MEDIUM dob. MatchScore.hard_conflicts
non-empty is an absolute VETO, exactly as in tiers.decide().
"""

from typing import Optional
from engine.structures import MatchScore, MatchDecision, ScoringConfig
from engine.scoring.tiers import TierResult, decide as tiers_decide
from engine.ruleset.config import RulesetConfig, get_default_ruleset

_STRONG_PREFIXES = ("mobile:", "email:", "document:")


class DecisionEngine:
    """
    Evaluates match scores to determine the resolution action.

    `config` (ScoringConfig) is kept alive only because api/routes_config.py
    binds its attribute names directly; it is no longer consulted for
    thresholds. Ruleset behavior comes from policies/ruleset_v2.yaml via
    RulesetConfig -- pass one explicitly to pin a specific ruleset version.
    """

    def __init__(self, config: Optional[ScoringConfig] = None, ruleset: Optional[RulesetConfig] = None):
        self.config = config or ScoringConfig()
        self.ruleset = ruleset or get_default_ruleset()

    def make_decision(
        self,
        match_score: MatchScore
    ) -> MatchDecision:
        """
        Reconstructs a TierResult from MatchScore.signals_hit/hard_conflicts
        and applies the exact same rule as engine.scoring.tiers.decide().
        """
        strong_count = sum(
            1 for s in match_score.signals_hit if s.startswith(_STRONG_PREFIXES)
        )
        medium_name = any(
            s.startswith("name_rare_token_jaccard_1.0:") for s in match_score.signals_hit
        )
        medium_dob = any(
            s.startswith("dob_full_exact:") for s in match_score.signals_hit
        )

        tier = TierResult(
            strong_count=strong_count,
            medium_name=medium_name,
            medium_dob=medium_dob,
            vetoes=list(match_score.hard_conflicts),
            signals_hit=list(match_score.signals_hit),
        )
        return tiers_decide(tier, self.ruleset)
