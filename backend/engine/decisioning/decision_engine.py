"""
CUIN v2 - Decision Engine (Stage 2: confidence-based)

Re-derives a MatchDecision from a MatchScore for callers that only
have the wire-format MatchScore (routes_graph.py's preview paths), not
the raw evidence dict engine.scoring.confidence.score_pair() consumes.

Previously reconstructed a TierResult by STRING-PREFIX-PARSING
MatchScore.signals_hit ("mobile:", "document:",
"name_rare_token_jaccard_1.0:", ...) -- brittle by construction, since
any change to those strings would silently degrade every reconstructed
decision. Since engine.pipeline orchestrators now set
MatchScore.score directly to the pair's confidence_pct/100 (Stage 2),
this can compare that value against the active MatchRuleset's
thresholds directly -- no string parsing, and it stays correct even
if a banker changes per-field confidence weights (a signal-prefix
reconstruction could never have accounted for that: it only ever knew
"this signal fired", not "how many points it was worth").
"""

from typing import Optional
from engine.structures import MatchScore, MatchDecision, ScoringConfig
from engine.rules.match_rules import MatchRuleset, DEFAULT_MATCH_RULESET


class DecisionEngine:
    """
    `config` (ScoringConfig) is kept alive only because api/routes_config.py
    binds its attribute names directly; it is no longer consulted for
    thresholds. `match_ruleset` supplies the active confidence
    thresholds -- pass the run's own (engine.rules.store.get_active_catalog()
    .match_ruleset) to pin a specific version; defaults to the seed.
    """

    def __init__(self, config: Optional[ScoringConfig] = None, match_ruleset: Optional[MatchRuleset] = None):
        self.config = config or ScoringConfig()
        self.match_ruleset = match_ruleset or DEFAULT_MATCH_RULESET

    def make_decision(self, match_score: MatchScore) -> MatchDecision:
        if match_score.hard_conflicts:
            return MatchDecision.REJECT

        confidence_pct = match_score.score * 100.0
        if confidence_pct >= self.match_ruleset.auto_link_min_confidence:
            return MatchDecision.AUTO_LINK
        if confidence_pct >= self.match_ruleset.review_min_confidence:
            return MatchDecision.REVIEW
        return MatchDecision.REJECT
