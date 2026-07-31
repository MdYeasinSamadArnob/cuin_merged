from engine.rules.catalog import (
    BlockingRule,
    BlockingRuleType,
    RuleGuards,
    DEFAULT_BLOCKING_RULES,
)
from engine.rules.scoring_rules import ScoringRules, DEFAULT_SCORING_RULES
from engine.rules.match_rules import MatchRule, MatchRuleset, DEFAULT_MATCH_RULES, DEFAULT_MATCH_RULESET

__all__ = [
    "BlockingRule",
    "BlockingRuleType",
    "RuleGuards",
    "DEFAULT_BLOCKING_RULES",
    # Legacy tier-based scoring config -- no longer read by a real run's
    # decision logic (see engine.rules.match_rules module docstring and
    # engine.rules.scoring_rules's "KNOWN GAP" note). Kept importable
    # for pre-Stage-2 policy_versions rows and tests/unit/test_decision_sql_parity.py.
    "ScoringRules",
    "DEFAULT_SCORING_RULES",
    # Confidence-based scoring config -- what a real run's decision
    # logic actually reads as of Stage 2.
    "MatchRule",
    "MatchRuleset",
    "DEFAULT_MATCH_RULES",
    "DEFAULT_MATCH_RULESET",
]
