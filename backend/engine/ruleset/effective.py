"""
CUIN v2 - Effective Ruleset (Stage 1 of the banker-rule-engine migration)

Closes the "two config systems" gap documented at
engine/rules/scoring_rules.py:34-46: engine.ruleset.config.RulesetConfig
(loaded from policies/ruleset_v2.yaml) is what a real pipeline run
actually reads; engine.rules.scoring_rules.ScoringRules (UI-editable,
policy_versions-backed) was previously read ONLY by the redecide/
reblock SQL preview. A banker changing a threshold in the Settings UI
therefore changed what the preview showed without changing what an
actual run decided.

EffectiveRuleset resolves both into one object: the 5 decision-
affecting fields the two share (auto_link_min_strong_only,
auto_link_min_strong_with_name, review_min_strong, max_cluster_size,
min_density) come from the ACTIVE CATALOG when one exists, falling
back to the YAML defaults otherwise -- so a policy_version==0 (no
catalog ever saved) run is byte-identical to today's YAML-only
behavior (see tests/unit/test_config_agreement.py, which proves the
YAML and UI defaults already agree, making this fallback lossless).
Everything else (normalization/validation constants, suppression
thresholds) stays exactly as RulesetConfig already provides it --
EffectiveRuleset is a thin resolution layer, not a new source of truth.

Both orchestrators build ONE EffectiveRuleset per run and pass it to
BOTH engine.scoring.tiers.decide() and
engine.rules.decision_compiler.compile_decision_sql() (going forward,
engine.scoring.confidence.score_pair() and
engine.rules.confidence_compiler once Stage 2 lands), so the real
per-pair loop and the SQL preview can no longer disagree about which
threshold is active.
"""

from dataclasses import dataclass

from engine.ruleset.config import RulesetConfig, get_default_ruleset
from engine.rules.scoring_rules import ScoringRules


# The fields RulesetConfig and ScoringRules both define today -- see
# tests/unit/test_config_agreement.py, which pins that they agree at
# their respective defaults.
_SHARED_FIELDS = (
    "auto_link_min_strong_only",
    "auto_link_min_strong_with_name",
    "review_min_strong",
    "max_cluster_size",
    "min_density",
)


@dataclass(frozen=True)
class EffectiveRuleset:
    """
    Duck-types as a RulesetConfig for every existing caller of
    tiers.decide()/compile_decision_sql() (same 5 shared attribute
    names), plus carries the full RulesetConfig for normalization/
    suppression callers that need it (`.base`).
    """
    auto_link_min_strong_only: int
    auto_link_min_strong_with_name: int
    review_min_strong: int
    max_cluster_size: int
    min_density: float
    base: RulesetConfig
    source: str  # "catalog" | "yaml_default" -- for run-record audit logging

    # Suppression + normalization constants pass through unchanged from
    # YAML -- not yet UI-editable, see the migration plan's Stage 1 scope.
    @property
    def suppression_mobile_max(self) -> int:
        return self.base.suppression_mobile_max

    @property
    def suppression_email_max(self) -> int:
        return self.base.suppression_email_max

    @property
    def suppression_document_max(self) -> int:
        return self.base.suppression_document_max

    @property
    def suppression_name_token_max(self) -> int:
        return self.base.suppression_name_token_max

    @property
    def suppression_address_max(self) -> int:
        return self.base.suppression_address_max


def resolve_effective_ruleset(scoring_rules: ScoringRules = None, source: str = "yaml_default") -> EffectiveRuleset:
    """
    `scoring_rules=None` (or omitted) resolves purely from YAML --
    identical to calling get_default_ruleset() directly, kept as the
    explicit no-catalog path. Pass the active catalog's scoring_rules
    (engine.rules.store.get_active_catalog().scoring_rules) to let a
    saved catalog's thresholds take precedence.
    """
    base = get_default_ruleset()
    if scoring_rules is None:
        return EffectiveRuleset(
            auto_link_min_strong_only=base.auto_link_min_strong_only,
            auto_link_min_strong_with_name=base.auto_link_min_strong_with_name,
            review_min_strong=base.review_min_strong,
            max_cluster_size=base.max_cluster_size,
            min_density=base.min_density,
            base=base,
            source="yaml_default",
        )
    return EffectiveRuleset(
        auto_link_min_strong_only=scoring_rules.auto_link_min_strong_only,
        auto_link_min_strong_with_name=scoring_rules.auto_link_min_strong_with_name,
        review_min_strong=scoring_rules.review_min_strong,
        max_cluster_size=scoring_rules.max_cluster_size,
        min_density=scoring_rules.min_density,
        base=base,
        source=source,
    )


def resolve_from_active_catalog() -> EffectiveRuleset:
    """
    The common case: resolve from whatever policy_versions currently
    has active. Cohesion guards (max_cluster_size, min_density) come
    from the catalog's `match_ruleset` -- the object Stage 2's UI
    actually edits and keeps in sync on every save. The three tier
    thresholds (auto_link_min_strong_only/with_name, review_min_strong)
    are vestigial for real decisions since Stage 2 (confidence scoring
    reads MatchRuleset.auto_link_min_confidence/review_min_confidence
    instead) -- kept sourced from YAML only for engine.scoring.tiers'
    other callers (the frozen oracle, api/routes_matches.py's
    DecisionEngine), which still need SOME RulesetConfig-shaped object.
    """
    from engine.rules.store import get_active_catalog
    catalog = get_active_catalog()
    source = "catalog" if catalog.policy_version > 0 else "yaml_default"
    base = get_default_ruleset()
    return EffectiveRuleset(
        auto_link_min_strong_only=base.auto_link_min_strong_only,
        auto_link_min_strong_with_name=base.auto_link_min_strong_with_name,
        review_min_strong=base.review_min_strong,
        max_cluster_size=catalog.match_ruleset.max_cluster_size,
        min_density=catalog.match_ruleset.min_density,
        base=base,
        source=source,
    )
