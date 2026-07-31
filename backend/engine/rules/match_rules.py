"""
CUIN v2 - Confidence-Based Match Rules (Stage 2 of the banker-rule-engine migration)

Replaces the "strong/medium tier count" vocabulary
(engine.scoring.tiers.TierResult) with a per-field confidence
percentage a banker sets directly: "National ID matches exactly = 60%".
A pair's confidence is the sum of every matching field's contribution,
capped at 100. AUTO_LINK / REVIEW / REJECT are point thresholds on
that same 0-100 scale.

Proven equivalent to today's tier logic by exhaustive enumeration
(tests/unit/test_confidence_enumeration.py, 7,290 synthetic evidence
combinations) at the seed below: every strong identifier = 50%, name
(rare-token Jaccard == 1.0) = 45%, DOB (both FULL precision, equal) =
25%, auto_link >= 95%, review >= 50%. Zero mismatches against
engine.scoring.tiers.decide() across the full enumeration.

Two fields exist specifically because a flat (attribute, comparator,
confidence_pct) tuple silently breaks the baseline:

- `aggregation`: engine.scoring.tiers.classify() counts EVIDENCE ROWS,
  not attributes -- a pair sharing both an NID and a TIN document
  produces TWO evidence rows (grouped by (customer_code, id_type,
  doc_type), see engine.scoring.evidence.py:38) and therefore
  strong_count=2, not 1. A match rule that fired once per pair
  regardless of how many DOCUMENT sub_types matched would silently
  turn every NID+TIN auto-link into a review. `aggregation="per_sub_type"`
  reproduces the row-counting behavior; `aggregation="once"` (mobile,
  email, name, dob -- each is naturally a single evidence row) fires
  its confidence contribution at most once no matter how many
  sub_types happen to exist.
- `veto_kind`: lets a bank say "if both sides HAVE this field and it
  clearly doesn't match, never auto-link -- no matter what else
  matches". Off (None) by default for every rule except the two
  built-ins where it's on by the seed (document, date of birth) --
  a bank can flip it on any rule, including a custom RAW_COLUMN one
  ("MOTHER_NAME" mismatch should hard-reject, say). The two kinds are
  structurally different predicates, not interchangeable booleans, and
  BOTH generalize past the 2 built-ins they originally modeled:
    - "both_present_no_overlap": both sides have a validated,
      non-empty value/array for this (attribute, sub_type) and share
      NONE of them -- e.g. two different NID numbers, or two RAW_COLUMN
      arrays with zero overlap. Maps to tiers.py's document-mismatch
      veto; generalizes to ANY set/array-valued rule (set_intersect,
      token_jaccard, token_containment).
    - "both_qualified_and_differ": both sides have a value (and, for
      DOB specifically, meet a qualifier filter like precision ==
      FULL) and the comparator says they DON'T match. Maps to
      tiers.py's DOB-mismatch veto; generalizes to ANY scalar-valued
      rule (exact, prefix, numeric_tolerance, date_tolerance).
  A veto ALWAYS forces REJECT regardless of accumulated confidence --
  see engine.scoring.confidence.score_pair().
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------------------------------------------------
# Vocabulary
# ----------------------------------------------------------------------

AGGREGATION_ONCE = "once"
AGGREGATION_PER_SUB_TYPE = "per_sub_type"
VALID_AGGREGATIONS = (AGGREGATION_ONCE, AGGREGATION_PER_SUB_TYPE)

# A match rule's `attribute` is normally one of the 6 built-ins below.
# "RAW_COLUMN" opens this up to ANY column the schema discovers (Stage
# 5.1 -- "bank users can give weights to all the registered fields...
# dynamically, as new fields like mother name/cousin name come in"):
# the actual column name lives in `params["column"]`, validated by the
# same injection-safety regex engine.rules.catalog uses for blocking's
# RAW_COLUMN rule type, and `params["is_array"]` says whether it's a
# multi-valued source column. See engine.scoring.confidence's
# _score_raw_column_rule and engine.rules.confidence_compiler's
# _raw_column_confidence_cte for how it's scored.
ATTRIBUTE_RAW_COLUMN = "RAW_COLUMN"
_RAW_COLUMN_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")

VETO_BOTH_PRESENT_NO_OVERLAP = "both_present_no_overlap"
VETO_BOTH_QUALIFIED_AND_DIFFER = "both_qualified_and_differ"
VALID_VETO_KINDS = (None, VETO_BOTH_PRESENT_NO_OVERLAP, VETO_BOTH_QUALIFIED_AND_DIFFER)


@dataclass(frozen=True)
class MatchRule:
    """
    One field's contribution to a pair's confidence score.

    attribute: which evidence source this reads -- one of "DOCUMENT" |
        "MOBILE" | "EMAIL" | "FULL_ADDRESS" (all read from
        pair_identifier_evidence, keyed by id_type), "NAME" |
        "BIRTH_DATE" (read from pair_name_dob_evidence), or
        "RAW_COLUMN" (any other schema column a bank adds -- the
        column name lives in params["column"], params["is_array"]
        says whether it's multi-valued; see ATTRIBUTE_RAW_COLUMN).
    sub_type: optional filter on doc_type (e.g. "NID", "TIN") -- only
        meaningful for attribute="DOCUMENT". None means "any doc_type".
    comparator: id from engine.rules.comparators.COMPARATORS, or the
        two legacy-oracle-only ids "set_intersect_nonempty" (mobile/
        email/document: "share at least one value") and
        "token_jaccard_threshold" (name: jaccard >= params['min']).
    confidence_pct: this field's contribution when it matches, 0-100.
    aggregation: see module docstring.
    veto_kind: see module docstring. When set, a match on this
        condition's veto predicate forces REJECT regardless of
        accumulated confidence.
    """
    rule_id: str
    attribute: str
    comparator: str
    confidence_pct: float
    sub_type: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    aggregation: str = AGGREGATION_ONCE
    veto_kind: Optional[str] = None
    enabled: bool = True
    label: str = ""

    def validate(self) -> None:
        if self.aggregation not in VALID_AGGREGATIONS:
            raise ValueError(f"{self.rule_id}: invalid aggregation {self.aggregation!r}")
        if self.veto_kind not in VALID_VETO_KINDS:
            raise ValueError(f"{self.rule_id}: invalid veto_kind {self.veto_kind!r}")
        if not (0 <= self.confidence_pct <= 100):
            raise ValueError(f"{self.rule_id}: confidence_pct must be 0-100, got {self.confidence_pct}")
        if self.attribute == ATTRIBUTE_RAW_COLUMN:
            from engine.rules.comparators import get_comparator
            column = self.params.get("column")
            if not column or not _RAW_COLUMN_RE.match(column):
                raise ValueError(f"{self.rule_id}: '{column}' is not a safe column name ({_RAW_COLUMN_RE.pattern})")
            comparator = get_comparator(self.comparator)
            if comparator is None or not comparator.implemented:
                raise ValueError(f"{self.rule_id}: comparator {self.comparator!r} is not available for matching")

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "attribute": self.attribute,
            "sub_type": self.sub_type,
            "comparator": self.comparator,
            "params": self.params,
            "confidence_pct": self.confidence_pct,
            "aggregation": self.aggregation,
            "veto_kind": self.veto_kind,
            "enabled": self.enabled,
            "label": self.label,
        }

    @staticmethod
    def from_dict(d: dict) -> "MatchRule":
        return MatchRule(
            rule_id=d["rule_id"],
            attribute=d["attribute"],
            sub_type=d.get("sub_type"),
            comparator=d["comparator"],
            params=d.get("params", {}),
            confidence_pct=d["confidence_pct"],
            aggregation=d.get("aggregation", AGGREGATION_ONCE),
            veto_kind=d.get("veto_kind"),
            enabled=d.get("enabled", True),
            label=d.get("label", ""),
        )


@dataclass(frozen=True)
class MatchRuleset:
    """
    A complete confidence-based scoring configuration: the per-field
    rules plus the two point thresholds. Stage 5 wraps one of these
    per segment; at a single segment (today's default) this IS the
    active scoring configuration.
    """
    match_rules: Tuple[MatchRule, ...]
    auto_link_min_confidence: float = 95.0
    review_min_confidence: float = 50.0
    confidence_cap: float = 100.0
    # Clustering cohesion guards -- unchanged from ScoringRules/RulesetConfig.
    max_cluster_size: int = 12
    min_density: float = 0.35

    def to_dict(self) -> dict:
        return {
            "match_rules": [r.to_dict() for r in self.match_rules],
            "auto_link_min_confidence": self.auto_link_min_confidence,
            "review_min_confidence": self.review_min_confidence,
            "confidence_cap": self.confidence_cap,
            "max_cluster_size": self.max_cluster_size,
            "min_density": self.min_density,
        }

    @staticmethod
    def from_dict(d: dict) -> "MatchRuleset":
        return MatchRuleset(
            match_rules=tuple(MatchRule.from_dict(r) for r in d.get("match_rules", [])),
            auto_link_min_confidence=d.get("auto_link_min_confidence", 95.0),
            review_min_confidence=d.get("review_min_confidence", 50.0),
            confidence_cap=d.get("confidence_cap", 100.0),
            max_cluster_size=d.get("max_cluster_size", 12),
            min_density=d.get("min_density", 0.35),
        )


def raw_columns_in_ruleset(ruleset: "MatchRuleset") -> List[str]:
    """Column names (sorted, deduped) any ENABLED RAW_COLUMN rule in this ruleset reads from `raw`."""
    cols = set()
    for rule in ruleset.match_rules:
        if rule.enabled and rule.attribute == ATTRIBUTE_RAW_COLUMN:
            col = rule.params.get("column")
            if col:
                cols.add(col)
    return sorted(cols)


def raw_column_specs_in_ruleset(ruleset: "MatchRuleset") -> Dict[str, bool]:
    """{column: is_array} for every ENABLED RAW_COLUMN rule in this ruleset."""
    specs: Dict[str, bool] = {}
    for rule in ruleset.match_rules:
        if rule.enabled and rule.attribute == ATTRIBUTE_RAW_COLUMN:
            col = rule.params.get("column")
            if col:
                specs[col] = specs.get(col, False) or bool(rule.params.get("is_array"))
    return specs


def raw_column_specs_in_catalog(catalog) -> Dict[str, bool]:
    """
    {column: is_array} across a catalog's default ruleset AND every
    per-segment override (segments can use different custom fields) --
    what an orchestrator needs to bulk-fetch from `raw` once per run,
    and (on Doris) which columns need JSON-array parsing on the way
    back. Duck-typed on `.match_ruleset` / `.match_rulesets_by_segment`
    rather than importing engine.rules.store.RuleCatalogVersion, to
    avoid a circular import (store.py already imports this module).
    """
    specs = dict(raw_column_specs_in_ruleset(catalog.match_ruleset))
    for seg_ruleset in getattr(catalog, "match_rulesets_by_segment", {}).values():
        for col, is_array in raw_column_specs_in_ruleset(seg_ruleset).items():
            specs[col] = specs.get(col, False) or is_array
    return specs


def raw_columns_in_catalog(catalog) -> List[str]:
    return sorted(raw_column_specs_in_catalog(catalog))


# ----------------------------------------------------------------------
# The seed: provably equivalent to today's tier logic at these values
# (see tests/unit/test_confidence_enumeration.py).
# ----------------------------------------------------------------------

DEFAULT_MATCH_RULES: Tuple[MatchRule, ...] = (
    MatchRule(
        rule_id="document_match", attribute="DOCUMENT", sub_type=None,
        comparator="set_intersect_nonempty", params={}, confidence_pct=50.0,
        aggregation=AGGREGATION_PER_SUB_TYPE, veto_kind=VETO_BOTH_PRESENT_NO_OVERLAP,
        label="National ID / document matches exactly",
    ),
    MatchRule(
        rule_id="mobile_match", attribute="MOBILE", sub_type=None,
        comparator="set_intersect_nonempty", params={}, confidence_pct=50.0,
        aggregation=AGGREGATION_ONCE, veto_kind=None,
        label="Phone number matches",
    ),
    MatchRule(
        rule_id="email_match", attribute="EMAIL", sub_type=None,
        comparator="set_intersect_nonempty", params={}, confidence_pct=50.0,
        aggregation=AGGREGATION_ONCE, veto_kind=None,
        label="Email address matches",
    ),
    MatchRule(
        # 0% seed is deliberate, not "disabled": address evidence rows
        # already exist in pair_identifier_evidence (evidence.py has no
        # id_type filter) and contribute nothing to today's decision --
        # a missing rule and a 0%-confidence rule are equivalent for
        # the decision but not for the audit panel, which should show
        # every profiled field, including ones contributing zero.
        rule_id="address_match", attribute="FULL_ADDRESS", sub_type=None,
        comparator="set_intersect_nonempty", params={}, confidence_pct=0.0,
        aggregation=AGGREGATION_ONCE, veto_kind=None,
        label="Address matches",
    ),
    MatchRule(
        rule_id="name_match", attribute="NAME", sub_type=None,
        comparator="token_jaccard_threshold", params={"min": 1.0}, confidence_pct=45.0,
        aggregation=AGGREGATION_ONCE, veto_kind=None,
        label="Name is the same words",
    ),
    MatchRule(
        rule_id="dob_match", attribute="BIRTH_DATE", sub_type=None,
        comparator="exact", params={"require_qualifier": "FULL", "both_sides": True},
        confidence_pct=25.0,
        aggregation=AGGREGATION_ONCE, veto_kind=VETO_BOTH_QUALIFIED_AND_DIFFER,
        label="Date of birth matches exactly",
    ),
)

DEFAULT_MATCH_RULESET = MatchRuleset(
    match_rules=DEFAULT_MATCH_RULES,
    auto_link_min_confidence=95.0,
    review_min_confidence=50.0,
    confidence_cap=100.0,
    max_cluster_size=12,
    min_density=0.35,
)


def migrate_scoring_rules(scoring) -> MatchRuleset:
    """
    Best-effort migration of a legacy ScoringRules (or EffectiveRuleset)
    object into a MatchRuleset. Only the 5 shared thresholds carry
    over (max_cluster_size, min_density map directly; the tier
    thresholds don't have a principled percentage mapping other than
    the seed itself) -- match_rules always come from the seed, since
    ScoringRules never had a per-field confidence concept to migrate
    FROM. This exists so engine.rules.store can read a pre-Stage-2
    policy_versions row without crashing; it does not attempt to
    reconstruct a banker's prior custom thresholds because none could
    have existed under the old model.
    """
    return MatchRuleset(
        match_rules=DEFAULT_MATCH_RULES,
        auto_link_min_confidence=95.0,
        review_min_confidence=50.0,
        confidence_cap=100.0,
        max_cluster_size=getattr(scoring, "max_cluster_size", 12),
        min_density=getattr(scoring, "min_density", 0.35),
    )
