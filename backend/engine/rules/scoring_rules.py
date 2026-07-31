"""
CUIN v2 - Scoring/Decision Rules as Data

Makes the tier model + decisioning thresholds in
policies/ruleset_v2.yaml (`tiers`, `hard_conflicts`, `decisioning`)
UI-editable and versionable, without touching engine.scoring.tiers --
tiers.py remains the tested Python "oracle" (see
engine.rules.decision_compiler for the SQL that must agree with it).

DEFAULT_SCORING_RULES reproduces ruleset_v2.yaml's `decisioning` and
`clustering` sections exactly.
"""

from dataclasses import dataclass, field
from typing import Tuple


@dataclass(frozen=True)
class ScoringRules:
    # Identifier types counted as STRONG evidence when their validated,
    # non-suppressed value sets intersect (engine.scoring.tiers.classify).
    strong_identifier_types: Tuple[str, ...] = ("mobile", "email", "document")

    # MEDIUM: rare-token name Jaccard must be at least this to count
    # (module docstring in tiers.py: partial overlap is NOT medium at
    # the default 1.0 -- only an EXACT rare-token match, to avoid the
    # JW-style false-positive class). engine.rules.decision_compiler
    # compiles this as `jaccard >= threshold`; at the default 1.0 that
    # is mathematically identical to `jaccard == 1.0` (jaccard can
    # never exceed 1.0), so the fix from an earlier `=` comparison to
    # `>=` is a zero-behavior-change generalization at this default --
    # it only starts to differ once a value below 1.0 is actually set.
    #
    # KNOWN GAP: this field is consulted ONLY by decision_compiler's
    # SQL-compiled redecide/reblock PREVIEW path. The real per-pair
    # decision loop in both orchestrators calls
    # engine.scoring.tiers.classify(), which still hardcodes the
    # equivalent check to a literal 1.0 (engine.ruleset.config.
    # RulesetConfig, the config object classify()/decide() actually
    # read, has no such field at all -- it is a separate, older config
    # system from this one). Changing this value today would change
    # what the Settings UI's live preview shows WITHOUT changing what
    # an actual pipeline run decides. Unifying the two config systems
    # is required before this field can be safely exposed as an
    # editable threshold in the UI -- do that before wiring a slider
    # to it, not after.
    medium_name_jaccard_threshold: float = 1.0

    # MEDIUM: both sides must have this DOB precision and be equal.
    medium_dob_requires_precision: str = "FULL"

    # VETO: same-type document present on both sides with empty
    # intersection -- absolute, no override.
    veto_document_type_mismatch: bool = True
    # VETO: both sides FULL-precision DOB and they differ.
    veto_dob_full_mismatch: bool = True

    # Decisioning thresholds -- mirrors ruleset_v2.yaml `decisioning`.
    auto_link_min_strong_only: int = 2
    auto_link_min_strong_with_name: int = 1
    review_min_strong: int = 1
    # NEVER READ (by decision_compiler.py, tiers.py, or anywhere else) --
    # stored/serialized only. Do not surface as an editable UI control
    # until it is actually wired to something.
    review_requires_medium_name_and_dob: bool = True
    # Name similarity alone (medium_name, strong_count == 0) NEVER
    # creates a link or review item -- display-only corroboration.
    # NEVER READ AS A FIELD -- this behavior is instead implemented
    # structurally by decide()'s branch ordering (no branch there ever
    # triggers on medium_name without strong_count). Kept here as
    # documentation of that invariant, not as a live toggle -- flipping
    # it does nothing today.
    name_alone_gates_nothing: bool = True

    # Clustering cohesion guard -- mirrors ruleset_v2.yaml `clustering`.
    max_cluster_size: int = 12
    min_density: float = 0.35

    def to_dict(self) -> dict:
        return {
            "strong_identifier_types": list(self.strong_identifier_types),
            "medium_name_jaccard_threshold": self.medium_name_jaccard_threshold,
            "medium_dob_requires_precision": self.medium_dob_requires_precision,
            "veto_document_type_mismatch": self.veto_document_type_mismatch,
            "veto_dob_full_mismatch": self.veto_dob_full_mismatch,
            "auto_link_min_strong_only": self.auto_link_min_strong_only,
            "auto_link_min_strong_with_name": self.auto_link_min_strong_with_name,
            "review_min_strong": self.review_min_strong,
            "review_requires_medium_name_and_dob": self.review_requires_medium_name_and_dob,
            "name_alone_gates_nothing": self.name_alone_gates_nothing,
            "max_cluster_size": self.max_cluster_size,
            "min_density": self.min_density,
        }

    @staticmethod
    def from_dict(d: dict) -> "ScoringRules":
        d = d or {}
        defaults = ScoringRules()
        return ScoringRules(
            strong_identifier_types=tuple(d.get("strong_identifier_types", defaults.strong_identifier_types)),
            medium_name_jaccard_threshold=d.get("medium_name_jaccard_threshold", defaults.medium_name_jaccard_threshold),
            medium_dob_requires_precision=d.get("medium_dob_requires_precision", defaults.medium_dob_requires_precision),
            veto_document_type_mismatch=d.get("veto_document_type_mismatch", defaults.veto_document_type_mismatch),
            veto_dob_full_mismatch=d.get("veto_dob_full_mismatch", defaults.veto_dob_full_mismatch),
            auto_link_min_strong_only=d.get("auto_link_min_strong_only", defaults.auto_link_min_strong_only),
            auto_link_min_strong_with_name=d.get("auto_link_min_strong_with_name", defaults.auto_link_min_strong_with_name),
            review_min_strong=d.get("review_min_strong", defaults.review_min_strong),
            review_requires_medium_name_and_dob=d.get("review_requires_medium_name_and_dob", defaults.review_requires_medium_name_and_dob),
            name_alone_gates_nothing=d.get("name_alone_gates_nothing", defaults.name_alone_gates_nothing),
            max_cluster_size=d.get("max_cluster_size", defaults.max_cluster_size),
            min_density=d.get("min_density", defaults.min_density),
        )


DEFAULT_SCORING_RULES = ScoringRules()
