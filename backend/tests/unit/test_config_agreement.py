"""
Proves the two parallel scoring-configuration objects agree TODAY, on
the fields they actually share, before Stage 1 of the banker-rule-
engine migration merges them into one `EffectiveRuleset`.

engine.ruleset.config.RulesetConfig (loaded from policies/ruleset_v2.yaml,
what engine.scoring.tiers.decide() actually reads on a real run) and
engine.rules.scoring_rules.ScoringRules (UI-editable, read only by the
redecide/reblock SQL preview -- see scoring_rules.py:34-46's "KNOWN GAP"
note) overlap on exactly 5 fields: auto_link_min_strong_only,
auto_link_min_strong_with_name, review_min_strong, max_cluster_size,
min_density. (The suppression_* fields live only on RulesetConfig
today -- ScoringRules has no suppression fields at all, contrary to an
earlier assumption in the migration plan; verified by inspection here,
not carried forward as a stale claim.)

If this test is red, merging the two into EffectiveRuleset would
silently change which threshold value a real run uses -- catch that
BEFORE the merge, not after.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.ruleset.config import get_default_ruleset
from engine.rules.scoring_rules import DEFAULT_SCORING_RULES

_SHARED_FIELDS = (
    "auto_link_min_strong_only",
    "auto_link_min_strong_with_name",
    "review_min_strong",
    "max_cluster_size",
    "min_density",
)


def test_shared_fields_agree_between_yaml_and_ui_defaults():
    ruleset = get_default_ruleset()
    scoring = DEFAULT_SCORING_RULES

    mismatches = []
    for field in _SHARED_FIELDS:
        yaml_val = getattr(ruleset, field)
        ui_val = getattr(scoring, field)
        if yaml_val != ui_val:
            mismatches.append((field, yaml_val, ui_val))

    assert not mismatches, (
        f"RulesetConfig (YAML, what real runs use) and ScoringRules (UI defaults, what "
        f"only the preview uses) disagree on: {mismatches}. A real run and the Settings "
        f"UI preview are currently showing different thresholds for these fields -- fix "
        f"the drift before Stage 1 merges them into one EffectiveRuleset, not after."
    )


def test_scoring_rules_has_no_suppression_fields_yet():
    """
    Documents the current (pre-Stage-1) shape precisely, so a future
    reader doesn't assume suppression thresholds are already
    UI-editable when they aren't -- RulesetConfig alone owns them.
    """
    scoring_fields = set(DEFAULT_SCORING_RULES.__dataclass_fields__.keys())
    suppression_fields = {f for f in scoring_fields if f.startswith("suppression_")}
    assert suppression_fields == set(), (
        f"ScoringRules unexpectedly has suppression fields now: {suppression_fields}. "
        f"If Stage 1 added these deliberately, update this test to assert the new set "
        f"agrees with RulesetConfig instead of asserting absence."
    )


if __name__ == "__main__":
    test_shared_fields_agree_between_yaml_and_ui_defaults()
    test_scoring_rules_has_no_suppression_fields_yet()
    print("config agreement: OK")
