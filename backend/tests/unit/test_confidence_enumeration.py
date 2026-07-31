"""
The Stage-2 gate: exhaustive enumeration proving
engine.scoring.confidence.score_pair() (the new confidence model)
reproduces engine.scoring.tiers.classify()/decide() (the frozen
legacy oracle) EXACTLY, at the seed values in
engine.rules.match_rules.DEFAULT_MATCH_RULESET, before either touches
a real evidence table.

The evidence space per pair is finite. This enumerates the full
cross-product: 3 states each for MOBILE / EMAIL / FULL_ADDRESS /
DOCUMENT-NID / DOCUMENT-TIN (absent / present-no-overlap / present-
with-overlap) x 5 name states x 6 dob states = 3**5 * 5 * 6 = 7,290
synthetic pairs, asserting BOTH `decision` and `signals_hit` agree
between the two implementations on every single one.

Two document sub_types (not one) is the minimum needed to exercise
the multiplicity trap this stage exists to catch: a pair whose NID AND
TIN both match must score 100% (50% x 2, aggregation="per_sub_type")
and AUTO_LINK, not 50%/REVIEW -- see MatchRule.aggregation's
docstring.
"""

import itertools
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.scoring.tiers import classify as legacy_classify, decide as legacy_decide
from engine.scoring.confidence import score_pair
from engine.rules.match_rules import DEFAULT_MATCH_RULESET
from engine.ruleset.config import get_default_ruleset

_STATES = ("absent", "no_overlap", "overlap")


def _id_row(id_type, doc_type, state):
    if state == "absent":
        return None
    if state == "no_overlap":
        return {
            "id_type": id_type, "doc_type": doc_type,
            "values_a": ["A_VALUE"], "values_b": ["B_VALUE"], "intersection": [],
        }
    return {
        "id_type": id_type, "doc_type": doc_type,
        "values_a": ["SHARED"], "values_b": ["SHARED"], "intersection": ["SHARED"],
    }


_NAME_STATES = {
    "no_tokens": {"token_union": [], "token_intersection": []},
    "jaccard_0": {"token_union": ["A", "B"], "token_intersection": []},
    "jaccard_1_of_3": {"token_union": ["A", "B", "C"], "token_intersection": ["A"]},
    "jaccard_1.0_2tok": {"token_union": ["A", "B"], "token_intersection": ["A", "B"]},
    "jaccard_1.0_1tok": {"token_union": ["A"], "token_intersection": ["A"]},
}

_DOB_STATES = {
    "both_null": {"dob_a": None, "dob_b": None, "dob_precision_a": None, "dob_precision_b": None},
    "full_full_eq": {"dob_a": "2000-01-01", "dob_b": "2000-01-01", "dob_precision_a": "FULL", "dob_precision_b": "FULL"},
    "full_full_neq": {"dob_a": "2000-01-01", "dob_b": "2000-01-02", "dob_precision_a": "FULL", "dob_precision_b": "FULL"},
    "full_year_eq": {"dob_a": "2000-01-01", "dob_b": "2000-01-01", "dob_precision_a": "FULL", "dob_precision_b": "YEAR_ONLY"},
    "year_year_eq": {"dob_a": "2000-01-01", "dob_b": "2000-01-01", "dob_precision_a": "YEAR_ONLY", "dob_precision_b": "YEAR_ONLY"},
    "year_year_neq": {"dob_a": "2000-01-01", "dob_b": "2001-01-01", "dob_precision_a": "YEAR_ONLY", "dob_precision_b": "YEAR_ONLY"},
}


def _build_evidence(mobile_state, email_state, address_state, nid_state, tin_state, name_state, dob_state):
    identifiers = []
    for row in (
        _id_row("mobile", None, mobile_state),
        _id_row("email", None, email_state),
        _id_row("address", None, address_state),
        _id_row("document", "NID", nid_state),
        _id_row("document", "TIN", tin_state),
    ):
        if row is not None:
            identifiers.append(row)

    name_dob = {**_NAME_STATES[name_state], **_DOB_STATES[dob_state]}
    return {"identifiers": identifiers, "name_dob": name_dob}


def _all_cases():
    combos = itertools.product(_STATES, _STATES, _STATES, _STATES, _STATES, _NAME_STATES, _DOB_STATES)
    for mobile, email, address, nid, tin, name, dob in combos:
        yield (mobile, email, address, nid, tin, name, dob)


def test_confidence_model_matches_legacy_oracle_exhaustively():
    ruleset = get_default_ruleset()
    match_ruleset = DEFAULT_MATCH_RULESET

    cases = list(_all_cases())
    assert len(cases) == 3 ** 5 * 5 * 6 == 7290

    mismatches = []
    for case in cases:
        evidence = _build_evidence(*case)

        legacy_tier = legacy_classify(evidence)
        legacy_decision = legacy_decide(legacy_tier, ruleset).value

        new_score = score_pair(evidence, match_ruleset)

        if new_score.decision != legacy_decision or new_score.signals_hit != legacy_tier.signals_hit:
            mismatches.append({
                "case": case,
                "legacy_decision": legacy_decision, "new_decision": new_score.decision,
                "legacy_signals": legacy_tier.signals_hit, "new_signals": new_score.signals_hit,
                "confidence": new_score.confidence_pct,
            })

    assert not mismatches, (
        f"{len(mismatches)}/{len(cases)} cases diverge between the legacy tier oracle and the "
        f"confidence model. First 5: {mismatches[:5]}"
    )


def test_named_boundary_cases():
    """
    The exact boundaries from the migration plan, asserted individually
    so a future default-value change fails with a legible message
    instead of a diff buried in 7,290 rows.
    """
    ruleset = get_default_ruleset()

    # (1 strong, name) = 95 -- exactly AUTO_LINK
    ev = _build_evidence("overlap", "absent", "absent", "absent", "absent", "jaccard_1.0_2tok", "both_null")
    score = score_pair(ev)
    assert score.confidence_pct == 95.0
    assert score.decision == "AUTO_LINK"
    assert legacy_decide(legacy_classify(ev), ruleset).value == "AUTO_LINK"

    # (name, dob) = 70 -- REVIEW
    ev = _build_evidence("absent", "absent", "absent", "absent", "absent", "jaccard_1.0_2tok", "full_full_eq")
    score = score_pair(ev)
    assert score.confidence_pct == 70.0
    assert score.decision == "REVIEW"
    assert legacy_decide(legacy_classify(ev), ruleset).value == "REVIEW"

    # name alone = 45 -- REJECT
    ev = _build_evidence("absent", "absent", "absent", "absent", "absent", "jaccard_1.0_2tok", "both_null")
    score = score_pair(ev)
    assert score.confidence_pct == 45.0
    assert score.decision == "REJECT"
    assert legacy_decide(legacy_classify(ev), ruleset).value == "REJECT"

    # 1 strong alone = 50 -- exactly REVIEW
    ev = _build_evidence("overlap", "absent", "absent", "absent", "absent", "no_tokens", "both_null")
    score = score_pair(ev)
    assert score.confidence_pct == 50.0
    assert score.decision == "REVIEW"
    assert legacy_decide(legacy_classify(ev), ruleset).value == "REVIEW"

    # NID + TIN both match = 100 -- AUTO_LINK (the multiplicity trap this stage exists to catch)
    ev = _build_evidence("absent", "absent", "absent", "overlap", "overlap", "no_tokens", "both_null")
    score = score_pair(ev)
    assert score.confidence_pct == 100.0
    assert score.decision == "AUTO_LINK"
    assert legacy_decide(legacy_classify(ev), ruleset).value == "AUTO_LINK"

    # document mismatch is a hard veto regardless of other evidence
    ev = _build_evidence("overlap", "overlap", "absent", "no_overlap", "absent", "jaccard_1.0_2tok", "full_full_eq")
    score = score_pair(ev)
    assert score.decision == "REJECT"
    assert legacy_decide(legacy_classify(ev), ruleset).value == "REJECT"

    # DOB mismatch (both FULL) is a hard veto regardless of other evidence
    ev = _build_evidence("overlap", "overlap", "absent", "absent", "absent", "jaccard_1.0_2tok", "full_full_neq")
    score = score_pair(ev)
    assert score.decision == "REJECT"
    assert legacy_decide(legacy_classify(ev), ruleset).value == "REJECT"


if __name__ == "__main__":
    test_confidence_model_matches_legacy_oracle_exhaustively()
    test_named_boundary_cases()
    print("confidence enumeration: OK (7290/7290 cases agree)")
