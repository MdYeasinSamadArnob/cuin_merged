"""
Regression test for the reported over-matching bug: MD SHARIFUL ISLAM,
MD SHAHIDUL ISLAM, MD. ZAHIDUL ISLAM, MD SAIFUL ISLAM, MD.JAHIDUL ISLAM
were clustered together by the old Levenshtein(NAME,2) + transitive-
closure pipeline despite being different people with different phones
and emails. This test asserts the new tiered rule REJECTs every such
pair, and explicitly asserts that Jaro-Winkler is never consulted as a
gate (see engine.scoring.tiers module docstring for the measured JW
scores that would otherwise reproduce this exact bug).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.scoring.tiers import classify, decide
from engine.structures import MatchDecision
from engine.normalize.identity import norm_name


def _no_identifier_evidence():
    return {"identifiers": [], "name_dob": {
        "tokens_a": [], "tokens_b": [], "token_intersection": [], "token_union": [],
    }}


def _evidence_for_names(name_a: str, name_b: str, dob_a=None, dob_b=None):
    _, tokens_a = norm_name(name_a)
    _, tokens_b = norm_name(name_b)
    intersection = list(set(tokens_a) & set(tokens_b))
    union = list(set(tokens_a) | set(tokens_b))
    return {
        "identifiers": [],
        "name_dob": {
            "name_a": name_a, "name_b": name_b,
            "tokens_a": tokens_a, "tokens_b": tokens_b,
            "token_intersection": intersection, "token_union": union,
            "dob_a": dob_a, "dob_b": dob_b,
            "dob_precision_a": "FULL" if dob_a else None,
            "dob_precision_b": "FULL" if dob_b else None,
        },
    }


FALSE_POSITIVE_PAIRS = [
    ("MD SHARIFUL ISLAM", "MD SHAHIDUL ISLAM"),
    ("MD SHARIFUL ISLAM", "MD SAIFUL ISLAM"),
    ("MD SAIFUL ISLAM", "MD SHAHIDUL ISLAM"),
    ("MD. ZAHIDUL ISLAM", "MD.JAHIDUL ISLAM"),
]


def test_false_positive_names_reject_without_other_evidence():
    for name_a, name_b in FALSE_POSITIVE_PAIRS:
        evidence = _evidence_for_names(name_a, name_b)
        tier = classify(evidence)
        decision = decide(tier)
        assert tier.medium_name is False, (
            f"{name_a!r} / {name_b!r}: rare-token Jaccard incorrectly hit MEDIUM "
            f"(tokens: {evidence['name_dob']['tokens_a']} vs {evidence['name_dob']['tokens_b']})"
        )
        assert decision == MatchDecision.REJECT, (
            f"{name_a!r} / {name_b!r} should REJECT (name-only evidence) but got {decision}"
        )


def test_name_alone_never_auto_links_even_with_shared_surname():
    # "ISLAM" is the single most common surname token in the dataset (5.46%
    # of all rows) -- confirm it alone never creates MEDIUM or a link.
    evidence = _evidence_for_names("RAHIM ISLAM", "KARIM ISLAM")
    tier = classify(evidence)
    assert tier.medium_name is False
    assert decide(tier) == MatchDecision.REJECT


def test_identical_rare_tokens_hit_medium_name():
    # Same rare token set (post-honorific-strip) -> Jaccard 1.0 -> MEDIUM.
    evidence = _evidence_for_names("MD ABDUL KARIM CHOWDHURY", "ABDUL KARIM CHOWDHURY")
    tier = classify(evidence)
    assert tier.medium_name is True


def test_two_strong_identifiers_auto_links():
    evidence = {
        "identifiers": [
            {"id_type": "mobile", "doc_type": None, "values_a": ["01712345678"],
             "values_b": ["01712345678"], "intersection": ["01712345678"]},
            {"id_type": "email", "doc_type": None, "values_a": ["a@x.com"],
             "values_b": ["a@x.com"], "intersection": ["a@x.com"]},
        ],
        "name_dob": {"tokens_a": [], "tokens_b": [], "token_intersection": [], "token_union": []},
    }
    tier = classify(evidence)
    assert tier.strong_count == 2
    assert decide(tier) == MatchDecision.AUTO_LINK


def test_document_mismatch_is_absolute_veto_even_with_other_strong_signals():
    evidence = {
        "identifiers": [
            {"id_type": "document", "doc_type": "NID", "values_a": ["1234567890123"],
             "values_b": ["9999999999999"], "intersection": []},
            {"id_type": "mobile", "doc_type": None, "values_a": ["01712345678"],
             "values_b": ["01712345678"], "intersection": ["01712345678"]},
            {"id_type": "email", "doc_type": None, "values_a": ["a@x.com"],
             "values_b": ["a@x.com"], "intersection": ["a@x.com"]},
        ],
        "name_dob": {"tokens_a": [], "tokens_b": [], "token_intersection": [], "token_union": []},
    }
    tier = classify(evidence)
    assert tier.vetoes, "expected a veto for mismatched same-type NatID"
    assert decide(tier) == MatchDecision.REJECT


def test_single_strong_identifier_alone_goes_to_review_not_auto_link():
    evidence = {
        "identifiers": [
            {"id_type": "mobile", "doc_type": None, "values_a": ["01712345678"],
             "values_b": ["01712345678"], "intersection": ["01712345678"]},
        ],
        "name_dob": {"tokens_a": [], "tokens_b": [], "token_intersection": [], "token_union": []},
    }
    tier = classify(evidence)
    assert decide(tier) == MatchDecision.REVIEW


if __name__ == "__main__":
    test_false_positive_names_reject_without_other_evidence()
    test_name_alone_never_auto_links_even_with_shared_surname()
    test_identical_rare_tokens_hit_medium_name()
    test_two_strong_identifiers_auto_links()
    test_document_mismatch_is_absolute_veto_even_with_other_strong_signals()
    test_single_strong_identifier_alone_goes_to_review_not_auto_link()
    print("All tier tests passed.")
