"""
CUIN v2 - Tiered Evidence Classification (Ruleset v2, Layer 4)

Classifies a pair's evidence into STRONG/MEDIUM/WEAK signal counts plus
any VETO, per policies/ruleset_v2.yaml. This is the module that
replaces Splink's learned match_probability with a fixed, auditable
rule.

CRITICAL DESIGN NOTE (do not regress this): name similarity is judged
by rare-token Jaccard, NOT whole-string Jaro-Winkler. Measured directly
against the reported false-positive cluster:

    MD SHARIFUL ISLAM / MD SHAHIDUL ISLAM   JW=0.9529  (passes a 0.92 gate)
    MD SHARIFUL ISLAM / MD SAIFUL ISLAM      JW=0.9765  (passes a 0.92 gate)
    SHARIFUL ISLAM / SHAHIDUL ISLAM (honorific stripped)  JW=0.9333 (still passes)

A Jaro-Winkler threshold reproduces the exact bug being fixed, because
the shared token "ISLAM" inflates the score. After dropping
frequency-suppressed tokens (MD, ISLAM fall out via Layer 2), the rare
token sets are {SHARIFUL}, {SHAHIDUL}, {SAIFUL} -- Jaccard 0.0 for
every pair -- correctly producing no name signal at all.
"""

from dataclasses import dataclass, field
from typing import Dict, List

from engine.ruleset.config import RulesetConfig, get_default_ruleset
from engine.structures import MatchDecision


@dataclass
class TierResult:
    strong_count: int
    medium_name: bool
    medium_dob: bool
    vetoes: List[str] = field(default_factory=list)
    signals_hit: List[str] = field(default_factory=list)


def classify(evidence: Dict) -> TierResult:
    """
    evidence: the dict returned by engine.scoring.evidence.load_pair_evidence
    """
    strong_count = 0
    signals_hit: List[str] = []
    vetoes: List[str] = []

    documents = [e for e in evidence["identifiers"] if e["id_type"] == "document"]
    mobiles = [e for e in evidence["identifiers"] if e["id_type"] == "mobile"]
    emails = [e for e in evidence["identifiers"] if e["id_type"] == "email"]

    # --- VETO: same-type document present on both sides, no intersection ---
    for doc in documents:
        both_present = len(doc["values_a"]) > 0 and len(doc["values_b"]) > 0
        if both_present and not doc["intersection"]:
            vetoes.append(
                f"document({doc.get('doc_type', '?')})_mismatch: "
                f"{doc['values_a']} vs {doc['values_b']}"
            )
        elif doc["intersection"]:
            strong_count += 1
            signals_hit.append(f"document:{doc['intersection'][0]}")

    for mob in mobiles:
        if mob["intersection"]:
            strong_count += 1
            signals_hit.append(f"mobile:{mob['intersection'][0]}")

    for eml in emails:
        if eml["intersection"]:
            strong_count += 1
            signals_hit.append(f"email:{eml['intersection'][0]}")

    nd = evidence["name_dob"]

    # --- MEDIUM: rare-token Jaccard == 1.0 (see module docstring) ---
    medium_name = False
    tokens_union = nd.get("token_union") or []
    tokens_intersection = nd.get("token_intersection") or []
    if tokens_union:
        jaccard = len(tokens_intersection) / len(tokens_union)
        if jaccard == 1.0:
            medium_name = True
            signals_hit.append(f"name_rare_token_jaccard_1.0:{tokens_intersection}")

    # --- MEDIUM / VETO: full-precision DOB ---
    medium_dob = False
    dob_a, dob_b = nd.get("dob_a"), nd.get("dob_b")
    prec_a, prec_b = nd.get("dob_precision_a"), nd.get("dob_precision_b")
    if dob_a and dob_b:
        both_full = prec_a == "FULL" and prec_b == "FULL"
        if both_full:
            if dob_a == dob_b:
                medium_dob = True
                signals_hit.append(f"dob_full_exact:{dob_a}")
            else:
                vetoes.append(f"dob_full_precision_mismatch: {dob_a} vs {dob_b}")

    return TierResult(
        strong_count=strong_count,
        medium_name=medium_name,
        medium_dob=medium_dob,
        vetoes=vetoes,
        signals_hit=signals_hit,
    )


def decide(tier: TierResult, ruleset: RulesetConfig = None) -> MatchDecision:
    """
    Pure rule application. Name similarity ALONE (medium_name with
    strong_count == 0) never creates a link or a review item -- it is
    display-only corroboration, per ruleset.name_alone_gates_nothing.
    """
    ruleset = ruleset or get_default_ruleset()

    if tier.vetoes:
        return MatchDecision.REJECT

    if tier.strong_count >= ruleset.auto_link_min_strong_only:
        return MatchDecision.AUTO_LINK

    if tier.strong_count >= ruleset.auto_link_min_strong_with_name and tier.medium_name:
        return MatchDecision.AUTO_LINK

    if tier.strong_count >= ruleset.review_min_strong:
        return MatchDecision.REVIEW

    if tier.medium_name and tier.medium_dob:
        return MatchDecision.REVIEW

    return MatchDecision.REJECT
