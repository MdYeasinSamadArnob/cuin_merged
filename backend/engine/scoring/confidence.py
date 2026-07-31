"""
CUIN v2 - Confidence-Based Pair Scoring (Stage 2 of the banker-rule-engine migration)

Python implementation of the per-field confidence model
(engine.rules.match_rules.MatchRuleset) -- the replacement for
engine.scoring.tiers.classify()/decide(). Consumes the exact same
evidence dict shape tiers.classify() does (built by
engine.scoring.evidence.load_pair_evidence / the bulk-read dicts both
orchestrators already construct), so no evidence-building code changes
in this stage.

`score_pair()` emits `signals_hit` strings BYTE-IDENTICAL to
tiers.py's for every rule in DEFAULT_MATCH_RULESET, at the seed
values proven equivalent by tests/unit/test_confidence_enumeration.py.
This is deliberate, not cosmetic: engine.determinism.fingerprint_edges
hashes these strings, and engine.decisioning.decision_engine.py
string-prefix-parses them -- diverging the format would silently
break both. A rule with NO legacy equivalent (confidence_pct == 0 at
the seed, e.g. the address rule) emits no signal at all, matching
tiers.py's behavior of not tracking address as a signal.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from engine.rules.match_rules import (
    MatchRule, MatchRuleset, ATTRIBUTE_RAW_COLUMN,
    AGGREGATION_PER_SUB_TYPE,
    VETO_BOTH_PRESENT_NO_OVERLAP, VETO_BOTH_QUALIFIED_AND_DIFFER,
)

# Array/set-valued comparators -- VETO_BOTH_PRESENT_NO_OVERLAP (zero
# shared values) is only a coherent predicate for these; scalar
# comparators (exact, prefix, numeric_tolerance, date_tolerance) use
# VETO_BOTH_QUALIFIED_AND_DIFFER instead. See _score_raw_column_rule.
_ARRAY_COMPARATORS = frozenset({"set_intersect", "token_jaccard", "token_containment"})

# MatchRule.attribute -> the id_type value pair_identifier_evidence
# rows actually carry. Stage 3 replaces this closed mapping with the
# field mapping's attribute names directly.
_ID_TYPE_BY_ATTRIBUTE = {
    "DOCUMENT": "document",
    "MOBILE": "mobile",
    "EMAIL": "email",
    "FULL_ADDRESS": "address",
}
_IDENTIFIER_ATTRIBUTES = frozenset(_ID_TYPE_BY_ATTRIBUTE)

# Attributes with a legacy tiers.py signal format to reproduce exactly.
# FULL_ADDRESS is deliberately absent -- tiers.py never tracks it.
_LEGACY_SIGNAL_ATTRIBUTES = frozenset({"DOCUMENT", "MOBILE", "EMAIL", "NAME", "BIRTH_DATE"})


@dataclass
class Contribution:
    """One rule's effect on this pair, for the audit/UI panel."""
    rule_id: str
    label: str
    matched: bool
    confidence_pct: float  # awarded amount (0 if not matched)
    detail: str = ""

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id, "label": self.label, "matched": self.matched,
            "confidence_pct": self.confidence_pct, "detail": self.detail,
        }


@dataclass
class PairScore:
    confidence_pct: float
    decision: str  # "AUTO_LINK" | "REVIEW" | "REJECT"
    contributions: List[Contribution] = field(default_factory=list)
    vetoes: List[str] = field(default_factory=list)
    signals_hit: List[str] = field(default_factory=list)


def _score_identifier_rule(rule: MatchRule, id_rows: List[dict]) -> tuple:
    """Returns (confidence_delta, contributions, vetoes, signals)."""
    id_type = _ID_TYPE_BY_ATTRIBUTE[rule.attribute]
    rows = [
        r for r in id_rows
        if r["id_type"] == id_type and (rule.sub_type is None or r.get("doc_type") == rule.sub_type)
    ]

    confidence_delta = 0.0
    contributions = []
    vetoes = []
    signals = []
    emit_signal = rule.attribute in _LEGACY_SIGNAL_ATTRIBUTES

    if rule.veto_kind == VETO_BOTH_PRESENT_NO_OVERLAP:
        for r in rows:
            both_present = len(r["values_a"]) > 0 and len(r["values_b"]) > 0
            if both_present and not r["intersection"]:
                vetoes.append(
                    f"document({r.get('doc_type', '?')})_mismatch: "
                    f"{r['values_a']} vs {r['values_b']}"
                )

    matched_rows = [r for r in rows if r["intersection"]]
    if rule.aggregation == AGGREGATION_PER_SUB_TYPE:
        for r in matched_rows:
            confidence_delta += rule.confidence_pct
            contributions.append(Contribution(
                rule_id=rule.rule_id, label=rule.label, matched=True,
                confidence_pct=rule.confidence_pct,
                detail=f"{r.get('doc_type') or id_type}: {r['intersection'][0]}",
            ))
            if emit_signal:
                signals.append(f"{id_type}:{r['intersection'][0]}")
    elif matched_rows:
        confidence_delta += rule.confidence_pct
        contributions.append(Contribution(
            rule_id=rule.rule_id, label=rule.label, matched=True,
            confidence_pct=rule.confidence_pct,
            detail=f"{matched_rows[0]['intersection'][0]}",
        ))
        if emit_signal:
            signals.append(f"{id_type}:{matched_rows[0]['intersection'][0]}")

    if not matched_rows and not contributions:
        contributions.append(Contribution(
            rule_id=rule.rule_id, label=rule.label, matched=False, confidence_pct=0.0,
        ))

    return confidence_delta, contributions, vetoes, signals


def _score_name_rule(rule: MatchRule, name_dob: dict) -> tuple:
    tokens_union = name_dob.get("token_union") or []
    tokens_intersection = name_dob.get("token_intersection") or []
    threshold = rule.params.get("min", 1.0)

    if tokens_union:
        jaccard = len(tokens_intersection) / len(tokens_union)
        if jaccard >= threshold:
            # Legacy string is a literal "1.0" regardless of the
            # configured threshold -- mathematically forced, since
            # jaccard can never exceed 1.0, so a match against a >=1.0
            # threshold only ever occurs when jaccard IS exactly 1.0.
            signal = f"name_rare_token_jaccard_1.0:{tokens_intersection}"
            return rule.confidence_pct, [Contribution(
                rule_id=rule.rule_id, label=rule.label, matched=True,
                confidence_pct=rule.confidence_pct,
                detail=f"jaccard={jaccard:.2f}, shared: {tokens_intersection}",
            )], [], [signal]

    return 0.0, [Contribution(
        rule_id=rule.rule_id, label=rule.label, matched=False, confidence_pct=0.0,
    )], [], []


def _score_dob_rule(rule: MatchRule, name_dob: dict) -> tuple:
    dob_a, dob_b = name_dob.get("dob_a"), name_dob.get("dob_b")
    prec_a, prec_b = name_dob.get("dob_precision_a"), name_dob.get("dob_precision_b")
    require_qualifier = rule.params.get("require_qualifier")
    both_sides_required = rule.params.get("both_sides", True)

    if both_sides_required and not (dob_a and dob_b):
        return 0.0, [Contribution(
            rule_id=rule.rule_id, label=rule.label, matched=False, confidence_pct=0.0,
        )], [], []

    qualified = True
    if require_qualifier:
        qualified = (prec_a == require_qualifier and prec_b == require_qualifier)

    if qualified and dob_a and dob_b:
        if dob_a == dob_b:
            return rule.confidence_pct, [Contribution(
                rule_id=rule.rule_id, label=rule.label, matched=True,
                confidence_pct=rule.confidence_pct, detail=dob_a,
            )], [], [f"dob_full_exact:{dob_a}"]
        elif rule.veto_kind == VETO_BOTH_QUALIFIED_AND_DIFFER:
            return 0.0, [Contribution(
                rule_id=rule.rule_id, label=rule.label, matched=False, confidence_pct=0.0,
                detail=f"{dob_a} vs {dob_b}",
            )], [f"dob_full_precision_mismatch: {dob_a} vs {dob_b}"], []

    return 0.0, [Contribution(
        rule_id=rule.rule_id, label=rule.label, matched=False, confidence_pct=0.0,
    )], [], []


def _score_raw_column_rule(rule: MatchRule, raw_fields: Dict[str, tuple]) -> tuple:
    """
    Scores a bank-added RAW_COLUMN rule -- any schema column, not one
    of the 6 built-ins (see ATTRIBUTE_RAW_COLUMN). `raw_fields`:
    {column_name: (raw_value_a, raw_value_b)} for THIS pair, built
    once per run by the orchestrator from a bulk customer_code ->
    {column: value} fetch over `raw` (only for columns actually
    referenced by an enabled RAW_COLUMN rule). No legacy signal is
    emitted -- there is no tiers.py equivalent for a field a bank
    added themselves, matching the address rule's precedent for a
    rule with nothing to reproduce.
    """
    from engine.rules.comparators import prep_raw_value, evaluate_python

    column = rule.params.get("column")
    is_array = bool(rule.params.get("is_array"))
    raw_a, raw_b = raw_fields.get(column, (None, None))

    value_a = prep_raw_value(raw_a, rule.comparator, is_array)
    value_b = prep_raw_value(raw_b, rule.comparator, is_array)

    present_a = bool(value_a) if isinstance(value_a, list) else value_a is not None
    present_b = bool(value_b) if isinstance(value_b, list) else value_b is not None
    both_present = present_a and present_b

    matched = both_present and evaluate_python(rule.comparator, value_a, value_b, rule.params)

    if matched:
        contributions = [Contribution(
            rule_id=rule.rule_id, label=rule.label, matched=True,
            confidence_pct=rule.confidence_pct, detail=f"{value_a}",
        )]
        confidence_delta = rule.confidence_pct
    else:
        contributions = [Contribution(
            rule_id=rule.rule_id, label=rule.label, matched=False, confidence_pct=0.0,
        )]
        confidence_delta = 0.0

    vetoes = []
    if both_present and not matched:
        if rule.veto_kind == VETO_BOTH_PRESENT_NO_OVERLAP and rule.comparator in _ARRAY_COMPARATORS:
            # A veto here means ZERO shared values, not merely "below
            # the match threshold" -- distinct for token_jaccard, where
            # a partial (nonzero) overlap under threshold should NOT
            # hard-reject, only fail to earn confidence.
            if not (set(value_a) & set(value_b)):
                vetoes.append(f"{column}_no_overlap: {value_a} vs {value_b}")
        elif rule.veto_kind == VETO_BOTH_QUALIFIED_AND_DIFFER and rule.comparator not in _ARRAY_COMPARATORS:
            vetoes.append(f"{column}_mismatch: {value_a} vs {value_b}")

    return confidence_delta, contributions, vetoes, []


def score_pair(evidence: Dict, ruleset: MatchRuleset = None) -> PairScore:
    """
    evidence: {"identifiers": [{"id_type","doc_type","values_a","values_b","intersection"}, ...],
               "name_dob": {"token_union","token_intersection","dob_a","dob_b",
                             "dob_precision_a","dob_precision_b", ...},
               "raw_fields": {column_name: (raw_value_a, raw_value_b), ...}}
    -- "identifiers"/"name_dob" are exactly the shape
    engine.scoring.tiers.classify() consumes; "raw_fields" is new
    (Stage 5.1) and only needs entries for columns an enabled
    RAW_COLUMN rule actually references -- absent/missing keys score
    as "not present" (see _score_raw_column_rule), not an error, so a
    ruleset with no custom fields uses this identically to before.
    """
    from engine.rules.match_rules import DEFAULT_MATCH_RULESET
    ruleset = ruleset or DEFAULT_MATCH_RULESET

    id_rows = evidence.get("identifiers", [])
    name_dob = evidence.get("name_dob", {})
    raw_fields = evidence.get("raw_fields", {})

    confidence = 0.0
    contributions: List[Contribution] = []
    vetoes: List[str] = []
    signals_hit: List[str] = []

    for rule in ruleset.match_rules:
        if not rule.enabled:
            continue

        if rule.attribute in _IDENTIFIER_ATTRIBUTES:
            delta, contribs, rule_vetoes, signals = _score_identifier_rule(rule, id_rows)
        elif rule.attribute == "NAME":
            delta, contribs, rule_vetoes, signals = _score_name_rule(rule, name_dob)
        elif rule.attribute == "BIRTH_DATE":
            delta, contribs, rule_vetoes, signals = _score_dob_rule(rule, name_dob)
        elif rule.attribute == ATTRIBUTE_RAW_COLUMN:
            delta, contribs, rule_vetoes, signals = _score_raw_column_rule(rule, raw_fields)
        else:
            raise ValueError(f"{rule.rule_id}: unknown attribute {rule.attribute!r}")

        confidence += delta
        contributions.extend(contribs)
        vetoes.extend(rule_vetoes)
        signals_hit.extend(signals)

    confidence = min(confidence, ruleset.confidence_cap)

    if vetoes:
        decision = "REJECT"
    elif confidence >= ruleset.auto_link_min_confidence:
        decision = "AUTO_LINK"
    elif confidence >= ruleset.review_min_confidence:
        decision = "REVIEW"
    else:
        decision = "REJECT"

    return PairScore(
        confidence_pct=confidence, decision=decision,
        contributions=contributions, vetoes=vetoes, signals_hit=signals_hit,
    )
