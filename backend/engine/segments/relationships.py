"""
CUIN v2 - Cross-segment relationship detection (Stage 5)

When two records land in different segments (e.g. one COMPANY, one
INDIVIDUAL -- see engine.segments.classifier), they are never scored
for identity merge: engine.scoring.confidence.score_pair() answers
"how likely is it these are the SAME entity", which is a category
error across segments. But the pair still exists because blocking
found real shared evidence between them (same phone, same document,
etc.), and that connection is exactly what a bank often needs to
trace -- a person's personal mobile number also registered against a
business they own, for instance. This module extracts which fields
actually matched (re-checking the evidence tables, not just trusting
that blocking found *something*) so that connection can be persisted
as a traceable relationship (db/migrations/004_entity_relationships.sql)
instead of silently discarded.
"""

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class RelationshipEvidence:
    field: str
    value: str

    def to_dict(self) -> dict:
        return {"field": self.field, "value": self.value}


def detect_relationship_evidence(evidence: Dict) -> List[RelationshipEvidence]:
    """
    evidence: the same {"identifiers": [...], "name_dob": {...}} shape
    engine.scoring.confidence.score_pair() and engine.scoring.tiers.
    classify() both consume. Returns every field that actually shows a
    non-empty match -- empty when blocking matched them on a field
    that doesn't independently confirm in the evidence tables (e.g. a
    since-suppressed identifier), in which case no relationship should
    be recorded for this pair.
    """
    out: List[RelationshipEvidence] = []

    for id_ev in evidence.get("identifiers", []):
        if id_ev.get("intersection"):
            out.append(RelationshipEvidence(field=id_ev["id_type"], value=str(id_ev["intersection"][0])))

    nd = evidence.get("name_dob", {})
    tokens_union = nd.get("token_union") or []
    tokens_intersection = nd.get("token_intersection") or []
    if tokens_union and tokens_intersection:
        jaccard = len(tokens_intersection) / len(tokens_union)
        if jaccard >= 1.0:
            out.append(RelationshipEvidence(field="name", value=",".join(tokens_intersection)))

    if nd.get("dob_a") and nd.get("dob_b") and nd["dob_a"] == nd["dob_b"]:
        out.append(RelationshipEvidence(field="dob", value=nd["dob_a"]))

    return out
