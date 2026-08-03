"""
CUIN v2 - Evidence Building (Ruleset v2, Layer 4)

For every candidate pair, fetches per-field evidence -- built by
engine.scoring.evidence_dialect.build_pair_evidence, dialect-portable,
against Doris -- by set-intersection of each record's validated,
non-suppressed identifier values. Records the literal intersecting
values in evidence, so the audit row shows concrete proof, not just a
boolean.

Reuses engine.structures.FieldEvidence unchanged (the shape
routes_matches.py already serializes).
"""

from typing import Dict, List

from engine.structures import FieldEvidence


def load_pair_evidence(con, a_key: str, b_key: str) -> Dict:
    """
    Fetch the full evidence bundle for a single pair, used both by
    classify() in tiers.py and by the /matches/{pair_id} API to render
    FieldEvidence rows.
    """
    id_rows = con.execute("""
        SELECT id_type, doc_type, values_a, values_b, intersection
        FROM pair_identifier_evidence
        WHERE a_key = ? AND b_key = ?
    """, [a_key, b_key]).fetchall()

    name_dob = con.execute("""
        SELECT name_a, name_b, tokens_a, tokens_b, token_intersection, token_union,
               dob_a, dob_b, dob_precision_a, dob_precision_b
        FROM pair_name_dob_evidence
        WHERE a_key = ? AND b_key = ?
    """, [a_key, b_key]).fetchone()

    return {
        "identifiers": [
            {
                "id_type": r[0],
                "doc_type": r[1],
                "values_a": r[2] or [],
                "values_b": r[3] or [],
                "intersection": r[4] or [],
            }
            for r in id_rows
        ],
        "name_dob": {
            "name_a": name_dob[0] if name_dob else None,
            "name_b": name_dob[1] if name_dob else None,
            "tokens_a": name_dob[2] if name_dob else [],
            "tokens_b": name_dob[3] if name_dob else [],
            "token_intersection": name_dob[4] if name_dob else [],
            "token_union": name_dob[5] if name_dob else [],
            "dob_a": name_dob[6] if name_dob else None,
            "dob_b": name_dob[7] if name_dob else None,
            "dob_precision_a": name_dob[8] if name_dob else None,
            "dob_precision_b": name_dob[9] if name_dob else None,
        } if name_dob else {"tokens_a": [], "tokens_b": [], "token_intersection": [], "token_union": []},
    }


def evidence_to_field_evidence(evidence: Dict) -> List[FieldEvidence]:
    """Convert the raw evidence bundle into the wire-format FieldEvidence list."""
    out: List[FieldEvidence] = []

    for id_ev in evidence["identifiers"]:
        has_match = len(id_ev["intersection"]) > 0
        out.append(FieldEvidence(
            field_name=id_ev["id_type"],
            value_a=",".join(id_ev["values_a"]) if id_ev["values_a"] else None,
            value_b=",".join(id_ev["values_b"]) if id_ev["values_b"] else None,
            comparison_type="exact_set_intersection",
            similarity_score=1.0 if has_match else 0.0,
            match_weight=1.0 if has_match else 0.0,
            explanation=(
                f"Matched on {','.join(id_ev['intersection'])}" if has_match
                else "No intersecting validated value"
            ),
        ))

    nd = evidence["name_dob"]
    if nd.get("token_union"):
        jaccard = (
            len(nd["token_intersection"]) / len(nd["token_union"])
            if nd["token_union"] else 0.0
        )
        out.append(FieldEvidence(
            field_name="name",
            value_a=nd.get("name_a"),
            value_b=nd.get("name_b"),
            comparison_type="rare_token_jaccard",
            similarity_score=jaccard,
            match_weight=jaccard,
            explanation=(
                f"Rare-token overlap: {nd['token_intersection']} (Jaccard={jaccard:.2f})"
                if jaccard > 0 else "No shared rare name tokens"
            ),
        ))

    if nd.get("dob_a") and nd.get("dob_b"):
        equal = nd["dob_a"] == nd["dob_b"]
        both_full = nd.get("dob_precision_a") == "FULL" and nd.get("dob_precision_b") == "FULL"
        out.append(FieldEvidence(
            field_name="dob",
            value_a=nd["dob_a"],
            value_b=nd["dob_b"],
            comparison_type="exact" if both_full else "year_only",
            similarity_score=1.0 if equal else 0.0,
            match_weight=1.0 if (equal and both_full) else 0.0,
            explanation=(
                f"DOB {'exact match' if equal else 'differs'} "
                f"({'full precision' if both_full else 'year-only precision'})"
            ),
        ))

    return out
