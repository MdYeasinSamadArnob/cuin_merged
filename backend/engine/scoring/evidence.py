"""
CUIN v2 - Evidence Building (Ruleset v2, Layer 4)

For every candidate pair, computes per-field evidence by set-intersection
of each record's validated, non-suppressed identifier values. Records
the literal intersecting values in evidence, so the audit row shows
concrete proof, not just a boolean.

Reuses engine.structures.FieldEvidence unchanged (the shape
routes_matches.py already serializes).

build_pair_evidence() is DuckDB-only, used only by the (still-live,
pending removal) pipeline.duckdb_orchestrator.DuckDBPipelineOrchestrator
and its test oracles -- see engine.scoring.evidence_dialect for the
dialect-portable twin Doris actually uses. load_pair_evidence() and
evidence_to_field_evidence() below are engine-agnostic and load-bearing
for both engines (engine.ports.run_session, pipeline.parallel_scoring,
api/routes_workbench.py, api/routes_public_identity_api.py) -- keep
those two regardless of build_pair_evidence's fate.
"""

import duckdb
from typing import Dict, List

from engine.structures import FieldEvidence


def build_pair_evidence(con: duckdb.DuckDBPyConnection) -> None:
    """
    Requires `candidate_pairs`, `identifiers`, `customer_scalars` already
    built. Produces `pair_evidence`:
        a_key, b_key, id_type, values_a (list), values_b (list),
        intersection (list), has_intersection (bool)
    for mobile/email/document, plus name/dob comparison columns.
    """
    # Materialized as a real TABLE, not a WITH-clause CTE: this CTE is
    # referenced twice below (once as `a`, once as `b`). Measured on the
    # full 1.5M-row dataset (~550K candidate pairs): as an inline CTE this
    # query did not finish in 120s; materializing agg_a as a table first
    # (giving the optimizer real row-count statistics to plan the join
    # around) brings both steps under 1s combined. Likely cause: without
    # materialization, DuckDB has no cardinality estimate for the
    # aggregated relation and either re-evaluates it per reference or
    # picks a poor join order.
    # values_/intersection/union are all list_sort()-ed to match
    # evidence_dialect.py's array_sort() -- verified live (this session)
    # that DuckDB's unsorted list_intersect/list_distinct is genuinely
    # hash-ordered, not merely differently-but-deterministically ordered:
    # array order leaks into engine.scoring.tiers.classify()'s signal
    # strings (e.g. f"document:{doc['intersection'][0]}"), which
    # engine.determinism.fingerprint_edges hashes -- so an unsorted vs
    # sorted mismatch here silently produces a DIFFERENT
    # output_fingerprint between the DuckDB and Doris pipelines despite
    # identical decisions. See tests/unit/test_evidence_dialect_parity.py.
    con.execute("""
        CREATE OR REPLACE TABLE _agg_identifiers AS
        SELECT customer_code, id_type, doc_type, list_sort(list(DISTINCT value_norm)) AS values_
        FROM identifiers
        WHERE is_valid AND NOT is_suppressed
        GROUP BY customer_code, id_type, doc_type
    """)

    con.execute("""
        CREATE OR REPLACE TABLE pair_identifier_evidence AS
        SELECT
            p.a_key,
            p.b_key,
            a.id_type,
            a.doc_type,
            a.values_ AS values_a,
            b.values_ AS values_b,
            list_sort(list_intersect(a.values_, b.values_)) AS intersection
        FROM candidate_pairs p
        JOIN _agg_identifiers a ON a.customer_code = p.a_key
        JOIN _agg_identifiers b ON b.customer_code = p.b_key AND b.id_type = a.id_type
            AND (a.id_type != 'document' OR a.doc_type = b.doc_type)
    """)

    con.execute("""
        CREATE OR REPLACE TABLE pair_name_dob_evidence AS
        SELECT
            p.a_key,
            p.b_key,
            sa.name_norm AS name_a,
            sb.name_norm AS name_b,
            sa.name_tokens AS tokens_a,
            sb.name_tokens AS tokens_b,
            list_sort(list_intersect(sa.name_tokens, sb.name_tokens)) AS token_intersection,
            -- NOT sorted, matching evidence_dialect.py's dialect.array_union_distinct()
            -- (dedup only, no sort, on both engines) -- token_union's order affects
            -- neither a signal string (only token_intersection is embedded in one,
            -- see tiers.py:84) nor any decision logic (only array_size(token_union)
            -- is read, for the jaccard denominator), so canonicalizing it would be
            -- pure churn. Sorting it here would in fact BREAK parity with
            -- evidence_dialect.py, which deliberately leaves it unsorted.
            list_distinct(list_concat(sa.name_tokens, sb.name_tokens)) AS token_union,
            sa.dob_iso AS dob_a,
            sb.dob_iso AS dob_b,
            sa.dob_precision AS dob_precision_a,
            sb.dob_precision AS dob_precision_b
        FROM candidate_pairs p
        JOIN customer_scalars sa ON sa.customer_code = p.a_key
        JOIN customer_scalars sb ON sb.customer_code = p.b_key
    """)


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
