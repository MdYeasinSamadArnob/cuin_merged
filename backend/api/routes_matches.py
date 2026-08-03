"""
CUIN v2 Control Plane - Matches API Routes

Endpoints for match scores and decisions.
"""

import os
from typing import Optional

from fastapi import APIRouter, HTTPException

from services.run_service import get_run_service
from engine.structures import MatchDecision
from api import doris_run_reader

router = APIRouter()


# ============================================
# Routes
# ============================================

@router.get("/run/{run_id}/scores")
async def list_match_scores(
    run_id: str,
    page: int = 1,
    page_size: int = 50,
    min_score: Optional[float] = None,
    max_score: Optional[float] = None
) -> dict:
    """
    List match scores for a run.

    Reads from the run's own Doris database (pair_decisions/
    pair_contributions) when one exists -- see api/doris_run_reader.py's
    module docstring for why (Stage 6: candidate_pairs/match_scores/
    match_decisions are no longer written to Postgres at all, and this
    works regardless of whether the run's orchestrator instance is
    still alive in RunService's registry). Falls back to the in-memory
    orchestrator for non-Doris-backed runs (duckdb/spark engines,
    which still populate self._scores the original way).
    """
    run_service = get_run_service()

    if doris_run_reader.run_has_doris_data(run_id):
        result = doris_run_reader.fetch_scores(run_id, page, page_size, min_score, max_score)
        return {**result, "page": page, "page_size": page_size}

    orchestrator = run_service.get_orchestrator(run_id)

    if not orchestrator:
        run = run_service.get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return {
            "scores": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "message": "Run not yet executed or scores not available"
        }

    scores = list(orchestrator.get_scores().values())

    # Filter by score range
    if min_score is not None:
        scores = [s for s in scores if s.score >= min_score]
    if max_score is not None:
        scores = [s for s in scores if s.score <= max_score]

    # Sort by score descending
    scores.sort(key=lambda x: x.score, reverse=True)

    # Paginate
    total = len(scores)
    start = (page - 1) * page_size
    end = start + page_size
    paged = scores[start:end]

    return {
        "scores": [
            {
                "pair_id": s.pair_id,
                "a_key": s.a_key,
                "b_key": s.b_key,
                "score": s.score,
                "signals_hit": s.signals_hit,
                "hard_conflicts": s.hard_conflicts,
            }
            for s in paged
        ],
        "total": total,
        "page": page,
        "page_size": page_size
    }


@router.get("/run/{run_id}/decisions")
async def list_decisions(
    run_id: str,
    decision: Optional[str] = None,
    page: int = 1,
    page_size: int = 50
) -> dict:
    """
    List decisions for a run. See list_match_scores' docstring -- same
    Doris-first, in-memory-orchestrator-fallback pattern.
    """
    run_service = get_run_service()

    if doris_run_reader.run_has_doris_data(run_id):
        dec_filter = None
        if decision:
            try:
                dec_filter = MatchDecision(decision.upper()).value
            except ValueError:
                pass
        result = doris_run_reader.fetch_scores(run_id, page, page_size, decision=dec_filter)
        # fetch_scores' rows already carry pair_id/a_key/b_key/score/
        # decision/signals_hit/hard_conflicts -- exactly this route's shape.
        return {
            "decisions": result["scores"],
            "total": result["total"],
            "page": page,
            "page_size": page_size,
        }

    orchestrator = run_service.get_orchestrator(run_id)

    if not orchestrator:
        run = run_service.get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Run not found")
        return {
            "decisions": [],
            "total": 0,
            "page": page,
            "page_size": page_size,
            "message": "Run not yet executed or decisions not available"
        }
    
    decisions = orchestrator.get_decisions()
    scores = orchestrator.get_scores()
    
    # Build decision list with score info
    decision_list = []
    for pair_id, dec in decisions.items():
        score_obj = scores.get(pair_id)
        if score_obj:
            decision_list.append({
                "pair_id": pair_id,
                "a_key": score_obj.a_key,
                "b_key": score_obj.b_key,
                "score": score_obj.score,
                "decision": dec.value,
                "signals_hit": score_obj.signals_hit,
                "hard_conflicts": score_obj.hard_conflicts,
            })
    
    # Filter by decision type
    if decision:
        try:
            dec_enum = MatchDecision(decision.upper())
            decision_list = [d for d in decision_list if d['decision'] == dec_enum.value]
        except ValueError:
            pass
    
    # Sort by score descending
    decision_list.sort(key=lambda x: x['score'], reverse=True)
    
    # Paginate
    total = len(decision_list)
    start = (page - 1) * page_size
    end = start + page_size
    
    return {
        "decisions": decision_list[start:end],
        "total": total,
        "page": page,
        "page_size": page_size
    }


@router.get("/run/{run_id}/summary")
async def get_decision_summary(run_id: str) -> dict:
    """
    Get decision summary for a run. See list_match_scores' docstring
    -- same Doris-first, in-memory-orchestrator-fallback pattern (the
    run.counters fallback beneath that, unchanged, still covers the
    case where NEITHER Doris data nor a live orchestrator exists).
    """
    run_service = get_run_service()
    run = run_service.get_run(run_id)

    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    if doris_run_reader.run_has_doris_data(run_id):
        counts = doris_run_reader.fetch_decision_summary(run_id)
        auto_link = counts.get("AUTO_LINK", 0)
        review = counts.get("REVIEW", 0)
        reject = counts.get("REJECT", 0)
        total = auto_link + review + reject
        return {
            "run_id": run_id,
            "status": run.status.value,
            "auto_link": auto_link,
            "review": review,
            "reject": reject,
            "total": total,
            "auto_link_pct": (auto_link / total * 100) if total else 0,
            "review_pct": (review / total * 100) if total else 0,
        }

    orchestrator = run_service.get_orchestrator(run_id)

    if not orchestrator:
        return {
            "run_id": run_id,
            "status": run.status.value,
            "auto_link": run.counters.auto_links,
            "review": run.counters.review_items,
            "reject": run.counters.rejected,
            "total": run.counters.pairs_scored,
        }

    decisions = orchestrator.get_decisions()

    auto_link = sum(1 for d in decisions.values() if d == MatchDecision.AUTO_LINK)
    review = sum(1 for d in decisions.values() if d == MatchDecision.REVIEW)
    reject = sum(1 for d in decisions.values() if d == MatchDecision.REJECT)

    return {
        "run_id": run_id,
        "status": run.status.value,
        "auto_link": auto_link,
        "review": review,
        "reject": reject,
        "total": len(decisions),
        "auto_link_pct": (auto_link / len(decisions) * 100) if decisions else 0,
        "review_pct": (review / len(decisions) * 100) if decisions else 0,
    }


@router.get("/scores")
async def list_match_scores_query(
    run_id: str,
    page: int = 1,
    page_size: int = 50,
    min_score: Optional[float] = None,
    decision: Optional[str] = None,
) -> dict:
    """
    List match scores for a run using query parameters.
    Reads from the run-specific scoring CSV saved after each pipeline run.

    `decision` filters to AUTO_LINK/REVIEW/REJECT when the CSV has a
    `decision` column (written by pipeline.duckdb_orchestrator since the
    Ruleset v2 rework -- older CSVs without it ignore this filter rather
    than erroring, so this stays backward compatible).
    """
    import pandas as pd

    csv_path = f"data/runs/{run_id}_scores.csv"

    if not os.path.exists(csv_path):
        # Fallback: try the global scoring_results.csv if it exists
        csv_path = "scoring_results.csv"
        if not os.path.exists(csv_path):
            return {"scores": [], "total": 0, "page": page, "page_size": page_size}

    try:
        # dtype=str on the code columns is required: CUSTOMER_CODEs are
        # all-digit strings (e.g. "00000301"), and pandas' default type
        # inference silently parses an all-digit column as an integer,
        # stripping the leading zeros that make the code the correct
        # length for lookup against the source parquet.
        df = pd.read_csv(csv_path, dtype={"CUSTOMER_CODE_l": str, "CUSTOMER_CODE_r": str})
        has_decision_col = "decision" in df.columns

        if min_score is not None and "match_probability" in df.columns:
            df = df[df["match_probability"] >= min_score]

        if decision and has_decision_col:
            df = df[df["decision"] == decision.upper()]

        if "match_probability" in df.columns:
            df = df.sort_values("match_probability", ascending=False)

        total = len(df)
        start = (page - 1) * page_size
        end = start + page_size
        paged = df.iloc[start:end]

        def _split(value) -> list:
            if not value or (isinstance(value, float) and pd.isna(value)):
                return []
            return [v for v in str(value).split(";") if v]

        scores = []
        for _, row in paged.iterrows():
            id_l = str(row.get("CUSTOMER_CODE_l", row.get("unique_id_l", "")))
            id_r = str(row.get("CUSTOMER_CODE_r", row.get("unique_id_r", "")))
            scores.append({
                "pair_id": f"{id_l}:{id_r}",
                "a_key": id_l,
                "b_key": id_r,
                "score": float(row.get("match_probability", 0)),
                "decision": row.get("decision", "") if has_decision_col else "",
                "signals_hit": _split(row.get("signals_hit")),
                "hard_conflicts": _split(row.get("hard_conflicts")),
            })

        return {"scores": scores, "total": total, "page": page, "page_size": page_size}

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to read scores CSV: {e}")
        return {"scores": [], "total": 0, "page": page, "page_size": page_size}


def _load_two_customer_rows(codes: list) -> dict:
    """
    On-demand lookup of raw parquet rows for exactly the two requested
    customer codes. Deterministic and always correct regardless of
    which run_id (if any) is currently selected in the UI -- the
    pipeline is a pure function of this same source data, so evidence
    computed fresh here matches what any run would have computed.
    """
    import pyarrow.parquet as pq
    from pipeline.duckdb_orchestrator import PARQUET_PATH

    table = pq.read_table(
        PARQUET_PATH,
        columns=["CUSTOMER_CODE", "NAME", "BIRTH_DATE", "MOBILE", "EMAIL", "DOCUMENT", "FULL_ADDRESS"],
        filters=[("CUSTOMER_CODE", "in", codes)],
    )

    return {
        r["CUSTOMER_CODE"]: {
            "CUSTOMER_CODE": r["CUSTOMER_CODE"], "NAME": r["NAME"], "BIRTH_DATE": r["BIRTH_DATE"],
            "MOBILE": r["MOBILE"] or [], "EMAIL": r["EMAIL"] or [],
            "DOCUMENT": r["DOCUMENT"] or [], "FULL_ADDRESS": r["FULL_ADDRESS"] or [],
        }
        for r in table.to_pylist()
    }


def _build_display_record(raw: dict) -> dict:
    from engine.normalize.identity import norm_name, norm_dob, norm_mobile_bd, norm_email, norm_address

    name_norm, _ = norm_name(raw.get("NAME"))
    dob_iso, _ = norm_dob(raw.get("BIRTH_DATE"))
    mobile = next((norm_mobile_bd(m)[0] for m in raw.get("MOBILE", []) if norm_mobile_bd(m)[0]), None)
    email = next((norm_email(e)[0] for e in raw.get("EMAIL", []) if norm_email(e)[0]), None)
    address = next((norm_address(a) for a in raw.get("FULL_ADDRESS", []) if norm_address(a)), None)

    code = raw["CUSTOMER_CODE"]
    return {
        "customer_key": code, "source_customer_id": code,
        "name": raw.get("NAME") or "", "name_norm": name_norm or "",
        "email": email or "", "email_norm": email or "",
        "phone": mobile or "", "phone_norm": mobile or "",
        "dob": raw.get("BIRTH_DATE") or "", "dob_norm": dob_iso or "",
        "address": (raw.get("FULL_ADDRESS") or [""])[0] if raw.get("FULL_ADDRESS") else "",
        "address_norm": address or "",
        "natid": (raw.get("DOCUMENT") or [""])[0] if raw.get("DOCUMENT") else "",
        "status": "ACT",
    }


def _compute_pair_evidence_fresh(raw_a: dict, raw_b: dict):
    """
    Computes evidence + tier classification for exactly one pair using
    the same rules as the bulk pipeline (engine.normalize.identity,
    engine.scoring.tiers), without needing any persisted run artifact.
    This is what makes match details available for EVERY pair --
    AUTO_LINK, REVIEW, or REJECT, from any run, even runs whose
    in-memory orchestrator or file artifacts no longer exist.
    """
    from engine.normalize.identity import norm_name, norm_dob, norm_mobile_bd, norm_email, norm_address, parse_document
    from engine.scoring.tiers import classify, decide
    from engine.structures import FieldEvidence

    def identifiers_for(raw, kind):
        if kind == "mobile":
            return {v for v in (norm_mobile_bd(m)[0] for m in raw.get("MOBILE", [])) if v}
        if kind == "email":
            return {v for v in (norm_email(e)[0] for e in raw.get("EMAIL", [])) if v}
        return set()

    def documents_for(raw):
        out = {}
        for d in raw.get("DOCUMENT", []):
            dtype, value, _ = parse_document(d)
            if value:
                out.setdefault(dtype, set()).add(value)
        return out

    name_a, tokens_a = norm_name(raw_a.get("NAME"))
    name_b, tokens_b = norm_name(raw_b.get("NAME"))
    dob_a, prec_a = norm_dob(raw_a.get("BIRTH_DATE"))
    dob_b, prec_b = norm_dob(raw_b.get("BIRTH_DATE"))

    docs_a, docs_b = documents_for(raw_a), documents_for(raw_b)
    id_evidence = []
    for kind in ("mobile", "email"):
        vals_a, vals_b = identifiers_for(raw_a, kind), identifiers_for(raw_b, kind)
        id_evidence.append({
            "id_type": kind, "doc_type": None,
            "values_a": sorted(vals_a), "values_b": sorted(vals_b),
            "intersection": sorted(vals_a & vals_b),
        })
    for dtype in set(docs_a) | set(docs_b):
        vals_a, vals_b = docs_a.get(dtype, set()), docs_b.get(dtype, set())
        id_evidence.append({
            "id_type": "document", "doc_type": dtype,
            "values_a": sorted(vals_a), "values_b": sorted(vals_b),
            "intersection": sorted(vals_a & vals_b),
        })

    tokens_a, tokens_b = tokens_a or [], tokens_b or []
    evidence = {
        "identifiers": id_evidence,
        "name_dob": {
            "name_a": name_a, "name_b": name_b,
            "tokens_a": tokens_a, "tokens_b": tokens_b,
            "token_intersection": sorted(set(tokens_a) & set(tokens_b)),
            "token_union": sorted(set(tokens_a) | set(tokens_b)),
            "dob_a": dob_a, "dob_b": dob_b,
            "dob_precision_a": prec_a, "dob_precision_b": prec_b,
        },
    }

    tier = classify(evidence)
    decision = decide(tier)

    field_evidence = []
    for id_ev in id_evidence:
        has_match = len(id_ev["intersection"]) > 0
        field_evidence.append(FieldEvidence(
            field_name=id_ev["id_type"] if not id_ev["doc_type"] else f"document({id_ev['doc_type']})",
            value_a=",".join(id_ev["values_a"]) or None,
            value_b=",".join(id_ev["values_b"]) or None,
            comparison_type="exact_set_intersection",
            similarity_score=1.0 if has_match else 0.0,
            match_weight=1.0 if has_match else 0.0,
            explanation=f"Matched on {','.join(id_ev['intersection'])}" if has_match else "No intersecting validated value",
        ))
    nd = evidence["name_dob"]
    if nd["token_union"]:
        jaccard = len(nd["token_intersection"]) / len(nd["token_union"])
        field_evidence.append(FieldEvidence(
            field_name="name", value_a=name_a, value_b=name_b,
            comparison_type="rare_token_jaccard", similarity_score=jaccard, match_weight=jaccard,
            explanation=(f"Rare-token overlap: {nd['token_intersection']} (Jaccard={jaccard:.2f})"
                         if jaccard > 0 else "No shared rare name tokens"),
        ))
    if dob_a and dob_b:
        equal = dob_a == dob_b
        both_full = prec_a == "FULL" and prec_b == "FULL"
        field_evidence.append(FieldEvidence(
            field_name="dob", value_a=dob_a, value_b=dob_b,
            comparison_type="exact" if both_full else "year_only",
            similarity_score=1.0 if equal else 0.0,
            match_weight=1.0 if (equal and both_full) else 0.0,
            explanation=f"DOB {'exact match' if equal else 'differs'} ({'full precision' if both_full else 'year-only precision'})",
        ))

    score_value = {"AUTO_LINK": 0.99, "REVIEW": 0.65, "REJECT": 0.20}[decision.value]
    return field_evidence, tier, decision, score_value


@router.get("/{pair_id}")
async def get_match_details(pair_id: str) -> dict:
    """
    Get detailed match information for a specific pair.
    pair_id format: "a_key:b_key" (as generated by /matches/scores).

    Evidence and decision are computed FRESH from the source parquet
    (via _compute_pair_evidence_fresh), not read from a persisted CSV
    or _records.json snapshot. Those files only ever contain data for
    customers involved in AUTO_LINK/REVIEW pairs that survived into a
    specific run's clusters -- a REVIEW or REJECT pair's customers are
    routinely absent from them, which is exactly why "Record details
    unavailable" was showing. Recomputing here works identically for
    AUTO_LINK, REVIEW, and REJECT pairs, from any run, since the
    pipeline is a deterministic function of this same source data.
    """
    decoded = pair_id.replace("%3A", ":")
    parts = decoded.split(":")
    if len(parts) < 2:
        raise HTTPException(status_code=400, detail="pair_id must be 'a_key:b_key'")
    id1, id2 = parts[0], parts[1]

    try:
        raw_rows = _load_two_customer_rows([id1, id2])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load records: {e}")

    if id1 not in raw_rows or id2 not in raw_rows:
        raise HTTPException(status_code=404, detail="One or both customer codes not found in source data")

    record_a = _build_display_record(raw_rows[id1])
    record_b = _build_display_record(raw_rows[id2])
    field_evidence, tier, decision, score_value = _compute_pair_evidence_fresh(raw_rows[id1], raw_rows[id2])

    return {
        "pair_id": decoded,
        "a_key": id1,
        "b_key": id2,
        "score": score_value,
        "decision": decision.value,
        "signals_hit": tier.signals_hit,
        "hard_conflicts": tier.vetoes,
        "evidence": [
            {"field": ev.field_name, "value_a": ev.value_a, "value_b": ev.value_b,
             "comparison_type": ev.comparison_type, "similarity": ev.similarity_score,
             "weight": ev.match_weight, "explanation": ev.explanation}
            for ev in field_evidence
        ],
        "record_a": record_a,
        "record_b": record_b,
    }


@router.get("/run/{run_id}/uniques")
async def list_unique_records(
    run_id: str,
    page: int = 1,
    page_size: int = 50
) -> dict:
    """
    List unique records (singletons) for a run.
    """
    run_service = get_run_service()
    orchestrator = run_service.get_orchestrator(run_id)
    
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Run/Orchestrator not found")
        
    uniques = orchestrator.get_uniques()
    
    # Simple list sort by name for stability
    uniques.sort(key=lambda r: r.get('name_norm', ''))
    
    # Paginate
    total = len(uniques)
    start = (page - 1) * page_size
    end = start + page_size
    
    return {
        "records": uniques[start:end],
        "total": total,
        "page": page,
        "page_size": page_size
    }


@router.get("/run/{run_id}/clusters")
async def list_clusters(
    run_id: str,
    page: int = 1,
    page_size: int = 50
) -> dict:
    """
    List resolved clusters (merged entities) for a run.
    """
    run_service = get_run_service()
    orchestrator = run_service.get_orchestrator(run_id)
    
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Run/Orchestrator not found")
        
    clusters = orchestrator.get_result_clusters()
    
    # Sort by size descending (interesting ones first)
    clusters.sort(key=lambda c: c['size'], reverse=True)
    
    # Paginate
    total = len(clusters)
    start = (page - 1) * page_size
    end = start + page_size
    
    return {
        "clusters": clusters[start:end],
        "total": total,
        "page": page,
        "page_size": page_size
    }


@router.post("/{pair_id}/explain")
async def explain_match(pair_id: str) -> dict:
    """
    Generate an AI explanation for a match pair using the Referee Agent.
    """
    from agents.referee_agent import get_referee
    
    referee = get_referee()
    
    # Check if already exists
    if referee.has_explanation(pair_id):
        explanation = referee.get_explanation(pair_id)
        return {
            "explanation": explanation.explanation_text,
            "judgement": explanation.judgement,
            "meta": {
                "model": explanation.model_name,
                "created_at": explanation.created_at
            }
        }

    # Need data to generate explanation
    # Reusing search logic from get_match_details
    run_service = get_run_service()
    runs, _ = run_service.list_runs(page=1, page_size=50) # Search recent runs
    
    found_data = None
    
    for run in runs:
        orchestrator = run_service.get_orchestrator(run.run_id)
        if orchestrator:
            scores = orchestrator.get_scores()
            if pair_id in scores:
                score = scores[pair_id]
                record_a = orchestrator._records.get(score.a_key)
                record_b = orchestrator._records.get(score.b_key)
                
                # Format evidence for Agent
                evidence = [
                    {
                        'field': ev.field_name,
                        'type': ev.comparison_type,
                        'similarity': ev.similarity_score
                    }
                    for ev in score.evidence
                ]
                
                found_data = {
                    "pair_id": pair_id,
                    "run_id": run.run_id,
                    "record_a": record_a,
                    "record_b": record_b,
                    "score": score.score,
                    "evidence": evidence,
                    "signals": score.signals_hit,
                    "conflicts": score.hard_conflicts
                }
                break
    
    if not found_data:
        raise HTTPException(status_code=404, detail="Match pair not found in active runs")

    # Generate explanation
    explanation = referee.generate_explanation(
        pair_id=found_data['pair_id'],
        run_id=found_data['run_id'],
        record_a=found_data['record_a'],
        record_b=found_data['record_b'],
        score=found_data['score'],
        evidence=found_data['evidence'],
        signals=found_data['signals'],
        hard_conflicts=found_data['conflicts']
    )
    
    return {
        "explanation": explanation.explanation_text,
        "judgement": explanation.judgement,
        "meta": {
            "model": explanation.model_name,
            "created_at": explanation.created_at
        }
    }
