"""
CUIN v2 Control Plane - Matches API Routes

Endpoints for match scores and decisions.
"""

import os
from typing import Optional

from fastapi import APIRouter, HTTPException

from services.run_service import get_run_service
from engine.structures import MatchDecision

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
    """
    run_service = get_run_service()
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
    List decisions for a run.
    """
    run_service = get_run_service()
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
    Get decision summary for a run.
    """
    run_service = get_run_service()
    run = run_service.get_run(run_id)
    
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    
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
) -> dict:
    """
    List match scores for a run using query parameters.
    Reads from the run-specific scoring CSV saved after each pipeline run.
    """
    import pandas as pd

    csv_path = f"data/runs/{run_id}_scores.csv"

    if not os.path.exists(csv_path):
        # Fallback: try the global scoring_results.csv if it exists
        csv_path = "scoring_results.csv"
        if not os.path.exists(csv_path):
            return {"scores": [], "total": 0, "page": page, "page_size": page_size}

    try:
        df = pd.read_csv(csv_path)

        if min_score is not None:
            df = df[df["match_probability"] >= min_score]

        df = df.sort_values("match_probability", ascending=False)

        total = len(df)
        start = (page - 1) * page_size
        end = start + page_size
        paged = df.iloc[start:end]

        scores = []
        for _, row in paged.iterrows():
            id_l = str(row.get("CUSTOMER_CODE_l", row.get("unique_id_l", "")))
            id_r = str(row.get("CUSTOMER_CODE_r", row.get("unique_id_r", "")))
            scores.append({
                "pair_id": f"{id_l}:{id_r}",
                "a_key": id_l,
                "b_key": id_r,
                "score": float(row.get("match_probability", 0)),
                "signals_hit": [],
                "hard_conflicts": [],
            })

        return {"scores": scores, "total": total, "page": page, "page_size": page_size}

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Failed to read scores CSV: {e}")
        return {"scores": [], "total": 0, "page": page, "page_size": page_size}


@router.get("/{pair_id}")
async def get_match_details(pair_id: str) -> dict:
    """
    Get detailed match information for a specific pair.
    pair_id format: "id1:id2" (as generated by /matches/scores)
    Falls back to in-memory orchestrator for legacy compatibility.
    """
    import pandas as pd
    import glob

    # Decode URL-encoded colon (%3A)
    decoded = pair_id.replace("%3A", ":")
    parts = decoded.split(":")
    id1 = parts[0] if len(parts) >= 1 else ""
    id2 = parts[1] if len(parts) >= 2 else ""

    # 1. Try reading from run-specific scores CSVs first
    csv_files = sorted(glob.glob("data/runs/*_scores.csv"), reverse=True)
    if not csv_files:
        csv_files = ["scoring_results.csv"] if os.path.exists("scoring_results.csv") else []

    for csv_path in csv_files:
        try:
            df = pd.read_csv(csv_path)
            # Match row where CUSTOMER_CODE_l==id1 and CUSTOMER_CODE_r==id2 (or vice versa)
            df["id_l"] = df["CUSTOMER_CODE_l"].astype(str)
            df["id_r"] = df["CUSTOMER_CODE_r"].astype(str)
            mask = ((df["id_l"] == id1) & (df["id_r"] == id2)) | \
                   ((df["id_l"] == id2) & (df["id_r"] == id1))
            row_df = df[mask]
            if row_df.empty:
                continue
            row = row_df.iloc[0]

            # Find run_id from filename
            run_id_from_file = os.path.basename(csv_path).replace("_scores.csv", "")
            if run_id_from_file == "scoring_results":
                run_id_from_file = None

            # Load records from matching run records file
            record_a, record_b = None, None
            if run_id_from_file:
                rec_path = f"data/runs/{run_id_from_file}_records.json"
                if os.path.exists(rec_path):
                    import json
                    with open(rec_path) as f:
                        recs = json.load(f)
                    record_a = recs.get(id1)
                    record_b = recs.get(id2)

            # Build evidence from CSV columns
            evidence = []
            field_map = {
                "name": ("NAME_l", "NAME_r", "gamma_name"),
                "dob": ("BIRTH_DATE_l", "BIRTH_DATE_r", "gamma_dob"),
                "document": (None, None, "gamma_document"),
                "mobile": (None, None, "gamma_mobile"),
                "email": (None, None, "gamma_email"),
            }
            for field, (col_l, col_r, gamma_col) in field_map.items():
                gamma = float(row.get(gamma_col, 0)) if gamma_col and gamma_col in row else 0
                val_a = str(row.get(col_l, "")) if col_l and col_l in row else ""
                val_b = str(row.get(col_r, "")) if col_r and col_r in row else ""
                evidence.append({
                    "field": field,
                    "value_a": val_a,
                    "value_b": val_b,
                    "comparison_type": "exact" if gamma == 1 else "no_match",
                    "similarity": gamma,
                    "weight": float(row.get(f"bf_{field}", 0)) if f"bf_{field}" in row else 0,
                    "explanation": "Exact match" if gamma == 1 else "No match",
                })

            prob = float(row.get("match_probability", 0))
            return {
                "pair_id": decoded,
                "run_id": run_id_from_file,
                "a_key": id1,
                "b_key": id2,
                "score": prob,
                "decision": "AUTO_LINK" if prob >= 0.85 else "REVIEW" if prob >= 0.6 else "REJECT",
                "signals_hit": [e["field"] for e in evidence if e["similarity"] == 1],
                "hard_conflicts": [],
                "evidence": evidence,
                "record_a": record_a,
                "record_b": record_b,
            }
        except Exception:
            continue

    # 2. Fallback: search in-memory orchestrators
    run_service = get_run_service()
    runs, _ = run_service.list_runs(page=1, page_size=100)
    for run in runs:
        orchestrator = run_service.get_orchestrator(run.run_id)
        if orchestrator:
            scores = orchestrator.get_scores()
            if pair_id in scores:
                score = scores[pair_id]
                decision = orchestrator.get_decisions().get(pair_id)
                record_a = orchestrator._records.get(score.a_key)
                record_b = orchestrator._records.get(score.b_key)
                evidence = [
                    {"field": ev.field_name, "value_a": ev.value_a, "value_b": ev.value_b,
                     "comparison_type": ev.comparison_type, "similarity": ev.similarity_score,
                     "weight": ev.match_weight, "explanation": ev.explanation}
                    for ev in score.evidence
                ]
                return {
                    "pair_id": pair_id, "run_id": run.run_id,
                    "a_key": score.a_key, "b_key": score.b_key,
                    "score": score.score,
                    "decision": decision.value if decision else None,
                    "signals_hit": score.signals_hit, "hard_conflicts": score.hard_conflicts,
                    "evidence": evidence, "record_a": record_a, "record_b": record_b,
                }

    raise HTTPException(status_code=404, detail="Match not found")


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
