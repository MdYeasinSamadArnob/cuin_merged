"""
CUIN v2 Control Plane - Metrics API Routes

Dashboard KPIs and analytics endpoints.
"""

from typing import Optional
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException

from services.run_service import get_run_service
from services.review_service import get_review_service
from engine.clustering import get_cluster_manager

router = APIRouter()


# ============================================
# Routes
# ============================================

@router.get("/dashboard")
async def get_dashboard_metrics() -> dict:
    """
    Get main dashboard KPIs.
    """
    run_service = get_run_service()
    return run_service.get_dashboard_metrics()


@router.get("/trends")
async def get_trends(
    days: int = 7
) -> dict:
    """
    Get trend data for charts.
    """
    run_service = get_run_service()
    runs, _ = run_service.list_runs(page=1, page_size=100)
    
    # Group runs by day
    now = datetime.utcnow()
    cutoff = now - timedelta(days=days)
    
    daily_stats = {}
    for i in range(days):
        date = (now - timedelta(days=i)).strftime('%Y-%m-%d')
        daily_stats[date] = {
            'runs': 0,
            'records': 0,
            'duplicates': 0,
            'auto_links': 0,
            'reviews': 0,
        }
    
    for run in runs:
        if run.started_at < cutoff:
            continue
        
        date = run.started_at.strftime('%Y-%m-%d')
        if date in daily_stats:
            daily_stats[date]['runs'] += 1
            daily_stats[date]['records'] += run.counters.records_in
            daily_stats[date]['duplicates'] += run.counters.auto_links + run.counters.review_items
            daily_stats[date]['auto_links'] += run.counters.auto_links
            daily_stats[date]['reviews'] += run.counters.review_items
    
    return {
        "period_days": days,
        "daily_stats": daily_stats,
        "generated_at": now.isoformat()
    }


@router.get("/run/{run_id}")
async def get_run_metrics(run_id: str) -> dict:
    """
    Get detailed metrics for a specific run.
    """
    run_service = get_run_service()
    run = run_service.get_run(run_id)
    
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    
    # Calculate additional metrics
    counters = run.counters
    
    metrics = {
        "run_id": run_id,
        "status": run.status.value,
        "mode": run.mode.value,
        "duration_seconds": run.duration_seconds,
        
        # Volume metrics
        "records_in": counters.records_in,
        "records_normalized": counters.records_normalized,
        "normalization_success_rate": (
            (counters.records_normalized / counters.records_in * 100)
            if counters.records_in > 0 else 0
        ),
        
        # Blocking metrics
        "blocks_created": counters.blocks_created,
        "candidates_generated": counters.candidates_generated,
        "blocking_reduction_rate": 0,  # Would calculate from max possible pairs
        
        # Scoring metrics
        "pairs_scored": counters.pairs_scored,
        
        # Decision metrics
        "auto_links": counters.auto_links,
        "review_items": counters.review_items,
        "rejected": counters.rejected,
        "auto_link_rate": (
            (counters.auto_links / counters.pairs_scored * 100)
            if counters.pairs_scored > 0 else 0
        ),
        "review_rate": (
            (counters.review_items / counters.pairs_scored * 100)
            if counters.pairs_scored > 0 else 0
        ),
    }
    
    return metrics


@router.get("/blocking/{run_id}")
async def get_blocking_metrics(run_id: str) -> dict:
    """
    Get blocking-specific metrics for a run.
    """
    run_service = get_run_service()
    orchestrator = run_service.get_orchestrator(run_id)
    
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Run not found or orchestrator unavailable")
    
    stats = orchestrator.blocker.get_blocking_stats()
    
    return {
        "run_id": run_id,
        "total_records": stats['total_records'],
        "total_blocking_keys": stats['total_keys'],
        "suppressed_keys": stats['suppressed_keys'],
        "avg_keys_per_record": stats['avg_keys_per_record'],
        
        # Blocking method breakdown (placeholder)
        "method_breakdown": {
            "phone_last10": 0,
            "email_exact": 0,
            "soundex_full": 0,
            "name_token": 0,
            "dob_exact": 0,
        }
    }


@router.get("/scoring/{run_id}")
async def get_scoring_metrics(run_id: str) -> dict:
    """
    Get scoring distribution metrics for a run.
    """
    run_service = get_run_service()
    orchestrator = run_service.get_orchestrator(run_id)
    
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Run not found or orchestrator unavailable")
    
    scores = orchestrator.get_scores()
    
    # Calculate score distribution
    distribution = {
        "0.0-0.2": 0,
        "0.2-0.4": 0,
        "0.4-0.6": 0,
        "0.6-0.8": 0,
        "0.8-1.0": 0,
    }
    
    for score in scores.values():
        s = score.score
        if s < 0.2:
            distribution["0.0-0.2"] += 1
        elif s < 0.4:
            distribution["0.2-0.4"] += 1
        elif s < 0.6:
            distribution["0.4-0.6"] += 1
        elif s < 0.8:
            distribution["0.6-0.8"] += 1
        else:
            distribution["0.8-1.0"] += 1
    
    # Calculate average score
    avg_score = 0
    if scores:
        avg_score = sum(s.score for s in scores.values()) / len(scores)
    
    return {
        "run_id": run_id,
        "total_pairs": len(scores),
        "average_score": avg_score,
        "score_distribution": distribution,
        
        # Threshold analysis
        "above_auto_threshold": sum(1 for s in scores.values() if s.score >= 0.92),
        "in_review_zone": sum(1 for s in scores.values() if 0.55 <= s.score < 0.92),
        "below_threshold": sum(1 for s in scores.values() if s.score < 0.55),
    }


@router.get("/clusters")
async def get_cluster_metrics() -> dict:
    """
    Get clustering statistics.
    """
    cluster_manager = get_cluster_manager()
    stats = cluster_manager.get_stats()
    
    clusters = cluster_manager.get_clusters()
    
    # Size distribution
    size_distribution = {}
    for cluster_id, members in clusters.items():
        size = len(members)
        size_key = f"{size}" if size <= 5 else "6+"
        size_distribution[size_key] = size_distribution.get(size_key, 0) + 1
    
    return {
        **stats,
        "size_distribution": size_distribution,
        "golden_records_count": len(cluster_manager._golden_records),
    }
