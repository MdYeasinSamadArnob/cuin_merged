"""
CUIN v2 - Graph API Routes

Legacy graph/cluster projection and visualization endpoints, backed by
the in-memory ClusterManager and per-run parquet/JSON snapshots (not
Neo4j, despite this module's old docstring -- still used by /explorer,
/pipeline, and /runs/[id]; the live /graph page is Postgres-backed via
routes_graph_v2.py instead).
"""

from fastapi import APIRouter, HTTPException, Query
from starlette.concurrency import run_in_threadpool
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime
from uuid import uuid4
import logging
import json
import os
import time
from engine.clustering import ClusterManager, get_cluster_manager
from engine.structures import GoldenRecord, ScoringConfig, MatchDecision
from engine.matching.custom_scorer import CustomProbabilisticScorer
from engine.decisioning.decision_engine import DecisionEngine
from services.run_service import get_run_service

logger = logging.getLogger(__name__)

router = APIRouter()


# ============================================
# Response Models
# ============================================


class NodeModel(BaseModel):
    """Graph node representation."""
    id: str
    label: str
    type: str  # "record", "cluster", "golden_record"
    properties: dict


class EdgeModel(BaseModel):
    """Graph edge representation."""
    source: str
    target: str
    type: str  # "MEMBER_OF", "MATCHES"
    weight: float = 1.0
    properties: dict = {}


class GraphResponse(BaseModel):
    """Graph data for visualization."""
    nodes: List[NodeModel]
    edges: List[EdgeModel]
    stats: dict


class ClusterSummary(BaseModel):
    """Summary of a single cluster."""
    cluster_id: str
    size: int
    members: List[dict] # Changed from List[str] to List[dict] to include profiles
    golden_record: Optional[dict] = None
    representative_record: Optional[dict] = None # Added for frontend compatibility
    created_at: str


class ClusterListResponse(BaseModel):
    clusters: List[ClusterSummary]
    total: int
    page: int
    page_size: int


# ============================================
# Routes
# ============================================


from services.run_service import get_run_service
from engine.matching.custom_scorer import CustomProbabilisticScorer
from engine.decisioning.decision_engine import DecisionEngine


class PreviewRequest(BaseModel):
    run_id: Optional[str] = None
    scoring: dict


def _sync_preview_clustering(request: PreviewRequest) -> GraphResponse:
    """
    Preview clustering results with temporary configuration.
    """
    # 1. Setup Config
    try:
        scoring_config = ScoringConfig(**request.scoring)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid configuration: {str(e)}")
    
    # 2. Get Data (Records)
    service = get_run_service()
    records = {}
    
    # Try to get records from run
    if request.run_id:
        if request.run_id in service._orchestrators:
            orch = service._orchestrators[request.run_id]
            records = orch._records
        else:
            # Try load from disk with retry
            for attempt in range(3):
                try:
                    file_path = f'data/runs/{request.run_id}_records.json'
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as f:
                            records = json.load(f)
                        logger.info(f"Loaded {len(records)} records from disk for run {request.run_id}")
                        break
                except Exception as e:
                    logger.warning(f"Attempt {attempt+1}: Failed to load records from disk: {e}")
                    time.sleep(0.1)
    
    # Fallback: Get records from ClusterManager members
    if not records:
        manager = get_cluster_manager()
        for member in manager._members:
            if member.valid_to is None:
                # Need profile
                # Try to pass run_id if we have it, to give get_record_profile a chance to load (though inefficient loop)
                profile = get_record_profile(member.customer_key, run_id=request.run_id)
                records[member.customer_key] = profile

    if not records:
        return GraphResponse(nodes=[], edges=[], stats={"preview": True, "message": "No records found"})

    # 3. Generate Candidates (All Pairs if small, else use existing candidates)
    candidates = []
    
    if len(records) < 500:
        # Generate all pairs
        keys = list(records.keys())
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                candidates.append((keys[i], keys[j])) # Tuple of keys
    else:
        # Try to use existing candidates
        if request.run_id and request.run_id in service._orchestrators:
            orch = service._orchestrators[request.run_id]
            candidates = [(c.a_key, c.b_key) for c in orch._candidates]
        else:
                # Too many records for full preview without candidates
                # Just take first 500 records to avoid timeout
                keys = list(records.keys())[:500]
                records = {k: records[k] for k in keys}
                for i in range(len(keys)):
                    for j in range(i + 1, len(keys)):
                        candidates.append((keys[i], keys[j]))

    # 4. Re-Score & Decide
    scorer = CustomProbabilisticScorer(scoring_config)
    decision_engine = DecisionEngine(scoring_config)
    temp_manager = ClusterManager()
    
    nodes = []
    edges = []
    
    # Reset temp manager internal state for this request
    temp_manager._members = []
    temp_manager._uf = type(temp_manager._uf)() # New UnionFind
    temp_manager._cluster_ids = {}

    for a_key, b_key in candidates:
        rec_a = records.get(a_key)
        rec_b = records.get(b_key)
        
        if not rec_a or not rec_b:
            continue
            
        # Create dummy pair_id
        pair_id = f"{a_key}:{b_key}"
        
        score = scorer.score_pair(pair_id, rec_a, rec_b)
        decision = decision_engine.make_decision(score)
        
        if decision == MatchDecision.AUTO_LINK:
            temp_manager.link(a_key, b_key)
            edges.append(EdgeModel(
                source=a_key,
                target=b_key,
                type="MATCHES",
                weight=score.score,
                properties={"decision": "AUTO_LINK"}
            ))
        elif decision == MatchDecision.REVIEW:
                edges.append(EdgeModel(
                source=a_key,
                target=b_key,
                type="REVIEW",
                weight=score.score,
                properties={"decision": "REVIEW"}
            ))
            # Also add nodes for REVIEW edges even if not clustered
            # (Logic below adds clustered nodes, we might miss unclustered review pairs)

    # 5. Build Graph Response
    # Iterate clusters in temp_manager
    clusters = temp_manager.get_clusters()
    
    processed_nodes = set()

    for cid, members in clusters.items():
        # Filter singletons if needed (but user wants to see them now)
        # We'll allow singletons in preview for consistency
        
        # Create cluster node
        # Use representative name
        primary_member_id = members[0]
        cluster_profile = records.get(primary_member_id, {})
        
        if len(members) > 1:
            cluster_name = f"{cluster_profile.get('name', cluster_profile.get('name_norm', 'Cluster'))} (Preview)"
        else:
            cluster_name = f"{cluster_profile.get('name', cluster_profile.get('name_norm', 'Cluster'))} (Singleton)"

        cluster_node = NodeModel(
            id=cid,
            label=cluster_name,
            type="cluster",
            properties={"size": len(members)}
        )
        nodes.append(cluster_node)
        
        for m in members:
            # Try to get profile from records, fallback to get_record_profile
            p = records.get(m)
            if not p:
                p = get_record_profile(m, records, request.run_id)
            
            if m not in processed_nodes:
                nodes.append(NodeModel(
                    id=m,
                    label=p.get('name', p.get('name_norm', 'Unknown')),
                    type="record",
                    properties=p
                ))
                processed_nodes.add(m)
            
            # Add edge to cluster
            edges.append(EdgeModel(
                source=m,
                target=cid,
                type="MEMBER_OF",
                weight=1.0
            ))

    # Add remaining nodes that are part of REVIEW edges but not in clusters
    for edge in edges:
        if edge.type == "REVIEW":
            if edge.source not in processed_nodes:
                p = records.get(edge.source) or get_record_profile(edge.source, records, request.run_id)
                nodes.append(NodeModel(id=edge.source, label=p.get('name', p.get('name_norm', 'Unknown')), type="record", properties=p))
                processed_nodes.add(edge.source)
            if edge.target not in processed_nodes:
                p = records.get(edge.target) or get_record_profile(edge.target, records, request.run_id)
                nodes.append(NodeModel(id=edge.target, label=p.get('name', p.get('name_norm', 'Unknown')), type="record", properties=p))
                processed_nodes.add(edge.target)

    # Add remaining singletons (records not in any cluster or review edge)
    # Ensure we iterate over ALL loaded records, not just what was in candidates
    for rid in records:
        if rid not in processed_nodes:
            p = records.get(rid) or get_record_profile(rid, records, request.run_id)
            # Create singleton cluster node
            cluster_name = f"{p.get('name', p.get('name_norm', 'Record'))} (Singleton)"
            cluster_id = f"singleton_{rid}"
            
            nodes.append(NodeModel(
                id=cluster_id,
                label=cluster_name,
                type="cluster",
                properties={"size": 1}
            ))
            
            nodes.append(NodeModel(
                id=rid,
                label=p.get('name', p.get('name_norm', 'Unknown')),
                type="record",
                properties=p
            ))
            
            edges.append(EdgeModel(
                source=rid,
                target=cluster_id,
                type="MEMBER_OF",
                weight=1.0
            ))
            
            processed_nodes.add(rid)
    
    return GraphResponse(
        nodes=nodes,
        edges=edges,
        stats={
            "total_clusters": len([c for c in clusters.values() if len(c) > 1]),
            "total_members": len(records),
            "preview": True
        }
    )


@router.post("/preview", response_model=GraphResponse)
async def preview_clustering(request: PreviewRequest):
    """
    Preview clustering results with temporary configuration.
    """
    return await run_in_threadpool(_sync_preview_clustering, request)


# Cache of {run_id: {customer_key: record_dict}} loaded from
# data/runs/{run_id}_records.json, so the tier-2 disk fallback below
# (used by every get_record_profile() call whose caller didn't already
# preload run_records) reads each run's file at most once per process,
# not once per record in a loop -- the exact performance concern the
# previous version of this function's comments raised, but never
# actually implemented. Unbounded but small in practice: one entry per
# run touched this process's lifetime, a few tens of MB each.
_run_records_disk_cache: dict = {}


def _load_run_records_from_disk(run_id: str) -> dict:
    if run_id in _run_records_disk_cache:
        return _run_records_disk_cache[run_id]
    records = {}
    file_path = f"data/runs/{run_id}_records.json"
    if os.path.exists(file_path):
        try:
            with open(file_path) as f:
                records = json.load(f)
        except Exception:
            logger.warning(f"Failed to load run records from {file_path}", exc_info=True)
    _run_records_disk_cache[run_id] = records
    return records


def _format_record_profile(rid: str, r: dict) -> dict:
    return {
        "customer_key": rid,
        "source_customer_id": r.get('source_customer_id', rid),
        "name": r.get('name_norm', r.get('name', 'Unknown')),
        "name_norm": r.get('name_norm', ''),
        "product": r.get('product', 'Unknown'), # Might not be in norm
        "riskLevel": r.get('risk_level', 'Low'),
        "balance": r.get('balance', '0.00'),
        "email": r.get('email_norm', r.get('email', 'N/A')),
        "email_norm": r.get('email_norm', ''),
        "phone": r.get('phone_norm', r.get('phone', 'N/A')),
        "phone_norm": r.get('phone_norm', ''),
        "dob_norm": r.get('dob_norm', ''),
        "address_norm": r.get('address_norm', ''),
        "city": r.get('city_norm', r.get('city', '')),
        "status": r.get('status', 'ACT'),
        "kycStatus": "VERIFIED" if r.get('status', 'ACT') == 'ACT' else "PENDING",
        "metadata": r.get('metadata', {}),
        "resolved": True,
    }


# Real Data Lookup (in-memory run records -> per-run disk cache -> explicit "not found").
#
# A mock-data branch used to live here: any record id starting
# "0005"/"50"/"DUP" got a randomly-generated fake name/balance/product,
# seeded by rid so it looked "deterministic". Verified live against
# production data that 9,059 REAL customers match that pattern (e.g.
# "00050002" is genuinely "MDTANVIR RAHMAN") -- meaning a bank officer
# reviewing those records could have merged entities based on entirely
# invented identity data. Deleted outright: this function must never
# return data it did not read from somewhere real.
def get_record_profile(rid: str, run_records: Optional[dict] = None, run_id: Optional[str] = None):
    if run_records and rid in run_records:
        return _format_record_profile(rid, run_records[rid])

    if run_id:
        disk_records = _load_run_records_from_disk(run_id)
        if rid in disk_records:
            return _format_record_profile(rid, disk_records[rid])

    # Genuinely not found in this run's records -- explicit, not fabricated.
    short_id = rid[:8] + "..." if len(rid) > 8 else rid
    return {
        "customer_key": rid,
        "source_customer_id": rid,
        "name": f"Unknown ({short_id})",
        "name_norm": f"UNKNOWN ({short_id})",
        "status": "Incomplete",
        "kycStatus": "PENDING",
        "resolved": False,
    }


def _sync_list_clusters(
    page: int,
    page_size: int,
    min_size: int,
    run_id: Optional[str],
) -> ClusterListResponse:
    """
    List resolved entities (clusters) with pagination.
    Returns rich profiles for display in the Explorer.
    """
    manager = get_cluster_manager()
    
    # Persistence: Load snapshot if needed
    if run_id:
        path = f'data/runs/{run_id}_clusters.json'
        if os.path.exists(path):
             # Load if empty OR different run loaded
             if not manager._members or getattr(manager, 'loaded_run_id', None) != run_id:
                 manager.load_snapshot(path, run_id)
            
    # Get all clusters
    all_clusters = manager.get_clusters()
    
    # Filter by size
    filtered_clusters = [
        (cid, members) 
        for cid, members in all_clusters.items() 
        if len(members) >= min_size
    ]
    
    # Sort by size (descending)
    filtered_clusters.sort(key=lambda x: len(x[1]), reverse=True)
    
    # Pagination
    total = len(filtered_clusters)
    start = (page - 1) * page_size
    end = start + page_size
    paged = filtered_clusters[start:end]
    
    # Get Run Records if run_id provided (for speed)
    run_records = None
    if run_id:
        service = get_run_service()
        if run_id in service._orchestrators:
            run_records = service._orchestrators[run_id]._records
        else:
            # Try load from disk with retry
            for attempt in range(3):
                try:
                    file_path = f'data/runs/{run_id}_records.json'
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as f:
                            run_records = json.load(f)
                        logger.info(f"Loaded {len(run_records)} records from disk for run {run_id}")
                        break
                except Exception as e:
                    logger.warning(f"Attempt {attempt+1}: Failed to load records from disk: {e}")
                    time.sleep(0.1)
    
    results = []
    for cid, members in paged:
        # Determine representative record (consensus or first)
        member_profiles = [get_record_profile(m_id, run_records, run_id) for m_id in members[:20]]
        
        # Simple consensus on name
        name_counts = {}
        for p in member_profiles:
            name = p.get('name', 'Unknown')
            name_counts[name] = name_counts.get(name, 0) + 1
            
        golden_name = max(name_counts, key=name_counts.get) if name_counts else "Unknown"
        
        # Find profile matching golden name
        golden_profile = next((p for p in member_profiles if p.get('name') == golden_name), member_profiles[0] if member_profiles else {})
        
        results.append(ClusterSummary(
            cluster_id=cid,
            size=len(members),
            members=member_profiles, # Now returning full profiles
            representative_record=golden_profile,
            created_at=datetime.utcnow().isoformat()
        ))
        
    return ClusterListResponse(
        clusters=results,
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/entities", response_model=ClusterListResponse)
async def list_clusters(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=5000),
    min_size: int = Query(1, ge=1, description="Minimum cluster size to include"),
    run_id: Optional[str] = Query(None, description="Filter by run ID (optional)")
):
    """
    List resolved entities (clusters) with pagination.
    Returns rich profiles for display in the Explorer.
    """
    return await run_in_threadpool(_sync_list_clusters, page, page_size, min_size, run_id)


def _sync_get_cluster_graph(
    cluster_id: Optional[str],
    limit: int,
    run_id: Optional[str],
    include_singletons: bool,
) -> GraphResponse:
    """
    Get cluster graph for visualization.

    Returns nodes and edges suitable for D3.js or similar graph libraries.
    """
    manager = get_cluster_manager()
    
    # Persistence: Load snapshot if needed
    if run_id:
        path = f'data/runs/{run_id}_clusters.json'
        if os.path.exists(path):
             # Load if empty OR different run loaded
             if not manager._members or getattr(manager, 'loaded_run_id', None) != run_id:
                 manager.load_snapshot(path, run_id)
    
    nodes: List[NodeModel] = []
    edges: List[EdgeModel] = []
    
    # Get cluster stats
    stats = manager.get_stats()
    
    # Get Run Records (Real Data)
    service = get_run_service()
    run_records = {}
    
    if run_id:
        if run_id in service._orchestrators:
            run_records = service._orchestrators[run_id]._records
        else:
            # Try load from disk with retry
            for attempt in range(3):
                try:
                    file_path = f'data/runs/{run_id}_records.json'
                    if os.path.exists(file_path):
                        with open(file_path, 'r') as f:
                            run_records = json.load(f)
                        logger.info(f"Loaded {len(run_records)} records from disk for run {run_id}")
                        break
                except Exception as e:
                    logger.warning(f"Attempt {attempt+1}: Failed to load records from disk: {e}")
                    time.sleep(0.1)
    else:
        # Combine records from all runs if no specific run requested
        for r_id, orch in service._orchestrators.items():
            run_records.update(orch._records)
            
    # Get all clusters or filter by ID
    
    # Get all clusters (with limit)
    if cluster_id:
        members = manager.get_cluster_members(cluster_id)
        if not members:
            raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")
        
        # Create cluster node
        nodes.append(NodeModel(
            id=cluster_id,
            label=f"Cluster-{cluster_id[:8]}",
            type="cluster",
            properties={"size": len(members)}
        ))
        
        # Create member nodes and edges
        for member in members:
            profile = get_record_profile(member, run_records, run_id)
            nodes.append(NodeModel(
                id=member,
                label=profile['name'], # Use Name as Label
                type="record",
                properties=profile
            ))
            edges.append(EdgeModel(
                source=member,
                target=cluster_id,
                type="MEMBER_OF",
                weight=1.0
            ))
        
        # Add golden record if exists
        golden = manager.get_golden_record(cluster_id)
        if golden:
            nodes.append(NodeModel(
                id=f"golden_{cluster_id}",
                label=f"Golden: {golden.payload.get('name', 'Record')}",
                type="golden_record",
                properties=golden.payload
            ))
            edges.append(EdgeModel(
                source=f"golden_{cluster_id}",
                target=f"c_{cluster_id}",
                type="REPRESENTS",
                weight=1.0
            ))
    else:
        # Get all clusters (with limit)
        all_clusters = manager.get_clusters()

        # Synthesize Singletons for records not in ClusterManager
        if run_id and run_records:
            clustered_members = set()
            for mems in all_clusters.values():
                clustered_members.update(mems)
            
            missing_records = [rid for rid in run_records if rid not in clustered_members]
            logger.info(f"Synthesizing {len(missing_records)} singleton clusters for run {run_id}")
            
            for rid in missing_records:
                # Use record ID as cluster ID for singletons
                all_clusters[rid] = [rid]

        cluster_count = 0
        
        for cid, members in all_clusters.items():
            if cluster_count >= limit:
                break
            
            # Filter singletons if requested
            if not include_singletons and len(members) <= 1:
                continue
                
            # Create "Golden" profile for the cluster
            primary_member_id = members[0]
            cluster_profile = get_record_profile(primary_member_id, run_records, run_id)
            
            # Distinguish cluster node label
            if len(members) > 1:
                cluster_label = f"{cluster_profile['name']} (Composite)"
            else:
                cluster_label = f"{cluster_profile['name']} (Singleton)"

            nodes.append(NodeModel(
                id=f"c_{cid}",
                label=cluster_label,
                type="cluster",
                properties={
                    "size": len(members),
                    "cluster_id": cid, # Store original ID
                    **cluster_profile # Include rich data
                }
            ))
            
            # Show more members per cluster for better visualization
            member_limit = 50 if len(members) < 100 else 20
            for member in members[:member_limit]: 
                profile = get_record_profile(member, run_records, run_id)
                nodes.append(NodeModel(
                    id=f"r_{member}",
                    label=profile['name'], # Use Name as Label
                    type="record",
                    properties=profile
                ))
                edges.append(EdgeModel(
                    source=f"r_{member}",
                    target=f"c_{cid}",
                    type="MEMBER_OF",
                    weight=1.0
                ))
            
            cluster_count += 1

    
    return GraphResponse(
        nodes=nodes[:limit*10], # Allow more nodes since we expanded members
        edges=edges,
        stats={
            "total_clusters": len(all_clusters),
            "total_members": sum(len(m) for m in all_clusters.values()),
            "avg_cluster_size": stats.get("avg_cluster_size", 0),
            "nodes_returned": len(nodes),
            "edges_returned": len(edges)
        }
    )


@router.get("/clusters", response_model=GraphResponse)
async def get_cluster_graph(
    cluster_id: Optional[str] = Query(None, description="Filter to specific cluster"),
    limit: int = Query(500, le=2000, description="Max nodes to return"),
    run_id: Optional[str] = Query(None, description="Filter by run ID (optional)"),
    include_singletons: bool = Query(True, description="Include singleton clusters (size=1)"),
):
    """
    Get cluster graph for visualization.

    Returns nodes and edges suitable for D3.js or similar graph libraries.
    """
    return await run_in_threadpool(
        _sync_get_cluster_graph, cluster_id, limit, run_id, include_singletons
    )


@router.get("/cluster/{cluster_id}", response_model=ClusterSummary)
async def get_cluster_details(cluster_id: str):
    """Get details for a specific cluster."""
    manager = get_cluster_manager()
    
    members = manager.get_cluster_members(cluster_id)
    if not members:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")
    
    golden = manager.get_golden_record(cluster_id)
    
    return ClusterSummary(
        cluster_id=cluster_id,
        size=len(members),
        members=members,
        golden_record=golden.payload if golden else None,
        created_at=datetime.utcnow().isoformat()
    )


@router.post("/merge")
async def merge_clusters(
    cluster_a: str = Query(..., description="First cluster ID"),
    cluster_b: str = Query(..., description="Second cluster ID"),
    reviewer: str = Query("system", description="Who initiated the merge")
):
    """
    Manually merge two clusters.
    
    This is typically done after human review confirms two clusters
    represent the same entity.
    """
    manager = get_cluster_manager()
    
    # Get members of both clusters
    members_a = manager.get_cluster_members(cluster_a)
    members_b = manager.get_cluster_members(cluster_b)
    
    if not members_a:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_a} not found")
    if not members_b:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_b} not found")
    
    # Merge by linking a member from each
    if members_a and members_b:
        manager.link(members_a[0], members_b[0])
    
    new_cluster_id = manager.find(members_a[0])
    new_members = manager.get_cluster_members(new_cluster_id)
    
    return {
        "message": "Clusters merged successfully",
        "new_cluster_id": new_cluster_id,
        "new_size": len(new_members),
        "merged_by": reviewer
    }


@router.post("/split")
async def split_record_from_cluster(
    record_id: str = Query(..., description="Record to split out"),
    reviewer: str = Query("system", description="Who initiated the split")
):
    """
    Split a record out of its cluster into its own cluster.
    
    This is used when human review determines a record was incorrectly linked.
    Note: Current implementation creates a new single-member cluster.
    """
    manager = get_cluster_manager()
    
    # For now, we can't really "split" with union-find
    # Instead, we'd need to rebuild the cluster without this member
    # This is a placeholder for future implementation
    
    return {
        "message": "Split operation recorded",
        "record_id": record_id,
        "note": "Full split implementation requires cluster rebuild",
        "split_by": reviewer
    }


@router.get("/data")
async def get_graph_data(
    run_id: Optional[str] = Query(None, description="Run ID to load graph for"),
    limit: int = Query(2000, le=5000, description="Max nodes to return"),
):
    """
    Primary graph visualization endpoint used by the frontend Graph page.
    Returns nodes and edges for the D3 force-directed graph.
    Delegates to /graph/clusters logic.
    """
    return await get_cluster_graph(
        cluster_id=None,
        limit=limit,
        run_id=run_id,
        include_singletons=True,
    )


def _sync_get_unique_records(run_id: Optional[str], page: int, page_size: int) -> dict:
    """
    Returns singleton records - customer IDs that belong to no accepted
    cluster for this run. Used by the Explorer UNIQUE tab.

    Reads the lightweight code-only manifest written by
    pipeline.doris_orchestrator._persist_run_artifacts
    (data/runs/{run_id}_singletons.csv) rather than deriving singletons
    from {run_id}_records.json -- that file only ever contains CLUSTER
    MEMBERS (see _load_records_for_members), so subtracting "clustered"
    from it was always empty by construction, regardless of how many
    true singletons existed. The manifest holds only customer_code
    (cheap even at ~1.2M rows); full profile data for the current page
    is looked up on demand from the source parquet, so this never
    materializes more than one page's worth of full records at a time.
    """
    singletons_path = f"data/runs/{run_id}_singletons.csv" if run_id else None

    if not run_id or not os.path.exists(singletons_path):
        return {"records": [], "total": 0, "page": page, "page_size": page_size}

    import csv as csv_module
    with open(singletons_path, "r", newline="") as f:
        reader = csv_module.reader(f)
        next(reader, None)  # header
        all_codes = [row[0] for row in reader if row]

    total = len(all_codes)
    start = (page - 1) * page_size
    end = start + page_size
    page_codes = all_codes[start:end]

    records = []
    if page_codes:
        try:
            import pyarrow.parquet as pq
            from pipeline.doris_orchestrator import PARQUET_PATH

            def _first_non_null(arr):
                if not arr:
                    return None
                for x in arr:
                    if x is not None:
                        return x
                return None

            table = pq.read_table(
                PARQUET_PATH,
                columns=["CUSTOMER_CODE", "NAME", "BIRTH_DATE", "MOBILE", "EMAIL", "FULL_ADDRESS", "DOCUMENT"],
                filters=[("CUSTOMER_CODE", "in", page_codes)],
            )

            for r in table.to_pylist():
                code = r["CUSTOMER_CODE"]
                name = r["NAME"]
                dob = r["BIRTH_DATE"]
                mobile = _first_non_null(r["MOBILE"])
                email = _first_non_null(r["EMAIL"])
                address = _first_non_null(r["FULL_ADDRESS"])
                natid = _first_non_null(r["DOCUMENT"])
                records.append({
                    "customer_key": code,
                    "source_customer_id": code,
                    "name": name or "",
                    "name_norm": (name or "").upper(),
                    "email": email or "",
                    "email_norm": (email or "").lower(),
                    "phone": mobile or "",
                    "phone_norm": mobile or "",
                    "dob": dob or "",
                    "dob_norm": dob or "",
                    "address": address or "",
                    "address_norm": (address or "").upper(),
                    "natid": natid or "",
                    "natid_norm": (natid or "").upper(),
                    "status": "ACT",
                })
        except Exception as e:
            logger.error(f"Failed to load singleton profiles for run {run_id}: {e}")

    return {
        "records": records,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/uniques")
async def get_unique_records(
    run_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=5000),
):
    """
    Returns singleton records for the Explorer UNIQUE tab. See
    _sync_get_unique_records for the full explanation of the data source.
    """
    return await run_in_threadpool(_sync_get_unique_records, run_id, page, page_size)


@router.get("/cluster-entities", response_model=ClusterListResponse)
async def list_cluster_entities(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=5000),
    min_size: int = Query(2, ge=1, description="Minimum cluster size"),
    run_id: Optional[str] = Query(None),
):
    """
    Alias for /graph/entities used by the Explorer ENTITIES tab.
    Returns clusters with full member profiles.
    """
    return await list_clusters(page=page, page_size=page_size, min_size=min_size, run_id=run_id)


@router.get("/stats")
async def get_graph_stats():
    """Get overall graph statistics."""
    manager = get_cluster_manager()
    stats = manager.get_stats()
    
    return {
        "total_clusters": stats.get("total_clusters", 0),
        "total_members": stats.get("total_members", 0),
        "golden_records_count": stats.get("golden_records_count", 0),
    }

class PreviewRequest(BaseModel):
    run_id: Optional[str] = None
    scoring: dict

@router.post("/preview", response_model=GraphResponse)
async def preview_clustering(request: PreviewRequest):
    """
    Preview clustering results with temporary configuration.
    """
    # 1. Setup Config
    scoring_config = ScoringConfig(**request.scoring)
    
    # 2. Get Data (Records)
    service = get_run_service()
    records = {}
    
    # Try to get records from run
    if request.run_id and request.run_id in service._orchestrators:
        orch = service._orchestrators[request.run_id]
        records = orch._records
    
    # Fallback: Get records from ClusterManager members
    if not records:
        manager = get_cluster_manager()
        for member in manager._members:
            if member.valid_to is None:
                # Need profile
                profile = get_record_profile(member.customer_key)
                records[member.customer_key] = profile

    if not records:
        return GraphResponse(nodes=[], edges=[], stats={})

    # 3. Generate Candidates (All Pairs if small, else use existing candidates)
    candidates = []
    
    if len(records) < 500:
        # Generate all pairs
        keys = list(records.keys())
        for i in range(len(keys)):
            for j in range(i + 1, len(keys)):
                candidates.append((keys[i], keys[j])) # Tuple of keys
    else:
        # Try to use existing candidates
        if request.run_id and request.run_id in service._orchestrators:
            orch = service._orchestrators[request.run_id]
            candidates = [(c.a_key, c.b_key) for c in orch._candidates]
        else:
             # Too many records for full preview without candidates
             raise HTTPException(status_code=400, detail="Too many records for preview without active run")

    # 4. Re-Score & Decide
    scorer = CustomProbabilisticScorer(scoring_config)
    decision_engine = DecisionEngine(scoring_config)
    temp_manager = ClusterManager()
    
    nodes = []
    edges = []
    
    for a_key, b_key in candidates:
        rec_a = records.get(a_key)
        rec_b = records.get(b_key)
        
        if not rec_a or not rec_b:
            continue
            
        # Create dummy pair_id
        pair_id = f"{a_key}:{b_key}"
        
        score = scorer.score_pair(pair_id, rec_a, rec_b)
        decision = decision_engine.make_decision(score)
        
        if decision == MatchDecision.AUTO_LINK:
            temp_manager.link(a_key, b_key)
            edges.append(EdgeModel(
                source=a_key,
                target=b_key,
                type="MATCHES",
                weight=score.score,
                properties={"decision": "AUTO_LINK"}
            ))
        elif decision == MatchDecision.REVIEW:
             edges.append(EdgeModel(
                source=a_key,
                target=b_key,
                type="REVIEW",
                weight=score.score,
                properties={"decision": "REVIEW"}
            ))

    # 5. Build Graph Response
    # Iterate clusters in temp_manager
    clusters = temp_manager.get_clusters()
    
    for cid, members in clusters.items():
        if len(members) > 1:
            # Create cluster node
            # Use representative name
            primary_member_id = members[0]
            cluster_profile = records.get(primary_member_id, {})
            cluster_name = f"{cluster_profile.get('name', 'Cluster')} (Preview)"

            cluster_node = NodeModel(
                id=cid,
                label=cluster_name,
                type="cluster",
                properties={"size": len(members)}
            )
            nodes.append(cluster_node)
            
            for m in members:
                p = records.get(m, {})
                nodes.append(NodeModel(
                    id=m,
                    label=p.get('name', 'Unknown'),
                    type="record",
                    properties=p
                ))
                # Add edge to cluster
                edges.append(EdgeModel(
                    source=m,
                    target=cid,
                    type="MEMBER_OF",
                    weight=1.0
                ))
    
    return GraphResponse(
        nodes=nodes,
        edges=edges,
        stats={
            "total_clusters": len(clusters),
            "total_members": len(records),
            "preview": True
        }
    )
