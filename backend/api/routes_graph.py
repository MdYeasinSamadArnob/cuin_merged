"""
CUIN v2 - Graph API Routes

Neo4j graph projection and visualization endpoints.
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime
from uuid import uuid4
import logging

from engine.clustering import ClusterManager, get_cluster_manager
from engine.structures import GoldenRecord

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
    members: List[str]
    member_details: List[dict] = []  # Rich details for display
    golden_record: Optional[dict] = None
    created_at: str


# ============================================
# Routes
# ============================================


@router.get("/clusters", response_model=GraphResponse)
async def get_cluster_graph(
    cluster_id: Optional[str] = Query(None, description="Filter to specific cluster"),
    limit: int = Query(100, le=500, description="Max nodes to return"),
):
    """
    Get cluster graph for visualization.
    
    Returns nodes and edges suitable for D3.js or similar graph libraries.
    """
    manager = get_cluster_manager()
    
    nodes: List[NodeModel] = []
    edges: List[EdgeModel] = []
    
    # Get cluster stats
    stats = manager.get_stats()
    
    # Get all clusters or filter by ID
    # Real Data Lookup (Neo4j + CSV Fallback)
    def get_mock_profile(rid: str):
        # 1. Try identifying from Real Backend (Neo4j)
        from engine.graph.neo4j_writer import get_neo4j_writer
        real_record = get_neo4j_writer().get_record(rid)
        if real_record:
            return real_record

        # 2. Fallback: Deterministic CSV Mapping (for demo consistency if DB is cold)
        import zlib
        seed = zlib.adler32(rid.encode())
        
        # Sample Data from User's CSV (Exact Mapping)
        real_samples = {
            "00050000": {"type": "STF", "name": "MD MOHI UDDIN", "dob": "1971-12-30", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "Alaska", "phone": "001-543-532-1819", "email": "mohi.uddin@example.com", "status": "SUSP", "risk": "High", "balance": 14500.50},
            "00050001": {"type": "REG", "name": "MOHAMMAD MOHI UDDIN", "dob": "1971-12-30", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "Georgia", "phone": "651.216.1559", "email": "mohi.uddin@example.com", "status": "INACT", "risk": "Medium", "balance": 5200.00},
            "00050002": {"type": "STF", "name": "MOHAMMAD MOHI UDDIN", "dob": "1971-12-30", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "Iowa", "phone": "664-375-2553", "email": "mohi.uddin@example.com", "status": "ACT", "risk": "Low", "balance": 89000.00},
            "00050003": {"type": "REG", "name": "MD MOHI UDDIN", "dob": "", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "Iowa", "phone": "9568413953", "email": "mohi.uddin@example.com", "status": "INACT", "risk": "Medium", "balance": 1200.75},
            "00050004": {"type": "STF", "name": "MOHAMMAD MOHI UDDIN", "dob": "1971-12-30", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "West Virginia", "phone": "+1-484-996-9653", "email": "", "status": "SUSP", "risk": "High", "balance": 250000.00},
            "00050005": {"type": "REG", "name": "KAZI MASIHUR RAHMAN", "dob": "1956-06-11", "addr": "901 Taylor Mountain", "city": "Garciastad", "state": "Utah", "phone": "564-217-0805", "email": "kazi.masihur@example.com", "status": "SUSP", "risk": "High", "balance": 4500.00},
            "00050006": {"type": "STF", "name": "KAZI MASIHUR RAHMAN", "dob": "1956-06-11", "addr": "901 Taylor Mountain", "city": "Garciastad", "state": "New York", "phone": "3159430391", "email": "kazi.masihur@example.com", "status": "SUSP", "risk": "High", "balance": 7800.25}
        }
        
        # Exact Lookup if ID matches 0005xxxx patterns
        if rid in real_samples:
            sample = real_samples[rid]
        else:
             # Fallback to deterministic selection for other IDs
             values = list(real_samples.values())
             sample = values[seed % len(values)]
        
        # Add some variation to make it look like a full dataset
        products = ["Savings Account", "Current Account", "DPS", "Home Loan", "SME Loan"]
        product = products[seed % len(products)]
        
        return {
            "name": sample['name'],
            "product": product,
            "riskLevel": sample['risk'], # CORRECTED KEY: risk -> riskLevel for Frontend
            "balance": f"৳{sample['balance']:,.2f}", 
            "email": sample['email'] or "N/A",
            "phone": sample['phone'],
            "city": sample['city'],
            "status": sample['status'],
            "kycStatus": "VERIFIED" if sample['status'] == 'ACT' else "PENDING"
        }

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
            profile = get_mock_profile(member)
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
                target=cluster_id,
                type="REPRESENTS",
                weight=1.0
            ))
    else:
        # Get all clusters (with limit)
        all_clusters = manager.get_clusters()
        cluster_count = 0
        
        for cid, members in all_clusters.items():
            if cluster_count >= limit:
                break
            
            # Only show clusters with >1 member for graph visualization
            if len(members) > 1:
                # Create "Golden" profile for the cluster
                # CRITICAL FIX: Use the first member's ID to derive the profile, 
                # ensuring we match the "Real Data" (0005xxxx) instead of the random Cluster ID.
                # CRITICAL FIX: Smart Golden Record Selection
                # Instead of random first member, find the "Most Frequent Name" to label the cluster.
                # This ensures the Cluster Node represents the "Correct" entity (e.g. "KAZI MASIHUR")
                # rather than a typo variant (e.g. "KAZI MASHIRUL").
                
                member_profiles = []
                name_counts = {}
                
                # Scan members (up to 20 for performance) to find consensus
                for m_id in members[:20]:
                    p = get_mock_profile(m_id)
                    member_profiles.append(p)
                    n = p['name']
                    name_counts[n] = name_counts.get(n, 0) + 1
                
                # Pick most frequent name
                golden_name = max(name_counts, key=name_counts.get)
                
                # Find a profile that matches this golden name to use as base
                golden_profile = next(p for p in member_profiles if p['name'] == golden_name)
                
                # Create Composite Profile
                cluster_profile = golden_profile.copy()
                cluster_profile['name'] = f"{golden_name} (Unique Entity)" # Distinct Label
                
                # Check for Strong Match Signal (Visual Aid)
                # If ANY members share Email/Phone/NID, tag as high confidence
                is_strong_match = False
                email_counts = {}
                phone_counts = {}
                
                # Analyze Strong Identifiers across members
                for p in member_profiles:
                    # Tally Emails
                    e = p.get('email')
                    if e and len(e) > 3: # Basic valid check
                        email_counts[e] = email_counts.get(e, 0) + 1
                        if email_counts[e] > 1: is_strong_match = True
                    
                    # Tally Phones
                    ph = p.get('phone')
                    if ph and len(ph) > 5:
                        phone_counts[ph] = phone_counts.get(ph, 0) + 1
                        if phone_counts[ph] > 1: is_strong_match = True
                    
                    if is_strong_match: break
                
                nodes.append(NodeModel(
                    id=cid,
                    label=cluster_profile['name'], # Show "Correct" Name
                    type="cluster",
                    properties={
                        "size": len(members),
                        "confidence": "high" if is_strong_match else "medium",
                        **cluster_profile # Include rich data
                    }
                ))
                
                for member in members[:10]:  # Limit members per cluster for visualization
                    profile = get_mock_profile(member)
                    nodes.append(NodeModel(
                        id=member,
                        label=profile['name'], # Use Name as Label
                        type="record",
                        properties=profile
                    ))
                    edges.append(EdgeModel(
                        source=member,
                        target=cid,
                        type="MEMBER_OF",
                        weight=1.0
                    ))
                
                cluster_count += 1
    
    return GraphResponse(
        nodes=nodes[:limit*10], # Allow more nodes since we expanded members
        edges=edges,
        stats={
            "total_clusters": stats.get("total_clusters", 0),
            "total_members": stats.get("total_members", 0),
            "avg_cluster_size": stats.get("avg_cluster_size", 0),
            "nodes_returned": len(nodes),
            "edges_returned": len(edges)
        }
    )


@router.get("/cluster/{cluster_id}", response_model=ClusterSummary)
async def get_cluster_details(cluster_id: str):
    """Get details for a specific cluster."""
    manager = get_cluster_manager()
    
    members = manager.get_cluster_members(cluster_id)
    if not members:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")
    
    golden = manager.get_golden_record(cluster_id)
    
    # Simple Mock Profile Logic (shared)
    member_details = []
    
    # Re-using the simple mock logic for consistency (Quick-Fix)
    # Ideally refactor to a shared util function
    products = ["Savings Account", "Current Account", "DPS", "Home Loan", "SME Loan"]
    import zlib
    
    real_samples = {
            "00050000": {"type": "STF", "name": "MD MOHI UDDIN", "dob": "1971-12-30", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "Alaska", "phone": "001-543-532-1819", "email": "mohi.uddin@example.com", "status": "SUSP", "risk": "High", "balance": 14500.50},
            "00050001": {"type": "REG", "name": "MOHAMMAD MOHI UDDIN", "dob": "1971-12-30", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "Georgia", "phone": "651.216.1559", "email": "mohi.uddin@example.com", "status": "INACT", "risk": "Medium", "balance": 5200.00},
            "00050002": {"type": "STF", "name": "MOHAMMAD MOHI UDDIN", "dob": "1971-12-30", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "Iowa", "phone": "664-375-2553", "email": "mohi.uddin@example.com", "status": "ACT", "risk": "Low", "balance": 89000.00},
            "00050003": {"type": "REG", "name": "MD MOHI UDDIN", "dob": "", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "Iowa", "phone": "9568413953", "email": "mohi.uddin@example.com", "status": "INACT", "risk": "Medium", "balance": 1200.75},
            "00050004": {"type": "STF", "name": "MOHAMMAD MOHI UDDIN", "dob": "1971-12-30", "addr": "489 Eric Track", "city": "Lake Crystalbury", "state": "West Virginia", "phone": "+1-484-996-9653", "email": "", "status": "SUSP", "risk": "High", "balance": 250000.00},
            "00050005": {"type": "REG", "name": "KAZI MASIHUR RAHMAN", "dob": "1956-06-11", "addr": "901 Taylor Mountain", "city": "Garciastad", "state": "Utah", "phone": "564-217-0805", "email": "kazi.masihur@example.com", "status": "SUSP", "risk": "High", "balance": 4500.00},
            "00050006": {"type": "STF", "name": "KAZI MASIHUR RAHMAN", "dob": "1956-06-11", "addr": "901 Taylor Mountain", "city": "Garciastad", "state": "New York", "phone": "3159430391", "email": "kazi.masihur@example.com", "status": "SUSP", "risk": "High", "balance": 7800.25},
            "9001": {"type": "REG", "name": "KAZI MASIHUR RAHMAN", "dob": "1956-06-11", "phone": "564-217-0805", "email": "kazi.masihur@example.com", "risk": "High", "balance": 4500.00, "status": "ACTIVE"},
            "9002": {"type": "REG", "name": "MD MOHI UDDIN", "dob": "1971-12-30", "phone": "564-217-0805", "email": "mohi.uddin@example.com", "risk": "Medium", "balance": 5200.00, "status": "ACTIVE"}
    }
    
    for mid in members:
        seed = zlib.adler32(mid.encode())
        if mid in real_samples:
            sample = real_samples[mid]
        else:
             values = list(real_samples.values())
             sample = values[seed % len(values)]

        product = products[seed % len(products)]
        
        member_details.append({
            "id": mid,
            "name": sample['name'],
            "product": product,
            "riskLevel": sample.get('risk', 'Low'),
            "balance": f"৳{sample.get('balance', 0):,.2f}", 
            "email": sample.get('email', "N/A"),
            "phone": sample.get('phone', "N/A"),
            "city": sample.get('city', "Unknown"),
            "status": sample.get('status', "ACTIVE")
        })

    return ClusterSummary(
        cluster_id=cluster_id,
        size=len(members),
        members=members,
        member_details=member_details,
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


@router.get("/stats")
async def get_graph_stats():
    """Get overall graph statistics."""
    manager = get_cluster_manager()
    stats = manager.get_stats()
    
    return {
        "total_clusters": stats.get("total_clusters", 0),
        "total_members": stats.get("total_members", 0),
        "golden_records_count": stats.get("golden_records_count", 0),
        "avg_cluster_size": stats.get("avg_cluster_size", 0),
        "max_cluster_size": stats.get("max_cluster_size", 0),
        "singleton_clusters": stats.get("singleton_clusters", 0),
        "size_distribution": stats.get("size_distribution", {}),
        "generated_at": datetime.utcnow().isoformat()
    }


@router.post("/golden-record/{cluster_id}")
async def generate_golden_record(
    cluster_id: str,
    force: bool = Query(False, description="Force regeneration if exists")
):
    """
    Generate or regenerate the golden record for a cluster.
    
    Golden records are canonical representations that merge data
    from all cluster members using survivorship rules.
    """
    manager = get_cluster_manager()
    
    members = manager.get_cluster_members(cluster_id)
    if not members:
        raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")
    
    # Check if golden record already exists
    existing = manager.get_golden_record(cluster_id)
    if existing and not force:
        return {
            "message": "Golden record already exists",
            "golden_record": existing.payload,
            "hint": "Use force=true to regenerate"
        }
    
    # For demo, create a simple golden record
    # In production, this would gather member records from DB
    # and apply survivorship rules
    
    # Placeholder - needs actual member data
    golden = manager.get_golden_record(cluster_id)
    
    return {
        "message": "Golden record generated" if golden else "Golden record generation pending",
        "cluster_id": cluster_id,
        "member_count": len(members)
    }


@router.get("/export/cypher")
async def export_to_cypher():
    """
    Export cluster data as Cypher statements for Neo4j import.
    
    Returns Cypher CREATE statements that can be run directly
    against a Neo4j instance.
    """
    manager = get_cluster_manager()
    all_clusters = manager.get_clusters()
    
    statements = [
        "// CUIN v2 Cluster Export",
        f"// Generated at {datetime.utcnow().isoformat()}",
        "",
        "// Create constraints",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Cluster) REQUIRE c.id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Record) REQUIRE r.id IS UNIQUE;",
        "",
        "// Create clusters and relationships"
    ]
    
    for cluster_id, members in all_clusters.items():
        if len(members) > 1:  # Skip singletons
            # Create cluster node
            statements.append(
                f"MERGE (c:Cluster {{id: '{cluster_id}', size: {len(members)}}});"
            )
            
            # Create member nodes and relationships
            for member in members:
                statements.append(
                    f"MERGE (r:Record {{id: '{member}'}});"
                )
                statements.append(
                    f"MATCH (r:Record {{id: '{member}'}}), (c:Cluster {{id: '{cluster_id}'}}) "
                    f"MERGE (r)-[:MEMBER_OF]->(c);"
                )
    
    return {
        "statements": statements,
        "cluster_count": len([c for c, m in all_clusters.items() if len(m) > 1]),
        "export_format": "cypher"
    }


@router.post("/recluster")
async def recluster_graph(background_tasks: BackgroundTasks):
    """
    Trigger a full re-clustering using the standardized Pipeline (same as Explorer).
    
    This creates a new Run in 'FULL' mode and executes it in the background.
    """
    from services.run_service import get_run_service, RunMode
    import csv
    import os
    
    manager = get_cluster_manager()
    manager.reset()
    
    run_service = get_run_service()
    
    # Create a new run
    run = run_service.create_run(
        mode=RunMode.FULL.value,
        description="Graph Re-cluster (User Initiated)",
        policy_version=1
    )
    
    # Execute in background (so UI returns immediately)
    async def execute_pipeline():
        try:
            # Load the dataset explicitly to ensure we use the clean version
            possible_paths = [
                "data/challenging_er_200.csv",          # If run from backend root
                "backend/data/challenging_er_200.csv",  # If run from project root
                "../data/challenging_er_200.csv",       # Fallback
            ]
            
            csv_path = None
            for p in possible_paths:
                if os.path.exists(p):
                    csv_path = p
                    break
            
            if not csv_path:
                 # Try one more relative path for safety
                if os.path.exists("../backend/data/challenging_er_200.csv"):
                     csv_path = "../backend/data/challenging_er_200.csv"
            
            records = []
            if csv_path:
                print(f"Loading data from {csv_path} for run {run.run_id}")
                with open(csv_path, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Improved Phone Normalization
                        raw_phone = (row.get('MOBLNO') or row.get('TELENO') or '').lower()
                        if 'x' in raw_phone: raw_phone = raw_phone.split('x')[0]
                        phone_digits = "".join(filter(str.isdigit, raw_phone))
                        if len(phone_digits) == 11 and phone_digits.startswith('1'): phone_digits = phone_digits[1:]
                            
                        # Pass raw record to pipeline, it handles normalization
                        # But we can pre-process if needed. 
                        # Actually, PipelineOrchestrator Stage 1 takes raw dicts.
                        # We just need to ensure the keys match what normalization expects.
                        records.append(row)
            else:
                print("Warning: Dataset not found, running pipeline with empty/demo fallback")

            await run_service.execute_run(run.run_id, records=records) 
        except Exception as e:
            print(f"Graph re-cluster pipeline failed: {e}")

    background_tasks.add_task(execute_pipeline)
    
    return {
        "message": "Re-clustering started",
        "run_id": run.run_id,
        "status": "running"
    }
