"""
Neo4j Helper Functions

This module provides utility functions for storing entity resolution
results in Neo4j database.
"""

from typing import List, Dict


def create_entity_resolution_graph(driver, matches: List[Dict], clusters: List[Dict], records: List[dict]) -> Dict:
    """
    Create entity resolution graph in Neo4j with clusters and entities.
    
    This creates:
    - EntityCluster nodes for each cluster
    - Entity nodes for records in clusters
    - MEMBER_OF relationships from entities to clusters
    - DUPLICATE_OF relationships between matching entities
    
    Args:
        driver: Neo4j driver instance
        matches: List of match dictionaries with keys:
                 - 'record1_idx' or 'id1': First record index
                 - 'record2_idx' or 'id2': Second record index
                 - 'probability' or 'similarity': Similarity score
        clusters: List of cluster dictionaries with keys:
                  - 'cluster_id': Unique cluster identifier
                  - 'member_indices': List of record indices in cluster
                  OR list of sets (Flink-ER format)
        records: Original record data
        
    Returns:
        Dictionary with creation statistics
    """
    stats = {
        'clusters_created': 0,
        'entities_created': 0,
        'member_relationships': 0,
        'duplicate_relationships': 0
    }
    
    with driver.session() as session:
        # Clear existing entity resolution graph
        print("Clearing existing EntityCluster and Entity nodes...")
        session.run("MATCH (n:EntityCluster) DETACH DELETE n")
        session.run("MATCH (n:Entity) DETACH DELETE n")
        
        # Create clusters and entities in batches
        print(f"Creating {len(clusters)} entity clusters...")
        
        for idx, cluster_info in enumerate(clusters):
            # Handle both cluster dict formats (from XGBoost and Flink-ER)
            if isinstance(cluster_info, dict):
                cluster_id = f"cluster_{cluster_info.get('cluster_id', idx)}"
                member_indices = cluster_info.get('member_indices', [])
            else:
                # If clusters is a list of sets (Flink-ER format)
                cluster_id = f"cluster_{idx}"
                member_indices = list(cluster_info)
            
            if not member_indices:
                continue
            
            # Get sample entity info from first member
            sample_record = records[member_indices[0]]
            cluster_name = sample_record.get('CUSNMF', 'Unknown')
            
            # Create cluster node
            session.run("""
                CREATE (c:EntityCluster {
                    clusterId: $cluster_id,
                    name: $name,
                    size: $size,
                    memberIds: $member_ids
                })
            """, cluster_id=cluster_id, name=cluster_name, size=len(member_indices), member_ids=member_indices)
            stats['clusters_created'] += 1
            
            # Prepare entity data for batch creation
            entity_data = []
            for member_idx in member_indices:
                record = records[member_idx]
                entity_data.append({
                    'entityId': member_idx,
                    'accountId': record.get('CUSCOD', ''),
                    'firstName': record.get('CUSNMF', ''),
                    'lastName': record.get('CUSNML', ''),
                    'dob': record.get('CUSDOB', ''),
                    'phone': record.get('TELENO', ''),
                    'email': record.get('MAILID', ''),
                    'city': record.get('CITYNM', ''),
                    'clusterId': cluster_id
                })
            
            # Batch create entities and relationships for this cluster
            session.run("""
                UNWIND $entities AS entity
                CREATE (e:Entity)
                SET e = entity
                WITH e, entity.clusterId as clusterId
                MATCH (c:EntityCluster {clusterId: clusterId})
                CREATE (e)-[:MEMBER_OF]->(c)
            """, entities=entity_data)
            
            stats['entities_created'] += len(entity_data)
            stats['member_relationships'] += len(entity_data)
        
        # Create DUPLICATE_OF relationships in batches
        print(f"Creating DUPLICATE_OF relationships for {len(matches)} matches...")
        
        # Prepare match data for batch creation
        match_data = []
        for match in matches:
            # Handle different match dictionary formats
            id1 = match.get('record1_idx', match.get('id1'))
            id2 = match.get('record2_idx', match.get('id2'))
            similarity = match.get('probability', match.get('similarity', 0.0))
            
            match_data.append({
                'id1': id1,
                'id2': id2,
                'similarity': similarity
            })
        
        # Batch create all DUPLICATE_OF relationships with consistent batch size
        batch_size = 1000
        for i in range(0, len(match_data), batch_size):
            batch = match_data[i:i + batch_size]
            session.run("""
                UNWIND $matches AS match
                MATCH (e1:Entity {entityId: match.id1})
                MATCH (e2:Entity {entityId: match.id2})
                CREATE (e1)-[:DUPLICATE_OF {similarity: match.similarity}]->(e2)
                CREATE (e2)-[:DUPLICATE_OF {similarity: match.similarity}]->(e1)
            """, matches=batch)
            stats['duplicate_relationships'] += len(batch) * 2
    
    print(f"✅ Entity resolution graph created successfully!")
    print(f"  • Clusters: {stats['clusters_created']}")
    print(f"  • Entities: {stats['entities_created']}")
    print(f"  • MEMBER_OF relationships: {stats['member_relationships']}")
    print(f"  • DUPLICATE_OF relationships: {stats['duplicate_relationships']}")
    
    return stats
