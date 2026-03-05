"""
Neo4j Entity Resolution Integration

This module provides integration with Neo4j for entity resolution.
It loads records from Neo4j, performs entity resolution, and creates
DUPLICATE_OF relationships between matching entities.
"""

import json
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase

from engine.spark_er.utils.normalize import preprocess_record
from engine.spark_er.utils.entity_resolution import compute_similarity
from engine.spark_er.blocking.multi_pass_blocking import MultiPassBlocker, calculate_reduction


class Neo4jEntityResolver:
    """
    Neo4j-integrated entity resolution system.
    
    Connects to Neo4j database, loads entities, performs entity resolution,
    and creates DUPLICATE_OF relationships.
    """
    
    def __init__(self,
                 uri: str = "bolt://localhost:7687",
                 user: str = "neo4j",
                 password: str = "password123",
                 similarity_threshold: float = 0.75,
                 use_blocking: bool = True):
        """
        Initialize Neo4j entity resolver.
        
        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
            similarity_threshold: Minimum similarity for matches
            use_blocking: Whether to use multi-pass blocking
        """
        self.uri = uri
        self.user = user
        self.password = password
        self.similarity_threshold = similarity_threshold
        self.use_blocking = use_blocking
        
        # Connect to Neo4j
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
        # Initialize blocker
        if use_blocking:
            self.blocker = MultiPassBlocker(
                use_lsh=True,
                use_soundex=True,
                use_geohash=False,
                use_rules=True
            )
        else:
            self.blocker = None
        
        # Statistics
        self.stats = {
            'total_records': 0,
            'total_comparisons': 0,
            'total_matches': 0,
            'total_clusters': 0,
            'relationships_created': 0
        }
    
    def close(self):
        """Close Neo4j connection."""
        if self.driver:
            self.driver.close()
    
    def load_entities(self, label: str = "Customer", limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Load entities from Neo4j.
        
        Args:
            label: Node label to query
            limit: Optional limit on number of records
            
        Returns:
            List of entity dictionaries with 'id' and properties
        """
        with self.driver.session() as session:
            if limit:
                query = f"""
                MATCH (n:{label})
                RETURN elementId(n) as id, properties(n) as props
                LIMIT {limit}
                """
            else:
                query = f"""
                MATCH (n:{label})
                RETURN elementId(n) as id, properties(n) as props
                """
            
            result = session.run(query)
            
            entities = []
            for record in result:
                entity = {
                    'neo4j_id': record['id'],
                    **record['props']
                }
                entities.append(entity)
            
            return entities
    
    def create_duplicate_relationships(self, matches: List[tuple], label: str = "Customer"):
        """
        Create DUPLICATE_OF relationships in Neo4j for matching entities.
        
        Args:
            matches: List of (id1, id2, similarity) tuples
            label: Node label
        """
        with self.driver.session() as session:
            for neo4j_id1, neo4j_id2, similarity in matches:
                # Create bidirectional DUPLICATE_OF relationship
                query = """
                MATCH (a), (b)
                WHERE elementId(a) = $id1 AND elementId(b) = $id2
                MERGE (a)-[r:DUPLICATE_OF {similarity: $similarity}]->(b)
                MERGE (b)-[s:DUPLICATE_OF {similarity: $similarity}]->(a)
                RETURN r, s
                """
                
                session.run(query, id1=neo4j_id1, id2=neo4j_id2, similarity=similarity)
                self.stats['relationships_created'] += 2
    
    def delete_duplicate_relationships(self, label: str = "Customer"):
        """
        Delete all existing DUPLICATE_OF relationships.
        
        Args:
            label: Node label
        """
        with self.driver.session() as session:
            query = f"""
            MATCH (n:{label})-[r:DUPLICATE_OF]-()
            DELETE r
            """
            session.run(query)
    
    def resolve_entities(self, label: str = "Customer", limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Perform complete entity resolution on Neo4j data.
        
        Args:
            label: Node label to process
            limit: Optional limit on records
            
        Returns:
            Dictionary with matches and statistics
        """
        print(f"\nLoading entities from Neo4j (label: {label})...")
        entities = self.load_entities(label, limit)
        self.stats['total_records'] = len(entities)
        print(f"Loaded {len(entities)} entities")
        
        if len(entities) < 2:
            print("Not enough entities for resolution")
            return {'matches': [], 'statistics': self.stats}
        
        # Preprocess entities
        print("Preprocessing entities...")
        preprocessed = [preprocess_record(entity) for entity in entities]
        
        # Find candidates using blocking
        print("Finding candidate pairs...")
        if self.use_blocking and self.blocker:
            blocks = self.blocker.create_blocks(preprocessed)
            candidates = self.blocker.get_candidate_pairs(blocks)
            
            total_possible = (len(entities) * (len(entities) - 1)) // 2
            reduction = calculate_reduction(len(entities), len(candidates))
            print(f"Candidate pairs: {len(candidates):,} (reduction: {reduction:.2f}%)")
        else:
            candidates = set()
            for i in range(len(entities)):
                for j in range(i + 1, len(entities)):
                    candidates.add((i, j))
            print(f"Candidate pairs: {len(candidates):,} (no blocking)")
        
        self.stats['total_comparisons'] = len(candidates)
        
        # Compare candidates
        print("Comparing candidates...")
        matches = []
        
        # Auto-detect weights
        field_types = self._auto_detect_fields(preprocessed)
        weights = self._auto_detect_weights(field_types)
        
        for idx1, idx2 in candidates:
            similarity = compute_similarity(preprocessed[idx1], preprocessed[idx2], weights)
            
            if similarity >= self.similarity_threshold:
                # Store with Neo4j IDs
                neo4j_id1 = entities[idx1]['neo4j_id']
                neo4j_id2 = entities[idx2]['neo4j_id']
                matches.append((neo4j_id1, neo4j_id2, similarity))
        
        self.stats['total_matches'] = len(matches)
        print(f"Found {len(matches)} matches")
        
        # Create relationships in Neo4j
        if matches:
            print("Creating DUPLICATE_OF relationships...")
            self.delete_duplicate_relationships(label)
            self.create_duplicate_relationships(matches, label)
            print(f"Created {self.stats['relationships_created']} relationships")
        
        # Prepare results
        results = {
            'matches': [
                {
                    'neo4j_id1': m[0],
                    'neo4j_id2': m[1],
                    'similarity': m[2]
                }
                for m in matches
            ],
            'statistics': self.stats
        }
        
        return results
    
    def _auto_detect_fields(self, records: List[dict]) -> Dict[str, str]:
        """Auto-detect field types."""
        if not records:
            return {}
        
        field_types = {}
        sample = records[0]
        
        for field_name in sample.keys():
            if field_name == 'neo4j_id':
                continue
            
            field_lower = field_name.lower()
            
            if any(x in field_lower for x in ['name', 'nmf', 'nml']):
                field_types[field_name] = 'name'
            elif any(x in field_lower for x in ['phone', 'tel', 'mobile', 'fax']):
                field_types[field_name] = 'phone'
            elif any(x in field_lower for x in ['email', 'mail']):
                field_types[field_name] = 'email'
            elif any(x in field_lower for x in ['dob', 'birth', 'date']):
                field_types[field_name] = 'date'
            elif any(x in field_lower for x in ['address', 'addr', 'city']):
                field_types[field_name] = 'address'
            else:
                field_types[field_name] = 'text'
        
        return field_types
    
    def _auto_detect_weights(self, field_types: Dict[str, str]) -> Dict[str, float]:
        """Auto-detect field weights."""
        type_weights = {
            'name': 0.30,
            'date': 0.20,
            'phone': 0.15,
            'email': 0.10,
            'address': 0.05,
            'text': 0.05
        }
        
        from collections import defaultdict
        type_counts = defaultdict(int)
        for field_type in field_types.values():
            type_counts[field_type] += 1
        
        weights = {}
        for field_name, field_type in field_types.items():
            base_weight = type_weights.get(field_type, 0.05)
            weight = base_weight / max(type_counts[field_type], 1)
            weights[field_name] = weight
        
        total_weight = sum(weights.values())
        if total_weight > 0:
            weights = {k: v / total_weight for k, v in weights.items()}
        
        return weights
    
    def print_statistics(self):
        """Print entity resolution statistics."""
        print("\n" + "=" * 60)
        print("NEO4J ENTITY RESOLUTION STATISTICS")
        print("=" * 60)
        print(f"Total Records: {self.stats['total_records']}")
        print(f"Total Comparisons: {self.stats['total_comparisons']:,}")
        print(f"Total Matches: {self.stats['total_matches']}")
        print(f"Relationships Created: {self.stats['relationships_created']}")
        
        if self.use_blocking:
            total_possible = (self.stats['total_records'] * (self.stats['total_records'] - 1)) // 2
            if total_possible > 0:
                reduction = calculate_reduction(self.stats['total_records'], self.stats['total_comparisons'])
                print(f"Blocking Reduction: {reduction:.2f}%")
        
        print("=" * 60 + "\n")


# Example usage
if __name__ == "__main__":
    import sys
    import os
    
    # Configuration
    NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password123")
    
    # Create resolver
    resolver = Neo4jEntityResolver(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD,
        similarity_threshold=0.75,
        use_blocking=True
    )
    
    try:
        # Perform entity resolution
        results = resolver.resolve_entities(label="Customer", limit=200)
        
        # Print statistics
        resolver.print_statistics()
        
        # Save results to JSON
        output_file = "neo4j_entity_resolution_results.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"Results saved to {output_file}")
        
    finally:
        resolver.close()
