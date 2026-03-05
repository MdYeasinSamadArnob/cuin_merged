"""
Generic Entity Resolution Engine

This module provides a generic entity resolution system that works with ANY entity type.
It auto-detects fields, applies configurable matching strategies, and performs
entity resolution using multi-pass blocking and similarity algorithms.

Perfect for the challenge_er_200 dataset regardless of entity type.
"""

import json
from typing import List, Dict, Any, Optional, Set, Tuple
from collections import defaultdict

from engine.spark_er.utils.normalize import preprocess_record
from engine.spark_er.utils.entity_resolution import compute_similarity
from engine.spark_er.blocking.multi_pass_blocking import MultiPassBlocker, calculate_reduction


class GenericEntityResolver:
    """
    Generic entity resolution engine that works with any entity type.
    
    Features:
    - Auto-detects fields and types
    - Configurable matching strategies
    - Multi-pass blocking for efficiency
    - Similarity-based matching
    - Transitive closure for clusters
    """
    
    def __init__(self,
                 similarity_threshold: float = 0.75,
                 field_weights: Optional[Dict[str, float]] = None,
                 use_blocking: bool = True,
                 blocking_config: Optional[Dict[str, Any]] = None):
        """
        Initialize generic entity resolver.
        
        Args:
            similarity_threshold: Minimum similarity score for matches (0.0 to 1.0)
            field_weights: Optional custom weights for fields
            use_blocking: Whether to use multi-pass blocking
            blocking_config: Configuration for blocking strategies
        """
        self.similarity_threshold = similarity_threshold
        self.field_weights = field_weights
        self.use_blocking = use_blocking
        
        # Initialize blocker
        if use_blocking:
            if blocking_config is None:
                blocking_config = {
                    'use_lsh': True,
                    'use_soundex': True,
                    'use_geohash': False,
                    'use_rules': True
                }
            self.blocker = MultiPassBlocker(**blocking_config)
        else:
            self.blocker = None
        
        # Statistics
        self.stats = {
            'total_records': 0,
            'total_comparisons': 0,
            'total_matches': 0,
            'total_clusters': 0,
            'reduction_percentage': 0.0
        }
    
    def auto_detect_fields(self, records: List[dict]) -> Dict[str, str]:
        """
        Auto-detect field types from a sample of records.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Dictionary mapping field names to detected types
        """
        if not records:
            return {}
        
        field_types = {}
        sample = records[0]
        
        for field_name in sample.keys():
            field_lower = field_name.lower()
            
            # Detect field type based on name patterns
            if any(x in field_lower for x in ['name', 'nmf', 'nml']):
                field_types[field_name] = 'name'
            elif any(x in field_lower for x in ['phone', 'tel', 'mobile', 'fax']):
                field_types[field_name] = 'phone'
            elif any(x in field_lower for x in ['email', 'mail']):
                field_types[field_name] = 'email'
            elif any(x in field_lower for x in ['dob', 'birth', 'date']):
                field_types[field_name] = 'date'
            elif any(x in field_lower for x in ['address', 'addr', 'city', 'street']):
                field_types[field_name] = 'address'
            elif any(x in field_lower for x in ['id', 'code', 'number']):
                field_types[field_name] = 'id'
            else:
                field_types[field_name] = 'text'
        
        return field_types
    
    def auto_detect_weights(self, field_types: Dict[str, str]) -> Dict[str, float]:
        """
        Auto-detect field weights based on field types.
        
        Args:
            field_types: Dictionary of field names to types
            
        Returns:
            Dictionary of field weights
        """
        # Default weights by type
        type_weights = {
            'name': 0.30,
            'date': 0.20,
            'phone': 0.15,
            'email': 0.10,
            'id': 0.15,
            'address': 0.05,
            'text': 0.05
        }
        
        # Count fields of each type
        type_counts = defaultdict(int)
        for field_type in field_types.values():
            type_counts[field_type] += 1
        
        # Assign weights
        weights = {}
        for field_name, field_type in field_types.items():
            base_weight = type_weights.get(field_type, 0.05)
            # Distribute weight among fields of same type
            weight = base_weight / max(type_counts[field_type], 1)
            weights[field_name] = weight
        
        # Normalize to sum to 1.0
        total_weight = sum(weights.values())
        if total_weight > 0:
            weights = {k: v / total_weight for k, v in weights.items()}
        
        return weights
    
    def preprocess_records(self, records: List[dict]) -> List[dict]:
        """
        Preprocess all records by normalizing fields.
        
        Args:
            records: List of raw records
            
        Returns:
            List of preprocessed records
        """
        return [preprocess_record(record) for record in records]
    
    def find_candidates(self, records: List[dict]) -> Set[Tuple[int, int]]:
        """
        Find candidate pairs using blocking or brute force.
        
        Args:
            records: List of preprocessed records
            
        Returns:
            Set of candidate pairs (i, j) where i < j
        """
        if self.use_blocking and self.blocker:
            # Use multi-pass blocking
            blocks = self.blocker.create_blocks(records)
            candidates = self.blocker.get_candidate_pairs(blocks)
            
            # Update statistics
            total_possible = (len(records) * (len(records) - 1)) // 2
            self.stats['reduction_percentage'] = calculate_reduction(len(records), len(candidates))
        else:
            # Brute force: compare all pairs
            candidates = set()
            for i in range(len(records)):
                for j in range(i + 1, len(records)):
                    candidates.add((i, j))
        
        return candidates
    
    def compare_records(self, record1: dict, record2: dict, weights: Dict[str, float]) -> float:
        """
        Compare two records and compute similarity.
        
        Args:
            record1: First record
            record2: Second record
            weights: Field weights
            
        Returns:
            Similarity score (0.0 to 1.0)
        """
        return compute_similarity(record1, record2, weights)
    
    def find_matches(self, records: List[dict]) -> List[Tuple[int, int, float]]:
        """
        Find all matching record pairs.
        
        Args:
            records: List of preprocessed records
            
        Returns:
            List of tuples (index1, index2, similarity_score)
        """
        # Auto-detect fields if weights not provided
        if self.field_weights is None:
            field_types = self.auto_detect_fields(records)
            weights = self.auto_detect_weights(field_types)
        else:
            weights = self.field_weights
        
        # Find candidate pairs
        candidates = self.find_candidates(records)
        
        # Compare candidates
        matches = []
        self.stats['total_comparisons'] = len(candidates)
        
        for idx1, idx2 in candidates:
            similarity = self.compare_records(records[idx1], records[idx2], weights)
            
            if similarity >= self.similarity_threshold:
                matches.append((idx1, idx2, similarity))
        
        self.stats['total_matches'] = len(matches)
        
        return matches
    
    def build_clusters(self, matches: List[Tuple[int, int, float]], num_records: int) -> List[Set[int]]:
        """
        Build clusters using transitive closure (Union-Find).
        
        Args:
            matches: List of matching pairs
            num_records: Total number of records
            
        Returns:
            List of clusters (sets of record indices)
        """
        # Initialize Union-Find
        parent = list(range(num_records))
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        # Union matching pairs
        for idx1, idx2, _ in matches:
            union(idx1, idx2)
        
        # Build clusters
        cluster_map = defaultdict(set)
        for i in range(num_records):
            root = find(i)
            cluster_map[root].add(i)
        
        # Convert to list of sets
        clusters = [cluster for cluster in cluster_map.values() if len(cluster) > 1]
        
        self.stats['total_clusters'] = len(clusters)
        
        return clusters
    
    def resolve(self, records: List[dict]) -> Dict[str, Any]:
        """
        Perform complete entity resolution.
        
        Args:
            records: List of raw records
            
        Returns:
            Dictionary containing matches, clusters, and statistics
        """
        # Update statistics
        self.stats['total_records'] = len(records)
        
        # Preprocess records
        preprocessed = self.preprocess_records(records)
        
        # Find matches
        matches = self.find_matches(preprocessed)
        
        # Build clusters
        clusters = self.build_clusters(matches, len(records))
        
        # Prepare results
        results = {
            'matches': [
                {
                    'record1_idx': idx1,
                    'record2_idx': idx2,
                    'similarity': similarity
                }
                for idx1, idx2, similarity in matches
            ],
            'clusters': [
                {
                    'cluster_id': i,
                    'record_indices': sorted(list(cluster)),
                    'size': len(cluster)
                }
                for i, cluster in enumerate(clusters)
            ],
            'statistics': self.stats
        }
        
        return results
    
    def print_statistics(self):
        """Print entity resolution statistics."""
        print("\n" + "=" * 60)
        print("ENTITY RESOLUTION STATISTICS")
        print("=" * 60)
        print(f"Total Records: {self.stats['total_records']}")
        print(f"Total Comparisons: {self.stats['total_comparisons']:,}")
        print(f"Total Matches: {self.stats['total_matches']}")
        print(f"Total Clusters: {self.stats['total_clusters']}")
        
        if self.use_blocking:
            print(f"Reduction from Blocking: {self.stats['reduction_percentage']:.2f}%")
        
        total_possible = (self.stats['total_records'] * (self.stats['total_records'] - 1)) // 2
        if total_possible > 0:
            print(f"Total Possible Pairs: {total_possible:,}")
            efficiency = (1 - self.stats['total_comparisons'] / total_possible) * 100
            print(f"Overall Efficiency: {efficiency:.2f}%")
        
        print("=" * 60 + "\n")


def load_csv_records(csv_path: str) -> List[dict]:
    """
    Load records from CSV file.
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        List of record dictionaries
    """
    import csv
    
    records = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    
    return records


def save_results(results: Dict[str, Any], output_path: str):
    """
    Save entity resolution results to JSON file.
    
    Args:
        results: Results dictionary
        output_path: Output file path
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)


# Example usage
if __name__ == "__main__":
    import sys
    
    # Check arguments
    if len(sys.argv) < 2:
        print("Usage: python generic_entity_resolution.py <csv_file> [output_json]")
        print("Example: python generic_entity_resolution.py csv/challenging_er_200.csv results.json")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "entity_resolution_results.json"
    
    # Load records
    print(f"Loading records from {csv_file}...")
    records = load_csv_records(csv_file)
    print(f"Loaded {len(records)} records")
    
    # Create resolver
    resolver = GenericEntityResolver(
        similarity_threshold=0.75,
        use_blocking=True,
        blocking_config={
            'use_lsh': True,
            'use_soundex': True,
            'use_geohash': False,
            'use_rules': True
        }
    )
    
    # Perform entity resolution
    print("\nPerforming entity resolution...")
    results = resolver.resolve(records)
    
    # Print statistics
    resolver.print_statistics()
    
    # Print sample results
    print(f"Found {len(results['matches'])} matches")
    print(f"Found {len(results['clusters'])} clusters")
    
    if results['clusters']:
        print("\nSample clusters:")
        for cluster in results['clusters'][:5]:
            print(f"  Cluster {cluster['cluster_id']}: {cluster['size']} records - indices {cluster['record_indices']}")
    
    # Save results
    save_results(results, output_file)
    print(f"\nResults saved to {output_file}")
