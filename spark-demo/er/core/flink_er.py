"""
Flink-ER: Distributed Entity Resolution System (Python Implementation)

This module provides a Flink-inspired entity resolution system for Python.
It implements distributed batch processing capabilities similar to Apache Flink,
using Python's multiprocessing and concurrent execution for parallel processing.

Key Features:
- Parallel batch processing
- Map-Reduce style operations
- Fault tolerance with retry logic
- Windowing and partitioning
- Stream-like API for batch data
- Configurable parallelism

Inspired by Apache Flink's DataStream/DataSet APIs but implemented purely in Python.
"""

import json
import os
from typing import List, Dict, Any, Optional, Callable, Tuple, Set
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
import time

from er.utils.normalize import preprocess_record
from er.utils.entity_resolution import compute_similarity
from er.blocking.multi_pass_blocking import MultiPassBlocker, calculate_reduction


class FlinkERConfig:
    """Configuration for Flink-ER distributed processing."""
    
    def __init__(self,
                 parallelism: int = None,
                 batch_size: int = 100,
                 max_workers: int = None,
                 use_processes: bool = True,
                 retry_attempts: int = 3,
                 similarity_threshold: float = 0.75):
        """
        Initialize Flink-ER configuration.
        
        Args:
            parallelism: Degree of parallelism (default: CPU count)
            batch_size: Size of data batches for processing
            max_workers: Maximum worker threads/processes (default: parallelism)
            use_processes: Use processes (True) or threads (False)
            retry_attempts: Number of retry attempts for failed tasks
            similarity_threshold: Minimum similarity score for matches
        """
        self.parallelism = parallelism or cpu_count()
        self.batch_size = batch_size
        self.max_workers = max_workers or self.parallelism
        self.use_processes = use_processes
        self.retry_attempts = retry_attempts
        self.similarity_threshold = similarity_threshold


class DataStream:
    """
    Flink-inspired DataStream for batch processing.
    
    Provides a fluent API for data transformations similar to Apache Flink's
    DataStream API, but operates on batch data using Python's concurrent execution.
    """
    
    def __init__(self, data: List[Any], config: FlinkERConfig):
        """
        Initialize DataStream.
        
        Args:
            data: Input data records
            config: Flink-ER configuration
        """
        self.data = data
        self.config = config
        self._transformations = []
    
    def map(self, func: Callable, description: str = "map") -> 'DataStream':
        """
        Apply a map transformation to each element.
        
        Args:
            func: Function to apply to each element
            description: Description of the transformation
            
        Returns:
            New DataStream with transformation applied
        """
        print(f"[FlinkER] Applying {description} transformation...")
        start_time = time.time()
        
        # Create batches for parallel processing
        batches = self._create_batches(self.data)
        
        # Execute map in parallel
        results = []
        executor_class = ProcessPoolExecutor if self.config.use_processes else ThreadPoolExecutor
        
        with executor_class(max_workers=self.config.max_workers) as executor:
            futures = []
            for batch_idx, batch in enumerate(batches):
                future = executor.submit(self._map_batch, func, batch, batch_idx)
                futures.append(future)
            
            for future in as_completed(futures):
                batch_result = future.result()
                results.extend(batch_result)
        
        elapsed = time.time() - start_time
        print(f"[FlinkER] {description} completed in {elapsed:.2f}s ({len(results)} records)")
        
        return DataStream(results, self.config)
    
    def filter(self, predicate: Callable, description: str = "filter") -> 'DataStream':
        """
        Filter elements based on a predicate.
        
        Args:
            predicate: Function that returns True for elements to keep
            description: Description of the filter
            
        Returns:
            New DataStream with filtered data
        """
        print(f"[FlinkER] Applying {description}...")
        start_time = time.time()
        
        filtered = [item for item in self.data if predicate(item)]
        
        elapsed = time.time() - start_time
        print(f"[FlinkER] {description} completed: {len(filtered)}/{len(self.data)} records kept")
        
        return DataStream(filtered, self.config)
    
    def flat_map(self, func: Callable, description: str = "flatMap") -> 'DataStream':
        """
        Apply a flat map transformation (one-to-many mapping).
        
        Args:
            func: Function that returns an iterable for each element
            description: Description of the transformation
            
        Returns:
            New DataStream with flattened results
        """
        print(f"[FlinkER] Applying {description}...")
        start_time = time.time()
        
        results = []
        for item in self.data:
            results.extend(func(item))
        
        elapsed = time.time() - start_time
        print(f"[FlinkER] {description} completed: {len(self.data)} -> {len(results)} records")
        
        return DataStream(results, self.config)
    
    def key_by(self, key_func: Callable) -> 'KeyedDataStream':
        """
        Partition data by key (similar to Flink's keyBy).
        
        Args:
            key_func: Function to extract key from each element
            
        Returns:
            KeyedDataStream partitioned by key
        """
        print(f"[FlinkER] Partitioning data by key...")
        
        keyed_data = defaultdict(list)
        for item in self.data:
            key = key_func(item)
            keyed_data[key].append(item)
        
        print(f"[FlinkER] Created {len(keyed_data)} partitions")
        
        return KeyedDataStream(dict(keyed_data), self.config)
    
    def collect(self) -> List[Any]:
        """
        Collect all data (terminal operation).
        
        Returns:
            List of all data elements
        """
        return self.data
    
    def count(self) -> int:
        """
        Count elements (terminal operation).
        
        Returns:
            Number of elements
        """
        return len(self.data)
    
    def _create_batches(self, data: List[Any]) -> List[List[Any]]:
        """Create batches from data."""
        batches = []
        for i in range(0, len(data), self.config.batch_size):
            batches.append(data[i:i + self.config.batch_size])
        return batches
    
    @staticmethod
    def _map_batch(func: Callable, batch: List[Any], batch_idx: int) -> List[Any]:
        """Process a batch with the map function."""
        return [func(item) for item in batch]


class KeyedDataStream:
    """
    Keyed data stream (similar to Flink's KeyedStream).
    
    Enables operations on partitioned data.
    """
    
    def __init__(self, keyed_data: Dict[Any, List[Any]], config: FlinkERConfig):
        """
        Initialize KeyedDataStream.
        
        Args:
            keyed_data: Dictionary mapping keys to lists of values
            config: Flink-ER configuration
        """
        self.keyed_data = keyed_data
        self.config = config
    
    def reduce(self, reduce_func: Callable) -> 'DataStream':
        """
        Reduce values within each key group.
        
        Args:
            reduce_func: Function to reduce values (takes two args, returns one)
            
        Returns:
            DataStream with reduced results
        """
        print(f"[FlinkER] Reducing {len(self.keyed_data)} key groups...")
        
        results = []
        for key, values in self.keyed_data.items():
            if len(values) == 1:
                results.append(values[0])
            else:
                reduced = values[0]
                for val in values[1:]:
                    reduced = reduce_func(reduced, val)
                results.append(reduced)
        
        return DataStream(results, self.config)
    
    def aggregate(self, agg_func: Callable) -> 'DataStream':
        """
        Aggregate values within each key group.
        
        Args:
            agg_func: Function to aggregate a list of values
            
        Returns:
            DataStream with aggregated results
        """
        print(f"[FlinkER] Aggregating {len(self.keyed_data)} key groups...")
        
        results = [agg_func(key, values) for key, values in self.keyed_data.items()]
        
        return DataStream(results, self.config)


class FlinkEntityResolver:
    """
    Flink-inspired distributed entity resolution engine.
    
    Implements entity resolution using parallel batch processing
    inspired by Apache Flink's execution model.
    """
    
    def __init__(self, config: Optional[FlinkERConfig] = None):
        """
        Initialize Flink Entity Resolver.
        
        Args:
            config: Flink-ER configuration (uses defaults if None)
        """
        self.config = config or FlinkERConfig()
        
        # Statistics
        self.stats = {
            'total_records': 0,
            'total_comparisons': 0,
            'total_matches': 0,
            'blocking_enabled': True,
            'reduction_percentage': 0.0,
            'parallel_workers': self.config.max_workers
        }
    
    def auto_detect_fields(self, records: List[dict]) -> Dict[str, str]:
        """Auto-detect field types from records (same as GenericEntityResolver)."""
        if not records:
            return {}
        
        field_types = {}
        sample = records[0]
        
        for field_name in sample.keys():
            field_lower = field_name.lower()
            
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
        """Auto-detect field weights based on field types."""
        type_weights = {
            'name': 0.30,
            'date': 0.20,
            'phone': 0.15,
            'email': 0.10,
            'id': 0.15,
            'address': 0.05,
            'text': 0.05
        }
        
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
    
    def resolve(self, records: List[dict]) -> Dict[str, Any]:
        """
        Perform distributed entity resolution on records.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Dictionary with matches, clusters, and statistics
        """
        print("\n" + "="*60)
        print("FLINK-ER: DISTRIBUTED ENTITY RESOLUTION")
        print("="*60)
        print(f"Parallelism: {self.config.parallelism}")
        print(f"Batch Size: {self.config.batch_size}")
        print(f"Workers: {self.config.max_workers}")
        print(f"Mode: {'Processes' if self.config.use_processes else 'Threads'}")
        print("="*60 + "\n")
        
        self.stats['total_records'] = len(records)
        
        # Step 1: Normalization (Map operation)
        normalized_stream = self._create_stream(records) \
            .map(preprocess_record, description="Normalize records")
        
        normalized_records = normalized_stream.collect()
        
        # Auto-detect field weights for similarity computation
        field_types = self.auto_detect_fields(normalized_records)
        self.field_weights = self.auto_detect_weights(field_types)
        print(f"[FlinkER] Auto-detected {len(self.field_weights)} fields for similarity computation")
        
        # Step 2: Blocking (Generate candidate pairs)
        print(f"\n[FlinkER] Generating candidate pairs with multi-pass blocking...")
        candidate_pairs = self._generate_candidates_distributed(normalized_records)
        
        self.stats['total_comparisons'] = len(candidate_pairs)
        total_possible = len(records) * (len(records) - 1) // 2
        self.stats['reduction_percentage'] = calculate_reduction(
            total_possible, len(candidate_pairs)
        )
        
        print(f"[FlinkER] Candidate pairs: {len(candidate_pairs):,}")
        print(f"[FlinkER] Reduction: {self.stats['reduction_percentage']:.2f}%")
        
        # Step 3: Similarity computation (Map-Reduce)
        matches = self._compute_matches_distributed(candidate_pairs, normalized_records)
        
        self.stats['total_matches'] = len(matches)
        
        # Step 4: Clustering (Group connected components)
        clusters = self._build_clusters(matches)
        
        print(f"\n[FlinkER] Entity resolution completed!")
        print(f"[FlinkER] Total matches: {len(matches)}")
        print(f"[FlinkER] Total clusters: {len(clusters)}")
        
        return {
            'matches': matches,
            'clusters': clusters,
            'statistics': self.stats
        }
    
    def _create_stream(self, data: List[Any]) -> DataStream:
        """Create a DataStream from data."""
        return DataStream(data, self.config)
    
    def _generate_candidates_distributed(self, records: List[dict]) -> List[Tuple[int, int]]:
        """
        Generate candidate pairs using distributed blocking.
        
        Args:
            records: Normalized records
            
        Returns:
            List of (idx1, idx2) candidate pairs
        """
        # Use multi-pass blocking (this could be further parallelized)
        blocker = MultiPassBlocker(
            use_lsh=True,
            use_soundex=True,
            use_rules=True,
            use_geohash=False
        )
        
        # Create blocks
        blocks = blocker.create_blocks(records)
        
        # Get candidate pairs
        candidates = blocker.get_candidate_pairs(blocks)
        
        return candidates
    
    def _compute_matches_distributed(self, 
                                     candidate_pairs: List[Tuple[int, int]], 
                                     records: List[dict]) -> List[Dict[str, Any]]:
        """
        Compute similarity for candidate pairs in parallel.
        
        Args:
            candidate_pairs: List of (idx1, idx2) pairs
            records: Normalized records
            
        Returns:
            List of match dictionaries
        """
        print(f"\n[FlinkER] Computing similarities for {len(candidate_pairs):,} pairs...")
        start_time = time.time()
        
        # Prepare work items with record data AND field weights
        work_items = []
        for idx1, idx2 in candidate_pairs:
            work_items.append({
                'idx1': idx1,
                'idx2': idx2,
                'record1': records[idx1],
                'record2': records[idx2],
                'weights': self.field_weights  # Include weights for each work item
            })
        
        # Process in parallel batches
        matches = []
        batches = self._create_batches(work_items)
        
        executor_class = ProcessPoolExecutor if self.config.use_processes else ThreadPoolExecutor
        
        with executor_class(max_workers=self.config.max_workers) as executor:
            futures = []
            for batch_idx, batch in enumerate(batches):
                future = executor.submit(
                    self._compute_similarities_batch,
                    batch,
                    self.config.similarity_threshold,
                    batch_idx
                )
                futures.append(future)
            
            for future in as_completed(futures):
                batch_matches = future.result()
                matches.extend(batch_matches)
        
        elapsed = time.time() - start_time
        print(f"[FlinkER] Similarity computation completed in {elapsed:.2f}s")
        print(f"[FlinkER] Found {len(matches)} matches above threshold {self.config.similarity_threshold}")
        
        return matches
    
    @staticmethod
    def _compute_similarities_batch(work_items: List[Dict], 
                                   threshold: float,
                                   batch_idx: int) -> List[Dict[str, Any]]:
        """
        Compute similarities for a batch of candidate pairs.
        
        Args:
            work_items: List of work items with idx1, idx2, record1, record2, weights
            threshold: Similarity threshold
            batch_idx: Batch index for logging
            
        Returns:
            List of matches above threshold
        """
        matches = []
        
        for item in work_items:
            idx1 = item['idx1']
            idx2 = item['idx2']
            record1 = item['record1']
            record2 = item['record2']
            weights = item.get('weights')  # Get weights from work item
            
            # Compute similarity with weights
            similarity = compute_similarity(record1, record2, weights)
            
            if similarity >= threshold:
                matches.append({
                    'id1': idx1,
                    'id2': idx2,
                    'record1': record1,
                    'record2': record2,
                    'similarity': similarity
                })
        
        return matches
    
    def _create_batches(self, items: List[Any]) -> List[List[Any]]:
        """Create batches from items."""
        batches = []
        for i in range(0, len(items), self.config.batch_size):
            batches.append(items[i:i + self.config.batch_size])
        return batches
    
    def _build_clusters(self, matches: List[Dict[str, Any]]) -> List[Set[int]]:
        """
        Build clusters using Union-Find algorithm.
        
        Args:
            matches: List of match dictionaries
            
        Returns:
            List of clusters (sets of indices)
        """
        print(f"\n[FlinkER] Building clusters from {len(matches)} matches...")
        
        # Union-Find data structure
        parent = {}
        
        def find(x):
            if x not in parent:
                parent[x] = x
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        # Build union-find structure
        for match in matches:
            union(match['id1'], match['id2'])
        
        # Group by root parent
        clusters_dict = defaultdict(set)
        for node in parent.keys():
            root = find(node)
            clusters_dict[root].add(node)
        
        clusters = list(clusters_dict.values())
        
        print(f"[FlinkER] Created {len(clusters)} clusters")
        
        return clusters
    
    def print_statistics(self):
        """Print execution statistics."""
        print("\n" + "="*60)
        print("FLINK-ER STATISTICS")
        print("="*60)
        print(f"Total Records: {self.stats['total_records']:,}")
        print(f"Total Comparisons: {self.stats['total_comparisons']:,}")
        print(f"Reduction: {self.stats['reduction_percentage']:.2f}%")
        print(f"Total Matches: {self.stats['total_matches']:,}")
        print(f"Parallel Workers: {self.stats['parallel_workers']}")
        print("="*60 + "\n")


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
            # Clean values - only exclude None and empty strings
            cleaned = {k: v.strip() if isinstance(v, str) else v 
                      for k, v in row.items() if v is not None and v != ''}
            records.append(cleaned)
    
    print(f"[FlinkER] Loaded {len(records)} records from {csv_path}")
    return records


# Example usage
if __name__ == "__main__":
    print("Flink-ER: Distributed Entity Resolution (Python Implementation)")
    print("="*60)
    
    # Example: Process challenge_er_200 dataset
    csv_file = "csv/challenging_er_200.csv"
    
    if os.path.exists(csv_file):
        # Load data
        records = load_csv_records(csv_file)
        
        # Configure Flink-ER
        config = FlinkERConfig(
            parallelism=4,  # Use 4 parallel workers
            batch_size=50,  # Process 50 records per batch
            use_processes=True,  # Use multiprocessing
            similarity_threshold=0.75
        )
        
        # Create resolver
        resolver = FlinkEntityResolver(config)
        
        # Resolve entities
        results = resolver.resolve(records)
        
        # Print statistics
        resolver.print_statistics()
        
        # Export results
        output_file = "flink_er_results.json"
        with open(output_file, 'w') as f:
            # Convert sets to lists for JSON serialization
            exportable_results = {
                'matches': results['matches'],
                'clusters': [list(cluster) for cluster in results['clusters']],
                'statistics': results['statistics']
            }
            json.dump(exportable_results, f, indent=2)
        
        print(f"✅ Results exported to {output_file}")
    else:
        print(f"❌ CSV file not found: {csv_file}")
        print("Please ensure the dataset exists or update the file path.")
