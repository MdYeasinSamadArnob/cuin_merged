"""
Splink Clustering Operations

This module handles clustering operations for entity resolution using Splink.
It processes predictions and creates clusters of matching records.

All operations provide immediate step-by-step progress feedback.

Key Features:
- Cluster connected components from pairwise predictions
- Format results for compatibility with downstream systems
- Convert Spark DataFrames to pandas for processing
- Generate cluster summaries and statistics
- Real-time progress reporting

Usage:
    from engine.spark_er.clustering_operations import cluster_predictions, format_results
    
    clusters_df = cluster_predictions(linker, predictions, threshold)
    clusters, matches = format_results(predictions_pd, clusters_pd)
"""

from typing import List, Dict, Tuple, Any


def cluster_predictions(
    linker: Any,
    predictions: Any,
    threshold: float = 0.8
) -> Any:
    """
    Cluster pairwise predictions into connected components.
    
    Takes pairwise match predictions and groups them into clusters
    where all records within a cluster are considered duplicates.
    Uses graph-based clustering to find connected components.
    
    Provides immediate progress feedback at each step.
    
    Args:
        linker: Splink linker instance
        predictions: Splink predictions DataFrame
        threshold: Match probability threshold (0.0-1.0)
        
    Returns:
        Splink DataFrame with cluster assignments
    """
    print("=" * 70)
    print("STEP: Clustering Connected Components")
    print("=" * 70)
    print(f"  → Starting clustering with threshold: {threshold}")
    print(f"  → Building graph from pairwise matches...")
    
    clusters_df = linker.cluster_pairwise_predictions_at_threshold(
        predictions,
        threshold_match_probability=threshold
    )
    
    print(f"  ✓ Graph built successfully")
    print(f"  → Finding connected components...")
    
    # Convert to pandas for easier processing
    clusters_pd = clusters_df.as_pandas_dataframe()
    
    print(f"  ✓ Connected components found")
    print(f"  → Analyzing cluster distribution...")
    
    num_clusters = clusters_pd['cluster_id'].nunique()
    total_records = len(clusters_pd)
    avg_cluster_size = total_records / num_clusters if num_clusters > 0 else 0
    
    print(f"  ✓ Analysis complete")
    print()
    print(f"✓ SUCCESS: Created {num_clusters:,} clusters from {total_records:,} records")
    print(f"  Average cluster size: {avg_cluster_size:.2f} records")
    print("=" * 70)
    print()
    
    return clusters_df


def format_results(
    predictions_pd: Any,
    clusters_pd: Any
) -> Tuple[List[Dict], List[Dict]]:
    """
    Format clustering results for compatibility with existing systems.
    
    Converts Splink predictions and clusters into a standardized format
    that can be used by Neo4j export and other downstream processes.
    
    Provides immediate progress feedback during formatting.
    
    Args:
        predictions_pd: Pandas DataFrame with pairwise predictions
        clusters_pd: Pandas DataFrame with cluster assignments
        
    Returns:
        Tuple of (clusters, matches) where:
        - clusters: List of cluster dictionaries with cluster_id and member_indices
        - matches: List of match dictionaries with record indices and probabilities
    """
    print("=" * 70)
    print("STEP: Formatting Results")
    print("=" * 70)
    
    # Format matches for compatibility with existing Neo4j export
    print(f"  → Formatting {len(predictions_pd):,} matches...")
    matches = []
    batch_size = 10000
    for idx, row in enumerate(predictions_pd.iterrows()):
        _, row_data = row
        matches.append({
            'id1': row_data['unique_id_l'],
            'id2': row_data['unique_id_r'],
            'probability': row_data['match_probability'],
            'match_weight': row_data.get('match_weight', 0.0)
        })
        
        # Show progress for large datasets
        if (idx + 1) % batch_size == 0:
            print(f"    • Processed {idx + 1:,} matches...")
    
    print(f"  ✓ All matches formatted")
    
    # Format clusters
    print(f"  → Formatting clusters...")
    clusters = []
    cluster_groups = clusters_pd.groupby('cluster_id')['unique_id'].apply(list)
    
    batch_count = 0
    batch_size = 1000
    for original_cluster_id, member_indices in cluster_groups.items():
        member_list = member_indices.tolist() if hasattr(member_indices, 'tolist') else member_indices
        
        # Skip singletons (non-matching unique records) 
        if len(member_list) <= 1:
            continue
            
        clusters.append({
            'cluster_id': original_cluster_id,
            'member_indices': member_list
        })
        
        batch_count += 1
        if batch_count % batch_size == 0:
            print(f"    • Processed {batch_count:,} non-singleton clusters...")
    
    print(f"  ✓ All clusters formatted")
    print()
    print(f"✓ SUCCESS: Formatted {len(matches):,} matches and {len(clusters):,} clusters")
    print("=" * 70)
    print()
    
    return clusters, matches


def print_clustering_summary(
    total_records: int,
    num_matches: int,
    num_clusters: int,
    threshold: float
) -> None:
    """
    Print a summary of clustering results.
    
    Args:
        total_records: Total number of records processed
        num_matches: Number of pairwise matches found
        num_clusters: Number of clusters created
        threshold: Match probability threshold used
    """
    print("=" * 70)
    print("FINAL SUMMARY: Clustering Complete")
    print("=" * 70)
    print(f"  Records processed:    {total_records:,}")
    print(f"  Pairwise matches:     {num_matches:,}")
    print(f"  Clusters created:     {num_clusters:,}")
    print(f"  Match threshold:      {threshold}")
    print(f"  Avg matches/cluster:  {num_matches/num_clusters:.2f}" if num_clusters > 0 else "  N/A")
    print("=" * 70)
    print()

