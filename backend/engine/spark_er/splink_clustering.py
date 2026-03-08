"""
Splink Entity Resolution with Spark Integration

This module uses Splink (https://github.com/moj-analytical-services/splink) 
to perform entity resolution clustering on data loaded through Spark.

Splink is a probabilistic record linkage library that uses machine learning 
to identify duplicate records. It integrates with Spark for scalable processing.

Key Features:
- Uses Splink's probabilistic matching framework
- Leverages Spark for distributed computing
- Supports multiple blocking rules for efficiency
- Generates match probabilities and cluster assignments
- Exports results for further processing

Usage:
    python -m er.splink_clustering
    
    Or programmatically:
    from engine.spark_er.splink_clustering import run_splink_clustering
    clusters, matches = run_splink_clustering()
"""

import os
import sys
import re
import queue
import logging
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Check PySpark availability
try:
    from pyspark.sql import SparkSession
    from pyspark import SparkContext
except ImportError:
    print("=" * 70)
    print("ERROR: PySpark not installed")
    print("=" * 70)
    print("\nPySpark is required for Splink integration.")
    print("\nTo install:")
    print("  pip install pyspark")
    print("=" * 70)
    sys.exit(1)

# Check Splink availability
try:
    from splink.spark.spark_linker import SparkLinker
    import splink.spark.comparison_library as cl
except ImportError as e:
    print("=" * 70)
    print("ERROR: Splink not installed or failed to import")
    print(f"Exception details: {e}")
    print("=" * 70)
    sys.exit(1)

# Import separate modules for blocking, scoring, and clustering
from engine.spark_er.blocking_rules import get_blocking_rules
from engine.spark_er.scoring_rules import get_comparison_rules
from engine.spark_er.clustering_operations import (
    cluster_predictions,
    format_results,
    print_clustering_summary
)


# Configuration from environment variables
CACHE_DIR = os.getenv("CACHE_DIR", "data_source")
CACHE_FORMAT = os.getenv("CACHE_FORMAT", "parquet")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")


# ──────────────────────────────────────────────────────────────────────────────
# Real-time progress interception
# ──────────────────────────────────────────────────────────────────────────────

class SpLinkProgressHandler(logging.Handler):
    """
    Attaches to Splink's internal Python loggers and converts log messages into
    structured progress events that are placed on a thread-safe Queue.

    The async heartbeat loop in SparkPipelineOrchestrator drains this queue
    each tick so the frontend receives genuine sub-stage updates rather than
    time-based guesses.

    Progress events have the shape:
      {
        "sub_step":        str,   # Human-readable label
        "progress_pct":    int,   # 0-100 through the score stage
        "em_iteration":    int,   # Current EM iteration (0 if N/A)
        "em_max":          int,   # Max EM iterations configured
        "detail":          str,   # Raw log fragment for debugging
      }
    """

    _EM_ITER_RE = re.compile(r"Iteration\s+(\d+):")
    _EM_CONV_RE = re.compile(r"EM converged after\s+(\d+)")

    # Ordered sub-stages with their approximate % progress through the SCORE stage
    _SUB_STAGE_MAP = {
        "Estimating u probabilities":           ("Estimating u-probabilities (random sampling)",   12),
        "Estimated u probabilities":             ("u-probabilities complete",                        20),
        "Starting EM training":                  ("Starting EM model training...",                   25),
        "EM converged":                          ("EM converged — model trained",                    80),
        "Generating predictions":                ("Generating match predictions...",                 82),
        "Collecting predictions":                ("Collecting predictions from Spark executors",     90),
        "All paths were ignored":                None,   # suppress noisy Spark warning
    }

    def __init__(self, progress_queue: "queue.Queue", em_max: int = 10):
        super().__init__()
        self._q = progress_queue
        self._em_max = em_max

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()

        # EM iteration — most granular signal
        m = self._EM_ITER_RE.search(msg)
        if m:
            iteration = int(m.group(1))
            pct = 25 + int((iteration / self._em_max) * 55)   # 25 → 80 %
            self._q.put_nowait({
                "sub_step":     f"EM training: iteration {iteration}/{self._em_max}",
                "progress_pct": min(pct, 79),
                "em_iteration": iteration,
                "em_max":       self._em_max,
                "detail":       msg[:120],
            })
            return

        # EM converged
        m = self._EM_CONV_RE.search(msg)
        if m:
            iters = int(m.group(1))
            self._q.put_nowait({
                "sub_step":     f"EM converged after {iters} iterations",
                "progress_pct": 80,
                "em_iteration": iters,
                "em_max":       self._em_max,
                "detail":       msg[:120],
            })
            return

        # Named sub-stages
        for keyword, result in self._SUB_STAGE_MAP.items():
            if keyword in msg:
                if result is None:
                    return  # suppress
                label, pct = result
                self._q.put_nowait({
                    "sub_step":     label,
                    "progress_pct": pct,
                    "em_iteration": 0,
                    "em_max":       self._em_max,
                    "detail":       msg[:120],
                })
                return


def _attach_progress_handler(handler: SpLinkProgressHandler) -> List[logging.Logger]:
    """Register handler on all relevant Splink loggers. Returns loggers for cleanup."""
    names = [
        "splink.estimate_u",
        "splink.em_training_session",
        "splink.expectation_maximisation",
        "splink.settings",
        "splink.m_u_records_to_parameters",
    ]
    loggers = [logging.getLogger(n) for n in names]
    for lg in loggers:
        lg.addHandler(handler)
    return loggers


def _detach_progress_handler(handler: SpLinkProgressHandler, loggers: List[logging.Logger]) -> None:
    for lg in loggers:
        lg.removeHandler(handler)


def get_cache_file_path() -> str:
    """Get the path to the cached data file."""
    if CACHE_FORMAT == "parquet":
        return os.path.join(CACHE_DIR, "oracle_data.parquet")
    elif CACHE_FORMAT == "json":
        return os.path.join(CACHE_DIR, "oracle_data.json")
    elif CACHE_FORMAT == "csv":
        return os.path.join(CACHE_DIR, "oracle_data.csv")
    else:
        return os.path.join(CACHE_DIR, f"oracle_data.{CACHE_FORMAT}")


def create_splink_settings() -> Dict[str, Any]:
    """
    Create Splink configuration settings for entity resolution.
    
    This defines:
    - Which columns to use for blocking (to reduce comparisons)
    - Which columns to compare and how
    - Probability thresholds for matches
    
    Returns:
        Dictionary with Splink settings
    """
    # Get blocking rules from separate module
    blocking_rules = get_blocking_rules()
    
    # Get comparison rules from separate module
    comparisons = get_comparison_rules(cl)
    
    settings = {
        "link_type": "dedupe_only",  # We're finding duplicates within one dataset
        "blocking_rules_to_generate_predictions": blocking_rules,
        "comparisons": comparisons,
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": True,
        "max_iterations": 10,
        "em_convergence": 0.01,
    }
    
    return settings


def load_data_from_cache(spark: SparkSession):
    """
    Load data from cache into Spark DataFrame with proper partitioning
    for parallel processing across all CPU cores.
    
    Provides immediate step-by-step feedback during loading.
    
    Args:
        spark: SparkSession instance
        
    Returns:
        Spark DataFrame with cached data, properly partitioned
    """
    import multiprocessing
    
    cache_file = get_cache_file_path()
    
    if not os.path.exists(cache_file):
        print(f"✗ ERROR: Cache file not found: {cache_file}")
        raise FileNotFoundError(f"Cache file not found: {cache_file}")
    
    cpu_cores = multiprocessing.cpu_count()
    print(f"  → Cache file: {cache_file}")
    print(f"  → Cache format: {CACHE_FORMAT}")
    print(f"  → CPU cores available: {cpu_cores}")
    print(f"  → Reading data from cache...")
    
    if CACHE_FORMAT == "parquet":
        df = spark.read.parquet(cache_file)
    elif CACHE_FORMAT == "json":
        df = spark.read.json(cache_file)
    elif CACHE_FORMAT == "csv":
        df = spark.read.csv(cache_file, header=True, inferSchema=True)
    else:
        raise ValueError(f"Unsupported cache format: {CACHE_FORMAT}")
    
    print(f"  ✓ Data read from cache")
    
    # Add a unique ID column if it doesn't exist
    print(f"  → Checking for unique_id column...")
    if "unique_id" not in df.columns:
        from pyspark.sql.functions import monotonically_increasing_id
        print(f"    • Adding unique_id column...")
        df = df.withColumn("unique_id", monotonically_increasing_id())
        print(f"    ✓ unique_id column added")
    else:
        print(f"    ✓ unique_id column exists")
    
    # Repartition for optimal parallel processing across all CPU cores
    # Use 2x CPU cores for better parallelism
    optimal_partitions = cpu_cores * 2
    print(f"  → Repartitioning data for parallel processing...")
    print(f"    • Target partitions: {optimal_partitions} (2x CPU cores)")
    df = df.repartition(optimal_partitions)
    print(f"  ✓ Data repartitioned")
    
    print(f"  → Counting records...")
    record_count = df.count()
    print(f"  ✓ Record count complete: {record_count:,} records")
    print()
    print(f"  Data Summary:")
    print(f"    • Total records: {record_count:,}")
    print(f"    • Partitions: {optimal_partitions}")
    print(f"    • Records per partition: ~{record_count // optimal_partitions:,}")
    print()
    
    return df


def run_splink_clustering(
    spark: SparkSession = None,
    match_threshold: float = 0.95,
    progress_queue: Optional["queue.Queue"] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Run Splink entity resolution clustering on cached data.

    Args:
        spark:            Optional SparkSession (created if not provided)
        match_threshold:  Minimum probability threshold (0.0-1.0)
        progress_queue:   Optional thread-safe Queue; structured progress dicts
                          are put here as Splink runs so callers can relay them
                          to the async event loop for real-time UI updates.
    """
    # Attach real-time progress handler to Splink's loggers so we can relay
    # genuine sub-stage signals back to the async event loop.
    _prog_handler = None
    _prog_loggers: List[logging.Logger] = []
    if progress_queue is not None:
        _prog_handler = SpLinkProgressHandler(progress_queue, em_max=10)
        _prog_loggers = _attach_progress_handler(_prog_handler)

    try:
        return _run_splink_clustering_inner(spark, match_threshold)
    finally:
        if _prog_handler:
            _detach_progress_handler(_prog_handler, _prog_loggers)


def _run_splink_clustering_inner(
    spark: SparkSession,
    match_threshold: float,
) -> Tuple[List[Dict], List[Dict]]:
    """Internal implementation — called by run_splink_clustering."""
    print("=" * 70)
    print("SPLINK ENTITY RESOLUTION CLUSTERING (PARALLELIZED)")
    print("=" * 70)
    print()

    # Step 1: Create or use provided Spark session
    if spark is None:
        import multiprocessing
        cpu_cores = multiprocessing.cpu_count()

        print("=" * 70)
        print("STEP 1: Initializing Spark Session")
        print("=" * 70)
        print(f"  → Detecting CPU cores: {cpu_cores} cores available")
        print(f"  → Configuring Spark for parallel processing...")
        
        spark = SparkSession.builder \
            .appName("Splink Entity Resolution - Parallel") \
            .master(f"local[{cpu_cores}]") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.maxResultSize", "4g") \
            .config("spark.default.parallelism", str(cpu_cores * 2)) \
            .config("spark.sql.shuffle.partitions", str(cpu_cores * 2)) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.parquet.filterPushdown", "true") \
            .config("spark.sql.parquet.mergeSchema", "false") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .getOrCreate()
        
        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")
        
        import tempfile
        checkpoint_dir = tempfile.mkdtemp(prefix="spark_checkpoint_")
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        
        print(f"  ✓ Spark session created")
        print(f"  ✓ Parallelism configured: {cpu_cores * 2} partitions")
        print()
        print(f"✓ SUCCESS: Spark initialized with {cpu_cores} CPU cores")
        print("=" * 70)
        print()
    
    # Step 2: Load data
    print("=" * 70)
    print("STEP 2: Loading Data from Cache")
    print("=" * 70)
    df = load_data_from_cache(spark)
    print(f"✓ SUCCESS: Data loaded and partitioned")
    print("=" * 70)
    print()
    
    # Step 3: Create Splink settings
    print("=" * 70)
    print("STEP 3: Configuring Splink Settings")
    print("=" * 70)
    print(f"  → Loading blocking rules...")
    settings = create_splink_settings()
    print(f"  ✓ Blocking rules loaded")
    print(f"  → Loading scoring rules...")
    print(f"  ✓ Scoring rules loaded")
    print(f"  → Assembling configuration...")
    print(f"  ✓ Configuration complete")
    print()
    print(f"✓ SUCCESS: Splink settings configured")
    print("=" * 70)
    print()
    
    # Step 4: Initialize Splink linker
    print("=" * 70)
    print("STEP 4: Initializing Splink Linker")
    print("=" * 70)
    print(f"  → Creating linker instance...")
    linker = SparkLinker(
        df,
        settings,
        spark=spark
    )
    print(f"  ✓ Linker instance created")
    print(f"  → Registering data with Splink...")
    print(f"  ✓ Data registered")
    print()
    print(f"✓ SUCCESS: Splink linker initialized")
    print("=" * 70)
    print()
    
    # Step 5: Estimate model parameters using Expectation-Maximization
    print("=" * 70)
    print("STEP 5: Training Model (Expectation-Maximization)")
    print("=" * 70)
    print(f"  → Estimating u-probabilities using random sampling...")
    print(f"    (This estimates the probability that fields match by chance)")
    linker.estimate_u_using_random_sampling(max_pairs=1e6)
    print(f"  ✓ u-probabilities estimated")
    print()
    
    print(f"  → Estimating m-probabilities using EM algorithm...")
    print(f"    (This estimates the probability that fields match when records are duplicates)")
    print(f"    Blocking rule for training: l.NAME = r.NAME")
    # Estimate m probabilities for each comparison
    blocking_rule_for_training = "l.NAME = r.NAME"
    linker.estimate_parameters_using_expectation_maximisation(
        blocking_rule_for_training
    )
    print(f"  ✓ m-probabilities estimated")
    print(f"  → Model parameters converged")
    print()
    print(f"✓ SUCCESS: Model training complete")
    print("=" * 70)
    print()
    
    # Step 6: Generate predictions
    print("=" * 70)
    print("STEP 6: Generating Predictions (Parallel Execution)")
    print("=" * 70)
    print(f"  → Applying blocking rules in parallel...")
    print(f"    (Reducing comparison space using blocking)")
    print(f"  ✓ Blocking complete")
    print()
    print(f"  → Scoring record pairs in parallel...")
    print(f"    (Calculating match probabilities for candidate pairs)")
    print(f"    Match threshold: {match_threshold}")
    predictions = linker.predict(threshold_match_probability=match_threshold)
    print(f"  ✓ Scoring complete")
    print()
    print(f"  → Filtering predictions by threshold...")
    print(f"  ✓ Filtering complete")
    print()
    print(f"✓ SUCCESS: Predictions generated")
    print("=" * 70)
    print()
    
    # Step 7: Convert predictions to pandas for processing
    print("=" * 70)
    print("STEP 7: Converting Predictions to Pandas")
    print("=" * 70)
    print(f"  → Collecting predictions from Spark...")
    predictions_pd = predictions.as_pandas_dataframe()
    print(f"  ✓ Predictions collected")
    print()
    print(f"✓ SUCCESS: Found {len(predictions_pd):,} potential matches")
    print("=" * 70)
    print()
    
    # Step 8: Cluster the predictions using clustering_operations module
    clusters_df = cluster_predictions(linker, predictions, match_threshold)
    clusters_pd = clusters_df.as_pandas_dataframe()
    
    # Step 9: Format results using clustering_operations module
    clusters, matches = format_results(predictions_pd, clusters_pd)
    
    # Step 10: Print summary using clustering_operations module
    print_clustering_summary(
        total_records=df.count(),
        num_matches=len(matches),
        num_clusters=len(clusters),
        threshold=match_threshold
    )
    
    return clusters, matches


def main():
    """Main entry point for running Splink clustering."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Run Splink entity resolution clustering on cached data"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.95,
        help="Match probability threshold (0.0-1.0, default: 0.95)"
    )
    
    args = parser.parse_args()
    
    try:
        # Run clustering
        clusters, matches = run_splink_clustering(
            match_threshold=args.threshold
        )
        
        print("✓ Clustering complete!")
        print()
        print("Next steps:")
        print("1. Export to Neo4j: python -m er.splink_neo4j_export")
        print("2. Or use the clusters programmatically in your code")
        
        return 0
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
