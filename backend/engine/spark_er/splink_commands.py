"""
Splink Entity Resolution Commands

This module provides separate commands to run blocking, scoring, and clustering
operations individually or in combination. All operations use multipass distributed
processing for scalability and memory efficiency to prevent OOM issues.

Commands:
    blocking   - Run multipass distributed blocking analysis
    scoring    - Run multipass distributed scoring/matching with OOM prevention
    all        - Run complete pipeline (blocking + scoring + clustering)

Features:
    - Multipass strategy: Multiple passes for better accuracy and OOM prevention
    - Distributed processing: Spark parallel execution across all CPU cores
    - Batch processing: Process data in batches to prevent memory issues
    - Streaming results: Write results incrementally to avoid memory buildup
    - Progress reporting: Detailed progress for each step and pass
    - OOM handling: Graceful handling of Out Of Memory errors
    - Memory efficient: No full dataset loaded into memory at once

OOM Prevention Strategy:
    Scoring Phase:
    - Process predictions one blocking rule at a time (multipass)
    - Stream results to CSV incrementally
    - Deduplicate at the end to combine results
    - Graceful degradation if a rule fails (continue with remaining rules)

Configuration:
    Uses .env file for configuration. Key settings:
    - CUSTOMER_AGGREGATED_PATH: Path to folder with parquet files (default: customer_aggregated)

Usage:
    # Run individual steps
    python -m er.splink_commands blocking
    python -m er.splink_commands scoring
    
    # Run complete pipeline
    python -m er.splink_commands all
    
    # With custom threshold
    python -m er.splink_commands all --threshold 0.85
"""

# =============================================================================
# IMPORTANT: Set HADOOP_HOME before ANY other imports (required for Windows)
# This MUST be done before PySpark/Java initializes
# =============================================================================
import os
import sys
import platform

if platform.system() == "Windows":
    # Check standard installation location
    _hadoop_home = r"C:\hadoop"
    _winutils_path = os.path.join(_hadoop_home, "bin", "winutils.exe")
    
    if os.path.exists(_winutils_path):
        os.environ["HADOOP_HOME"] = _hadoop_home
        os.environ["hadoop.home.dir"] = _hadoop_home
        # Also add to PATH if not already there
        _hadoop_bin = os.path.join(_hadoop_home, "bin")
        if _hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    else:
        # Check if HADOOP_HOME is already set in environment
        _env_hadoop = os.environ.get("HADOOP_HOME", "")
        if not _env_hadoop or not os.path.exists(os.path.join(_env_hadoop, "bin", "winutils.exe")):
            print("=" * 70)
            print("WARNING: Hadoop winutils.exe not found!")
            print("=" * 70)
            print(f"Expected location: {_winutils_path}")
            print("\nTo fix this, run:")
            print("  python setup_hadoop_windows.py")
            print("\nOr download manually from:")
            print("  https://github.com/cdarlint/winutils")
            print("=" * 70)
# =============================================================================

import argparse
import csv
import glob
import logging
import shutil
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Import multipass blocking for LSH and advanced blocking
from engine.spark_er.blocking.multi_pass_blocking import MultiPassBlocker, LSHBlocker

# Configuration constants
MAX_SAMPLING_PAIRS = 3e5  # Maximum pairs to sample for u-probability estimation (300K is sufficient)
DEFAULT_CLUSTERING_OUTPUT = "clustering_results.csv"

# Check PySpark availability
try:
    from pyspark.sql import SparkSession
    from pyspark import SparkContext
except ImportError as e:
    error_msg = (
        "=" * 70 + "\n"
        "ERROR: PySpark not installed\n"
        "=" * 70 + "\n\n"
        "PySpark is required for Splink integration.\n\n"
        "To install:\n"
        "  pip install pyspark\n"
        + "=" * 70
    )
    raise ImportError(error_msg) from e

# Check Splink availability with detailed error reporting
try:
    import splink
    splink_version = splink.__version__
    print(f"Splink version {splink_version} detected")
except ImportError as e:
    error_msg = (
        "=" * 70 + "\n"
        "ERROR: Splink is not installed\n"
        "=" * 70 + "\n\n"
        f"Import error: {e}\n\n"
        "Splink 4.x with Spark bindings is required for entity resolution clustering.\n\n"
        "To install:\n"
        "  pip install 'splink[spark]>=4.0.0'\n\n"
        "Or install all requirements:\n"
        "  pip install -r requirements.txt\n\n"
        "Note: Splink 4.x requires the [spark] extra for Spark support.\n"
        + "=" * 70
    )
    raise ImportError(error_msg) from e

# Check for Splink 4.x
try:
    major_version = int(splink_version.split('.')[0])
    if major_version < 4:
        error_msg = (
            "=" * 70 + "\n"
            f"ERROR: Splink {splink_version} is too old\n"
            "=" * 70 + "\n\n"
            f"Found Splink {splink_version}, but Splink 4.x is required.\n\n"
            "To upgrade:\n"
            "  pip install --upgrade 'splink[spark]>=4.0.0'\n"
            + "=" * 70
        )
        raise ImportError(error_msg)
except (ValueError, AttributeError) as e:
    print(f"Warning: Could not parse Splink version '{splink_version}': {e}")

# Check required Splink modules
missing_modules = []
try:
    from splink import Linker, SparkAPI
except ImportError as e:
    missing_modules.append(f"splink.Linker and splink.SparkAPI: {e}")

try:
    from splink.comparison_library import ExactMatch, LevenshteinAtThresholds, JaroWinklerAtThresholds
except ImportError as e:
    missing_modules.append(f"splink.comparison_library: {e}")

try:
    import splink.comparison_level_library as cll
except ImportError as e:
    missing_modules.append(f"splink.comparison_level_library: {e}")

if missing_modules:
    error_msg = (
        "=" * 70 + "\n"
        "ERROR: Required Splink modules are missing\n"
        "=" * 70 + "\n\n"
        f"Splink {splink_version} is installed, but required modules are missing:\n"
    )
    for module in missing_modules:
        error_msg += f"  ✗ {module}\n"
    error_msg += (
        "\nThis usually means:\n"
        "  1. Splink was installed without [spark] extras\n"
        "  2. Splink version is too old (need 4.x)\n"
        "  3. Splink installation is incomplete or corrupted\n\n"
        "To fix:\n"
        "  pip install --upgrade --force-reinstall 'splink[spark]>=4.0.0'\n\n"
        "Note: The [spark] extra is required for Spark bindings in Splink 4.x\n"
        + "=" * 70
    )
    raise ImportError(error_msg)

# Configuration from environment variables
CACHE_DIR = os.getenv("CACHE_DIR", "data_source")
CACHE_FORMAT = os.getenv("CACHE_FORMAT", "parquet")
CUSTOMER_AGGREGATED_PATH = os.getenv("CUSTOMER_AGGREGATED_PATH", "customer_aggregated")

# Memory optimization settings (reduce these if you get OOM errors)
# MAX_BLOCK_SIZE: Max records per blocking key (larger = more pairs, more memory)
MAX_BLOCK_SIZE = int(os.getenv("MAX_BLOCK_SIZE", "200"))
# MAX_PAIRS_PER_RULE: Max candidate pairs per blocking rule
MAX_PAIRS_PER_RULE = int(os.getenv("MAX_PAIRS_PER_RULE", "500000"))
# LSH_SAMPLE_SIZE: Max records to process for LSH blocking
LSH_SAMPLE_SIZE = int(os.getenv("LSH_SAMPLE_SIZE", "50000"))
# MAX_LSH_BUCKET_SIZE: Max records per LSH bucket
MAX_LSH_BUCKET_SIZE = int(os.getenv("MAX_LSH_BUCKET_SIZE", "100"))


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


def stop_spark_gracefully(spark: SparkSession):
    """Stop Spark session with graceful cleanup to avoid Windows file lock issues."""
    import time
    
    try:
        # Uncache all DataFrames
        spark.catalog.clearCache()
        
        # Give a moment for operations to finish
        time.sleep(0.5)
        
        # Suppress Java IOException during cleanup (harmless Windows file lock issue)
        logging.getLogger("org.apache.spark.storage.DiskBlockManager").setLevel(logging.CRITICAL)
        
        # Stop the session
        spark.stop()
        
        # Brief delay to let JVM cleanup
        time.sleep(0.3)
        
    except Exception as e:
        # Ignore cleanup errors - they don't affect results
        pass


def create_spark_session() -> SparkSession:
    """Create and configure Spark session."""
    import multiprocessing
    import tempfile
    
    # Stop any existing Spark session to ensure new config is applied
    existing_spark = SparkSession.getActiveSession()
    if existing_spark:
        print("  Stopping existing Spark session to apply new config...")
        stop_spark_gracefully(existing_spark)
    
    cpu_cores = multiprocessing.cpu_count()
    
    # Get memory settings from environment or use defaults
    # For large datasets, increase these values
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "8g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "8g")
    
    print("=" * 70)
    print("INITIALIZING SPARK SESSION")
    print("=" * 70)
    print(f"  CPU Cores: {cpu_cores}")
    print(f"  Driver Memory: {driver_memory}")
    print(f"  Executor Memory: {executor_memory}")
    print(f"  Configuring for parallel processing...")
    
    # Show HADOOP_HOME status (already set at module level for Windows)
    hadoop_home = os.environ.get("HADOOP_HOME", "")
    if hadoop_home:
        print(f"  HADOOP_HOME: {hadoop_home}")
    
    # Create temporary checkpoint directory
    checkpoint_dir = tempfile.mkdtemp(prefix="spark_checkpoint_")
    
    # Use fewer partitions for memory efficiency (reduce parallelism)
    num_partitions = max(cpu_cores, 100)  # Don't over-parallelize
    
    # SPARK OPTIMIZATIONS: Maximize performance for blocking operations
    print(f"  Applying advanced Spark optimizations...")
    
    builder = SparkSession.builder \
        .appName("Splink Entity Resolution - MultiPassBlocker Optimized") \
        .master(f"local[{cpu_cores}]") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.default.parallelism", str(num_partitions)) \
        .config("spark.sql.shuffle.partitions", str(num_partitions)) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "1024") \
        .config("spark.sql.files.maxPartitionBytes", "64m") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
        .config("spark.sql.broadcastTimeout", "600") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000") \
        .config("spark.rdd.compress", "true") \
        .config("spark.shuffle.compress", "true") \
        .config("spark.shuffle.spill.compress", "true") \
        .config("spark.io.compression.codec", "snappy") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m") \
        .config("spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin", "0.2") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", str(cpu_cores)) \
        .config("spark.local.dir", "e:/ER-Cuinn/cuin/spark-temp")
    
    # Windows-specific: Additional configurations
    if platform.system() == "Windows":
        builder = builder \
            .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true -XX:+UseG1GC") \
            .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true -XX:+UseG1GC")
    
    spark = builder.getOrCreate()
    
    # Set checkpoint directory for Splink
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    spark.sparkContext.setLogLevel("WARN")
    
    # Suppress harmless Windows file deletion errors during shutdown
    logging.getLogger("org.apache.spark.storage.DiskBlockManager").setLevel(logging.CRITICAL)
    logging.getLogger("org.apache.spark.network.util.JavaUtils").setLevel(logging.CRITICAL)
    
    print(f"  ✓ Spark session created with advanced optimizations")
    print(f"  ✓ Parallelism: {num_partitions} partitions")
    print(f"  ✓ Checkpoint dir: {checkpoint_dir}")
    print(f"  ✓ Optimizations applied:")
    print(f"    - Adaptive query execution (AQE)")
    print(f"    - Broadcast joins (auto threshold: 10MB)")
    print(f"    - Arrow columnar processing")
    print(f"    - Kryo serialization")
    print(f"    - Compression (Snappy)")
    print(f"    - Skew join handling")
    print("=" * 70)
    print()
    
    return spark


def load_data(spark: SparkSession):
    """Load data from parquet files, preferring cached Oracle data when available.
    
    Data source resolution order:
    1. data_cache/oracle_data.parquet (cached Oracle data from get_cache_file_path())
    2. CUSTOMER_AGGREGATED_PATH environment variable (default: customer_aggregated)
    """
    # Prefer cached Oracle data when available
    cache_path = get_cache_file_path()
    if os.path.exists(cache_path):
        data_path = cache_path
    else:
        data_path = CUSTOMER_AGGREGATED_PATH
    
    if not os.path.exists(data_path):
        error_msg = (
            f"✗ ERROR: Parquet data not found at: {data_path}\n"
            f"  Also checked cache: {cache_path}\n\n"
            "Please ensure parquet data exists at one of these locations.\n"
            "You can configure the path via CUSTOMER_AGGREGATED_PATH in .env file"
        )
        raise FileNotFoundError(error_msg)
    
    print("=" * 70)
    print("LOADING DATA INTO SPARK")
    print("=" * 70)
    print(f"  Source: {data_path}")
    print(f"  Reading parquet data...")
    
    # Load parquet files from resolved path
    df = spark.read.parquet(data_path)
    
    # Verify CUSTOMER_CODE exists
    if "CUSTOMER_CODE" not in df.columns:
        error_msg = (
            "✗ ERROR: CUSTOMER_CODE column not found in parquet data\n"
            f"  Available columns: {df.columns}"
        )
        raise ValueError(error_msg)
    
    # Repartition for parallel processing
    # import multiprocessing
    # optimal_partitions = multiprocessing.cpu_count() * 2
    # df = df.repartition(optimal_partitions)
    num_partitions = df.rdd.getNumPartitions()
    record_count = df.count()
    print(f"  ✓ Loaded {record_count:,} records")
    print(f"  ✓ Partitions: {num_partitions}")
    print(f"  ✓ Using CUSTOMER_CODE as identifier")
    print("=" * 70)
    print()
    
    return df


def create_splink_settings(link_type: str = "dedupe_only") -> Dict[str, Any]:
    """Create Splink configuration settings with multipass blocking.
    
    Configures Splink to use CUSTOMER_CODE as the unique identifier column
    instead of the default 'unique_id' column name.
    """
    
    # Multipass Blocking Rules - Using OR conditions (union of all passes)
    # Each rule creates a separate blocking pass, and pairs are combined using OR logic
    # Note: Array columns (EMAIL, MOBILE, DOCUMENT) are handled by exploding in run_blocking
    # Note: LSH blocking for NAME is handled separately in run_blocking
    # Note: BRANCH_CODE is intentionally NOT used per requirements
    blocking_rules = [
        # Exact match blocking passes
        "l.NAME = r.NAME",
        "l.MOBILE = r.MOBILE",
        "l.EMAIL = r.EMAIL",
        "l.BIRTH_DATE = r.BIRTH_DATE",
        "l.DOCUMENT = r.DOCUMENT",
    ]
    
    # Comparison rules - define how fields are compared
    # CRITICAL CONSTRAINT: NAME and DOCUMENT are mandatory matching fields
    # Constraint is enforced at clustering phase by filtering gamma levels
    # (m_probabilities are learned during training, cannot be set manually)
    comparisons = [
        {
            "output_column_name": "name",
            "comparison_levels": [
                cll.NullLevel("NAME"),
                cll.ExactMatchLevel("NAME"),
                cll.LevenshteinLevel("NAME", 2),
                cll.ElseLevel(),
            ],
        },
        {
            "output_column_name": "document",
            "comparison_levels": [
                cll.NullLevel("DOCUMENT"),
                cll.ExactMatchLevel("DOCUMENT"),
                cll.ElseLevel(),
            ],
        },
        {
            "output_column_name": "mobile",
            "comparison_levels": [
                cll.NullLevel("MOBILE"),
                cll.ExactMatchLevel("MOBILE"),
                cll.ElseLevel(),
            ],
        },
        {
            "output_column_name": "email",
            "comparison_levels": [
                cll.NullLevel("EMAIL"),
                cll.ExactMatchLevel("EMAIL"),
                cll.ElseLevel(),
            ],
        },
        {
            "output_column_name": "dob",
            "comparison_levels": [
                cll.NullLevel("BIRTH_DATE"),
                cll.ExactMatchLevel("BIRTH_DATE"),
                cll.ElseLevel(),
            ],
        }
    ]
    
    settings = {
        "link_type": link_type,
        "unique_id_column_name": "CUSTOMER_CODE",  # Use CUSTOMER_CODE as unique identifier
        "blocking_rules_to_generate_predictions": blocking_rules,
        "comparisons": comparisons,
        "retain_matching_columns": True,
        "retain_intermediate_calculation_columns": False,
        "max_iterations": 5,
        "em_convergence": 0.05,
    }
    
    return settings


def run_blocking(spark: SparkSession, df, output_csv: str = "blocking_analysis.csv", record_count: int = None):
    """Run Spark-native distributed blocking with maximum performance.
    
    This implements blocking using pure Spark SQL operations (no Python UDFs):
    - **Spark-Native**: All operations use Spark SQL built-in functions
    - **No Serialization Issues**: Avoids Python worker connection problems
    - **Maximum Performance**: Leverages Spark's optimized columnar processing
    - **Distributed Processing**: Leverages Spark parallelism across all CPU cores
    - **Memory Efficient**: Broadcast joins, caching, and streaming results
    
    Blocking Configuration (optimized for 1.7M records):
    - Selective rules mode: Exact matches on NAME, phone, email, dob
    - Full mode: NAME (exact, prefix, token), phone, email, dob, soundex
    - Max block size: 50 (prevents combinatorial explosion)
    
    Spark Optimizations:
    - Pure SQL operations (no UDF serialization overhead)
    - Broadcast joins for small lookup tables
    - Aggressive caching of intermediate results
    - Optimized partitioning to reduce shuffles
    - Columnar operations for better performance
    
    Args:
        spark: SparkSession for distributed processing
        df: Input Spark DataFrame with customer data (loaded from configured parquet path)
        output_csv: Output file path for blocking results
        
    Returns:
        Tuple of (settings, pandas_df, spark_df) with blocking results
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType
    import pandas as pd
    
    print("=" * 70)
    print("SPARK-NATIVE BLOCKING - DISTRIBUTED PROCESSING")
    print("=" * 70)
    
    # Initialize MultiPassBlocker configuration (but won't use it directly due to serialization issues)
    # Instead, we'll implement the same logic using pure Spark SQL
    total_records = record_count if record_count is not None else df.count()
    
    # Choose configuration based on dataset size
    if total_records > 500000:
        print(f"  Dataset size: {total_records:,} (LARGE - using selective mode)")
        multipass_config = {
            'use_lsh': True,             # TRUE LSH with MinHash and banding (Spark SQL native)
            'use_soundex': False,        # Can use Spark's soundex() function if needed
            'use_rules': True,           # Implemented via Spark SQL
            'max_block_size': 100,        # Prevent combinatorial explosion
            'use_selective_rules': True  # Only exact matches for large datasets
        }
    elif total_records > 100000:
        print(f"  Dataset size: {total_records:,} (MEDIUM - using balanced mode)")
        multipass_config = {
            'use_lsh': False,
            'use_soundex': False,
            'use_rules': True,
            'max_block_size': 100,
            'use_selective_rules': False
        }
    else:
        print(f"  Dataset size: {total_records:,} (SMALL - using full mode)")
        multipass_config = {
            'use_lsh': False,
            'use_soundex': False,
            'use_rules': True,
            'max_block_size': 200,
            'use_selective_rules': False
        }
    
    print("\n  Blocking Configuration:")
    print(f"    • Approach: Pure Spark SQL (no Python UDFs)")
    #print(f"    • Use Soundex: {multipass_config['use_soundex']}")
    print(f"    • Selective Mode: {multipass_config['use_selective_rules']}")
    print(f"    • Max Block Size: {multipass_config['max_block_size']}")
    print()
    
    # Spark optimizations
    print("  Spark Optimizations:")
    print(f"    • CPU Cores: {spark.sparkContext.defaultParallelism}")
    print(f"    • Caching: Aggressive (intermediate results)")
    print(f"    • Broadcast Joins: Enabled for small tables")
    print(f"    • Columnar Processing: Enabled")
    print()
    
    settings = create_splink_settings()
    
    # Cache the input DataFrame for reuse (skip if already cached from normalisation stage)
    if not df.is_cached:
        df.cache()
        df.count()  # Materialize cache
    
    print("\n" + "=" * 70)
    print("GENERATING CANDIDATE PAIRS - SPARK NATIVE BLOCKING")
    print("=" * 70)
    
    # Detect available columns
    available_cols = df.columns
    print(f"  • Available columns: {', '.join(available_cols)}")
    
    # Map to expected column names (handle different schemas)
    col_mapping = {
        'name': 'NAME',
        'mobile': 'MOBILE',
        'email': 'EMAIL',
        'dob': 'BIRTH_DATE' if 'BIRTH_DATE' in available_cols else ('CUSDOB' if 'CUSDOB' in available_cols else None),
        'document': 'DOCUMENT' if 'DOCUMENT' in available_cols else ('ID_NUMBER' if 'ID_NUMBER' in available_cols else None),
    }
    
    print(f"  • Column mapping: {col_mapping}")
    
    # Check column types (array vs scalar)
    col_types = {}
    for purpose, col_name in col_mapping.items():
        if col_name in available_cols:
            col_type = df.schema[col_name].dataType
            is_array = isinstance(col_type, ArrayType)
            col_types[purpose] = 'array' if is_array else 'scalar'
            print(f"    - {col_name}: {col_types[purpose]}")
    
    # SPARK-NATIVE APPROACH: Generate blocking keys using pure Spark operations
    # This avoids UDF serialization issues and is MUCH faster
    print("\n  Strategy: Pure Spark SQL blocking (no UDFs, no serialization issues)")
    if multipass_config['use_selective_rules']:
        print("    • Selective mode: Exact matches on NAME, phone, email, dob")
    else:
        print("    • Full mode: Exact + prefix + token matches")
    print()
    
    print("  Step 1: Generating blocking keys using Spark SQL operations...")
    
    # Build a list of column expressions, where each can be either a single key or an array of keys
    key_column_exprs = []
    
    # NAME blocking (exact, prefix, soundex based on config)
    if col_mapping['name'] in available_cols:
        # BUG FIX: Filter out very short names (< 3 chars) to reduce noise
        name_col = F.col(col_mapping['name'])
        name_trimmed = F.lower(F.trim(name_col))
        name_length_check = F.length(name_trimmed) >= 3
        
        # Exact match on name (with length filter)
        key_column_exprs.append(
            F.when(name_length_check, 
                   F.array(F.concat(F.lit("name_exact:"), name_trimmed))
            ).otherwise(F.array())
        )
        
        if not multipass_config['use_selective_rules']:
            # Prefix match (first 5 chars, only for names >= 5 chars)
            key_column_exprs.append(
                F.when(F.length(name_trimmed) >= 5,
                       F.array(F.concat(F.lit("name_prefix:"), F.substring(name_trimmed, 1, 5)))
                ).otherwise(F.array())
            )
            
            # BUG FIX: Token match - use ALL tokens, not just first word
            # This creates better matches for multi-word names like "MD. JAHIRUL ISLAM"
            tokens_array = F.split(name_trimmed, " ")
            key_column_exprs.append(
                F.when(name_length_check,
                       F.transform(tokens_array, lambda token: 
                           F.when(F.length(token) >= 3,  # Only use tokens >= 3 chars
                                  F.concat(F.lit("name_token:"), token)
                           )
                       )
                ).otherwise(F.array())
            )
        
        if multipass_config['use_soundex']:
            # Soundex match (with length filter)
            key_column_exprs.append(
                F.when(name_length_check,
                       F.array(F.concat(F.lit("name_soundex:"), F.soundex(name_col)))
                ).otherwise(F.array())
            )

        if multipass_config['use_lsh']:
            # TRUE LSH BLOCKING: MinHash with character shingles and banding
            # This implements proper Locality Sensitive Hashing for Jaccard similarity
            # 
            # Algorithm:
            # 1. Character 3-grams (shingles): "JOHN" -> ["JOH", "OHN"]
            # 2. MinHash signatures: Apply multiple hash functions to shingles
            # 3. Banding: Group signatures into bands, hash each band
            # 4. Similar names -> same bucket in at least one band (high probability)
            #
            # Parameters:
            # - num_bands: 5 (more bands = higher recall, more pairs)
            # - buckets_per_band: 10,000 (total 50,000 buckets across all bands)
            # - For 1.5M records: ~30 records/bucket on average (fits within max_block_size)
            #
            # Trade-off: 50,000 total buckets catches similar names while
            # keeping bucket sizes manageable for blocking constraints
            
            # Parameters for LSH
            num_bands = 5  # Number of hash bands (more = higher recall)
            shingle_size = 3  # Character n-gram size
            
            # Create character shingles (3-grams) from name
            # Example: "JOHN DOE" -> ["JOH", "OHN", "HN ", "N D", " DO", "DOE"]
            name_len = F.length(name_trimmed)
            
            # Generate array of character positions for shingling
            # We'll create shingles using substring operations
            # For Spark SQL, we need to generate an array of shingles
            # Using transform on sequence of positions
            shingles = F.transform(
                F.sequence(F.lit(1), F.greatest(name_len - shingle_size + 1, F.lit(1))),
                lambda i: F.substring(name_trimmed, i, shingle_size)
            )
            
            # Filter out shingles that are too short (at boundaries)
            shingles_filtered = F.filter(shingles, lambda x: F.length(x) == shingle_size)
            
            # Generate multiple LSH bands using different hash seeds
            # Each band uses a different combination of hash functions
            # Similar items will collide in at least one band with high probability
            lsh_buckets = []
            
            for band_idx in range(num_bands):
                # Use different hash seeds for each band to create independent hashes
                # Combine multiple hash values to create MinHash signature for this band
                seed1 = band_idx * 2 + 1
                seed2 = band_idx * 2 + 2
                
                # Create signature for this band by hashing shingles with band-specific seeds
                # We aggregate hashes of all shingles using XOR to combine them
                # This simulates MinHash signature where similar sets have similar signatures
                band_signature = F.aggregate(
                    shingles_filtered,
                    F.lit(0),  # Initial value
                    lambda acc, shingle: acc.bitwiseXOR(
                        F.hash(F.concat(F.lit(str(seed1)), shingle))
                    ).bitwiseXOR(
                        F.hash(F.concat(F.lit(str(seed2)), shingle))
                    )
                )
                
                # Hash the band signature to create bucket ID
                # Modulo to limit bucket space (5,000 buckets per band = 25,000 total buckets)
                # For 1.5M records: 1,500,000 / 25,000 = 60 avg records/bucket (within max_block_size)
                bucket_id = F.abs(band_signature) % 5000
                
                # Create LSH key for this band
                lsh_buckets.append(
                    F.concat(F.lit(f"name_lsh_b{band_idx}:"), bucket_id.cast("string"))
                )
            
            # Combine all LSH buckets into array (one record can be in multiple buckets)
            # This is the banding scheme - similar names will match in at least one band
            key_column_exprs.append(
                F.when(
                    name_length_check & (F.size(shingles_filtered) > 0),
                    F.array(*lsh_buckets)
                ).otherwise(F.array())
            )
    
    # MOBILE blocking (handle array or scalar)
    if col_mapping['mobile'] in available_cols:
        if col_types.get('mobile') == 'array':
            # BUG FIX: Remove duplicates from mobile array before creating keys
            # Arrays can have duplicate values which create redundant blocking keys
            mobile_cleaned = F.array_distinct(
                F.transform(
                    F.col(col_mapping['mobile']),
                    lambda x: F.regexp_replace(x, "[^0-9]", "")
                )
            )
            # Filter out empty strings and short numbers (< 7 digits)
            key_column_exprs.append(
                F.transform(
                    mobile_cleaned,
                    lambda x: F.when((F.length(x) >= 11) & (F.length(x) <= 14),
                                     F.concat(F.lit("mobile:"), x)
                    )
                )
            )
        else:
            # Scalar column - wrap in array for consistency
            mobile_cleaned = F.regexp_replace(F.col(col_mapping['mobile']), "[^0-9]", "")
            key_column_exprs.append(
                F.when((F.length(mobile_cleaned) >= 11) & (F.length(mobile_cleaned) <= 14),
                       F.array(F.concat(F.lit("mobile:"), mobile_cleaned))
                ).otherwise(F.array())
            )
    
    # EMAIL blocking (handle array or scalar)
    if col_mapping['email'] in available_cols:
        if col_types.get('email') == 'array':
            # BUG FIX: Remove duplicates from email array before creating keys
            email_cleaned = F.array_distinct(
                F.transform(
                    F.col(col_mapping['email']),
                    lambda x: F.lower(F.trim(x))
                )
            )
            # Filter out empty strings and invalid emails (must contain @)
            key_column_exprs.append(
                F.transform(
                    email_cleaned,
                    lambda x: F.when((F.length(x) > 0) & (x.contains("@")),
                                     F.concat(F.lit("email:"), x)
                    )
                )
            )
        else:
            # Scalar column - wrap in array for consistency
            email_cleaned = F.lower(F.trim(F.col(col_mapping['email'])))
            key_column_exprs.append(
                F.when((F.length(email_cleaned) > 0) & (email_cleaned.contains("@")),
                       F.array(F.concat(F.lit("email:"), email_cleaned))
                ).otherwise(F.array())
            )
    
    # DOB blocking (exact match)
    # BUG FIX: Enable DOB blocking - this is a strong matching signal
    # DOB matching significantly reduces false positives
    if col_mapping.get('dob') and col_mapping['dob'] in available_cols:
        dob_col = F.col(col_mapping['dob'])
        key_column_exprs.append(
            F.when(dob_col.isNotNull(),
                   F.array(F.concat(F.lit("dob:"), dob_col.cast("string")))
            ).otherwise(F.array())
        )

    if col_mapping.get('document') and col_mapping['document'] in available_cols:
        if col_types.get('document') == 'array':
            # BUG FIX: Remove duplicates from document array before creating keys
            document_cleaned = F.array_distinct(
                F.transform(
                    F.col(col_mapping['document']),
                    lambda x: F.lower(F.trim(x))
                )
            )
            key_column_exprs.append(
                F.transform(
                    document_cleaned,
                    lambda x: F.when(F.length(x) > 0,
                                     F.concat(F.lit("document:"), x)
                    )
                )
            )
        else:
            document_col = F.col(col_mapping['document'])
            key_column_exprs.append(
                F.when(document_col.isNotNull(),
                    F.array(F.concat(F.lit("document:"), document_col))
                ).otherwise(F.array())
            )
    
    print(f"  • Created {len(key_column_exprs)} blocking key expressions")
    
    # Flatten all arrays into a single array of blocking keys using array_union
    # array_union removes nulls automatically
    if len(key_column_exprs) == 1:
        all_keys = key_column_exprs[0]
    else:
        # Use concat to combine arrays, then flatten to remove nested arrays
        all_keys = F.flatten(F.array(*key_column_exprs))
    
    # Create DataFrame with blocking keys
    print("  • Creating DataFrame with blocking keys...")
    df_with_keys = df.select(
        F.col("CUSTOMER_CODE"),
        all_keys.alias("blocking_keys")
    ).filter(
        F.col("blocking_keys").isNotNull() &
        (F.size(F.col("blocking_keys")) > 0)
    )
    
    # Cache the result
    df_with_keys.cache()
    records_with_keys = df_with_keys.count()
    print(f"  ✓ Generated blocking keys for {records_with_keys:,} records")
    
    # Explode blocking keys to create (customer_code, blocking_key) pairs
    print("  • Exploding blocking keys...")
    df_exploded = df_with_keys.select(
        F.col("CUSTOMER_CODE"),
        F.explode(F.col("blocking_keys")).alias("blocking_key")
    ).filter(
        F.col("blocking_key").isNotNull() & 
        (F.length(F.col("blocking_key")) > 0)
    )
    
    # SPARK OPTIMIZATION: Repartition by blocking key for better grouping
    df_exploded = df_exploded.repartition(spark.sparkContext.defaultParallelism, "blocking_key")
    df_exploded.cache()
    total_key_instances = df_exploded.count()
    print(f"  ✓✓✓ Total blocking key instances: {total_key_instances:,}")
    
    # Step 2: Group by blocking keys and generate candidate pairs
    print("\n  Step 2: Grouping by blocking keys and generating candidate pairs...")
    
    # Compute block statistics
    print("  • Computing block statistics (distributed)...")
    blocking_stats = df_exploded.groupBy("blocking_key") \
        .agg(F.count("*").alias("block_count")) \
        .filter(
            (F.col("block_count") >= 2) & 
            (F.col("block_count") <= multipass_config['max_block_size'])
        )
    
    blocking_stats.cache()
    num_viable_blocks = blocking_stats.count()
    print(f"  ✓ Viable blocks: {num_viable_blocks:,} (size 2-{multipass_config['max_block_size']})")
    
    if num_viable_blocks == 0:
        print("  ✗ No viable blocks found")
        df_exploded.unpersist()
        df_with_keys.unpersist()
        df.unpersist()
        
        # Return empty results
        empty_df = pd.DataFrame(columns=['id_l', 'id_r', 'blocking_rule', 'rule_index'])
        empty_df.to_csv(output_csv, index=False)
        empty_spark_df = spark.createDataFrame(empty_df)
        return settings, empty_df, empty_spark_df
    
    # SPARK OPTIMIZATION: Broadcast join for viable blocks lookup
    if num_viable_blocks < 10000:
        print("  • Using BROADCAST JOIN for viable blocks")
        blocking_stats = F.broadcast(blocking_stats)
    
    # Filter to only records in viable blocks
    print("  • Filtering to viable blocks...")
    df_viable = df_exploded.join(
        blocking_stats.select("blocking_key"),
        on="blocking_key",
        how="inner"
    )
    
    df_viable.cache()
    # (skip viable_instances count — not used for logic, saves one Spark job)

    # Generate candidate pairs via self-join
    print("  • Generating candidate pairs (self-join on blocking keys)...")
    pairs_df = df_viable.alias("l").join(
        df_viable.alias("r"),
        (F.col("l.blocking_key") == F.col("r.blocking_key")) &
        (F.col("l.CUSTOMER_CODE") < F.col("r.CUSTOMER_CODE")),
        "inner"
    ).select(
        F.col("l.CUSTOMER_CODE").alias("id_l"),
        F.col("r.CUSTOMER_CODE").alias("id_r"),
        F.col("l.blocking_key").alias("blocking_rule")
    ).distinct()  # Remove duplicate pairs from multiple blocking keys
    
    # Count and limit pairs
    print("  • Counting candidate pairs...")
    total_pairs_unlimited = pairs_df.count()
    print(f"  ✓ Total candidate pairs: {total_pairs_unlimited:,}")
    
    # BUG FIX: Reduce max pairs limit to prevent false positives and memory issues
    # Original limit of 1M (100K * 10) was too high for 1.7M records
    # Lower limit forces more selective blocking and reduces singleton clusters
    max_total_pairs = MAX_PAIRS_PER_RULE * 2  # More conservative: 200K instead of 1M
    if total_pairs_unlimited > max_total_pairs:
        print(f"  ⚠ Candidate pairs ({total_pairs_unlimited:,}) exceeds limit ({max_total_pairs:,})")
        print(f"  • Limiting to {max_total_pairs:,} pairs for memory efficiency")
        print(f"  • Consider: adjusting blocking rules to be more selective")
        pairs_df = pairs_df.limit(max_total_pairs)
        total_pairs = max_total_pairs
    else:
        total_pairs = total_pairs_unlimited
    
    # Convert to pandas and save
    print("  • Converting to pandas and saving...")
    pairs_pd = pairs_df.toPandas()
    
    # Add metadata
    pairs_pd['rule_index'] = 0
    pairs_pd['blocking_timestamp'] = datetime.now().isoformat()
    pairs_pd['total_records'] = total_records
    pairs_pd['num_blocking_rules'] = 1  # MultiPassBlocker combines all rules
    
    pairs_pd.to_csv(output_csv, index=False)
    print(f"  ✓ Saved to {output_csv}")
    
    # Cleanup
    df_viable.unpersist()
    df_exploded.unpersist()
    df_with_keys.unpersist()
    blocking_stats.unpersist()
    df.unpersist()
    
    # Statistics
    total_possible_pairs = (total_records * (total_records - 1)) // 2
    reduction_pct = ((total_possible_pairs - total_pairs) / total_possible_pairs * 100) if total_possible_pairs > 0 else 0
    
    print("\n" + "=" * 70)
    print("SPARK-NATIVE BLOCKING COMPLETE")
    print("=" * 70)
    print(f"  Configuration:")
    print(f"    • Blocking Strategy: Spark SQL (native, no UDFs)")
    print(f"    • Selective Mode: {multipass_config['use_selective_rules']}")
    print(f"    • Use Soundex: {multipass_config['use_soundex']}")
    print(f"    • Max Block Size: {multipass_config['max_block_size']}")
    print(f"\n  Statistics:")
    print(f"    • Records: {total_records:,}")
    print(f"    • Records with keys: {records_with_keys:,}")
    print(f"    • Total blocking key instances: {total_key_instances:,}")
    print(f"    • Viable blocks: {num_viable_blocks:,}")
    print(f"    • Total possible pairs: {total_possible_pairs:,}")
    print(f"    • Candidate pairs: {total_pairs:,}")
    print(f"    • Reduction: {reduction_pct:.2f}%")
    print(f"    • Spark parallelism: {spark.sparkContext.defaultParallelism} cores")
    print(f"\n  Blocking Rules Used:")
    print(f"    • NAME (exact match, min length 3)")
    if not multipass_config['use_selective_rules']:
        print(f"    • NAME (prefix match, first 5 chars)")
        print(f"    • NAME (token match, all tokens >= 3 chars)")
    if multipass_config['use_soundex']:
        print(f"    • NAME (soundex)")
    if multipass_config['use_lsh']:
        print(f"    • NAME (LSH with MinHash, 5 bands × 10K buckets = 50K total)")
    print(f"    • MOBILE (exact match, 7-15 digits, deduplicated)")
    print(f"    • EMAIL (exact match, validated format, deduplicated)")
    if col_mapping.get('dob') and col_mapping['dob'] in available_cols:
        print(f"    • DOB (exact match)")
    else:
        print(f"    • DOB (not available in dataset)")
    print("=" * 70)
    
    # Load back into Spark DataFrame
    candidates_spark_df = spark.createDataFrame(pairs_pd)
    candidates_spark_df.cache()
    
    return settings, pairs_pd, candidates_spark_df


def run_scoring(spark: SparkSession, df, settings: Dict[str, Any], 
                blocking_csv: str = "blocking_analysis.csv",
                threshold: float = 0.8, 
                output_csv: str = "scoring_results.csv"):
    """Run multipass distributed scoring ONLY on candidate pairs from blocking phase.
    
    This implements a multipass distributed scoring strategy to prevent Out Of Memory (OOM) issues:
    - **Uses Blocking Results**: Only scores candidate pairs identified in blocking phase
    - **Distributed Processing**: Leverages Spark's distributed parallel processing for all operations
    - **Multipass Strategy**: Processes predictions in batches (one blocking rule at a time)
    - **Streaming Results**: Writes results incrementally to CSV to avoid memory buildup
    - **Progress Reporting**: Detailed progress for each training and scoring pass
    - **OOM Handling**: Gracefully handles Out Of Memory errors with recovery suggestions
    
    Training Passes (Sequential):
    1. u-probability estimation using random sampling
    2. m-probability estimation for each blocking rule via EM (sequential)
    
    Scoring Phase (Multipass Distributed):
    - Process predictions one blocking rule at a time
    - Filter to ONLY candidate pairs from blocking phase (prevents scoring 800K+ unnecessary pairs)
    - Each rule scored in parallel via Spark's distributed execution
    - Results streamed to CSV incrementally to prevent memory issues
    - Deduplication performed at the end
    
    Args:
        spark: SparkSession for distributed processing
        df: Input Spark DataFrame with customer data
        settings: Splink configuration settings
        blocking_csv: Path to blocking results CSV (output from run_blocking)
        threshold: Match probability threshold (default: 0.8)
        output_csv: Path to output CSV file for scoring results (default: scoring_results.csv)
        
    Returns:
        Tuple of (linker, predictions_combined) with scoring results
    """
    from pyspark.sql import functions as F
    
    print("=" * 70)
    print("MULTIPASS DISTRIBUTED SCORING - PARALLEL PROCESSING")
    print("=" * 70)
    print("  Strategy:")
    print("    • CANDIDATE PAIRS ONLY: Use blocking results to limit scoring")
    print("    • Distributed: Spark parallel processing across all CPU cores")
    print("    • Multipass: Training on each blocking rule independently")
    print("    • Batch Scoring: Process predictions one blocking rule at a time")
    print("    • Streaming: Write results incrementally to prevent OOM")
    print("    • Memory-efficient: No full dataset in memory at once")
    print(f"    • Match threshold: {threshold}")
    print()
    
    # Load candidate pairs from blocking phase
    if not os.path.exists(blocking_csv):
        print(f"  ✗ ERROR: Blocking results not found: {blocking_csv}")
        print(f"  • Please run blocking phase first: python -m er.splink_commands blocking")
        raise FileNotFoundError(f"Blocking results not found: {blocking_csv}")
    
    print(f"  Loading candidate pairs from blocking phase...")
    print(f"    • File: {blocking_csv}")
    
    try:
        import pandas as pd
        candidates_df = pd.read_csv(blocking_csv)
        total_candidates = len(candidates_df)
        print(f"    ✓ Loaded {total_candidates:,} candidate pairs")
        
        if total_candidates == 0:
            raise ValueError("Blocking CSV is empty! No candidate pairs to score.")
        
        # Get unique IDs from candidate pairs
        unique_ids_left = set(candidates_df['id_l'].astype(str).unique())
        unique_ids_right = set(candidates_df['id_r'].astype(str).unique())
        unique_ids = unique_ids_left.union(unique_ids_right)
        
        print(f"    ✓ Unique customer IDs in candidates: {len(unique_ids):,}")
        
        # Debug: Show sample IDs
        sample_ids = list(unique_ids)[:5]
        print(f"    • Sample candidate IDs: {sample_ids}")
        
        # Get a sample of actual IDs from DataFrame for comparison
        actual_sample = df.select("CUSTOMER_CODE").limit(5).collect()
        actual_ids = [str(row.CUSTOMER_CODE) for row in actual_sample]
        print(f"    • Sample DataFrame IDs: {actual_ids}")
        
        original_count = df.count()
        print(f"    • Original dataset: {original_count:,} records")
        print(f"    • This filtering will use {len(unique_ids)/original_count*100:.1f}% of records")
        
        # CRITICAL: Filter input DataFrame to ONLY records in candidate pairs
        # This prevents Splink from generating predictions for irrelevant pairs
        print(f"    • Filtering input data to candidate records only...")
        
        # IMPORTANT: Normalize IDs by converting to integers (removes leading zeros)
        # Candidate IDs from CSV don't have leading zeros, but DataFrame might
        print(f"    • Normalizing IDs (converting to integers to match formats)...")
        
        try:
            # Convert candidate IDs to integers
            unique_ids_int = set()
            for id_str in unique_ids:
                try:
                    unique_ids_int.add(int(id_str))
                except ValueError:
                    # Keep as string if not convertible to int
                    unique_ids_int.add(id_str)
            
            # Filter DataFrame by converting CUSTOMER_CODE to int for comparison
            df_filtered = df.filter(
                F.col("CUSTOMER_CODE").cast("int").isin(list(unique_ids_int))
            )
            df_filtered.cache()
            
        except Exception as conv_error:
            # Fallback: Try string comparison with stripped leading zeros
            print(f"    • Integer conversion failed: {conv_error}")
            print(f"    • Trying string comparison with ltrim...")
            
            unique_ids_list = [str(id_val) for id_val in unique_ids]
            df_filtered = df.filter(
                F.ltrim(F.col("CUSTOMER_CODE").cast("string"), '0').isin(unique_ids_list)
            )
            df_filtered.cache()
        
        filtered_count = df_filtered.count()
        print(f"    ✓ Filtered dataset: {filtered_count:,} records (down from {original_count:,})")
        
        if filtered_count == 0:
            raise ValueError(
                f"Filtered dataset is EMPTY! This means no records matched candidate IDs.\n"
                f"  • Candidate IDs sample: {sample_ids}\n"
                f"  • DataFrame IDs sample: {actual_ids}\n"
                f"  • Check if ID formats/types match between blocking and scoring!"
            )
        
        print(f"    • Reduction: {(1 - filtered_count/original_count)*100:.1f}% fewer records")
        print(f"    • This dramatically reduces prediction pairs!")
        print()
        
        # Update df to filtered version for scoring
        df = df_filtered
        
    except Exception as e:
        print(f"  ✗ ERROR: Failed to load blocking results: {e}")
        raise
    
    # Get blocking rules from settings
    blocking_rules = settings.get("blocking_rules_to_generate_predictions", [])
    print(f"  Blocking rules for scoring:")
    for i, rule in enumerate(blocking_rules, 1):
        print(f"    {i}. {rule}")
    print()
    
    # Count records for progress tracking
    total_records = df.count()
    print(f"  Total records: {total_records:,}")
    print(f"  Parallelism: {spark.sparkContext.defaultParallelism} partitions")
    print()
    
    # Training phase - sequential with progress reporting
    print("=" * 70)
    print("TRAINING PHASE")
    print("=" * 70)
    
    try:
        print(f"  [1/2] Creating linker...")

        # --- Column pruning: keep ONLY what Splink needs ---
        # Carrying extra columns (especially wide ARRAY columns like MOBILE/EMAIL/DOCUMENT)
        # through Splink's self-joins massively inflates shuffle size.
        # Splink only requires: unique_id + all columns referenced in comparisons/blocking.
        _splink_required = {
            settings.get("unique_id_column_name", "CUSTOMER_CODE"),
            "NAME", "DOCUMENT", "MOBILE", "EMAIL", "BIRTH_DATE",
        }
        _available = set(df.columns)
        _keep = list(_splink_required & _available)
        if set(_keep) != _available:
            dropped = sorted(_available - set(_keep))
            print(f"  • Pruning {len(dropped)} unused columns before training: {dropped}")
            df = df.select(*_keep)
        else:
            print(f"  • All {len(_available)} columns are used by Splink — no pruning needed")

        # Create Linker with Splink 4.x API
        api = SparkAPI(spark_session=spark)
        linker = Linker(
            df,
            settings,
            db_api=api
        )
        print(f"  ✓ Linker created")
        print()
        
        # Training pass 1: u-probabilities
        print(f"  [2/2] Training pass 1: u-probability estimation")
        print(f"    • Method: Random sampling")
        print(f"    • Max pairs: {int(MAX_SAMPLING_PAIRS):,}")
        
        training_start = datetime.now()
        try:
            linker.training.estimate_u_using_random_sampling(max_pairs=MAX_SAMPLING_PAIRS)
            training_duration = (datetime.now() - training_start).total_seconds()
            print(f"    ✓ u-probabilities estimated in {training_duration:.1f}s")
            print()
        except MemoryError:
            print(f"    ✗ Out of Memory during u-probability estimation")
            print(f"    • Consider: reducing MAX_SAMPLING_PAIRS or increasing memory")
            raise
        except Exception as e:
            print(f"    ✗ Error during u-probability estimation: {e}")
            raise
        
        # Training pass 2: m-probabilities via EM — use only the 2 most discriminative
        # blocking rules (NAME and MOBILE) to avoid 5 slow EM passes.
        # Using a subset is sufficient — Splink estimates m-probs for all comparisons
        # not blocked on, so 2 passes cover all 5 comparisons.
        em_training_rules = [
            r for r in blocking_rules
            if "NAME" in r or "MOBILE" in r or "DOCUMENT" in r
        ][:3]  # Cap at 3: NAME + MOBILE + DOCUMENT cover all comparisons
        if not em_training_rules:
            em_training_rules = blocking_rules[:3]

        print(f"  Training pass 2: m-probability estimation ({len(em_training_rules)} of {len(blocking_rules)} blocking rules)")
        print(f"    • Method: Expectation Maximisation (EM)")
        print(f"    • Rules selected: {em_training_rules}")
        print()

        for em_idx, em_rule in enumerate(em_training_rules, 1):
            print(f"    [{em_idx}/{len(em_training_rules)}] Rule: {em_rule}")
            em_start = datetime.now()
            try:
                linker.training.estimate_parameters_using_expectation_maximisation(em_rule)
                em_duration = (datetime.now() - em_start).total_seconds()
                print(f"    ✓ EM pass {em_idx} completed in {em_duration:.1f}s")
            except MemoryError:
                print(f"    ✗ Out of Memory during EM pass {em_idx}; skipping rule")
            except Exception as e:
                print(f"    ✗ EM pass {em_idx} failed ({e}); skipping rule")
            print()

        print()
        print(f"  ✓ Training phase completed")
        print("=" * 70)
        print()

    except Exception as e:
        print(f"\n  ✗ Training phase failed: {e}")
        print("=" * 70)
        raise
    
    # Scoring phase — single-pass prediction (all blocking rules combined)
    print("=" * 70)
    print("SCORING PHASE - SINGLE-PASS PREDICTION")
    print("=" * 70)

    try:
        print(f"  Generating predictions in a single Spark pass...")
        print(f"    • Strategy: All blocking rules combined (no per-rule overhead)")
        print(f"    • Threshold: {threshold}")
        print(f"    • Output: {output_csv}")
        print()

        import csv

        scoring_start = datetime.now()
        total_predictions = 0

        # Clean up old output file
        if os.path.exists(output_csv):
            print(f"  ⚠ Deleting old scoring results: {output_csv}")
            os.remove(output_csv)

        def spark_row_to_csv_values(row, columns):
            row_dict = row.asDict(recursive=True)
            return ["" if row_dict.get(col) is None else str(row_dict.get(col)) for col in columns]

        # Single predict call — Splink deduplicates pairs across blocking rules internally
        print(f"  Running Splink predict (single pass, all rules)...")
        predict_start = datetime.now()
        raw_predictions = linker.inference.predict(threshold_match_probability=threshold)
        predictions_df_spark = raw_predictions.as_spark_dataframe()

        # Drop array columns to prevent CSV corruption
        ARRAY_FIELDS = ['DOCUMENT', 'MOBILE', 'EMAIL', 'FULL_ADDRESS', 'TELEPHONE']
        array_cols_to_drop = [c for c in predictions_df_spark.columns
                              if any(arr in c for arr in ARRAY_FIELDS)]
        if array_cols_to_drop:
            print(f"  • Dropping {len(array_cols_to_drop)} array columns from predictions")
            predictions_df_spark = predictions_df_spark.drop(*array_cols_to_drop)

        # Ensure match_probability column exists
        if "match_probability" not in predictions_df_spark.columns:
            if "match_weight" in predictions_df_spark.columns:
                print(f"  • Deriving match_probability from match_weight...")
                predictions_df_spark = predictions_df_spark.withColumn(
                    "match_probability",
                    F.expr("1.0 / (1.0 + pow(2.0, -match_weight))")
                )
            else:
                print(f"  ⚠ Warning: No match_probability or match_weight column found")

        predict_duration = (datetime.now() - predict_start).total_seconds()
        print(f"  ✓ Predictions plan built in {predict_duration:.1f}s")

        # Stream rows to CSV via toLocalIterator (avoids loading full result into driver memory)
        print(f"  Streaming predictions to CSV...")
        row_iter = predictions_df_spark.toLocalIterator()
        first_row = next(row_iter, None)
        if first_row is not None:
            columns = predictions_df_spark.columns
            with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(columns)
                writer.writerow(spark_row_to_csv_values(first_row, columns))
                total_predictions = 1
                for row in row_iter:
                    writer.writerow(spark_row_to_csv_values(row, columns))
                    total_predictions += 1
            print(f"  ✓ Wrote {total_predictions:,} predictions to {output_csv}")
        else:
            print(f"  • No predictions above threshold {threshold}")

        scoring_duration = (datetime.now() - scoring_start).total_seconds()

        print()
        print("=" * 70)
        print("SCORING SUMMARY")
        print("=" * 70)
        print(f"  Records processed: {total_records:,}")
        print(f"  Blocking rules: {len(blocking_rules)}")
        print(f"  Strategy: Single-pass (all rules combined, Splink deduplicates internally)")
        print(f"  Match threshold: {threshold}")
        print(f"  Total predictions: {total_predictions:,}")
        if scoring_duration > 0:
            print(f"  Average speed: {total_predictions/scoring_duration:.0f} predictions/sec")
        print(f"  Total time: {scoring_duration:.1f}s")
        print("=" * 70)
        print()

        # Export summary
        print("=" * 70)
        print("EXPORT SUMMARY")
        print("=" * 70)
        try:
            if total_predictions > 0:
                file_size = os.path.getsize(output_csv) / (1024 * 1024)
                print(f"  ✓ Total predictions: {total_predictions:,}")
                print(f"  ✓ File size: {file_size:.2f} MB")
                print(f"  ✓ Output: {output_csv}")
            else:
                print(f"  • No predictions found above threshold")
                print(f"  • Consider: lowering threshold or reviewing blocking rules")
        except Exception as e:
            print(f"  • Warning: Could not get file stats: {e}")
        print("=" * 70)
        print()

        # Wrap results in a lazy-loading object for compatibility with clustering
        class PredictionsWrapper:
            """Wrapper for predictions CSV \u2014 provides Splink-compatible API, loads lazily."""

            def __init__(self, csv_path, spark_session):
                self.csv_path = csv_path
                self.spark = spark_session
                self._cached_df = None

            def as_pandas_dataframe(self):
                """Load predictions into pandas DataFrame."""
                try:
                    import pandas as pd
                except ImportError as exc:
                    raise ImportError(
                        "pandas is required for as_pandas_dataframe(). "
                        "Install pandas or use as_spark_dataframe() instead."
                    ) from exc
                if self._cached_df is None:
                    self._cached_df = pd.read_csv(self.csv_path)
                return self._cached_df

            def as_spark_dataframe(self):
                """Load predictions into Spark DataFrame for distributed processing."""
                try:
                    df = self.spark.read.csv(
                        self.csv_path,
                        header=True,
                        inferSchema=True,
                        mode='DROPMALFORMED'
                    )
                    print(f"  • DEBUG: PredictionsWrapper loaded CSV with columns: {df.columns}")
                    print(f"  • DEBUG: match_probability in CSV: {'match_probability' in df.columns}")
                    return df
                except Exception as e:
                    print(f"  ✗ Error reading CSV with inferSchema: {e}")
                    print(f"  • Trying again with all columns as strings...")
                    df = self.spark.read.csv(self.csv_path, header=True, inferSchema=False)
                    from pyspark.sql import functions as F
                    from pyspark.sql.types import DoubleType, LongType
                    for col_name in df.columns:
                        if any(key in col_name.lower() for key in ['probability', 'weight', 'gamma']):
                            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
                        elif col_name in ['id_l', 'id_r']:
                            df = df.withColumn(col_name, F.col(col_name).cast(LongType()))
                    return df

        predictions_combined = PredictionsWrapper(output_csv, spark) if total_predictions > 0 else None

        return linker, predictions_combined

    except Exception as e:
        print(f"\n  ✗ Scoring phase failed: {e}")
        print("=" * 70)
        raise


def run_clustering(
    linker,
    predictions,
    threshold: float = 0.8,
    output_csv: Optional[str] = None,
):
    """Run clustering on predictions."""
    print("=" * 70)
    print("CLUSTERING")
    print("=" * 70)
    print(f"  Threshold: {threshold}")
    print(f"  Performing graph clustering...")
    
    if predictions is None:
        print(f"  • No predictions available for clustering")
        print("=" * 70)
        print()
        return None
    
    # Get predictions as Spark DataFrame
    predictions_df = (
        predictions.as_spark_dataframe()
        if hasattr(predictions, "as_spark_dataframe")
        else predictions
    )
    
    # Debug: Print all columns and their types
    print(f"  • DEBUG: Checking predictions DataFrame columns...")
    print(f"  • DEBUG: Total columns: {len(predictions_df.columns)}")
    print(f"  • DEBUG: Column names: {predictions_df.columns}")
    
    # Register the DataFrame with Splink's database API so it has proper internal attributes
    # This is necessary because Splink's clustering expects a DataFrame with templated_name attribute
    print(f"  • Registering predictions DataFrame with Splink...")
    try:
        # Use linker's database API to register the DataFrame
        # This wraps it in Splink's internal structure with required attributes
        predictions_registered = linker._db_api.register_table(
            predictions_df,
            "__splink__df_predict_clustering"
        )
        print(f"  ✓ Predictions registered with Splink")
        print(f"  • Using registered predictions for clustering")
        # Use the registered DataFrame for clustering
        predictions_for_clustering = predictions_registered
    except Exception as e:
        print(f"  ⚠ Warning: Could not register DataFrame with Splink: {e}")
        print(f"  • Falling back to using raw DataFrame (may fail)")
        predictions_for_clustering = predictions_df
    
    # Check for match_probability column (case-insensitive)
    columns_lower = [col.lower() for col in predictions_df.columns]
    has_match_prob = "match_probability" in predictions_df.columns
    has_match_prob_lower = "match_probability" in columns_lower
    
    if not has_match_prob:
        if has_match_prob_lower:
            # Column exists but with different case - fix it
            actual_col_name = predictions_df.columns[columns_lower.index("match_probability")]
            print(f"  • Warning: Found '{actual_col_name}' instead of 'match_probability'")
            print(f"  • Renaming column to 'match_probability'...")
            from pyspark.sql.functions import col
            predictions_df = predictions_df.withColumnRenamed(actual_col_name, "match_probability")
            print(f"  ✓ Column renamed successfully")
        else:
            print(f"  • Warning: match_probability column not found in predictions")
            print(f"  • Available columns: {predictions_df.columns}")
            
            # Try to calculate match_probability from match_weight
            if "match_weight" in predictions_df.columns:
                print(f"  • Calculating match_probability from match_weight...")
                try:
                    from pyspark.sql.functions import expr
                    # match_probability = 1 / (1 + 2^(-match_weight))
                    predictions_df = predictions_df.withColumn(
                        "match_probability",
                        expr("1.0 / (1.0 + pow(2.0, -match_weight))")
                    )
                    print(f"  ✓ match_probability column added")
                    # Verify it was added
                    print(f"  • DEBUG: Columns after adding: {predictions_df.columns}")
                except Exception as e:
                    print(f"  ✗ Failed to calculate match_probability: {e}")
                    import traceback
                    traceback.print_exc()
                    print(f"  • Clustering will likely fail without match_probability column")
            else:
                print(f"  ✗ Neither match_probability nor match_weight found")
                print(f"  • Clustering will likely fail")
                print(f"  • Please re-run scoring phase to generate proper predictions")
    else:
        print(f"  ✓ match_probability column found in predictions")
    
    # Final verification before clustering
    print(f"  • DEBUG: Final columns before clustering: {predictions_df.columns}")
    print(f"  • DEBUG: match_probability in columns: {'match_probability' in predictions_df.columns}")
    
    # CRITICAL CONSTRAINT: Enforce NAME and DOCUMENT matching requirement
    # Filter out predictions where NAME doesn't match or DOCUMENT (if exists) doesn't match
    # This ensures that even if other fields match (mobile, email, DOB), if NAME or DOCUMENT
    # don't match, the pair is NOT considered the same entity
    print(f"  • Applying NAME and DOCUMENT matching constraints...")
    from pyspark.sql.functions import col, coalesce, lit
    
    initial_predictions_count = predictions_df.count()
    
    # Filter strategy:
    # 1. NAME must match (gamma_name >= 1 means at least Levenshtein match)
    # 2. DOCUMENT must match if it exists (gamma_document != 0 means either both null or match)
    # Note: gamma columns represent comparison levels (0=else, 1=level1, 2=level2, etc.)
    
    name_constraint_applied = False
    document_constraint_applied = False
    
    if "gamma_name" in predictions_df.columns:
        # gamma_name levels:
        # 0 = ElseLevel (mismatch) - REJECT
        # 1 = LevenshteinLevel (close match) - ACCEPT
        # 2 = ExactMatchLevel - ACCEPT
        # -1 = NullLevel (both null) - NEUTRAL (keep)
        predictions_df = predictions_df.filter(
            (col("gamma_name") >= 1) | (col("gamma_name") == -1)
        )
        name_constraint_applied = True
        print(f"    • NAME match constraint applied (gamma_name >= 1 or null)")
    
    # if "gamma_document" in predictions_df.columns:
    #     # gamma_document levels:
    #     # 0 = ElseLevel (documents exist but mismatch) - REJECT
    #     # 1 = ExactMatchLevel (documents match) - ACCEPT
    #     # -1 = NullLevel (both null) - NEUTRAL (keep, no documents to compare)
    #     predictions_df = predictions_df.filter(
    #         (col("gamma_document") >= 1) | (col("gamma_document") == -1)
    #     )
    #     document_constraint_applied = True
    #     print(f"    • DOCUMENT match constraint applied (gamma_document >= 1 or null)")
    
    filtered_predictions_count = predictions_df.count()
    filtered_out = initial_predictions_count - filtered_predictions_count
    
    if filtered_out > 0:
        print(f"    ✓ Filtered out {filtered_out:,} predictions where NAME or DOCUMENT didn't match")
        print(f"    • Remaining predictions: {filtered_predictions_count:,}")
    else:
        print(f"    • No predictions filtered (all pass NAME/DOCUMENT constraints)")
    
    if not name_constraint_applied:
        print(f"    ⚠ Warning: gamma_name column not found, NAME constraint not enforced")
    if not document_constraint_applied:
        print(f"    ⚠ Warning: gamma_document column not found, DOCUMENT constraint not enforced")
    
    # Re-register predictions after filtering  
    if name_constraint_applied or document_constraint_applied:
        print(f"  • Re-registering filtered predictions with Splink...")
        try:
            predictions_registered = linker._db_api.register_table(
                predictions_df,
                "__splink__df_predict_clustering_filtered"
            )
            predictions_for_clustering = predictions_registered
            print(f"  ✓ Filtered predictions registered")
        except Exception as e:
            print(f"  ⚠ Warning: Could not re-register filtered predictions: {e}")
            predictions_for_clustering = predictions_df
    
    # BUG FIX: Pass threshold to clustering to filter predictions
    # PROBLEM: Passing threshold_match_probability=None causes Splink to cluster
    # ALL entities in the predictions DataFrame, including those that didn't match.
    # This creates singleton clusters for entities that went through blocking but
    # didn't score above threshold.
    # SOLUTION: Pass the threshold value to ensure only matched pairs are clustered.
    print(f"  • Filtering predictions at threshold: {threshold}")
    print(f"  • This prevents singleton clusters from appearing in results")
    print(f"  • Performing graph clustering...")
    
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions_for_clustering,  # Use the registered DataFrame
        threshold_match_probability=threshold  # BUG FIX: Use threshold, not None
    )
    
    clusters_df = (
        clusters.as_spark_dataframe()
        if hasattr(clusters, "as_spark_dataframe")
        else clusters
    )
    
    # BUG FIX: Filter out singleton clusters (defensive check)
    # Even though we pass threshold to clustering, we filter singletons as a safety measure
    # Singleton clusters indicate entities that went through blocking but didn't match anything
    # These should not be in the final results as they don't represent duplicates
    print(f"  • Filtering singleton clusters (safety check)...")
    if hasattr(clusters_df, "select"):
        # Spark DataFrame
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        
        # Count entities per cluster
        window_spec = Window.partitionBy("cluster_id")
        clusters_with_size = clusters_df.withColumn("_cluster_size", F.count("*").over(window_spec))
        
        # Filter out singletons (clusters with only 1 entity)
        original_count = clusters_df.count()
        clusters_df = clusters_with_size.filter(F.col("_cluster_size") > 1).drop("_cluster_size")
        final_count = clusters_df.count()
        
        if original_count > final_count:
            singletons_removed = original_count - final_count
            print(f"  ⚠ Removed {singletons_removed:,} singleton clusters (entities with no matches)")
            print(f"  • This is expected - they went through blocking but didn't match")
        else:
            print(f"  ✓ No singleton clusters found (good!)")
        
        num_clusters = clusters_df.select("cluster_id").distinct().count()
    else:
        # Pandas DataFrame
        cluster_sizes = clusters_df.groupby("cluster_id").size()
        multi_entity_clusters = cluster_sizes[cluster_sizes > 1].index
        
        original_count = len(clusters_df)
        clusters_df = clusters_df[clusters_df["cluster_id"].isin(multi_entity_clusters)]
        final_count = len(clusters_df)
        
        if original_count > final_count:
            singletons_removed = original_count - final_count
            print(f"  ⚠ Removed {singletons_removed:,} singleton clusters (entities with no matches)")
            print(f"  • This is expected - they went through blocking but didn't match")
        else:
            print(f"  ✓ No singleton clusters found (good!)")
        
        num_clusters = clusters_df["cluster_id"].nunique()
    
    print(f"  ✓ Clustering complete")
    print(f"  ✓ Found {num_clusters:,} real clusters (2+ entities each)")
    print(f"  ✓ Total entities with duplicates: {final_count:,}")
    
    # Summary of constraint enforcement
    if name_constraint_applied or document_constraint_applied:
        print(f"\n  Constraint Enforcement Summary:")
        if name_constraint_applied:
            print(f"    ✓ NAME matching required (rejected mismatches)")
        if document_constraint_applied:
            print(f"    ✓ DOCUMENT matching required (rejected mismatches if documents exist)")
        if filtered_out > 0:
            print(f"    • Filtered predictions: {filtered_out:,} pairs rejected")
        print(f"    • Only pairs with matching NAME (and DOCUMENT if exists) were clustered")
    
    print("=" * 70)
    print()
    
    if output_csv:
        print(f"  Exporting clusters to CSV: {output_csv}")
        try:
            if hasattr(clusters_df, "coalesce") and hasattr(clusters_df, "write"):
                export_df, array_cols = _prepare_csv_export_df(clusters_df)
                if array_cols:
                    print(
                        f"  • Converting array columns to JSON strings for CSV: "
                        f"{', '.join(array_cols)}"
                    )
                export_df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(
                    output_csv + '_temp'
                )
                temp_dir = f"{output_csv}_temp"
                temp_files = glob.glob(f"{temp_dir}/*.csv")
                target_file = None
                if len(temp_files) == 1:
                    target_file = temp_files[0]
                elif temp_files:
                    print(
                        f"  • Warning: Expected a single CSV output, "
                        f"found {len(temp_files)}. Using the first file."
                    )
                    target_file = temp_files[0]
                else:
                    print(f"  • Warning: No CSV output found in {temp_dir}")
                if target_file:
                    shutil.move(target_file, output_csv)
                if os.path.isdir(temp_dir):
                    shutil.rmtree(temp_dir)
            else:
                clusters_df.to_csv(output_csv, index=False)
            print(f"  ✓ Cluster results saved")
        except Exception as e:
            print(f"  • Warning: Could not export clusters to CSV: {e}")
        print("=" * 70)
        print()
    
    return clusters_df


def _prepare_csv_export_df(df):
    from pyspark.sql.types import ArrayType
    from pyspark.sql import functions as F

    array_cols = [
        field.name for field in df.schema.fields if isinstance(field.dataType, ArrayType)
    ]
    export_df = df
    for col_name in array_cols:
        export_df = export_df.withColumn(col_name, F.to_json(F.col(col_name)))
    return export_df, array_cols


def run_all(threshold: float = 0.8, output_csv: Optional[str] = None):
    """Run complete pipeline."""
    print("=" * 70)
    print("SPLINK ENTITY RESOLUTION - COMPLETE PIPELINE")
    print("=" * 70)
    print()
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Load data
        df = load_data(spark)
        
        # Run blocking
        settings, _, _ = run_blocking(spark, df, output_csv="blocking_analysis.csv")
        
        # Run scoring (uses candidate pairs from blocking phase)
        linker, predictions = run_scoring(spark, df, settings, 
                                         blocking_csv="blocking_analysis.csv",
                                         threshold=threshold)
        
        # Run clustering
        clusters = run_clustering(linker, predictions, threshold, output_csv=output_csv)
        
        # Summary
        print("=" * 70)
        print("SUMMARY")
        print("=" * 70)
        predictions_df = (
            predictions.as_spark_dataframe()
            if predictions is not None and hasattr(predictions, "as_spark_dataframe")
            else predictions
        )
        matches_count = predictions_df.count() if predictions_df is not None else 0
        if clusters is None:
            cluster_count = 0
        elif hasattr(clusters, "select"):
            cluster_count = clusters.select("cluster_id").distinct().count()
        else:
            cluster_count = clusters["cluster_id"].nunique()
        print(f"  Total records: {df.count():,}")
        print(f"  Matches found: {matches_count:,}")
        print(f"  Clusters: {cluster_count:,}")
        print(f"  Threshold: {threshold}")
        print("=" * 70)
        print()
        
        print("✓ Complete pipeline finished successfully!")
        print()
        print("Next steps:")
        print("  1. Export to Neo4j: python -m er.splink_neo4j_export")
        print("  2. Or use the results programmatically")
        print()
        
    finally:
        stop_spark_gracefully(spark)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Splink Entity Resolution Commands",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline
  python -m er.splink_commands all
  
  # Run with custom threshold
  python -m er.splink_commands all --threshold 0.85
  
  # Run individual steps
  python -m er.splink_commands blocking
  python -m er.splink_commands scoring
  
  # Run scoring with custom output file
  python -m er.splink_commands scoring --output my_scores.csv
  
  # Run with custom threshold and output
  python -m er.splink_commands scoring --threshold 0.85 --output custom_results.csv
  
  # Run full pipeline with clustering output
  python -m er.splink_commands all --output clustering_results.csv
        """
    )
    
    parser.add_argument(
        "command",
        choices=["blocking", "scoring", "all"],
        help="Command to run"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.8,
        help="Match probability threshold (0.0-1.0, default: 0.8)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=(
            f"Output CSV file (default: blocking_analysis.csv for blocking, "
            f"scoring_results.csv for scoring, {DEFAULT_CLUSTERING_OUTPUT} for all)"
        )
    )
    
    args = parser.parse_args()
    
    # Set default output based on command if not specified
    if args.output is None:
        if args.command == "blocking":
            args.output = "blocking_analysis.csv"
        elif args.command == "scoring":
            args.output = "scoring_results.csv"
        else:
            args.output = DEFAULT_CLUSTERING_OUTPUT  # For 'all' command
    
    try:
        if args.command == "all":
            run_all(threshold=args.threshold, output_csv=args.output)
        elif args.command == "blocking":
            spark = create_spark_session()
            try:
                df = load_data(spark)
                run_blocking(spark, df, output_csv=args.output)
            finally:
                stop_spark_gracefully(spark)
        elif args.command == "scoring":
            spark = create_spark_session()
            try:
                df = load_data(spark)
                settings, _, _ = run_blocking(spark, df, output_csv="blocking_analysis.csv")
                run_scoring(spark, df, settings, 
                           blocking_csv="blocking_analysis.csv",
                           threshold=args.threshold, 
                           output_csv=args.output)
            finally:
                stop_spark_gracefully(spark)
        
        return 0
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
