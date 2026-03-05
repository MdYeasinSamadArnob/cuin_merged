"""
Oracle to Complete Entity Resolution Pipeline

This module loads customer data from Oracle database and processes it through
the complete entity resolution pipeline:
1. Blocking (Multi-pass: LSH + Soundex + Rules)
2. Similarity computation
3. XGBoost classification
4. Threshold τ (tau) optimization
5. Neo4j storage

Flow:
Oracle DB → Blocking → Similarity → XGBoost → Threshold τ → Neo4j
"""

import oracledb
from neo4j import GraphDatabase
from datetime import datetime
import os
import json
import sys
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv

# Load environment variables - REQUIRED
load_dotenv()

# -----------------------------
# CHECK FOR COMMON TYPOS
# -----------------------------
def check_for_common_typos():
    """Check for common typos in environment variable names."""
    # Check for RACLE_DSN instead of ORACLE_DSN
    if "RACLE_DSN" in os.environ:
        print("\n" + "=" * 70)
        print("❌ CONFIGURATION ERROR: Typo detected in .env file")
        print("=" * 70)
        print("\n⚠️  Found 'RACLE_DSN' but it should be 'ORACLE_DSN'")
        print("   (Note: starts with 'O', not 'R')")
        print("\nPlease fix the typo in your .env file:")
        print(f"   Current: RACLE_DSN={os.environ.get('RACLE_DSN')}")
        print(f"   Correct: ORACLE_DSN={os.environ.get('RACLE_DSN')}")
        print("\nThen run the command again.")
        print("=" * 70)
        sys.exit(1)

# Run typo check first
check_for_common_typos()

# Import entity resolution components
from engine.spark_er.blocking.multi_pass_blocking import MultiPassBlocker
from engine.spark_er.ml.xgboost_classifier import XGBoostEntityClassifier
from engine.spark_er.utils.normalize import preprocess_record
from engine.spark_er.utils.entity_resolution import compute_similarity

# -----------------------------
# ORACLE CONNECTION
# -----------------------------
ORACLE_DSN = os.environ.get("ORACLE_DSN", "your_host:1527/your_database")
ORACLE_USER = os.environ.get("ORACLE_USER", "your_oracle_user")
ORACLE_PASSWORD = os.environ.get("ORACLE_PASSWORD", "your_oracle_password")
ORACLE_TABLE = os.environ.get("ORACLE_TABLE", "your_table_name")

# -----------------------------
# NEO4J CONNECTION
# -----------------------------
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password123")

# -----------------------------
# PROCESSING CONFIG
# -----------------------------
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))
RECORD_LIMIT = int(os.environ.get("RECORD_LIMIT", "20000"))  # Default: 20k records
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.75"))

# XGBoost Configuration
USE_XGBOOST = os.environ.get("USE_XGBOOST", "true").lower() == "true"
XGBOOST_TRAIN_SIZE = float(os.environ.get("XGBOOST_TRAIN_SIZE", "0.8"))  # 80% train, 20% test

# -----------------------------
# SELECTED FIELDS
# -----------------------------
FIELDS = [
    "CUSCOD", "CUSTYP", "CUSNMF", "CUSNML", "CUSDOB",
    "ADDRS1", "ADDRS2", "ADDRS3", "ADDRS4",
    "CITYNM", "TELENO", "MOBLNO", "TELXNO", "FAXNO", "MAILID",
    "SPONAM", "GENDER", "CUSSTS", "NATLID",
    "TIMSTAMP", "OPRBRA"
]


def validate_table_name(table_name: str) -> bool:
    """Validate Oracle table name to prevent SQL injection."""
    import re
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Table name must be a non-empty string")
    
    if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
        raise ValueError(
            f"Invalid table name '{table_name}'. "
            "Table name must contain only letters, numbers, and underscores."
        )
    return True


def clean_value(val: Any, col: str = None) -> Any:
    """Clean and normalize database values."""
    if val is None:
        return None
    
    if isinstance(val, str):
        val = val.replace('"', '').strip()
    
    # Convert datetime fields to ISO format
    if col in ("CUSDOB", "TIMSTAMP"):
        try:
            if isinstance(val, str):
                val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S").isoformat()
            elif isinstance(val, datetime):
                val = val.isoformat()
        except Exception:
            pass
    
    return val


def concatenate_address(row: Dict[str, Any]) -> str:
    """Concatenate address fields into a single address string."""
    parts = [clean_value(row.get(f"ADDRS{i}")) for i in range(1, 5)]
    parts = [p for p in parts if p]
    return ", ".join(parts) if parts else None


def load_from_oracle() -> List[Dict[str, Any]]:
    """
    Load customer data from Oracle database.
    
    Returns:
        List of customer records in format compatible with Flink-ER
    """
    print("=" * 70)
    print("STEP 1: LOADING DATA FROM ORACLE")
    print("=" * 70)
    
    # Validate configuration
    try:
        validate_table_name(ORACLE_TABLE)
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nPlease check your .env file and ensure ORACLE_TABLE is set correctly.")
        sys.exit(1)
    
    # Check for placeholder values
    if ORACLE_USER == "your_oracle_user" or ORACLE_DSN == "your_host:1527/your_database":
        print("❌ Configuration error: Please update your .env file with actual Oracle credentials.")
        print("\nSteps to configure:")
        print("1. Copy .env.example to .env: cp .env.example .env")
        print("2. Edit .env and update Oracle credentials")
        print("3. Run this command again")
        sys.exit(1)
    
    try:
        print(f"Connecting to Oracle at {ORACLE_DSN}...")
        print(f"Loading up to {RECORD_LIMIT:,} records from {ORACLE_TABLE}...")
        
        oracle_conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN
        )
        cursor = oracle_conn.cursor()
        cursor.arraysize = BATCH_SIZE
        
        # Build SQL query with proper subquery for ordered results
        fields_str = ', '.join(FIELDS)
        sql = f"""
            SELECT * FROM (
                SELECT {fields_str}
                FROM {ORACLE_TABLE}
                ORDER BY TIMSTAMP DESC
            )
            WHERE ROWNUM <= {RECORD_LIMIT}
        """
        
        print("Executing query...")
        cursor.execute(sql)
        columns = [c[0] for c in cursor.description]
        
        records = []
        while True:
            batch = cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break
            
            for row in batch:
                raw = {col: clean_value(val, col) for col, val in zip(columns, row)}
                
                # Skip records with invalid primary key
                if not raw.get("CUSCOD"):
                    continue
                
                # Build full name
                first = raw.get("CUSNMF", "")
                last = raw.get("CUSNML", "")
                name = " ".join(p for p in (first, last) if p)
                
                # Create record in Flink-ER format
                record = {
                    'CUSCOD': raw.get("CUSCOD"),
                    'CUSTYP': raw.get("CUSTYP"),
                    'CUSNMF': first,
                    'CUSNML': last,
                    'CUSDOB': raw.get("CUSDOB"),
                    'CITYNM': raw.get("CITYNM"),
                    'TELENO': raw.get("TELENO"),
                    'MOBLNO': raw.get("MOBLNO"),
                    'TELXNO': raw.get("TELXNO"),
                    'FAXNO': raw.get("FAXNO"),
                    'MAILID': raw.get("MAILID"),
                    'SPONAM': raw.get("SPONAM"),
                    'GENDER': raw.get("GENDER"),
                    'CUSSTS': raw.get("CUSSTS"),
                    'NATLID': raw.get("NATLID"),
                    'TIMSTAMP': raw.get("TIMSTAMP"),
                    'OPRBRA': raw.get("OPRBRA"),
                    'ADDRS1': raw.get("ADDRS1"),
                    'ADDRS2': raw.get("ADDRS2"),
                    'ADDRS3': raw.get("ADDRS3"),
                    'ADDRS4': raw.get("ADDRS4"),
                }
                
                records.append(record)
        
        cursor.close()
        oracle_conn.close()
        
        print(f"✅ Successfully loaded {len(records):,} records from Oracle")
        print()
        return records
        
    except oracledb.Error as e:
        print(f"❌ Oracle connection error: {e}")
        print("\nPlease verify:")
        print("1. Oracle database is accessible")
        print("2. Credentials in .env file are correct")
        print("3. Network connectivity to Oracle server")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def apply_blocking(records: List[Dict]) -> Tuple[List[Tuple[int, int]], Dict]:
    """
    Apply multi-pass blocking to generate candidate pairs.
    
    Args:
        records: Normalized records
        
    Returns:
        Tuple of (candidate_pairs, statistics)
    """
    print("=" * 70)
    print("STEP 2: BLOCKING - Generating Candidate Pairs")
    print("=" * 70)
    
    blocker = MultiPassBlocker(
        use_lsh=True,
        use_soundex=True,
        use_rules=True,
        use_geohash=False
    )
    
    print("Creating blocks with multi-pass blocking...")
    print("  • LSH (Locality-Sensitive Hashing)")
    print("  • Soundex (Phonetic matching)")
    print("  • Rule-based blocking")
    print()
    
    blocks = blocker.create_blocks(records)
    candidate_pairs = blocker.get_candidate_pairs(blocks)
    
    total_possible = len(records) * (len(records) - 1) // 2
    reduction = ((total_possible - len(candidate_pairs)) / total_possible * 100) if total_possible > 0 else 0
    
    print(f"✅ Blocking complete")
    print(f"   Total possible comparisons: {total_possible:,}")
    print(f"   Candidate pairs after blocking: {len(candidate_pairs):,}")
    print(f"   Reduction: {reduction:.2f}%")
    print()
    
    blocker.print_statistics()
    
    return candidate_pairs, {
        'total_possible': total_possible,
        'candidate_pairs': len(candidate_pairs),
        'reduction_percentage': reduction
    }


def compute_similarities(candidate_pairs: List[Tuple[int, int]], records: List[Dict]) -> List[Dict]:
    """
    Compute similarity scores for candidate pairs.
    
    Args:
        candidate_pairs: List of (idx1, idx2) tuples
        records: Normalized records
        
    Returns:
        List of match dictionaries with similarity scores
    """
    print("=" * 70)
    print("STEP 3: SIMILARITY COMPUTATION")
    print("=" * 70)
    print(f"Computing similarities for {len(candidate_pairs):,} candidate pairs...")
    print()
    
    matches = []
    for idx1, idx2 in candidate_pairs:
        record1 = records[idx1]
        record2 = records[idx2]
        
        similarity = compute_similarity(record1, record2)
        
        if similarity >= SIMILARITY_THRESHOLD:
            matches.append({
                'id1': idx1,
                'id2': idx2,
                'similarity': similarity,
                'record1': record1,
                'record2': record2
            })
    
    print(f"✅ Found {len(matches):,} matches above threshold {SIMILARITY_THRESHOLD}")
    print()
    
    return matches


def train_xgboost_classifier(candidate_pairs: List[Tuple[int, int]], records: List[Dict]) -> Tuple[XGBoostEntityClassifier, List[Dict], Dict]:
    """
    Train XGBoost classifier on candidate pairs.
    
    Args:
        candidate_pairs: List of candidate pairs
        records: Normalized records
        
    Returns:
        Tuple of (trained_classifier, predictions_with_probabilities, statistics)
    """
    print("=" * 70)
    print("STEP 4: XGBOOST CLASSIFICATION")
    print("=" * 70)
    
    # Initialize classifier
    classifier = XGBoostEntityClassifier(
        similarity_threshold=SIMILARITY_THRESHOLD,
        xgb_params={
            'objective': 'binary:logistic',
            'eval_metric': 'logloss',
            'max_depth': 6,
            'learning_rate': 0.1,
            'n_estimators': 100,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'random_state': 42
        }
    )
    
    print(f"Preparing training data from {len(candidate_pairs):,} candidate pairs...")
    
    # Generate training labels based on similarity threshold
    labels = {}
    for idx1, idx2 in candidate_pairs:
        similarity = compute_similarity(records[idx1], records[idx2])
        labels[(idx1, idx2)] = 1 if similarity >= SIMILARITY_THRESHOLD else 0
    
    # Prepare features
    X, y = classifier.prepare_training_data(records, candidate_pairs, labels)
    
    print(f"  • Features shape: {X.shape}")
    print(f"  • Positive samples: {sum(y)} ({sum(y)/len(y)*100:.1f}%)")
    print(f"  • Negative samples: {len(y)-sum(y)} ({(len(y)-sum(y))/len(y)*100:.1f}%)")
    print()
    
    # Split data
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=1-XGBOOST_TRAIN_SIZE, random_state=42, stratify=y
    )
    
    print(f"Training XGBoost model...")
    print(f"  • Train size: {len(X_train)} samples")
    print(f"  • Test size: {len(X_test)} samples")
    print()
    
    # Train model
    train_results = classifier.train(X_train, y_train, X_test, y_test)
    
    print(f"✅ Model trained successfully")
    print(f"   Train accuracy: {train_results['train_accuracy']:.3f}")
    print(f"   Test accuracy: {train_results['test_accuracy']:.3f}")
    print()
    
    # Get predictions with probabilities for all candidate pairs
    print("Predicting on all candidate pairs...")
    predictions = classifier.predict_pairs(records, candidate_pairs)
    
    # Convert to list of dicts with probabilities
    predictions_with_probs = []
    for idx, (idx1, idx2) in enumerate(candidate_pairs):
        pred_label = predictions[idx]
        # Get probability from model
        features = classifier.extract_features(records[idx1], records[idx2])
        X_single = pd.DataFrame([features])
        prob = classifier.model.predict_proba(X_single)[0][1]  # Probability of duplicate
        
        predictions_with_probs.append({
            'id1': idx1,
            'id2': idx2,
            'prediction': int(pred_label),
            'probability': float(prob),
            'record1': records[idx1],
            'record2': records[idx2]
        })
    
    duplicates_found = sum(p['prediction'] for p in predictions_with_probs)
    print(f"✅ Predictions complete")
    print(f"   Predicted duplicates: {duplicates_found:,}")
    print()
    
    return classifier, predictions_with_probs, {
        'train_accuracy': train_results['train_accuracy'],
        'test_accuracy': train_results['test_accuracy'],
        'total_predictions': len(predictions_with_probs),
        'predicted_duplicates': duplicates_found
    }


def find_optimal_threshold_xgboost(predictions: List[Dict]) -> Tuple[float, Dict]:
    """
    Find optimal threshold tau for XGBoost probability scores.
    
    Args:
        predictions: List of predictions with probabilities
        
    Returns:
        Tuple of (optimal_threshold, statistics)
    """
    print("=" * 70)
    print("STEP 5: FINDING OPTIMAL THRESHOLD TAU")
    print("=" * 70)
    
    if not predictions:
        print("⚠ No predictions found, using default threshold 0.5")
        return 0.5, {}
    
    # Extract probabilities
    probabilities = [p['probability'] for p in predictions]
    
    # Test thresholds from 0.3 to 0.9 in increments of 0.05
    thresholds_to_test = [round(t, 2) for t in np.arange(0.30, 0.91, 0.05)]
    
    best_threshold = 0.5
    best_score = 0
    threshold_stats = []
    
    print(f"Testing {len(thresholds_to_test)} threshold values on XGBoost probabilities...")
    print()
    
    for threshold in thresholds_to_test:
        # Count predictions above threshold
        matches_above = sum(1 for p in probabilities if p >= threshold)
        
        # Score: balance between match count and average probability
        if matches_above > 0:
            avg_prob_above = sum(p for p in probabilities if p >= threshold) / matches_above
            score = matches_above * avg_prob_above
        else:
            score = 0
        
        threshold_stats.append({
            'threshold': threshold,
            'matches_above': matches_above,
            'avg_probability': avg_prob_above if matches_above > 0 else 0,
            'score': score
        })
        
        print(f"  Threshold {threshold:.2f}: {matches_above:4d} matches, avg_prob={avg_prob_above if matches_above > 0 else 0:.3f}, score={score:.2f}")
        
        if score > best_score:
            best_score = score
            best_threshold = threshold
    
    print()
    print(f"✅ Optimal threshold τ (tau) = {best_threshold:.2f}")
    print(f"   Matches at optimal threshold: {sum(1 for p in probabilities if p >= best_threshold)}")
    print(f"   Score: {best_score:.2f}")
    print()
    
    return best_threshold, {
        'optimal_threshold': best_threshold,
        'best_score': best_score,
        'threshold_stats': threshold_stats
    }


def filter_predictions_by_threshold(predictions: List[Dict], threshold: float) -> List[Dict]:
    """Filter predictions to only include those above the threshold."""
    return [p for p in predictions if p['probability'] >= threshold]


def build_clusters_from_predictions(predictions: List[Dict]) -> List[set]:
    """Build clusters from filtered predictions using union-find."""
    # Union-Find to group connected entities
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
    
    # Union all predicted duplicate pairs
    for pred in predictions:
        union(pred['id1'], pred['id2'])
    
    # Group by cluster
    clusters_dict = {}
    for entity_id in parent.keys():
        root = find(entity_id)
        if root not in clusters_dict:
            clusters_dict[root] = set()
        clusters_dict[root].add(entity_id)
    
    return list(clusters_dict.values())


def store_results_in_neo4j(records: List[Dict], predictions: List[Dict], clusters: List[set], 
                           threshold_info: Dict, stats: Dict):
    """
    Store entity resolution results in Neo4j.
    
    Creates:
    1. Customer nodes from original records
    2. EntityCluster nodes for each cluster
    3. Entity nodes for entities in clusters
    4. MEMBER_OF and DUPLICATE_OF relationships
    
    Args:
        records: Original records from Oracle
        predictions: Filtered predictions above threshold
        clusters: Entity clusters
        threshold_info: Threshold optimization information
        stats: Processing statistics
    """
    print("=" * 70)
    print("STEP 6: STORING RESULTS IN NEO4J")
    print("=" * 70)
    
    try:
        print(f"Connecting to Neo4j at {NEO4J_URI}...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Test connection
        with driver.session() as session:
            session.run("RETURN 1")
        print("✅ Connected to Neo4j")
        print()
        
        # 4.1: Create Customer nodes
        print("Creating Customer nodes...")
        with driver.session() as session:
            # Delete existing Customer nodes
            session.run("MATCH (c:Customer) DETACH DELETE c")
            
            # Prepare customer data
            customer_data = []
            for record in records:
                first = record.get('CUSNMF', '')
                last = record.get('CUSNML', '')
                name = f"{first} {last}".strip()
                
                addr_parts = [
                    record.get('ADDRS1'),
                    record.get('ADDRS2'),
                    record.get('ADDRS3'),
                    record.get('ADDRS4')
                ]
                address = ', '.join([p for p in addr_parts if p])
                
                customer_data.append({
                    'accountId': record.get('CUSCOD'),
                    'type': record.get('CUSTYP'),
                    'name': name,
                    'firstName': first,
                    'lastName': last,
                    'dob': record.get('CUSDOB'),
                    'address': address,
                    'city': record.get('CITYNM'),
                    'phone': record.get('TELENO'),
                    'mobile': record.get('MOBLNO'),
                    'telex': record.get('TELXNO'),
                    'fax': record.get('FAXNO'),
                    'email': record.get('MAILID'),
                    'sponsorName': record.get('SPONAM'),
                    'gender': record.get('GENDER'),
                    'status': record.get('CUSSTS'),
                    'nationalId': record.get('NATLID'),
                    'timestamp': record.get('TIMSTAMP'),
                    'branchCode': record.get('OPRBRA')
                })
            
            # Batch insert
            batch_size = 1000
            for i in range(0, len(customer_data), batch_size):
                batch = customer_data[i:i+batch_size]
                session.run("""
                    UNWIND $records AS record
                    CREATE (c:Customer)
                    SET c = record
                """, records=batch)
            
            print(f"✅ Created {len(customer_data):,} Customer nodes")
        
        # 4.2: Create Entity resolution graph
        print()
        print("Creating Entity resolution graph...")
        
        with driver.session() as session:
            # Clear existing entity nodes
            session.run("MATCH (n:EntityCluster) DETACH DELETE n")
            session.run("MATCH (n:Entity) DETACH DELETE n")
            
            # Create clusters
            for cluster_idx, cluster in enumerate(clusters):
                cluster_id = f"cluster_{cluster_idx}"
                member_ids = list(cluster)
                
                # Get sample info from first member
                sample_record = records[member_ids[0]]
                cluster_name = sample_record.get('CUSNMF', 'Unknown')
                
                # Create cluster node
                session.run("""
                    CREATE (c:EntityCluster {
                        clusterId: $cluster_id,
                        name: $name,
                        size: $size,
                        memberIds: $member_ids,
                        threshold: $threshold
                    })
                """, cluster_id=cluster_id, name=cluster_name, size=len(member_ids), 
                     member_ids=member_ids, threshold=threshold_info.get('optimal_threshold', 0.75))
                
                # Create entity nodes
                entity_data = []
                for member_id in member_ids:
                    record = records[member_id]
                    entity_data.append({
                        'entityId': member_id,
                        'accountId': record.get('CUSCOD', ''),
                        'firstName': record.get('CUSNMF', ''),
                        'lastName': record.get('CUSNML', ''),
                        'dob': record.get('CUSDOB', ''),
                        'phone': record.get('TELENO', ''),
                        'email': record.get('MAILID', ''),
                        'city': record.get('CITYNM', ''),
                        'clusterId': cluster_id
                    })
                
                # Batch create entities and MEMBER_OF relationships
                session.run("""
                    UNWIND $entities AS entity
                    CREATE (e:Entity)
                    SET e = entity
                    WITH e, entity.clusterId as clusterId
                    MATCH (c:EntityCluster {clusterId: clusterId})
                    CREATE (e)-[:MEMBER_OF]->(c)
                """, entities=entity_data)
            
            print(f"✅ Created {len(clusters):,} EntityCluster nodes")
            
            # Create DUPLICATE_OF relationships
            prediction_data = []
            for pred in predictions:
                prediction_data.append({
                    'id1': pred['id1'],
                    'id2': pred['id2'],
                    'probability': pred['probability']
                })
            
            # Batch create relationships
            batch_size = 1000
            for i in range(0, len(prediction_data), batch_size):
                batch = prediction_data[i:i+batch_size]
                session.run("""
                    UNWIND $predictions AS pred
                    MATCH (e1:Entity {entityId: pred.id1})
                    MATCH (e2:Entity {entityId: pred.id2})
                    CREATE (e1)-[:DUPLICATE_OF {probability: pred.probability}]->(e2)
                    CREATE (e2)-[:DUPLICATE_OF {probability: pred.probability}]->(e1)
                """, predictions=batch)
            
            print(f"✅ Created {len(prediction_data)*2:,} DUPLICATE_OF relationships")
        
        driver.close()
        print()
        print("✅ Neo4j integration complete")
        print()
        
    except Exception as e:
        print(f"❌ Neo4j error: {e}")
        print("\nPlease verify:")
        print("1. Neo4j is running: docker-compose up -d")
        print("2. Neo4j credentials in .env are correct")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point for Oracle to Complete ER Pipeline."""
    
    print()
    print("=" * 70)
    print(" " * 10 + "ORACLE TO COMPLETE ENTITY RESOLUTION PIPELINE")
    print("=" * 70)
    print()
    print("This process will:")
    print("  1. Load data from Oracle database")
    print("  2. Blocking (Multi-pass: LSH + Soundex + Rules)")
    print("  3. Similarity computation")
    print("  4. XGBoost classification")
    print("  5. Find optimal threshold τ (tau)")
    print("  6. Store results in Neo4j")
    print()
    print("=" * 70)
    print()
    
    # Step 1: Load from Oracle
    records = load_from_oracle()
    
    # Normalize records
    print("Normalizing records...")
    normalized_records = [preprocess_record(r) for r in records]
    print(f"✅ {len(normalized_records):,} records normalized")
    print()
    
    # Step 2: Blocking
    candidate_pairs, blocking_stats = apply_blocking(normalized_records)
    
    # Step 3: Similarity (if not using XGBoost)
    # Step 4: XGBoost Classification
    if USE_XGBOOST:
        classifier, predictions, xgb_stats = train_xgboost_classifier(candidate_pairs, normalized_records)
        
        # Step 5: Find optimal threshold on XGBoost probabilities
        optimal_threshold, threshold_info = find_optimal_threshold_xgboost(predictions)
        
        # Filter by optimal threshold
        filtered_predictions = filter_predictions_by_threshold(predictions, optimal_threshold)
        
        # Build clusters
        clusters = build_clusters_from_predictions(filtered_predictions)
        
        print(f"After threshold filtering:")
        print(f"  • Predictions: {len(filtered_predictions):,}")
        print(f"  • Clusters: {len(clusters):,}")
        print()
        
        # Store in Neo4j
        store_results_in_neo4j(records, filtered_predictions, clusters, 
                              threshold_info, {**blocking_stats, **xgb_stats})
        
        # Export results
        print("=" * 70)
        print("STEP 7: EXPORTING RESULTS")
        print("=" * 70)
        
        output_file = "oracle_xgboost_er_results.json"
        export_data = {
            'config': {
                'oracle_dsn': ORACLE_DSN,
                'oracle_table': ORACLE_TABLE,
                'record_limit': RECORD_LIMIT,
                'base_similarity_threshold': SIMILARITY_THRESHOLD,
                'use_xgboost': USE_XGBOOST,
                'xgboost_train_size': XGBOOST_TRAIN_SIZE,
                'optimal_threshold': optimal_threshold
            },
            'statistics': {
                'total_records_loaded': len(records),
                'blocking': blocking_stats,
                'xgboost': xgb_stats,
                'predictions_before_threshold': len(predictions),
                'predictions_after_threshold': len(filtered_predictions),
                'clusters': len(clusters),
                'threshold_optimization': threshold_info
            },
            'sample_predictions': [
                {
                    'id1': p['id1'],
                    'id2': p['id2'],
                    'probability': p['probability'],
                    'record1_name': records[p['id1']].get('CUSNMF', 'N/A'),
                    'record2_name': records[p['id2']].get('CUSNMF', 'N/A')
                }
                for p in filtered_predictions[:10]
            ]
        }
    else:
        # Fallback to similarity-based matching (no XGBoost)
        matches = compute_similarities(candidate_pairs, normalized_records)
        
        # Simple threshold filtering
        optimal_threshold = SIMILARITY_THRESHOLD
        threshold_info = {'optimal_threshold': optimal_threshold}
        
        filtered_matches = [m for m in matches if m['similarity'] >= optimal_threshold]
        clusters = build_clusters_from_predictions(filtered_matches)
        
        store_results_in_neo4j(records, filtered_matches, clusters, 
                              threshold_info, blocking_stats)
        
        output_file = "oracle_similarity_er_results.json"
        export_data = {
            'config': {
                'oracle_dsn': ORACLE_DSN,
                'oracle_table': ORACLE_TABLE,
                'record_limit': RECORD_LIMIT,
                'similarity_threshold': SIMILARITY_THRESHOLD,
                'use_xgboost': USE_XGBOOST
            },
            'statistics': {
                'total_records_loaded': len(records),
                'blocking': blocking_stats,
                'matches': len(filtered_matches),
                'clusters': len(clusters)
            }
        }
    
    with open(output_file, 'w') as f:
        json.dump(export_data, f, indent=2)
    
    print(f"✅ Results exported to: {output_file}")
    print()
    
    # Final summary
    print("=" * 70)
    print(" " * 25 + "EXECUTION COMPLETE")
    print("=" * 70)
    print()
    print(f"✅ Loaded {len(records):,} records from Oracle")
    print(f"✅ Blocking reduction: {blocking_stats['reduction_percentage']:.2f}%")
    if USE_XGBOOST:
        print(f"✅ XGBoost model trained (accuracy: {xgb_stats['test_accuracy']:.3f})")
        print(f"✅ Found optimal threshold τ = {optimal_threshold:.2f}")
        print(f"✅ Identified {len(filtered_predictions):,} duplicate pairs")
    else:
        print(f"✅ Found {len(filtered_matches):,} matches above threshold")
    print(f"✅ Created {len(clusters):,} entity clusters")
    print(f"✅ Stored results in Neo4j")
    print()
    print(f"Results saved to: {output_file}")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
