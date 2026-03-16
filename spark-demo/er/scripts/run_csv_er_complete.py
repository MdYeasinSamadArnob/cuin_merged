"""
Complete Entity Resolution Pipeline for CSV Data

This script implements the complete entity resolution pipeline for CSV data:
1. Load Data from CSV
2. Normalization
3. Blocking (Multi-pass: LSH + Soundex + Rules)
4. Similarity Computation
5. XGBoost Classification
6. Threshold τ (tau) Optimization
7. Neo4j Insertion (Customer nodes + Entity resolution graph)

Usage:
    python -m er.scripts.run_csv_er_complete

Configuration:
    CSV file path and Neo4j settings are loaded from .env file
"""

import csv
import os
import json
import sys
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Tuple
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()

# Import entity resolution components
from er.blocking.multi_pass_blocking import MultiPassBlocker
from er.ml.xgboost_classifier import XGBoostEntityClassifier
from er.utils.normalize import preprocess_record
from er.utils.entity_resolution import compute_similarity

# -----------------------------
# NEO4J CONNECTION
# -----------------------------
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password123")

# -----------------------------
# CSV CONFIGURATION
# -----------------------------
CSV_FILE = os.environ.get("CSV_FILE", "csv/challenging_er_200.csv")

# -----------------------------
# PROCESSING CONFIG
# -----------------------------
SIMILARITY_THRESHOLD = float(os.environ.get("SIMILARITY_THRESHOLD", "0.75"))
USE_XGBOOST = os.environ.get("USE_XGBOOST", "true").lower() == "true"
XGBOOST_TRAIN_SIZE = float(os.environ.get("XGBOOST_TRAIN_SIZE", "0.8"))


def clean_value(val):
    """Clean CSV value."""
    if val is None or val == '':
        return None
    if isinstance(val, str):
        return val.strip()
    return val


def load_from_csv() -> List[Dict[str, Any]]:
    """
    Load customer data from CSV file.
    
    Returns:
        List of customer records in normalized format
    """
    print("=" * 70)
    print("LOADING DATA FROM CSV")
    print("=" * 70)
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ Error: CSV file not found at {CSV_FILE}")
        print("\nPlease ensure the file exists at the specified path.")
        sys.exit(1)
    
    print(f"Loading CSV from {CSV_FILE}...")
    
    records = []
    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Clean values
            cleaned = {k: clean_value(v) for k, v in row.items()}
            
            # Build full name
            first = cleaned.get('CUSNMF', '')
            last = cleaned.get('CUSNML', '')
            
            # Create record in standard format
            record = {
                'CUSCOD': cleaned.get('CUSCOD'),
                'CUSTYP': cleaned.get('CUSTYP'),
                'CUSNMF': first,
                'CUSNML': last,
                'CUSDOB': cleaned.get('CUSDOB'),
                'CITYNM': cleaned.get('CITYNM'),
                'TELENO': cleaned.get('TELENO'),
                'MOBLNO': cleaned.get('MOBLNO'),
                'TELXNO': cleaned.get('TELXNO'),
                'FAXNO': cleaned.get('FAXNO'),
                'MAILID': cleaned.get('MAILID'),
                'SPONAM': cleaned.get('SPONAM'),
                'GENDER': cleaned.get('GENDER'),
                'CUSSTS': cleaned.get('CUSSTS'),
                'NATLID': cleaned.get('NATLID'),
                'TIMSTAMP': cleaned.get('TIMSTAMP'),
                'OPRBRA': cleaned.get('OPRBRA'),
                'ADDRS1': cleaned.get('ADDRS1'),
                'ADDRS2': cleaned.get('ADDRS2'),
                'ADDRS3': cleaned.get('ADDRS3'),
                'ADDRS4': cleaned.get('ADDRS4'),
            }
            
            records.append(record)
    
    print(f"✅ Successfully loaded {len(records):,} records from CSV")
    print()
    return records


def apply_blocking(records: List[Dict]) -> Tuple[List[Tuple[int, int]], Dict]:
    """
    Apply multi-pass blocking to generate candidate pairs.
    
    Args:
        records: Normalized records
        
    Returns:
        Tuple of (candidate_pairs, statistics)
    """
    print("=" * 70)
    print("BLOCKING - Generating Candidate Pairs")
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
    print("SIMILARITY COMPUTATION")
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
    print("XGBOOST CLASSIFICATION")
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
    print("FINDING OPTIMAL THRESHOLD TAU")
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
        records: Original records from CSV
        predictions: Filtered predictions above threshold
        clusters: Entity clusters
        threshold_info: Threshold optimization information
        stats: Processing statistics
    """
    print("=" * 70)
    print("NEO4J INSERTION")
    print("=" * 70)
    
    try:
        print(f"Connecting to Neo4j at {NEO4J_URI}...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Test connection
        with driver.session() as session:
            session.run("RETURN 1")
        print("✅ Connected to Neo4j")
        print()
        
        # Create Customer nodes
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
        
        # Create Entity resolution graph
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
        print("✅ Neo4j insertion complete")
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
    """Main entry point for CSV Complete ER Pipeline."""
    
    print()
    print("=" * 70)
    print(" " * 10 + "COMPLETE ENTITY RESOLUTION PIPELINE - CSV")
    print("=" * 70)
    print()
    print("Pipeline Steps:")
    print("  1. Load Data from CSV")
    print("  2. Normalize Records")
    print("  3. Blocking (Multi-pass)")
    print("  4. Similarity Computation")
    print("  5. XGBoost Classification")
    print("  6. Threshold τ (tau) Optimization")
    print("  7. Neo4j Insertion")
    print()
    print("=" * 70)
    print()
    
    # Step 1: Load from CSV
    print("=" * 70)
    print("STEP 1: LOAD DATA FROM CSV")
    print("=" * 70)
    records = load_from_csv()
    
    # Step 2: Normalize records
    print("=" * 70)
    print("STEP 2: NORMALIZE RECORDS")
    print("=" * 70)
    print("Normalizing records...")
    normalized_records = [preprocess_record(r) for r in records]
    print(f"✅ {len(normalized_records):,} records normalized")
    print()
    
    # Step 3: Blocking
    print("=" * 70)
    print("STEP 3: BLOCKING")
    print("=" * 70)
    candidate_pairs, blocking_stats = apply_blocking(normalized_records)
    
    # Step 4-5: XGBoost Classification
    if USE_XGBOOST:
        print("=" * 70)
        print("STEP 4-5: XGBOOST CLASSIFICATION")
        print("=" * 70)
        classifier, predictions, xgb_stats = train_xgboost_classifier(candidate_pairs, normalized_records)
        
        # Step 6: Find optimal threshold on XGBoost probabilities
        print("=" * 70)
        print("STEP 6: THRESHOLD τ (TAU) OPTIMIZATION")
        print("=" * 70)
        optimal_threshold, threshold_info = find_optimal_threshold_xgboost(predictions)
        
        # Filter by optimal threshold
        filtered_predictions = filter_predictions_by_threshold(predictions, optimal_threshold)
        
        # Build clusters
        clusters = build_clusters_from_predictions(filtered_predictions)
        
        print(f"After threshold filtering:")
        print(f"  • Predictions: {len(filtered_predictions):,}")
        print(f"  • Clusters: {len(clusters):,}")
        print()
        
        # Step 7: Store in Neo4j
        print("=" * 70)
        print("STEP 7: NEO4J INSERTION")
        print("=" * 70)
        store_results_in_neo4j(records, filtered_predictions, clusters, 
                              threshold_info, {**blocking_stats, **xgb_stats})
        
        # Export results
        print("=" * 70)
        print("EXPORTING RESULTS")
        print("=" * 70)
        
        output_file = "csv_xgboost_er_results.json"
        export_data = {
            'config': {
                'csv_file': CSV_FILE,
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
        
        output_file = "csv_similarity_er_results.json"
        export_data = {
            'config': {
                'csv_file': CSV_FILE,
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
    print(f"✅ Loaded {len(records):,} records from CSV")
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
