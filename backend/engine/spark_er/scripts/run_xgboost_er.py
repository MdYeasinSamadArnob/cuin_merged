"""
Run XGBoost Entity Resolution

This script demonstrates entity resolution using XGBoost classifier
after similarity-based blocking. It combines traditional similarity
matching with machine learning for improved accuracy.

The workflow:
1. Load data from CSV
2. Apply multi-pass blocking to reduce candidate pairs
3. Extract similarity features from candidate pairs
4. Generate synthetic training labels using similarity threshold
5. Train XGBoost classifier on features
6. Predict duplicates using trained model
7. Export results to JSON
8. Store results in Neo4j (EntityCluster and Entity nodes)
"""

import csv
import json
import os
from typing import List, Dict, Tuple
from neo4j import GraphDatabase

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, will use system environment variables

from engine.spark_er.blocking.multi_pass_blocking import MultiPassBlocker
from engine.spark_er.ml.xgboost_classifier import XGBoostEntityClassifier
from engine.spark_er.utils.entity_resolution import compute_similarity
from engine.spark_er.utils.normalize import preprocess_record
from engine.spark_er.utils.neo4j_helpers import create_entity_resolution_graph


# File paths
CSV_FILE = "csv/challenging_er_200.csv"
OUTPUT_JSON = "xgboost_er_results.json"
MODEL_FILE = "xgboost_entity_model.json"

# Neo4j Configuration
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password123")

# Field weights for similarity calculation
# These match the CSV schema field names (CUSNMF, CUSDOB, etc.)
# Note: Weights don't need to sum to 1.0 as compute_similarity normalizes them
FIELD_WEIGHTS = {
    'CUSNMF': 0.4,   # Customer name (first)
    'CUSDOB': 0.2,   # Date of birth
    'TELENO': 0.15,  # Telephone
    'MOBLNO': 0.15,  # Mobile number
    'ADDRS1': 0.05,  # Address line 1
    'MAILID': 0.05   # Email/Mail ID
}


def load_csv_records(csv_path: str) -> List[dict]:
    """
    Load records from CSV file.
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        List of record dictionaries
    """
    print(f"Loading CSV from {csv_path}...")
    
    records = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Clean and preprocess
            cleaned = {k: v.strip() if v else '' for k, v in row.items()}
            preprocessed = preprocess_record(cleaned)
            records.append(preprocessed)
    
    print(f"  ✅ Loaded {len(records)} records")
    return records


def generate_training_labels(records: List[dict],
                            candidate_pairs: List[Tuple[int, int]],
                            similarity_threshold: float = 0.75) -> List[int]:
    """
    Generate training labels based on similarity threshold.
    
    This creates synthetic labels for training:
    - 1 (duplicate) if similarity >= threshold
    - 0 (non-duplicate) if similarity < threshold
    
    Args:
        records: List of records
        candidate_pairs: List of (idx1, idx2) tuples
        similarity_threshold: Threshold for labeling as duplicate
        
    Returns:
        List of labels (0 or 1)
    """
    labels = []
    for idx1, idx2 in candidate_pairs:
        similarity = compute_similarity(records[idx1], records[idx2], weights=FIELD_WEIGHTS)
        label = 1 if similarity >= similarity_threshold else 0
        labels.append(label)
    
    # If no positive labels found, lower the threshold slightly
    if sum(labels) == 0:
        print("  ⚠️  No duplicates found with threshold {:.2f}, trying lower threshold...".format(similarity_threshold))
        labels = []
        lower_threshold = similarity_threshold - 0.15  # Try 0.60 if original was 0.75
        for idx1, idx2 in candidate_pairs:
            similarity = compute_similarity(records[idx1], records[idx2], weights=FIELD_WEIGHTS)
            label = 1 if similarity >= lower_threshold else 0
            labels.append(label)
        print(f"     Using threshold {lower_threshold:.2f} instead")
    
    return labels


def main():
    """Main execution function."""
    print("\n" + "="*60)
    print("XGBoost Entity Resolution")
    print("="*60 + "\n")
    
    # 1. Load data
    if not os.path.exists(CSV_FILE):
        print(f"❌ Error: CSV file not found: {CSV_FILE}")
        print("Please ensure the file exists and try again.")
        return
    
    records = load_csv_records(CSV_FILE)
    
    # 2. Apply multi-pass blocking
    print("\n📊 Applying multi-pass blocking...")
    blocker = MultiPassBlocker(
        use_lsh=True,
        use_soundex=True,
        use_geohash=False,
        use_rules=True
    )
    
    blocks = blocker.create_blocks(records)
    candidate_pairs = blocker.get_candidate_pairs(blocks)
    
    print(f"  ✅ Reduced to {len(candidate_pairs)} candidate pairs")
    blocker.print_statistics()
    
    # 3. Generate training labels based on similarity
    print("\n🏷️  Generating training labels...")
    labels = generate_training_labels(records, candidate_pairs, similarity_threshold=0.75)
    
    positive_count = sum(labels)
    negative_count = len(labels) - positive_count
    print(f"  ✅ Generated {len(labels)} labels")
    print(f"     - Positive (duplicates): {positive_count}")
    print(f"     - Negative (non-duplicates): {negative_count}")
    
    # 4. Initialize XGBoost classifier
    print("\n🤖 Initializing XGBoost classifier...")
    classifier = XGBoostEntityClassifier(
        similarity_threshold=0.75,
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
    
    # 5. Prepare training data
    print("\n📝 Preparing training data...")
    X, y = classifier.prepare_training_data(records, candidate_pairs, labels)
    print(f"  ✅ Prepared {len(X)} training samples")
    print(f"     Features: {', '.join(classifier.feature_names)}")
    
    # 6. Split data for training and testing (80/20)
    from sklearn.model_selection import train_test_split
    
    # Check if we have positive samples for stratification
    if sum(y) == 0:
        print("  ⚠️  Cannot split with stratification (no positive samples)")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
    else:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
    
    # 7. Train the model
    print("\n🚀 Training XGBoost model...")
    training_results = classifier.train(X_train, y_train, X_test, y_test)
    
    # Check if training succeeded
    if 'error' in training_results:
        print(f"\n❌ Training failed: {training_results['error']}")
        print("\nExiting without creating results...")
        return
    
    print(f"\n📈 Training Results:")
    print(f"  - Train accuracy: {training_results['train_accuracy']:.3f}")
    if 'test_accuracy' in training_results:
        print(f"  - Test accuracy:  {training_results['test_accuracy']:.3f}")
    print(f"\n  Feature Importance:")
    for feat in training_results['feature_importance']:
        print(f"    - {feat['feature']:20s}: {feat['importance']:.3f}")
    
    if 'classification_report' in training_results:
        print(f"\n  Classification Report:")
        print(training_results['classification_report'])
    
    # 7.5. Find optimal threshold on test set
    print("\n🎯 Finding optimal classification threshold...")
    # Use F1 method as default
    optimal_method = 'f1'
    threshold_results = classifier.compare_thresholds(X_test, y_test, use_method=optimal_method)
    
    print("\n  Threshold Optimization Results:")
    for method, result in threshold_results.items():
        if 'error' not in result:
            print(f"\n  {method.upper()} Method:")
            print(f"    - Optimal threshold: {result['optimal_threshold']:.3f}")
            if 'f1_score' in result:
                print(f"    - F1 score:          {result['f1_score']:.3f}")
            if 'youden_j' in result:
                print(f"    - Youden's J:        {result['youden_j']:.3f}")
                print(f"    - ROC AUC:           {result['roc_auc']:.3f}")
            if 'precision' in result:
                print(f"    - Precision:         {result['precision']:.3f}")
                print(f"    - Recall:            {result['recall']:.3f}")
    
    print(f"\n  ✅ Using {optimal_method.upper()} method for classification")
    print(f"     Optimal threshold: {classifier.optimal_threshold:.3f}")
    
    # 8. Classify and create clusters
    print("\n🔍 Classifying entities and creating clusters...")
    results = classifier.classify_and_cluster(records, candidate_pairs)
    
    classifier.print_statistics()
    
    # 9. Save results
    print(f"\n💾 Saving results to {OUTPUT_JSON}...")
    
    # Convert to JSON-serializable format
    output = {
        'summary': {
            'total_records': len(records),
            'total_candidate_pairs': len(candidate_pairs),
            'predicted_duplicates': len(results['matches']),
            'total_clusters': len(results['clusters']),
            'training_accuracy': training_results['train_accuracy'],
            'test_accuracy': training_results.get('test_accuracy', 0.0),
            'optimal_threshold': classifier.optimal_threshold,
            'threshold_method': classifier.threshold_method
        },
        'threshold_optimization': threshold_results,
        'feature_importance': training_results['feature_importance'],
        'matches': results['matches'],
        'clusters': [
            {
                'cluster_id': c['cluster_id'],
                'size': c['size'],
                'member_indices': c['member_indices']
            }
            for c in results['clusters']
        ]
    }
    
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2)
    
    print(f"  ✅ Results saved to {OUTPUT_JSON}")
    
    # 10. Save trained model
    print(f"\n💾 Saving trained model to {MODEL_FILE}...")
    classifier.save_model(MODEL_FILE)
    
    # 11. Print summary
    print("\n" + "="*60)
    print("Summary")
    print("="*60)
    print(f"Total records:           {len(records)}")
    print(f"Candidate pairs:         {len(candidate_pairs)}")
    print(f"Predicted duplicates:    {len(results['matches'])}")
    print(f"Clusters found:          {len(results['clusters'])}")
    print(f"Training accuracy:       {training_results['train_accuracy']:.3f}")
    if 'test_accuracy' in training_results:
        print(f"Test accuracy:           {training_results['test_accuracy']:.3f}")
    print(f"Optimal threshold:       {classifier.optimal_threshold:.3f} ({classifier.threshold_method})")
    print("="*60 + "\n")
    
    # 12. Store results in Neo4j
    print("\n💾 Storing Results in Neo4j")
    print("="*60)
    
    try:
        # Connect to Neo4j
        print(f"Connecting to Neo4j at {NEO4J_URI}...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            result.single()
        print("  ✓ Connected to Neo4j")
        
        # Create entity resolution graph in Neo4j
        neo4j_stats = create_entity_resolution_graph(driver, results['matches'], results['clusters'], records)
        
        # Close connection
        driver.close()
        
        print("  ✓ Neo4j integration complete")
        
    except Exception as e:
        print(f"  ⚠️  Warning: Neo4j integration failed: {str(e)}")
        print(f"  ⚠️  Results are still available in {OUTPUT_JSON}")
        print(f"  ⚠️  Make sure Neo4j is running and accessible at {NEO4J_URI}")
    
    print("\n" + "="*60)
    print("✅ XGBoost entity resolution completed successfully!")
    print(f"   - Results: {OUTPUT_JSON}")
    print(f"   - Model:   {MODEL_FILE}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
