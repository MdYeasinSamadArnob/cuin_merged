"""
XGBoost Demo - Simple demonstration with known duplicates

This script creates a simple dataset with known duplicates
and demonstrates XGBoost entity resolution with Neo4j storage.
"""

import json
import os
from neo4j import GraphDatabase
from engine.spark_er.ml.xgboost_classifier import XGBoostEntityClassifier
from sklearn.model_selection import train_test_split
from engine.spark_er.utils.neo4j_helpers import create_entity_resolution_graph

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, will use system environment variables


# Neo4j Configuration
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password123")


def main():
    """Main demo function."""
    print("\n" + "="*60)
    print("XGBoost Entity Resolution - Simple Demo")
    print("="*60 + "\n")
    
    # Create sample records with known duplicates
    records = [
        # Cluster 1: John Smith
        {'CUSNMF': 'John Smith', 'MAILID': 'john@example.com', 'TELENO': '1234567890', 'ADDRS1': '123 Main St', 'CUSDOB': '1990-01-01'},
        {'CUSNMF': 'John Smith', 'MAILID': 'john@example.com', 'TELENO': '123-456-7890', 'ADDRS1': '123 Main Street', 'CUSDOB': '1990-01-01'},
        {'CUSNMF': 'J Smith', 'MAILID': 'john@example.com', 'TELENO': '1234567890', 'ADDRS1': '123 Main St', 'CUSDOB': '1990-01-01'},
        
        # Cluster 2: Jane Doe
        {'CUSNMF': 'Jane Doe', 'MAILID': 'jane@example.com', 'TELENO': '9876543210', 'ADDRS1': '456 Oak Ave', 'CUSDOB': '1985-05-15'},
        {'CUSNMF': 'Jane M Doe', 'MAILID': 'jane@example.com', 'TELENO': '987-654-3210', 'ADDRS1': '456 Oak Avenue', 'CUSDOB': '1985-05-15'},
        
        # Cluster 3: Bob Johnson
        {'CUSNMF': 'Bob Johnson', 'MAILID': 'bob@example.com', 'TELENO': '5551234567', 'ADDRS1': '789 Pine Rd', 'CUSDOB': '1975-12-25'},
        {'CUSNMF': 'Robert Johnson', 'MAILID': 'bob@example.com', 'TELENO': '555-123-4567', 'ADDRS1': '789 Pine Road', 'CUSDOB': '1975-12-25'},
        
        # Non-duplicates
        {'CUSNMF': 'Alice Williams', 'MAILID': 'alice@example.com', 'TELENO': '4445556666', 'ADDRS1': '321 Elm St', 'CUSDOB': '1995-03-10'},
        {'CUSNMF': 'Charlie Brown', 'MAILID': 'charlie@example.com', 'TELENO': '7778889999', 'ADDRS1': '654 Maple Dr', 'CUSDOB': '1980-07-20'},
        {'CUSNMF': 'Diana Prince', 'MAILID': 'diana@example.com', 'TELENO': '1112223333', 'ADDRS1': '987 Cedar Ln', 'CUSDOB': '1992-11-05'},
    ]
    
    # Define candidate pairs and labels
    # Positive pairs (duplicates)
    candidate_pairs = [
        (0, 1), (0, 2), (1, 2),  # John Smith cluster
        (3, 4),                  # Jane Doe cluster
        (5, 6),                  # Bob Johnson cluster
        # Negative pairs (non-duplicates)
        (0, 3), (0, 5), (0, 7), (0, 8), (0, 9),
        (1, 4), (1, 6), (1, 8),
        (2, 7), (2, 9),
        (3, 5), (3, 7), (3, 8),
        (4, 6), (4, 9),
        (5, 7), (5, 8),
        (6, 9),
        (7, 8), (7, 9),
        (8, 9)
    ]
    
    labels = [
        # Positive (duplicates)
        1, 1, 1,  # John Smith
        1,        # Jane Doe  
        1,        # Bob Johnson
        # Negative (non-duplicates)
        0, 0, 0, 0, 0,
        0, 0, 0,
        0, 0,
        0, 0, 0,
        0, 0,
        0, 0,
        0,
        0, 0,
        0
    ]
    
    print(f"Created demo dataset:")
    print(f"  - {len(records)} records")
    print(f"  - {len(candidate_pairs)} candidate pairs")
    print(f"  - {sum(labels)} positive pairs (duplicates)")
    print(f"  - {len(labels) - sum(labels)} negative pairs (non-duplicates)")
    
    # Initialize classifier
    print("\n🤖 Initializing XGBoost classifier...")
    classifier = XGBoostEntityClassifier(similarity_threshold=0.75)
    
    # Prepare training data
    print("\n📝 Preparing training data...")
    X, y = classifier.prepare_training_data(records, candidate_pairs, labels)
    print(f"  ✅ Prepared {len(X)} training samples")
    print(f"     Features: {', '.join(classifier.feature_names)}")
    
    # Split for training and testing
    print("\n✂️  Splitting data (80/20)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"  - Training set: {len(X_train)} samples")
    print(f"  - Test set: {len(X_test)} samples")
    
    # Train the model
    print("\n🚀 Training XGBoost model...")
    results = classifier.train(X_train, y_train, X_test, y_test)
    
    print(f"\n📈 Training Results:")
    print(f"  - Train accuracy: {results['train_accuracy']:.3f}")
    print(f"  - Test accuracy:  {results['test_accuracy']:.3f}")
    
    print(f"\n  Feature Importance:")
    for feat in results['feature_importance']:
        print(f"    - {feat['feature']:20s}: {feat['importance']:.3f}")
    
    print(f"\n  Classification Report:")
    print(results['classification_report'])
    
    # Find optimal threshold
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
            if 'precision' in result:
                print(f"    - Precision:         {result['precision']:.3f}")
                print(f"    - Recall:            {result['recall']:.3f}")
    
    print(f"\n  ✅ Using {classifier.threshold_method.upper()} method for classification")
    print(f"     Optimal threshold: {classifier.optimal_threshold:.3f}")
    
    # Classify and create clusters
    print("\n🔍 Classifying all pairs and creating clusters...")
    cluster_results = classifier.classify_and_cluster(records, candidate_pairs)
    
    print(f"\n📊 Results:")
    print(f"  - Predicted duplicates: {len(cluster_results['matches'])}")
    print(f"  - Clusters found: {len(cluster_results['clusters'])}")
    
    print(f"\n  Clusters:")
    for cluster in cluster_results['clusters']:
        print(f"    - Cluster {cluster['cluster_id']}: {cluster['size']} members")
        for idx in cluster['member_indices']:
            print(f"      {idx}: {records[idx]['CUSNMF']}")
    
    # Save results
    output_file = "xgboost_demo_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            'summary': {
                'total_records': len(records),
                'total_pairs': len(candidate_pairs),
                'predicted_duplicates': len(cluster_results['matches']),
                'clusters': len(cluster_results['clusters']),
                'train_accuracy': results['train_accuracy'],
                'test_accuracy': results['test_accuracy'],
                'optimal_threshold': classifier.optimal_threshold,
                'threshold_method': classifier.threshold_method
            },
            'threshold_optimization': threshold_results,
            'feature_importance': results['feature_importance'],
            'clusters': cluster_results['clusters']
        }, f, indent=2)
    
    print(f"\n💾 Results saved to {output_file}")
    
    # Save model
    model_file = "xgboost_demo_model.json"
    classifier.save_model(model_file)
    
    # Store results in Neo4j
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
        neo4j_stats = create_entity_resolution_graph(driver, cluster_results['matches'], cluster_results['clusters'], records)
        
        # Close connection
        driver.close()
        
        print("  ✓ Neo4j integration complete")
        
    except Exception as e:
        print(f"  ⚠️  Warning: Neo4j integration failed: {str(e)}")
        print(f"  ⚠️  Results are still available in {output_file}")
        print(f"  ⚠️  Make sure Neo4j is running and accessible at {NEO4J_URI}")
    
    print("\n" + "="*60)
    print("Demo completed successfully! ✅")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
