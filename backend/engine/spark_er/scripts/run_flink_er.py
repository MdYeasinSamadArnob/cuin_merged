"""
Run Flink-ER on Challenge ER 200 Dataset

This script demonstrates the Flink-ER distributed entity resolution system
on the challenging_er_200.csv dataset.
"""

import os
import sys
import json
import csv
from engine.spark_er.core.flink_er import FlinkEntityResolver, FlinkERConfig, load_csv_records
from neo4j import GraphDatabase

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


def clean_value(val):
    """Clean CSV value."""
    if val is None or val == '':
        return None
    if isinstance(val, str):
        return val.strip()
    return val


def create_customer_nodes(driver, csv_file):
    """
    Create Customer nodes in Neo4j from CSV file.
    
    This function creates Customer nodes similar to run_challenge_er_200.py
    so that the query MATCH (c:Customer) RETURN c LIMIT 10; works as expected.
    
    Args:
        driver: Neo4j driver instance
        csv_file: Path to CSV file
        
    Returns:
        Number of Customer nodes created
    """
    print("\n" + "="*70)
    print("CREATING CUSTOMER NODES IN NEO4J")
    print("="*70)
    
    print(f"Loading CSV from {csv_file}...")
    
    records = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Clean values
            cleaned = {k: clean_value(v) for k, v in row.items()}
            
            # Build full name
            first = cleaned.get('CUSNMF', '')
            last = cleaned.get('CUSNML', '')
            name = f"{first} {last}".strip()
            
            # Build address
            addr_parts = [
                cleaned.get('ADDRS1'),
                cleaned.get('ADDRS2'),
                cleaned.get('ADDRS3'),
                cleaned.get('ADDRS4')
            ]
            address = ', '.join([p for p in addr_parts if p])
            
            record = {
                'accountId': cleaned.get('CUSCOD'),
                'type': cleaned.get('CUSTYP'),
                'name': name,
                'firstName': first,
                'lastName': last,
                'dob': cleaned.get('CUSDOB'),
                'address': address,
                'city': cleaned.get('CITYNM'),
                'phone': cleaned.get('TELENO'),
                'mobile': cleaned.get('MOBLNO'),
                'telex': cleaned.get('TELXNO'),
                'fax': cleaned.get('FAXNO'),
                'email': cleaned.get('MAILID'),
                'sponsorName': cleaned.get('SPONAM'),
                'gender': cleaned.get('GENDER'),
                'status': cleaned.get('CUSSTS'),
                'nationalId': cleaned.get('NATLID'),
                'timestamp': cleaned.get('TIMSTAMP'),
                'branchCode': cleaned.get('OPRBRA')
            }
            
            records.append(record)
    
    print(f"Loaded {len(records)} records from CSV")
    
    # Delete existing Customer nodes
    print("Deleting existing Customer nodes...")
    with driver.session() as session:
        session.run("MATCH (c:Customer) DETACH DELETE c")
    
    # Insert records in batches
    print("Inserting records into Neo4j...")
    batch_size = 100
    
    with driver.session() as session:
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            
            query = """
            UNWIND $records AS record
            CREATE (c:Customer)
            SET c = record
            """
            
            session.run(query, records=batch)
            print(f"  Inserted {min(i+batch_size, len(records))}/{len(records)} records")
    
    print(f"✅ Successfully loaded {len(records)} Customer nodes into Neo4j\n")
    return len(records)


def create_entity_resolution_graph(driver, matches, clusters, records):
    """
    Create entity resolution graph in Neo4j with clusters and entities.
    
    This creates:
    1. EntityCluster nodes for each cluster
    2. Entity nodes for each record that's part of a cluster
    3. MEMBER_OF relationships from Entity to EntityCluster
    4. DUPLICATE_OF relationships between entities in the same cluster
    
    Args:
        driver: Neo4j driver instance
        matches: List of match dictionaries from Flink-ER
        clusters: List of sets containing entity IDs in each cluster
        records: Original records loaded from CSV
        
    Returns:
        Dictionary with statistics
    """
    print("\n" + "="*70)
    print("CREATING ENTITY RESOLUTION GRAPH IN NEO4J")
    print("="*70)
    
    stats = {
        'clusters_created': 0,
        'entities_created': 0,
        'member_relationships': 0,
        'duplicate_relationships': 0
    }
    
    with driver.session() as session:
        # Clear existing entity resolution graph
        print("Clearing existing EntityCluster and Entity nodes...")
        session.run("MATCH (n:EntityCluster) DETACH DELETE n")
        session.run("MATCH (n:Entity) DETACH DELETE n")
        
        # Create clusters and entities in batches
        print(f"Creating {len(clusters)} entity clusters...")
        
        for cluster_idx, cluster in enumerate(clusters):
            cluster_id = f"cluster_{cluster_idx}"
            member_ids = list(cluster)
            
            # Get sample entity info from first member
            sample_record = records[member_ids[0]]
            cluster_name = sample_record.get('CUSNMF', 'Unknown')
            
            # Create cluster node
            session.run("""
                CREATE (c:EntityCluster {
                    clusterId: $cluster_id,
                    name: $name,
                    size: $size,
                    memberIds: $member_ids
                })
            """, cluster_id=cluster_id, name=cluster_name, size=len(member_ids), member_ids=member_ids)
            stats['clusters_created'] += 1
            
            # Prepare entity data for batch creation
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
            
            # Batch create entities and relationships for this cluster
            session.run("""
                UNWIND $entities AS entity
                CREATE (e:Entity)
                SET e = entity
                WITH e, entity.clusterId as clusterId
                MATCH (c:EntityCluster {clusterId: clusterId})
                CREATE (e)-[:MEMBER_OF]->(c)
            """, entities=entity_data)
            
            stats['entities_created'] += len(entity_data)
            stats['member_relationships'] += len(entity_data)
        
        # Create DUPLICATE_OF relationships in batches
        print(f"Creating DUPLICATE_OF relationships for {len(matches)} matches...")
        
        # Prepare match data for batch creation
        match_data = []
        for match in matches:
            match_data.append({
                'id1': match['id1'],
                'id2': match['id2'],
                'similarity': match['similarity']
            })
        
        # Batch create all DUPLICATE_OF relationships
        batch_size = 1000
        for i in range(0, len(match_data), batch_size):
            batch = match_data[i:i + batch_size]
            session.run("""
                UNWIND $matches AS match
                MATCH (e1:Entity {entityId: match.id1})
                MATCH (e2:Entity {entityId: match.id2})
                CREATE (e1)-[:DUPLICATE_OF {similarity: match.similarity}]->(e2)
                CREATE (e2)-[:DUPLICATE_OF {similarity: match.similarity}]->(e1)
            """, matches=batch)
            stats['duplicate_relationships'] += len(batch) * 2
    
    print(f"✅ Entity resolution graph created successfully!")
    print(f"  • Clusters: {stats['clusters_created']}")
    print(f"  • Entities: {stats['entities_created']}")
    print(f"  • MEMBER_OF relationships: {stats['member_relationships']}")
    print(f"  • DUPLICATE_OF relationships: {stats['duplicate_relationships']}\n")
    
    return stats


def main():
    """Main entry point for Flink-ER demonstration."""
    
    print("\n" + "="*70)
    print(" "*15 + "FLINK-ER DISTRIBUTED ENTITY RESOLUTION")
    print("="*70)
    print("\nA Python implementation inspired by Apache Flink's distributed")
    print("processing model for high-performance entity resolution.\n")
    print("="*70 + "\n")
    
    # Configuration
    csv_file = "csv/challenging_er_200.csv"
    output_file = "flink_er_results.json"
    
    # Check if CSV exists
    if not os.path.exists(csv_file):
        print(f"❌ ERROR: CSV file not found: {csv_file}")
        print("\nPlease ensure the challenge dataset exists at the specified path.")
        sys.exit(1)
    
    # Load data
    print("STEP 1: Loading data")
    print("-" * 70)
    records = load_csv_records(csv_file)
    
    # Configure Flink-ER
    print("\nSTEP 2: Configuring Flink-ER")
    print("-" * 70)
    
    # Auto-detect optimal configuration
    import multiprocessing
    cpu_cores = multiprocessing.cpu_count()
    
    config = FlinkERConfig(
        parallelism=cpu_cores,           # Use all available CPU cores
        batch_size=50,                   # Process 50 records per batch
        max_workers=cpu_cores,           # Maximum parallel workers
        use_processes=True,              # Use multiprocessing for true parallelism
        retry_attempts=3,                # Retry failed tasks up to 3 times
        similarity_threshold=0.75        # 75% similarity threshold
    )
    
    print(f"  • Parallelism: {config.parallelism} (CPU cores)")
    print(f"  • Batch Size: {config.batch_size}")
    print(f"  • Max Workers: {config.max_workers}")
    print(f"  • Execution Mode: {'Multiprocessing' if config.use_processes else 'Threading'}")
    print(f"  • Similarity Threshold: {config.similarity_threshold}")
    
    # Create resolver
    print("\nSTEP 3: Initializing Flink-ER Engine")
    print("-" * 70)
    resolver = FlinkEntityResolver(config)
    print("  ✓ Flink-ER engine initialized")
    
    # Resolve entities
    print("\nSTEP 4: Running Distributed Entity Resolution")
    print("-" * 70)
    results = resolver.resolve(records)
    
    # Print detailed statistics
    print("\nSTEP 5: Results Summary")
    print("-" * 70)
    resolver.print_statistics()
    
    # Print sample matches
    if results['matches']:
        print("\nSample Matches (top 5):")
        print("-" * 70)
        for i, match in enumerate(results['matches'][:5]):
            print(f"\nMatch {i+1}:")
            print(f"  Similarity: {match['similarity']:.4f}")
            print(f"  Record 1: {match['record1'].get('CUSNMF', 'N/A')}")
            print(f"  Record 2: {match['record2'].get('CUSNMF', 'N/A')}")
    
    # Print sample clusters
    if results['clusters']:
        print("\n\nSample Clusters (top 3):")
        print("-" * 70)
        for i, cluster in enumerate(results['clusters'][:3]):
            print(f"\nCluster {i+1}: {len(cluster)} entities")
            print(f"  Entity IDs: {sorted(list(cluster))[:10]}{'...' if len(cluster) > 10 else ''}")
    
    # Export results
    print("\n\nSTEP 6: Exporting Results")
    print("-" * 70)
    
    # Prepare exportable results
    exportable_results = {
        'config': {
            'parallelism': config.parallelism,
            'batch_size': config.batch_size,
            'similarity_threshold': config.similarity_threshold,
            'execution_mode': 'multiprocessing' if config.use_processes else 'threading'
        },
        'matches': [
            {
                'id1': m['id1'],
                'id2': m['id2'],
                'similarity': m['similarity'],
                'record1_name': m['record1'].get('CUSNMF', 'N/A'),
                'record2_name': m['record2'].get('CUSNMF', 'N/A')
            }
            for m in results['matches']
        ],
        'clusters': [list(cluster) for cluster in results['clusters']],
        'statistics': results['statistics']
    }
    
    with open(output_file, 'w') as f:
        json.dump(exportable_results, f, indent=2)
    
    print(f"  ✓ Results exported to: {output_file}")
    
    # Store results in Neo4j
    print("\n\nSTEP 7: Storing Results in Neo4j")
    print("-" * 70)
    
    num_customers = 0
    try:
        # Connect to Neo4j
        print(f"Connecting to Neo4j at {NEO4J_URI}...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            result.single()
        print("  ✓ Connected to Neo4j")
        
        # Create Customer nodes from CSV
        num_customers = create_customer_nodes(driver, csv_file)
        
        # Create entity resolution graph in Neo4j
        neo4j_stats = create_entity_resolution_graph(driver, results['matches'], results['clusters'], records)
        
        # Close connection
        driver.close()
        
        print("  ✓ Neo4j integration complete")
        
    except Exception as e:
        print(f"  ⚠ Warning: Neo4j integration failed: {str(e)}")
        print(f"  ⚠ Results are still available in {output_file}")
        print(f"  ⚠ Make sure Neo4j is running and accessible at {NEO4J_URI}")
    
    # Final summary
    print("\n" + "="*70)
    print(" "*25 + "EXECUTION COMPLETE")
    print("="*70)
    print(f"\n✅ Successfully processed {results['statistics']['total_records']} records")
    print(f"✅ Found {results['statistics']['total_matches']} matches")
    print(f"✅ Created {len(results['clusters'])} entity clusters")
    print(f"✅ Comparison reduction: {results['statistics']['reduction_percentage']:.2f}%")
    if num_customers > 0:
        print(f"✅ Created {num_customers} Customer nodes in Neo4j")
    print(f"\nResults saved to: {output_file}\n")


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
