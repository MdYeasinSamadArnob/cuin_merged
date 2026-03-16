"""
Run Challenge ER 200 - Entity Resolution for Challenge Dataset

This script loads data from either CSV or Oracle database into Neo4j,
performs entity resolution, creates DUPLICATE_OF relationships,
and exports results to JSON.

Data Source Selection:
    Set DATA_SOURCE environment variable to either 'csv' or 'oracle'
    Default: 'csv' (backward compatible)

Usage:
    # CSV (default)
    python -m er.scripts.run_challenge_er_200
    
    # Oracle
    DATA_SOURCE=oracle python -m er.scripts.run_challenge_er_200
"""

import csv
import json
import os
import sys
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

# Data Source Configuration
DATA_SOURCE = os.environ.get("DATA_SOURCE", "csv").lower()  # 'csv' or 'oracle'

# File paths
CSV_FILE = os.environ.get("CSV_FILE", "csv/challenging_er_200.csv")
OUTPUT_JSON = "challenge_er_200_results.json"


def clean_value(val):
    """Clean CSV value."""
    if val is None or val == '':
        return None
    if isinstance(val, str):
        return val.strip()
    return val


def load_csv_to_neo4j(csv_path: str, driver):
    """
    Load CSV data into Neo4j as Customer nodes.
    
    Args:
        csv_path: Path to CSV file
        driver: Neo4j driver instance
    """
    print(f"Loading CSV from {csv_path}...")
    
    records = []
    with open(csv_path, 'r', encoding='utf-8') as f:
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
    
    print(f"✅ Successfully loaded {len(records)} Customer nodes into Neo4j")
    return len(records)


def load_oracle_to_neo4j(driver):
    """
    Load data from Oracle database into Neo4j as Customer nodes.
    
    Uses configuration from .env file:
        - ORACLE_DSN
        - ORACLE_USER
        - ORACLE_PASSWORD
        - ORACLE_TABLE
        - RECORD_LIMIT
    
    Args:
        driver: Neo4j driver instance
        
    Returns:
        Number of records loaded
    """
    print("Loading data from Oracle database...")
    
    try:
        from db.oracle_connector import load_from_oracle as oracle_loader
    except ImportError as e:
        print(f"\n❌ Error: Unable to import Oracle connector")
        print(f"   Make sure the 'db' module is properly installed")
        print(f"   Error details: {e}")
        sys.exit(1)
    
    # Load records from Oracle
    print("Connecting to Oracle and fetching records...")
    records = oracle_loader()
    
    if not records:
        print("❌ No records loaded from Oracle")
        sys.exit(1)
    
    print(f"Loaded {len(records)} records from Oracle")
    
    # Convert Oracle records to Neo4j format
    neo4j_records = []
    for rec in records:
        # Build full name
        first = rec.get('CUSNMF', '')
        last = rec.get('CUSNML', '')
        name = f"{first} {last}".strip()
        
        # Build address
        addr_parts = [
            rec.get('ADDRS1'),
            rec.get('ADDRS2'),
            rec.get('ADDRS3'),
            rec.get('ADDRS4')
        ]
        address = ', '.join([p for p in addr_parts if p])
        
        neo4j_rec = {
            'accountId': rec.get('CUSCOD'),
            'type': rec.get('CUSTYP'),
            'name': name,
            'firstName': first,
            'lastName': last,
            'dob': rec.get('CUSDOB'),
            'address': address,
            'city': rec.get('CITYNM'),
            'phone': rec.get('TELENO'),
            'mobile': rec.get('MOBLNO'),
            'telex': rec.get('TELXNO'),
            'fax': rec.get('FAXNO'),
            'email': rec.get('MAILID'),
            'sponsorName': rec.get('SPONAM'),
            'gender': rec.get('GENDER'),
            'status': rec.get('CUSSTS'),
            'nationalId': rec.get('NATLID'),
            'timestamp': rec.get('TIMSTAMP'),
            'branchCode': rec.get('OPRBRA')
        }
        
        neo4j_records.append(neo4j_rec)
    
    # Delete existing Customer nodes
    print("Deleting existing Customer nodes...")
    with driver.session() as session:
        session.run("MATCH (c:Customer) DETACH DELETE c")
    
    # Insert records in batches
    print("Inserting records into Neo4j...")
    batch_size = 100
    
    with driver.session() as session:
        for i in range(0, len(neo4j_records), batch_size):
            batch = neo4j_records[i:i+batch_size]
            
            query = """
            UNWIND $records AS record
            CREATE (c:Customer)
            SET c = record
            """
            
            session.run(query, records=batch)
            print(f"  Inserted {min(i+batch_size, len(neo4j_records))}/{len(neo4j_records)} records")
    
    print(f"✅ Successfully loaded {len(neo4j_records)} Customer nodes into Neo4j")
    return len(neo4j_records)


def run_entity_resolution(driver, similarity_threshold=0.75):
    """
    Run entity resolution using Neo4j data.
    
    Args:
        driver: Neo4j driver instance
        similarity_threshold: Minimum similarity for matches
        
    Returns:
        Results dictionary
    """
    from engine.spark_er.core.neo4j_entity_resolution import Neo4jEntityResolver
    
    print("\n" + "=" * 60)
    print("RUNNING ENTITY RESOLUTION")
    print("=" * 60)
    
    # Create resolver (reuse existing driver connection)
    resolver = Neo4jEntityResolver(
        uri=NEO4J_URI,
        user=NEO4J_USER,
        password=NEO4J_PASSWORD,
        similarity_threshold=similarity_threshold,
        use_blocking=True
    )
    
    # Close the resolver's driver and use the passed one
    resolver.driver.close()
    resolver.driver = driver
    
    try:
        # Perform entity resolution
        results = resolver.resolve_entities(label="Customer", limit=None)
        
        # Print statistics
        resolver.print_statistics()
        
        return results
    except Exception as e:
        print(f"Error during entity resolution: {e}")
        raise


def export_results(results: dict, output_path: str):
    """
    Export results to JSON file.
    
    Args:
        results: Results dictionary
        output_path: Output file path
    """
    print(f"\nExporting results to {output_path}...")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    
    print(f"✅ Results exported to {output_path}")


def print_summary(results: dict):
    """
    Print summary of entity resolution results.
    
    Args:
        results: Results dictionary
    """
    stats = results.get('statistics', {})
    matches = results.get('matches', [])
    
    print("\n" + "=" * 60)
    print("ENTITY RESOLUTION SUMMARY")
    print("=" * 60)
    print(f"Total Records: {stats.get('total_records', 0)}")
    print(f"Total Comparisons: {stats.get('total_comparisons', 0):,}")
    print(f"Total Matches Found: {len(matches)}")
    print(f"DUPLICATE_OF Relationships: {stats.get('relationships_created', 0)}")
    
    # Print sample matches
    if matches:
        print("\nSample Matches (Top 10 by similarity):")
        sorted_matches = sorted(matches, key=lambda x: x['similarity'], reverse=True)
        
        for i, match in enumerate(sorted_matches[:10], 1):
            print(f"  {i}. Similarity: {match['similarity']:.3f}")
            print(f"     IDs: {match['neo4j_id1']} <-> {match['neo4j_id2']}")
    
    print("=" * 60 + "\n")


def main():
    """Main execution function."""
    print("\n" + "=" * 60)
    print("CHALLENGE ER 200 - ENTITY RESOLUTION")
    print("=" * 60)
    print(f"Data Source: {DATA_SOURCE.upper()}")
    print("=" * 60 + "\n")
    
    # Validate data source
    if DATA_SOURCE not in ['csv', 'oracle']:
        print(f"❌ Error: Invalid DATA_SOURCE '{DATA_SOURCE}'")
        print("   Valid options: 'csv' or 'oracle'")
        print("   Set DATA_SOURCE environment variable in .env file")
        sys.exit(1)
    
    # Connect to Neo4j
    print("Connecting to Neo4j...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    try:
        # Test connection
        with driver.session() as session:
            result = session.run("RETURN 1 AS test")
            result.single()
        print("✅ Connected to Neo4j\n")
        
        # Step 1: Load data based on DATA_SOURCE
        if DATA_SOURCE == 'csv':
            print(f"Loading data from CSV: {CSV_FILE}\n")
            num_records = load_csv_to_neo4j(CSV_FILE, driver)
        elif DATA_SOURCE == 'oracle':
            print("Loading data from Oracle database\n")
            num_records = load_oracle_to_neo4j(driver)
        
        # Step 2: Run entity resolution
        results = run_entity_resolution(driver, similarity_threshold=0.75)
        
        # Step 3: Export results
        export_results(results, OUTPUT_JSON)
        
        # Step 4: Print summary
        print_summary(results)
        
        print("✅ Entity resolution completed successfully!")
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: CSV file not found at {CSV_FILE}")
        print(f"   Make sure the file exists and the path is correct.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        driver.close()
        print("\nNeo4j connection closed.")


if __name__ == "__main__":
    main()
