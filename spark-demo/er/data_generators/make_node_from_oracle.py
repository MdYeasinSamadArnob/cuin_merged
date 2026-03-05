import oracledb
from neo4j import GraphDatabase
from datetime import datetime
import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
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
# BATCH CONFIG
# -----------------------------
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))
RECORD_LIMIT = int(os.environ.get("RECORD_LIMIT", "20000"))  # Default: 20k records

# -----------------------------
# SELECTED FIELDS (include new fields)
# -----------------------------
fields = [
    "CUSCOD", "CUSTYP", "CUSNMF", "CUSNML", "CUSDOB",
    "ADDRS1", "ADDRS2", "ADDRS3", "ADDRS4",
    "CITYNM", "TELENO", "MOBLNO", "TELXNO", "FAXNO", "MAILID",
    "SPONAM", "GENDER", "CUSSTS", "NATLID",
    "TIMSTAMP",  "OPRBRA"
]

# -----------------------------
# RENAME MAP (Neo4j properties)
# -----------------------------
rename_map = {
    "CUSCOD": "accountId",
    "CUSTYP": "type",
    "CITYNM": "city",
    "TELENO": "phone",
    "MOBLNO": "mobile",
    "TELXNO": "telex",
    "FAXNO": "fax",
    "MAILID": "email",
    "CUSDOB": "dob",
    "TIMSTAMP": "createdAt",
    "SPONAM": "sponsorName",
    "GENDER": "gender",
    "CUSSTS": "status",  # updated here
    "NATLID": "nationalId",
    "OPRBRA": "branchCode"  # added here
}

# -----------------------------
# UTILITY FUNCTIONS
# -----------------------------
def clean_value(val, col=None):
    if val is None:
        return None

    if isinstance(val, str):
        val = val.replace('"', '').strip()

    if col in ("CUSDOB", "TIMSTAMP"):
        try:
            if isinstance(val, str):
                val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S").isoformat()
            elif isinstance(val, datetime):
                val = val.isoformat()
        except Exception:
            pass

    return val

def concatenate_address(row):
    parts = [clean_value(row.get(f"ADDRS{i}")) for i in range(1, 5)]
    parts = [p for p in parts if p]
    return ", ".join(parts) if parts else None

def validate_table_name(table_name):
    """
    Validate Oracle table name to prevent SQL injection.
    
    Args:
        table_name: Table name to validate
        
    Returns:
        True if valid, raises ValueError if invalid
    """
    import re
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Table name must be a non-empty string")
    
    # Allow only alphanumeric characters and underscores
    if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
        raise ValueError(
            f"Invalid table name '{table_name}'. "
            "Table name must contain only letters, numbers, and underscores."
        )
    
    return True

# -----------------------------
# CYPHER QUERY
# -----------------------------
CYPHER = """
UNWIND $rows AS row
MERGE (c:Customer {accountId: row.accountId})
SET
  c.type         = row.type,
  c.name         = row.name,
  c.city         = row.city,
  c.phone        = row.phone,
  c.mobile       = row.mobile,
  c.telex        = row.telex,
  c.fax          = row.fax,
  c.email        = row.email,
  c.address      = row.address,
  c.dob          = datetime(row.dob),
  c.createdAt    = datetime(row.createdAt),
  c.sponsorName  = row.sponsorName,
  c.gender       = row.gender,
  c.status       = row.status,
  c.nationalId   = row.nationalId,
  c.branchCode   = row.branchCode 
"""


def main():
    """
    Main entry point for the Oracle to Neo4j loader.
    
    This function loads customer data from Oracle database into Neo4j
    for entity resolution.
    """
    print("=" * 60)
    print("Oracle to Neo4j Customer Data Loader")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  Oracle DSN: {ORACLE_DSN}")
    print(f"  Oracle Table: {ORACLE_TABLE}")
    print(f"  Record Limit: {RECORD_LIMIT:,}")
    print(f"  Batch Size: {BATCH_SIZE:,}")
    print("=" * 60)
    
    # Validate configuration
    try:
        validate_table_name(ORACLE_TABLE)
    except ValueError as e:
        print(f"❌ Configuration error: {e}")
        print("\nPlease check your .env file and ensure ORACLE_TABLE is set correctly.")
        return
    
    # Check for placeholder values
    if ORACLE_USER == "your_oracle_user" or ORACLE_DSN == "your_host:1527/your_database":
        print("❌ Configuration error: Please update your .env file with actual Oracle credentials.")
        print("\nSteps to configure:")
        print("1. Copy .env.example to .env: cp .env.example .env")
        print("2. Edit .env and update Oracle credentials")
        print("3. Run this command again")
        return
    
    # -----------------------------
    # CONNECT TO ORACLE
    # -----------------------------
    try:
        print("Connecting to Oracle...")
        print(f"Loading up to {RECORD_LIMIT:,} records from {ORACLE_TABLE}...")
        oracle_conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN
        )
        cursor = oracle_conn.cursor()
        cursor.arraysize = BATCH_SIZE
        
        # Build SQL query with ROWNUM limit - use subquery to get top N ordered records
        fields_str = ', '.join(fields)
        sql = f"""
            SELECT * FROM (
                SELECT {fields_str}
                FROM {ORACLE_TABLE}
                ORDER BY TIMSTAMP DESC
            )
            WHERE ROWNUM <= {RECORD_LIMIT}
        """
        
        cursor.execute(sql)
        
        columns = [c[0] for c in cursor.description]
        
    except oracledb.Error as e:
        print(f"❌ Oracle connection error: {e}")
        print("\nPlease verify:")
        print("1. Oracle database is accessible")
        print("2. Credentials in .env file are correct")
        print("3. Network connectivity to Oracle server")
        return
    except Exception as e:
        print(f"❌ Unexpected error connecting to Oracle: {e}")
        return
    
    # -----------------------------
    # CONNECT TO NEO4J
    # -----------------------------
    try:
        print("Connecting to Neo4j...")
        driver = GraphDatabase.driver(
            NEO4J_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
    except Exception as e:
        print(f"❌ Neo4j connection error: {e}")
        print("\nPlease verify:")
        print("1. Neo4j is running: docker-compose up -d")
        print("2. Neo4j credentials in .env file are correct")
        print("3. Neo4j is accessible at {NEO4J_URI}")
        cursor.close()
        oracle_conn.close()
        return
    
    # -----------------------------
    # DELETE OLD DATA
    # -----------------------------
    print("Deleting previous Customer nodes...")
    with driver.session() as session:
        while True:
            deleted = session.execute_write(
                lambda tx: tx.run("""
                    MATCH (c:Customer)
                    WITH c LIMIT 50000
                    DETACH DELETE c
                    RETURN count(c) AS deleted
                """).single()["deleted"]
            )
            if deleted == 0:
                break
            print(f"Deleted {deleted}")
    
    print("✅ Cleanup complete")
    
    # -----------------------------
    # IMPORT DATA
    # -----------------------------
    print("Starting import...")
    with driver.session() as session:
        total = 0
    
        while True:
            batch = cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break
    
            rows = []
    
            for r in batch:
                raw = {col: clean_value(val, col) for col, val in zip(columns, r)}
    
                # Skip invalid primary key
                if not raw.get("CUSCOD"):
                    continue
    
                # Build full name
                first = raw.get("CUSNMF")
                last = raw.get("CUSNML")
                name = " ".join(p for p in (first, last) if p)
    
                # Start row with name and address
                row = {
                    "accountId": raw["CUSCOD"],
                    "name": name,
                    "address": concatenate_address(raw),
                }
    
                # Apply rename_map for other fields (skip accountId as it's already set)
                for src, dest in rename_map.items():
                    if src != "CUSCOD" and src in raw:
                        row[dest] = raw.get(src)
    
                rows.append(row)
    
            if rows:
                session.execute_write(lambda tx: tx.run(CYPHER, rows=rows))
                total += len(rows)
                print(f"Inserted {len(rows)} | Total: {total}")
    
    # -----------------------------
    # CLEANUP
    # -----------------------------
    cursor.close()
    oracle_conn.close()
    driver.close()
    
    print(f"✅ Import completed: {total} Customer nodes")
    print("=" * 60)


if __name__ == "__main__":
    main()

