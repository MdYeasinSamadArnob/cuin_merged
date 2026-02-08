import logging
import os
import time
import psycopg2
from pathlib import Path
from neo4j import GraphDatabase

from api.config import settings

logger = logging.getLogger(__name__)

MAX_RETRIES = 5
RETRY_DELAY = 3

def init_db():
    """
    Initialize Relational Database (PostgreSQL).
    Applies schema.sql if tables are missing.
    """
    logger.info("Initializing Database...")
    
    schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
    if not schema_path.exists():
        logger.error(f"Schema file not found at {schema_path}")
        return

    conn = None
    try:
        # Wait for DB to be ready
        for i in range(MAX_RETRIES):
            try:
                conn = psycopg2.connect(settings.DATABASE_URL)
                break
            except Exception as e:
                logger.warning(f"Database not ready yet ({i+1}/{MAX_RETRIES}): {e}")
                time.sleep(RETRY_DELAY)
        
        if not conn:
            logger.error("Could not connect to database after retries.")
            return

        cur = conn.cursor()
        
        # Check if 'runs' table exists as a proxy for schema existence
        cur.execute("SELECT to_regclass('public.runs');")
        exists = cur.fetchone()[0]
        
        if not exists:
            logger.info("Tables not found. Applying schema.sql...")
            with open(schema_path, "r") as f:
                schema_sql = f.read()
                
            # Execute schema
            # We split by statement if possible, or just run the whole block if using simple SQL
            # psycopg2 can execute multiple statements in one go usually
            cur.execute(schema_sql)
            conn.commit()
            logger.info("Schema applied successfully!")
        else:
            logger.info("Database schema already exists.")
            
        cur.close()
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def init_graph():
    """
    Initialize Graph Database (Neo4j).
    Creates necessary constraints and indexes.
    """
    logger.info("Initializing Graph Constraints...")
    
    driver = None
    try:
        # Wait for Neo4j
        for i in range(MAX_RETRIES):
            try:
                driver = GraphDatabase.driver(
                    settings.NEO4J_URI, 
                    auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
                )
                driver.verify_connectivity()
                break
            except Exception as e:
                logger.warning(f"Neo4j not ready yet ({i+1}/{MAX_RETRIES}): {e}")
                time.sleep(RETRY_DELAY)
                
        if not driver:
            logger.error("Could not connect to Neo4j after retries.")
            return

        with driver.session() as session:
            # Create constraints (idempotent-ish in Neo4j 5.x with IF NOT EXISTS usually, but let's try raw)
            
            # Constraint: Cluster ID must be unique
            session.run("CREATE CONSTRAINT cluster_id_unique IF NOT EXISTS FOR (c:Cluster) REQUIRE c.id IS UNIQUE")
            
            # Constraint: Entity ID unique (if we use Entity label)
            session.run("CREATE CONSTRAINT entity_id_unique IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE")
            
            # Index: Source System for faster lookups
            session.run("CREATE INDEX entity_source_idx IF NOT EXISTS FOR (e:Entity) ON (e.source_system)")
            
            # Index: Names for search
            session.run("CREATE INDEX entity_name_idx IF NOT EXISTS FOR (e:Entity) ON (e.name)")
            
            logger.info("Graph constraints verified.")

    except Exception as e:
        logger.error(f"Failed to initialize graph: {e}")
    finally:
        if driver:
            driver.close()
