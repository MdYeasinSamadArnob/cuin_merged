import logging
import os
import time
import psycopg2
from pathlib import Path

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

        _apply_migrations(conn, schema_path.parent / "migrations")

    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def _apply_migrations(conn, migrations_dir: Path):
    """
    Applies db/migrations/*.sql in filename order, tracked in
    schema_migrations. The base-schema guard above (checking whether
    `runs` exists) only decides whether to apply schema.sql -- it does
    NOT re-run schema.sql's ALTER-free CREATE TABLE IF NOT EXISTS
    statements, so an existing database never picks up new columns
    added after initial deployment. Migrations are the mechanism for
    that: numbered, idempotent (IF NOT EXISTS / ADD COLUMN IF NOT
    EXISTS), and applied exactly once each.
    """
    if not migrations_dir.exists():
        return

    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename VARCHAR(255) PRIMARY KEY,
            applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        )
    """)
    conn.commit()

    cur.execute("SELECT filename FROM schema_migrations")
    applied = {row[0] for row in cur.fetchall()}

    for migration_file in sorted(migrations_dir.glob("*.sql")):
        if migration_file.name in applied:
            continue
        logger.info(f"Applying migration: {migration_file.name}")
        with open(migration_file, "r") as f:
            cur.execute(f.read())
        cur.execute(
            "INSERT INTO schema_migrations (filename) VALUES (%s)",
            (migration_file.name,),
        )
        conn.commit()
        logger.info(f"Migration applied: {migration_file.name}")

    cur.close()
