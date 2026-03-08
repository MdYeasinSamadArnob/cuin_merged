"""
CUIN v2 - Admin API Routes

Administrative endpoints for system management.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import logging
import os
import shutil
import json
from pathlib import Path

logger = logging.getLogger(__name__)

router = APIRouter()


class ResetResponse(BaseModel):
    """Response from reset operation."""
    success: bool
    message: str
    deleted_files: int
    deleted_runs: int


@router.post("/reset", response_model=ResetResponse)
async def reset_all_data():
    """
    Reset all data in the system.
    
    This endpoint will:
    - Delete all files in data/runs/ directory
    - Clear the runs_index.json file
    - Truncate all database tables (except genesis audit event)
    - Clear Neo4j graph data
    - Reset in-memory state
    
    WARNING: This operation is irreversible!
    """
    try:
        deleted_files = 0
        deleted_runs = 0
        
        # 1. Delete all files in data/runs/ directory
        try:
            from api.config import settings
            runs_dir = Path(f"{settings.DATA_DIR}/runs")
            if runs_dir.exists():
                for item in runs_dir.iterdir():
                    if item.is_file():
                        item.unlink()
                        deleted_files += 1
                        logger.info(f"Deleted file: {item}")
                    elif item.is_dir():
                        shutil.rmtree(item)
                        deleted_files += 1
                        logger.info(f"Deleted directory: {item}")
        except Exception as e:
            logger.warning(f"Could not delete run files: {e}")

        # 2. Clear runs_index.json
        try:
            from api.config import settings
            runs_index_path = Path(f"{settings.DATA_DIR}/runs_index.json")
            if runs_index_path.exists():
                with open(runs_index_path, 'w') as f:
                    json.dump([], f)
                logger.info("Cleared runs_index.json")
        except Exception as e:
            logger.warning(f"Could not clear runs_index.json: {e}")

        # 3. Clear database tables
        try:
            import psycopg2
            from api.config import settings
            
            conn = psycopg2.connect(settings.DATABASE_URL)
            cur = conn.cursor()
            
            # Truncate tables in correct order (respecting foreign keys)
            # Note: We keep the genesis audit event for chain integrity
            tables_to_truncate = [
                "referee_explanations",
                "review_queue",
                "match_decisions",
                "match_scores",
                "candidate_pairs",
                "golden_records",
                "clusters",
                "customers_norm",
                "runs",
                "policy_versions"
            ]
            
            for table in tables_to_truncate:
                cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
                logger.info(f"Truncated table: {table}")
            
            # Clear audit events except genesis
            cur.execute("DELETE FROM audit_events WHERE audit_id != '00000000-0000-0000-0000-000000000000';")
            logger.info("Cleared audit events (kept genesis)")
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info("✅ Database tables cleared")
            
        except Exception as e:
            logger.error(f"Failed to clear database tables: {e}")
            # Continue with other cleanup even if DB fails
        
        # 4. Clear Neo4j graph data
        try:
            from api.config import settings
            from neo4j import GraphDatabase
            
            driver = GraphDatabase.driver(
                settings.NEO4J_URI,
                auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD)
            )
            
            with driver.session() as session:
                # Delete all nodes and relationships
                session.run("MATCH (n) DETACH DELETE n")
                logger.info("✅ Neo4j graph cleared")
            
            driver.close()
            
        except Exception as e:
            logger.warning(f"Failed to clear Neo4j (may not be running): {e}")
        
        # 5. Reset in-memory state
        try:
            from services.run_service import get_run_service
            from engine.clustering import get_cluster_manager
            
            # Reset run service
            run_service = get_run_service()
            deleted_runs = len(run_service._orchestrators)
            run_service._orchestrators.clear()
            run_service._runs.clear()  # Clear the runs dictionary too!
            logger.info(f"Cleared {deleted_runs} runs from memory")
            
            # Reset cluster manager
            cluster_manager = get_cluster_manager()
            cluster_manager._members.clear()
            cluster_manager._cluster_ids.clear()
            cluster_manager._uf = type(cluster_manager._uf)()  # New UnionFind instance
            logger.info("Reset cluster manager state")
            
        except Exception as e:
            logger.warning(f"Failed to reset in-memory state: {e}")
        
        logger.info(f"✅ Reset complete: {deleted_files} files deleted, {deleted_runs} runs cleared")
        
        return ResetResponse(
            success=True,
            message="All data has been successfully reset",
            deleted_files=deleted_files,
            deleted_runs=deleted_runs
        )
        
    except Exception as e:
        logger.error(f"Failed to reset data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reset data: {str(e)}"
        )
