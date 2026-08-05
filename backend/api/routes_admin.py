"""
CUIN v2 - Admin API Routes

Administrative endpoints for system management.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool
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
    deleted_doris_dbs: int = 0


def _sync_reset_all_data() -> ResetResponse:
    """
    Full synchronous body of reset_all_data -- does blocking file I/O
    (shutil.rmtree/unlink over data/runs/ and the Parquet lake, JSON
    index writes) and blocking DB calls (pymysql DDL against Doris,
    psycopg2 TRUNCATE against Postgres), so it runs off the event loop
    as one run_in_threadpool dispatch.
    """
    try:
        deleted_files = 0
        deleted_runs = 0
        
        # 1. Delete all files in data/runs/ directory
        runs_dir = Path("data/runs")
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
        
        # 2. Clear runs_index.json
        runs_index_path = Path("data/runs_index.json")
        if runs_index_path.exists():
            with open(runs_index_path, 'w') as f:
                json.dump([], f)
            logger.info("Cleared runs_index.json")

        # 2b. Clear the review-queue pair index (per-run queue/update
        # files themselves already went in step 1, since they live
        # under data/runs/ too) and reset the in-memory review service
        # singleton so a live process doesn't keep serving stale
        # pair->run lookups for runs that no longer exist on disk.
        try:
            review_index_path = Path("data/review_pair_index.json")
            if review_index_path.exists():
                with open(review_index_path, 'w') as f:
                    json.dump({}, f)
                logger.info("Cleared review_pair_index.json")

            from services.review_service import get_review_service
            review_service = get_review_service()
            review_service._items.clear()
            review_service._by_pair.clear()
            review_service._loaded_runs.clear()
            review_service._pair_index.clear()
            logger.info("Reset in-memory review service state")
        except Exception as e:
            logger.warning(f"Failed to reset review service state: {e}")

        # 2c. Clear the Parquet lake (engine.lake / LAKE_ROOT) -- both
        # engines read this in place, so leftover lake data from a
        # deleted run would otherwise keep being queryable after reset.
        try:
            from api.config import settings
            lake_root = Path(settings.LAKE_ROOT)
            if lake_root.exists():
                shutil.rmtree(lake_root)
                logger.info(f"Cleared lake root: {lake_root}")
        except Exception as e:
            logger.warning(f"Failed to clear lake root: {e}")

        # 2d. Drop every per-run Doris database (cuin_run_*). Without
        # this, step 1's local file deletion has no Doris-side
        # equivalent -- run databases accumulate in Doris forever
        # across resets, which is exactly the durability/
        # cleanup gap the lakehouse migration plan flagged (Doris data
        # now survives container restarts via named volumes, so it
        # doesn't disappear on its own the way an ephemeral container's
        # data used to).
        deleted_doris_dbs = 0
        try:
            import pymysql
            from api.config import settings

            admin = pymysql.connect(
                host=settings.DORIS_HOST, port=settings.DORIS_MYSQL_PORT,
                user=settings.DORIS_USER, password=settings.DORIS_PASSWORD,
                autocommit=True, connect_timeout=5,
            )
            with admin.cursor() as cur:
                cur.execute("SHOW DATABASES LIKE 'cuin\\_run\\_%'")
                db_names = [row[0] for row in cur.fetchall()]
                for db_name in db_names:
                    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
                    deleted_doris_dbs += 1
                    logger.info(f"Dropped Doris database: {db_name}")
            admin.close()
        except Exception as e:
            logger.warning(f"Failed to drop Doris run databases (Doris may not be running): {e}")
        
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
        
        # 4. Reset in-memory state
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
        
        logger.info(
            f"✅ Reset complete: {deleted_files} files deleted, {deleted_runs} runs cleared, "
            f"{deleted_doris_dbs} Doris databases dropped"
        )

        return ResetResponse(
            success=True,
            message="All data has been successfully reset",
            deleted_files=deleted_files,
            deleted_runs=deleted_runs,
            deleted_doris_dbs=deleted_doris_dbs,
        )
        
    except Exception as e:
        logger.error(f"Failed to reset data: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reset data: {str(e)}"
        )


@router.post("/reset", response_model=ResetResponse)
async def reset_all_data():
    """
    Reset all data in the system.

    This endpoint will:
    - Delete all files in data/runs/ directory
    - Clear the runs_index.json file
    - Truncate all database tables (except genesis audit event)
    - Reset in-memory state

    WARNING: This operation is irreversible!
    """
    return await run_in_threadpool(_sync_reset_all_data)


# ============================================
# Bearer token for the public Identity Recognition API (/api/v1)
# ============================================
# One global token (settings.PUBLIC_API_BEARER_TOKEN), not a
# per-caller DB-issued key -- see api/routes_public_identity_api.py's
# require_bearer_token. This endpoint just reads back the CURRENT
# effective value (auto-generated at process startup unless pinned via
# PUBLIC_API_BEARER_TOKEN in .env) so the control plane's /api page can
# show it to whoever needs to configure a bank integration partner.

class ApiTokenResponse(BaseModel):
    bearer_token: str
    source: str  # "env" if pinned via .env or a real exported env var, "auto-generated" if not (changes on every restart)


def _token_is_pinned() -> bool:
    """
    settings.PUBLIC_API_BEARER_TOKEN is loaded by pydantic-settings from
    TWO possible places (SettingsConfigDict(env_file=".env")): a real
    process env var, or a line in the .env file -- neither of which
    populates os.environ (pydantic-settings parses .env itself, it
    does not os.environ.setdefault it), so os.environ.get(...) alone
    would always report "auto-generated" even when a value IS pinned
    via .env. Check both actual sources directly, matching what
    pydantic-settings itself considers "set".
    """
    import os
    if os.environ.get("PUBLIC_API_BEARER_TOKEN"):
        return True
    try:
        with open(".env") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.split("=", 1)[0].strip() == "PUBLIC_API_BEARER_TOKEN":
                    return True
    except FileNotFoundError:
        pass
    return False


def _sync_get_api_token() -> ApiTokenResponse:
    """
    Full synchronous body of get_api_token -- _token_is_pinned() does a
    raw open(".env") file read, so this runs off the event loop via
    run_in_threadpool.
    """
    from api.config import settings

    source = "env" if _token_is_pinned() else "auto-generated"
    return ApiTokenResponse(bearer_token=settings.PUBLIC_API_BEARER_TOKEN, source=source)


@router.get("/api-token", response_model=ApiTokenResponse)
async def get_api_token():
    """Returns the current bearer token guarding /api/v1. Internal-only endpoint -- never exposed on the public sub-app itself."""
    return await run_in_threadpool(_sync_get_api_token)
