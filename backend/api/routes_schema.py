"""
CUIN v2 - Datasource Schema Discovery

Backs Step 1 ("Fields") of the banker-facing Settings UI: profiles
every column in the source Parquet dataset so a non-technical user can
see what's actually available -- not a hardcoded field list -- and get
an explosion-risk verdict on each one before ever touching a blocking
rule.
"""

import logging
import os
import time
from typing import Optional

from fastapi import APIRouter, HTTPException
from starlette.concurrency import run_in_threadpool

from engine.schema.discovery import profile_source

logger = logging.getLogger(__name__)

router = APIRouter()

PARQUET_PATH = "data_source/oracle_data.parquet"

# profile_source() is a genuinely expensive, CPU-bound pyarrow scan --
# ~17s measured live on the real 1.5M-row/10-column dataset (full-scan
# fill-rate/distinct-count/top-values/pairwise-explosion-verdict for
# EVERY column, every call). Two problems, two fixes:
# 1. It was being awaited directly in an `async def` route with no
#    thread-pool offload, so it blocked the ENTIRE FastAPI event loop
#    for its full duration -- every other concurrent request (Dashboard's
#    otherwise-instant /metrics/dashboard, /runs, WebSocket messages,
#    unrelated users entirely) queued behind it and looked "randomly
#    slow" for no reason connected to what THEY were doing. Fixed via
#    run_in_threadpool so it runs off the event loop.
# 2. The source file only changes when someone actually replaces it
#    with a new upload -- a rare, explicit action -- so re-paying the
#    full scan on every single Settings page visit is wasted work.
#    Cached below, keyed on the file's mtime so a real file swap still
#    invalidates it automatically.
_cache_lock_free_profiles = None  # type: Optional[list]
_cache_mtime: Optional[float] = None


async def _get_cached_profiles():
    global _cache_lock_free_profiles, _cache_mtime
    try:
        mtime = os.path.getmtime(PARQUET_PATH)
    except OSError:
        mtime = None

    if _cache_lock_free_profiles is not None and mtime == _cache_mtime:
        return _cache_lock_free_profiles

    start = time.monotonic()
    profiles = await run_in_threadpool(profile_source, PARQUET_PATH)
    logger.info(f"profile_source({PARQUET_PATH}) took {time.monotonic() - start:.1f}s (cache miss)")

    _cache_lock_free_profiles = profiles
    _cache_mtime = mtime
    return profiles


@router.get("")
async def get_schema():
    """Profile every column of the source Parquet dataset (cached until the file changes)."""
    try:
        profiles = await _get_cached_profiles()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to profile source schema: {e}")
    return {
        "source": PARQUET_PATH,
        "fields": [p.to_dict() for p in profiles],
    }
