"""
CUIN v2 - Datasource Schema Discovery

Backs Step 1 ("Fields") of the banker-facing Settings UI: profiles
every column in the source Parquet dataset so a non-technical user can
see what's actually available -- not a hardcoded field list -- and get
an explosion-risk verdict on each one before ever touching a blocking
rule.
"""

import json
import logging
import os
import time
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from starlette.concurrency import run_in_threadpool

from engine.schema.discovery import profile_source

logger = logging.getLogger(__name__)

router = APIRouter()

PARQUET_PATH = "data_source/oracle_data.parquet"
_DISK_CACHE_PATH = "data/schema_profile_cache.json"

# profile_source() is a genuinely expensive, CPU-bound pyarrow scan --
# 17-34s measured live on the real 1.5M-row/10-column dataset (full-scan
# fill-rate/distinct-count/top-values/pairwise-explosion-verdict for
# EVERY column, every call; the range depends on host load, not a code
# regression -- confirmed by re-measuring on the same unchanged code).
# Three problems, three fixes:
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
#    Cached in-process, keyed on the file's mtime so a real file swap
#    still invalidates it automatically.
# 3. An in-process cache alone still means every single backend
#    restart (a routine, frequent event during active development, and
#    a real one during any prod deploy/upgrade) throws the cache away
#    -- the next person to open Settings after ANY restart eats the
#    full 17-34s again, which is what was actually reported as
#    "Settings gets stuck loading." Fixed two ways: (a) the in-memory
#    cache is now backed by a disk file (data/schema_profile_cache.json,
#    also mtime-keyed against the source Parquet) so a restart with an
#    unchanged source file loads instantly instead of re-scanning, and
#    (b) main.py's startup hook fires off the profiling in the
#    background (fire-and-forget, does not block startup) so that even
#    on a genuine cold cache (first-ever boot, or the source file just
#    changed), the scan is usually already done by the time an actual
#    human clicks into Settings instead of them being the unlucky one
#    to trigger it synchronously.
_cache_fields = None  # type: Optional[list]
_cache_mtime: Optional[float] = None


def _source_mtime() -> Optional[float]:
    try:
        return os.path.getmtime(PARQUET_PATH)
    except OSError:
        return None


def _load_disk_cache(mtime: Optional[float]) -> Optional[list]:
    try:
        with open(_DISK_CACHE_PATH) as f:
            cached = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if cached.get("mtime") != mtime:
        return None
    return cached.get("fields")


def _save_disk_cache(mtime: Optional[float], fields: list) -> None:
    try:
        os.makedirs(os.path.dirname(_DISK_CACHE_PATH) or ".", exist_ok=True)
        with open(_DISK_CACHE_PATH, "w") as f:
            json.dump({"mtime": mtime, "fields": fields}, f)
    except OSError as e:
        logger.warning(f"Failed to write schema profile disk cache: {e}")


def _profile_and_serialize() -> list:
    """Runs the actual expensive scan (call only via run_in_threadpool). Returns JSON-ready dicts."""
    return [p.to_dict() for p in profile_source(PARQUET_PATH)]


async def _get_cached_fields() -> list:
    global _cache_fields, _cache_mtime
    mtime = _source_mtime()

    if _cache_fields is not None and mtime == _cache_mtime:
        return _cache_fields

    disk_fields = _load_disk_cache(mtime)
    if disk_fields is not None:
        logger.info(f"schema profile: loaded from disk cache ({_DISK_CACHE_PATH})")
        _cache_fields = disk_fields
        _cache_mtime = mtime
        return _cache_fields

    start = time.monotonic()
    fields = await run_in_threadpool(_profile_and_serialize)
    logger.info(f"profile_source({PARQUET_PATH}) took {time.monotonic() - start:.1f}s (cache miss)")

    _cache_fields = fields
    _cache_mtime = mtime
    _save_disk_cache(mtime, fields)
    return fields


async def warm_schema_cache() -> None:
    """
    Fire-and-forget at backend startup (see api/main.py's lifespan) --
    not awaited by the caller, just gets the scan started (or the disk
    cache loaded) before the first real request needs it.
    """
    try:
        await _get_cached_fields()
    except Exception as e:
        logger.warning(f"Background schema cache warm-up failed (will retry on next request): {e}")


@router.get("")
async def get_schema():
    """Profile every column of the source Parquet dataset (cached until the file changes)."""
    try:
        fields = await _get_cached_fields()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to profile source schema: {e}")
    return {
        "source": PARQUET_PATH,
        "fields": fields,
    }
