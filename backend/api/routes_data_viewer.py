"""
CUIN v2 - Raw Source Data Viewer API

Lets a bank officer browse the raw, un-normalized source Parquet file
directly -- before running any pipeline -- via a Doris-backed table
(see engine.ports.doris_raw_preview for the materialization/search
mechanics and why: real inverted-index full-text search, fast
pagination, dynamic per-dataset schema, all without touching any
per-run pipeline database).
"""

import json
import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.config import settings
from engine.ports import doris_conn, doris_raw_preview

logger = logging.getLogger(__name__)

router = APIRouter()


def _connect():
    return doris_conn.connect(
        host=settings.DORIS_HOST, port=settings.DORIS_MYSQL_PORT,
        user=settings.DORIS_USER, password=settings.DORIS_PASSWORD,
    )


class FilterCondition(BaseModel):
    column: str
    op: str
    value: Optional[str] = None


class RowsResponse(BaseModel):
    rows: List[dict]
    total: int
    page: int
    page_size: int
    columns: List[dict]


@router.get("/status")
async def get_status():
    """
    Whether the preview table has been materialized yet, its row
    count, and its (dynamically-discovered) column list -- cheap
    (DESCRIBE + COUNT, not a full re-profile), safe to call on every
    page load.
    """
    con = _connect()
    try:
        status = doris_raw_preview.get_status(con)
        if status is None:
            return {"materialized": False, "row_count": 0, "columns": []}
        return {"materialized": True, **status}
    except Exception as e:
        logger.error(f"Failed to get data viewer status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to reach Doris: {e}")
    finally:
        con.close()


@router.post("/refresh")
async def refresh():
    """
    (Re)loads the preview table from the current source Parquet file
    and rebuilds its inverted indexes. Call this after replacing
    data_source/oracle_data.parquet with a new upload -- the preview
    otherwise keeps showing whatever was last materialized.
    """
    con = _connect()
    try:
        result = doris_raw_preview.materialize(con)
        return {"success": True, **result}
    except Exception as e:
        logger.error(f"Failed to refresh data viewer preview: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to materialize source data: {e}")
    finally:
        con.close()


@router.get("/rows", response_model=RowsResponse)
async def get_rows(
    page: int = 1,
    page_size: int = 50,
    q: Optional[str] = None,
    filters: Optional[str] = None,  # JSON-encoded list[FilterCondition], via query string
    sort_col: Optional[str] = None,
    sort_dir: str = "asc",
):
    """
    Paginated, optionally filtered/searched rows from the raw preview
    table. `q` is a quick full-text search OR'd (MATCH_ANY) across
    every column with a usable inverted index. `filters` is a
    JSON-encoded array of {column, op, value} objects, AND'd together
    -- see engine.ports.doris_raw_preview's _OPERATORS/_ARRAY_OPERATORS
    for the valid `op` values per column type (from GET /status's
    `columns[].is_array`).
    """
    if page < 1:
        raise HTTPException(status_code=400, detail="page must be >= 1")
    if not (1 <= page_size <= 500):
        raise HTTPException(status_code=400, detail="page_size must be between 1 and 500")

    parsed_filters = None
    if filters:
        try:
            parsed_filters = json.loads(filters)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="filters must be valid JSON")

    con = _connect()
    try:
        if not doris_raw_preview.table_exists(con):
            raise HTTPException(
                status_code=404,
                detail="Raw data preview has not been materialized yet -- POST /data-viewer/refresh first.",
            )
        return doris_raw_preview.query_rows(
            con, page=page, page_size=page_size, q=q,
            filters=parsed_filters, sort_col=sort_col, sort_dir=sort_dir,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to query data viewer rows: {e}")
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")
    finally:
        con.close()
