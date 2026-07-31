"""
CUIN v2 - Datasource Schema Discovery

Backs Step 1 ("Fields") of the banker-facing Settings UI: profiles
every column in the source Parquet dataset so a non-technical user can
see what's actually available -- not a hardcoded field list -- and get
an explosion-risk verdict on each one before ever touching a blocking
rule.
"""

from fastapi import APIRouter, HTTPException

from engine.schema.discovery import profile_source

router = APIRouter()

PARQUET_PATH = "data_source/oracle_data.parquet"


@router.get("")
async def get_schema():
    """Profile every column of the source Parquet dataset."""
    try:
        profiles = profile_source(PARQUET_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to profile source schema: {e}")
    return {
        "source": PARQUET_PATH,
        "fields": [p.to_dict() for p in profiles],
    }
