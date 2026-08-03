"""
CUIN v2 - Entity/Record Search

Greenfield: the earlier audit of this codebase found no search
endpoint, no search UI, and no index -- GET /matches/{pair_id}
"searches" by scanning the 50 most recent runs' CSVs. This searches
the normalized `identifiers` / `customer_scalars` tables of a run's
persisted Doris database (an inverted-index lookup at 10B-row scale is
the eventual target, see the migration plan's Phase 8) -- exact/prefix
match on mobile, email, document value, or customer_code, and
token-containment on name -- rather than the raw source Parquet.
"""

import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from services.run_service import get_run_service
from engine.ports.run_session import open_run_readonly, doris_database_exists, doris_run_db_name

router = APIRouter()


def _run_has_persisted_data(run_id: str) -> bool:
    return doris_database_exists(doris_run_db_name(run_id))


def _most_recent_completed_run_id() -> Optional[str]:
    service = get_run_service()
    runs, _ = service.list_runs(page=1, page_size=50)
    for r in runs:
        if r.status.value == "COMPLETED" and _run_has_persisted_data(r.run_id):
            return r.run_id
    return None


@router.get("")
async def search(
    q: str = Query(..., min_length=1, description="Search term -- mobile/email/document value, name token, or customer code"),
    fields: Optional[str] = Query(None, description="Comma-separated subset of: mobile,email,document,name,customer_code (default: all)"),
    run_id: Optional[str] = Query(None, description="Defaults to the most recent completed run"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
):
    if run_id is None:
        run_id = _most_recent_completed_run_id()
        if run_id is None:
            raise HTTPException(status_code=404, detail="No completed run with a persisted search index found")

    allowed_fields = {"mobile", "email", "document", "name", "customer_code"}
    requested_fields = set(f.strip() for f in fields.split(",")) if fields else allowed_fields
    invalid = requested_fields - allowed_fields
    if invalid:
        raise HTTPException(status_code=400, detail=f"Unknown search field(s): {sorted(invalid)}")

    session = open_run_readonly(run_id)
    con = session.con
    try:
        q_norm = q.strip()
        matched_codes: set = set()

        if requested_fields & {"mobile", "email", "document"}:
            id_types = [f for f in ("mobile", "email", "document") if f in requested_fields]
            placeholders = ",".join(["?"] * len(id_types))
            rows = con.execute(f"""
                SELECT DISTINCT customer_code
                FROM identifiers
                WHERE id_type IN ({placeholders})
                  AND is_valid
                  AND (value_norm = ? OR value_norm LIKE ?)
            """, id_types + [q_norm, f"{q_norm}%"]).fetchall()
            matched_codes.update(r[0] for r in rows)

        if "customer_code" in requested_fields:
            rows = con.execute("""
                SELECT DISTINCT customer_code FROM identifiers
                WHERE customer_code = ? OR customer_code LIKE ?
            """, [q_norm, f"{q_norm}%"]).fetchall()
            matched_codes.update(r[0] for r in rows)
            rows2 = con.execute("""
                SELECT DISTINCT customer_code FROM customer_scalars
                WHERE customer_code = ? OR customer_code LIKE ?
            """, [q_norm, f"{q_norm}%"]).fetchall()
            matched_codes.update(r[0] for r in rows2)

        if "name" in requested_fields:
            q_token = q_norm.upper()
            rows = con.execute("""
                SELECT customer_code FROM customer_scalars
                WHERE array_contains(name_tokens, ?) OR name_norm LIKE ?
            """, [q_token, f"%{q_token}%"]).fetchall()
            matched_codes.update(r[0] for r in rows)

        total = len(matched_codes)
        codes_page = sorted(matched_codes)[(page - 1) * page_size: page * page_size]

        results = []
        if codes_page:
            placeholders = ",".join(["?"] * len(codes_page))
            scalar_rows = con.execute(f"""
                SELECT customer_code, name_norm, dob_iso, dob_precision
                FROM customer_scalars WHERE customer_code IN ({placeholders})
            """, codes_page).fetchall()
            scalars = {r[0]: {"name_norm": r[1], "dob_iso": r[2], "dob_precision": r[3]} for r in scalar_rows}

            id_rows = con.execute(f"""
                SELECT customer_code, id_type, value_norm
                FROM identifiers
                WHERE customer_code IN ({placeholders}) AND is_valid
            """, codes_page).fetchall()
            identifiers_by_code: dict = {}
            for code, id_type, value in id_rows:
                identifiers_by_code.setdefault(code, {}).setdefault(id_type, []).append(value)

            cluster_by_code = _load_cluster_lookup(run_id)

            for code in codes_page:
                results.append({
                    "customer_code": code,
                    "name_norm": scalars.get(code, {}).get("name_norm"),
                    "dob_iso": scalars.get(code, {}).get("dob_iso"),
                    "dob_precision": scalars.get(code, {}).get("dob_precision"),
                    "identifiers": identifiers_by_code.get(code, {}),
                    "cluster_id": cluster_by_code.get(code),
                })

        return {
            "run_id": run_id,
            "engine": session.engine,
            "query": q,
            "fields_searched": sorted(requested_fields),
            "total": total,
            "page": page,
            "page_size": page_size,
            "results": results,
        }
    finally:
        session.close()


def _load_cluster_lookup(run_id: str) -> dict:
    """Best-effort customer_code -> cluster_id map from this run's cluster snapshot."""
    import json
    path = f"data/runs/{run_id}_clusters.json"
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            snapshot = json.load(f)
        lookup = {}
        for cluster_id, members in (snapshot.get("clusters") or snapshot).items():
            member_list = members if isinstance(members, list) else members.get("members", [])
            for m in member_list:
                lookup[m] = cluster_id
        return lookup
    except Exception:
        return {}
