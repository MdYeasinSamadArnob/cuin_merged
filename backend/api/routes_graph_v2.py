"""
CUIN v2 - Identity Graph 360 v2 (paginated, entity-registry-backed)

Purely additive: does not touch, redirect, or replace routes_graph.py,
frontend/src/components/explorer/ClusterGraph.tsx, or TuningPanel.tsx --
those stay exactly as they are, still serving /explorer, /pipeline, and
/runs/[id] unmodified. Mounted at /graph/v2, a prefix that never collides
with the legacy router's real paths (/graph/data, /graph/clusters, etc.
-- see api/main.py).

The old /graph/data endpoint tried to force-simulate the ENTIRE dataset
at once (up to 20,000 raw nodes, no pagination) via an in-memory
ClusterManager singleton that recomputes a full union-find + SHA-256
walk on every single request. This module is built instead on the same
durable, already-proven-fast entity registry (entities/entity_members,
migration 005) and the same engine.ports.run_session read seam already
used throughout api/routes_workbench.py -- every query pattern here
(record_type filtering, name-search, member-name hydration, pairwise
match edges) is a direct adaptation of that file's already-live-verified
logic, not new SQL invented from scratch. See the "Identity Graph 360
rewrite" plan for the full design and the live benchmarks behind it.
"""

import logging
from typing import Any, Dict, List, Optional

import psycopg2
from fastapi import APIRouter, HTTPException, Query

from api.config import settings
from engine.ports.run_session import open_run_readonly
from engine.rules import store as rule_store
from engine.segments.classifier import classify_segment_python, segment_sql_expr, SegmentationConfig
from services.run_service import get_run_service, RunStatus

logger = logging.getLogger(__name__)

router = APIRouter()

# Same throwaway, read-time-only classification config as routes_workbench.py
# -- independent of whether segmentation was enabled for the run that
# produced the data (it's off by default). Never touches pipeline config.
_READ_TIME_SEGMENTATION = SegmentationConfig(enabled=True)
_VALID_RECORD_TYPES = {"ALL", "COMPANY", "INDIVIDUAL"}
_VALID_SORTS = {"size_desc", "size_asc"}


def _pg():
    return psycopg2.connect(settings.DATABASE_URL)


def _resolve_run_id(run_id: Optional[str]) -> str:
    if run_id:
        return run_id
    all_runs, _ = get_run_service().list_runs(page=1, page_size=200)
    for r in all_runs:
        if r.status == RunStatus.COMPLETED:
            return r.run_id
    raise HTTPException(status_code=404, detail="No completed run available")


def _table_exists(session, name: str) -> bool:
    if session.engine == "doris":
        row = session.con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = DATABASE()", [name],
        ).fetchone()
    else:
        row = session.con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]).fetchone()
    return bool(row and row[0])


def _effective_max_cluster_size() -> int:
    """
    The color-tier buckets must reflect the ACTIVE ruleset's cap, not a
    hardcoded 12 -- an officer can change max_cluster_size per policy
    version (routes_rules.py), and a stale constant here would silently
    mis-bucket clusters after such a change.
    """
    try:
        catalog = rule_store.get_active_catalog()
        return catalog.match_ruleset.max_cluster_size
    except Exception:
        logger.warning("Could not load active ruleset for max_cluster_size -- defaulting to 12", exc_info=True)
        return 12


def _size_tier(member_count: int, max_size: int) -> str:
    if member_count <= 1:
        return "small"
    span = max(max_size - 2, 1)
    lo_bound = 2 + span // 3
    hi_bound = 2 + (2 * span) // 3
    if member_count <= lo_bound:
        return "small"
    if member_count <= hi_bound:
        return "medium"
    return "large"


# ----------------------------------------------------------------------
# Overview -- the cluster map
# ----------------------------------------------------------------------

@router.get("/overview")
async def overview(
    run_id: Optional[str] = None, page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=200),
    sort: str = "size_desc", min_size: Optional[int] = None, max_size: Optional[int] = None,
    record_type: Optional[str] = None, has_global_ref: Optional[bool] = None, q: Optional[str] = None,
):
    if record_type is not None and record_type.upper() not in _VALID_RECORD_TYPES:
        raise HTTPException(status_code=400, detail=f"record_type must be one of {sorted(_VALID_RECORD_TYPES)}")
    if sort not in _VALID_SORTS:
        raise HTTPException(status_code=400, detail=f"sort must be one of {sorted(_VALID_SORTS)}")

    rid = _resolve_run_id(run_id)
    max_cluster_size = _effective_max_cluster_size()

    # Same degrade-gracefully pattern as routes_workbench.py::list_entities --
    # q/record_type filtering need the run engine; if it's unavailable, fall
    # back to Postgres-only filtering rather than failing the whole page.
    session = None
    try:
        session = open_run_readonly(rid)
    except Exception:
        logger.warning("Could not open run %s for overview search/filter -- degrading to Postgres-only", rid, exc_info=True)

    q_matched_codes: Optional[List[str]] = None
    company_codes: Optional[List[str]] = None
    try:
        if session and q:
            q_upper = q.strip().upper()
            codes: set = set()
            if _table_exists(session, "identifiers"):
                rows = session.con.execute(
                    "SELECT DISTINCT customer_code FROM identifiers WHERE is_valid AND (value_norm = ? OR value_norm LIKE ?) LIMIT 500",
                    [q_upper, q_upper + "%"],
                ).fetchall()
                codes.update(r[0] for r in rows)
            if _table_exists(session, "customer_scalars"):
                rows = session.con.execute(
                    "SELECT customer_code FROM customer_scalars WHERE customer_code = ? OR name_norm LIKE ? LIMIT 500",
                    [q_upper, "%" + q_upper + "%"],
                ).fetchall()
                codes.update(r[0] for r in rows)
            q_matched_codes = sorted(codes)[:500]

        if session and record_type and record_type.upper() != "ALL":
            expr = segment_sql_expr("name_norm", _READ_TIME_SEGMENTATION, session.dialect)
            rows = session.con.execute(f"SELECT customer_code FROM customer_scalars WHERE ({expr}) = ?", ["COMPANY"]).fetchall()
            company_codes = [r[0] for r in rows]

        pg_conn = _pg()
        try:
            cur = pg_conn.cursor()
            where = ["e.status = 'ACTIVE'"]
            params: list = []
            if has_global_ref is True:
                where.append("e.global_ref IS NOT NULL")
            elif has_global_ref is False:
                where.append("e.global_ref IS NULL")
            if q:
                clause = "(upper(e.global_ref) LIKE upper(%s) OR EXISTS (SELECT 1 FROM entity_members em2 WHERE em2.entity_id = e.entity_id AND em2.customer_code LIKE %s AND em2.valid_to IS NULL)"
                q_params = [f"%{q}%", f"%{q}%"]
                if q_matched_codes:
                    clause += " OR EXISTS (SELECT 1 FROM entity_members em3 WHERE em3.entity_id = e.entity_id AND em3.valid_to IS NULL AND em3.customer_code = ANY(%s::text[]))"
                    q_params.append(q_matched_codes)
                clause += ")"
                where.append(clause)
                params.extend(q_params)
            if record_type and record_type.upper() != "ALL" and company_codes is not None:
                rt = record_type.upper()
                exists_company_member = "EXISTS (SELECT 1 FROM entity_members em4 WHERE em4.entity_id = e.entity_id AND em4.valid_to IS NULL AND em4.customer_code = ANY(%s::text[]))"
                if rt == "COMPANY":
                    where.append(exists_company_member)
                else:
                    where.append(f"NOT {exists_company_member}")
                params.append(company_codes)
            where_sql = " AND ".join(where)

            base_from = f"""
                FROM entities e
                JOIN (SELECT entity_id, COUNT(*) AS member_count FROM entity_members WHERE valid_to IS NULL GROUP BY entity_id) mc
                    ON mc.entity_id = e.entity_id
                WHERE {where_sql}
            """
            size_params: list = []
            if min_size is not None:
                base_from += " AND mc.member_count >= %s"
                size_params.append(min_size)
            if max_size is not None:
                base_from += " AND mc.member_count <= %s"
                size_params.append(max_size)
            all_params = params + size_params

            cur.execute(f"SELECT COUNT(*) {base_from}", all_params)
            total = cur.fetchone()[0]

            order_sql = "mc.member_count DESC" if sort == "size_desc" else "mc.member_count ASC"
            offset = (page - 1) * page_size
            cur.execute(
                f"SELECT e.entity_id::text, mc.member_count, e.global_ref, e.global_ref_state, e.created_at "
                f"{base_from} ORDER BY {order_sql}, e.entity_id LIMIT %s OFFSET %s",
                all_params + [page_size, offset],
            )
            page_rows = cur.fetchall()

            entity_ids = [r[0] for r in page_rows]
            preview_codes_by_entity: Dict[str, List[str]] = {}
            if entity_ids:
                cur.execute("""
                    SELECT entity_id, customer_code FROM (
                        SELECT entity_id::text AS entity_id, customer_code,
                               ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY customer_code) AS rn
                        FROM entity_members WHERE entity_id::text = ANY(%s::text[]) AND valid_to IS NULL
                    ) ranked WHERE rn <= 3
                """, (entity_ids,))
                for eid, code in cur.fetchall():
                    preview_codes_by_entity.setdefault(eid, []).append(code)
        finally:
            pg_conn.close()

        preview_names_by_code: Dict[str, str] = {}
        all_preview_codes = sorted({c for codes in preview_codes_by_entity.values() for c in codes})
        if session and all_preview_codes:
            placeholders = ",".join("?" for _ in all_preview_codes)
            rows = session.con.execute(
                f"SELECT customer_code, name_norm FROM customer_scalars WHERE customer_code IN ({placeholders})",
                all_preview_codes,
            ).fetchall()
            preview_names_by_code = dict(rows)
    finally:
        if session:
            session.close()

    items = []
    for eid, member_count, ref, ref_state, created in page_rows:
        preview_codes = preview_codes_by_entity.get(eid, [])
        preview_names = [preview_names_by_code.get(c) or c for c in preview_codes]
        record_type_val = None
        if preview_names:
            record_type_val = "COMPANY" if any(
                classify_segment_python(n, _READ_TIME_SEGMENTATION) == "COMPANY" for n in preview_names
            ) else "INDIVIDUAL"
        items.append({
            "entity_id": eid, "member_count": member_count, "global_ref": ref, "global_ref_state": ref_state,
            "created_at": created.isoformat() if created else None,
            "representative_names": preview_names, "record_type": record_type_val,
            "size_tier": _size_tier(member_count, max_cluster_size),
        })

    return {
        "items": items, "total": total, "page": page, "page_size": page_size,
        "run_id": rid, "max_cluster_size": max_cluster_size,
    }


# ----------------------------------------------------------------------
# Stats -- the insights bar
# ----------------------------------------------------------------------

@router.get("/stats")
async def stats(run_id: Optional[str] = None):
    rid = _resolve_run_id(run_id)
    max_cluster_size = _effective_max_cluster_size()

    engine = None
    singletons = 0
    total_records = 0
    company_records = 0
    individual_records = 0
    session = open_run_readonly(rid)
    try:
        engine = session.engine
        if _table_exists(session, "customer_scalars"):
            row = session.con.execute("SELECT COUNT(*) FROM customer_scalars").fetchone()
            total_records = row[0] if row else 0

            expr = segment_sql_expr("name_norm", _READ_TIME_SEGMENTATION, session.dialect)
            rows = session.con.execute(f"SELECT ({expr}) AS seg, COUNT(*) FROM customer_scalars GROUP BY 1").fetchall()
            for seg, n in rows:
                if seg == "COMPANY":
                    company_records = n
                elif seg == "INDIVIDUAL":
                    individual_records = n

        if _table_exists(session, "customer_scalars") and _table_exists(session, "candidate_pairs"):
            row = session.con.execute("""
                SELECT COUNT(*) FROM customer_scalars cs
                WHERE NOT EXISTS (SELECT 1 FROM candidate_pairs cp WHERE cp.a_key = cs.customer_code OR cp.b_key = cs.customer_code)
            """).fetchone()
            singletons = row[0] if row else 0
    finally:
        session.close()

    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute("""
            SELECT COUNT(*), COALESCE(MAX(mc.member_count), 0), COALESCE(AVG(mc.member_count), 0)
            FROM entities e
            JOIN (SELECT entity_id, COUNT(*) AS member_count FROM entity_members WHERE valid_to IS NULL GROUP BY entity_id) mc
                ON mc.entity_id = e.entity_id
            WHERE e.status = 'ACTIVE'
        """)
        total_clusters, largest_cluster, avg_size = cur.fetchone()

        cur.execute("SELECT COUNT(*) FROM entities WHERE status = 'ACTIVE' AND global_ref_state = 'CONFIRMED'")
        with_global_ref = cur.fetchone()[0]
    finally:
        pg_conn.close()

    return {
        "run_id": rid, "engine": engine,
        "total_clusters": total_clusters, "total_records": total_records, "singletons": singletons,
        "largest_cluster_size": largest_cluster, "avg_cluster_size": round(float(avg_size), 2),
        "entities_with_global_ref": with_global_ref,
        "company_records": company_records, "individual_records": individual_records,
        "max_cluster_size": max_cluster_size,
    }


# ----------------------------------------------------------------------
# Canvas -- Classic mode's data source
# ----------------------------------------------------------------------

@router.get("/canvas")
async def canvas(
    run_id: Optional[str] = None, page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=60),
    sort: str = "size_desc", min_size: Optional[int] = None, max_size: Optional[int] = None,
    record_type: Optional[str] = None, has_global_ref: Optional[bool] = None, q: Optional[str] = None,
):
    """
    Classic mode's data source -- a small PAGE of clusters (default 20,
    capped at 60: each cluster expands to up to max_cluster_size member
    nodes plus their pairwise edges, so this stays bounded to roughly
    page_size * max_cluster_size nodes, not the old page's up-to-20,000
    in one shot). Returns the SAME {nodes, edges} shape the legacy
    /graph/data endpoint did (NodeModel/EdgeModel, routes_graph.py) so it
    drops straight into the forked ClassicClusterGraph component with no
    client-side reshaping. Filter-building logic mirrors /overview
    exactly (same q/record_type two-step pattern) -- duplicated rather
    than refactored into a shared helper to avoid any risk of changing
    /overview's already-verified behavior.
    """
    if record_type is not None and record_type.upper() not in _VALID_RECORD_TYPES:
        raise HTTPException(status_code=400, detail=f"record_type must be one of {sorted(_VALID_RECORD_TYPES)}")
    if sort not in _VALID_SORTS:
        raise HTTPException(status_code=400, detail=f"sort must be one of {sorted(_VALID_SORTS)}")

    rid = _resolve_run_id(run_id)
    max_cluster_size = _effective_max_cluster_size()

    session = None
    try:
        session = open_run_readonly(rid)
    except Exception:
        logger.warning("Could not open run %s for canvas search/filter -- degrading to Postgres-only", rid, exc_info=True)

    q_matched_codes: Optional[List[str]] = None
    company_codes: Optional[List[str]] = None
    try:
        if session and q:
            q_upper = q.strip().upper()
            codes: set = set()
            if _table_exists(session, "identifiers"):
                rows = session.con.execute(
                    "SELECT DISTINCT customer_code FROM identifiers WHERE is_valid AND (value_norm = ? OR value_norm LIKE ?) LIMIT 500",
                    [q_upper, q_upper + "%"],
                ).fetchall()
                codes.update(r[0] for r in rows)
            if _table_exists(session, "customer_scalars"):
                rows = session.con.execute(
                    "SELECT customer_code FROM customer_scalars WHERE customer_code = ? OR name_norm LIKE ? LIMIT 500",
                    [q_upper, "%" + q_upper + "%"],
                ).fetchall()
                codes.update(r[0] for r in rows)
            q_matched_codes = sorted(codes)[:500]

        if session and record_type and record_type.upper() != "ALL":
            expr = segment_sql_expr("name_norm", _READ_TIME_SEGMENTATION, session.dialect)
            rows = session.con.execute(f"SELECT customer_code FROM customer_scalars WHERE ({expr}) = ?", ["COMPANY"]).fetchall()
            company_codes = [r[0] for r in rows]

        pg_conn = _pg()
        try:
            cur = pg_conn.cursor()
            where = ["e.status = 'ACTIVE'"]
            params: list = []
            if has_global_ref is True:
                where.append("e.global_ref IS NOT NULL")
            elif has_global_ref is False:
                where.append("e.global_ref IS NULL")
            if q:
                clause = "(upper(e.global_ref) LIKE upper(%s) OR EXISTS (SELECT 1 FROM entity_members em2 WHERE em2.entity_id = e.entity_id AND em2.customer_code LIKE %s AND em2.valid_to IS NULL)"
                q_params = [f"%{q}%", f"%{q}%"]
                if q_matched_codes:
                    clause += " OR EXISTS (SELECT 1 FROM entity_members em3 WHERE em3.entity_id = e.entity_id AND em3.valid_to IS NULL AND em3.customer_code = ANY(%s::text[]))"
                    q_params.append(q_matched_codes)
                clause += ")"
                where.append(clause)
                params.extend(q_params)
            if record_type and record_type.upper() != "ALL" and company_codes is not None:
                rt = record_type.upper()
                exists_company_member = "EXISTS (SELECT 1 FROM entity_members em4 WHERE em4.entity_id = e.entity_id AND em4.valid_to IS NULL AND em4.customer_code = ANY(%s::text[]))"
                if rt == "COMPANY":
                    where.append(exists_company_member)
                else:
                    where.append(f"NOT {exists_company_member}")
                params.append(company_codes)
            where_sql = " AND ".join(where)

            base_from = f"""
                FROM entities e
                JOIN (SELECT entity_id, COUNT(*) AS member_count FROM entity_members WHERE valid_to IS NULL GROUP BY entity_id) mc
                    ON mc.entity_id = e.entity_id
                WHERE {where_sql}
            """
            size_params: list = []
            if min_size is not None:
                base_from += " AND mc.member_count >= %s"
                size_params.append(min_size)
            if max_size is not None:
                base_from += " AND mc.member_count <= %s"
                size_params.append(max_size)
            all_params = params + size_params

            cur.execute(f"SELECT COUNT(*) {base_from}", all_params)
            total = cur.fetchone()[0]

            order_sql = "mc.member_count DESC" if sort == "size_desc" else "mc.member_count ASC"
            offset = (page - 1) * page_size
            cur.execute(
                f"SELECT e.entity_id::text, mc.member_count, e.global_ref, e.global_ref_state "
                f"{base_from} ORDER BY {order_sql}, e.entity_id LIMIT %s OFFSET %s",
                all_params + [page_size, offset],
            )
            entity_rows = cur.fetchall()

            entity_ids = [r[0] for r in entity_rows]
            member_rows: List[tuple] = []
            if entity_ids:
                cur.execute(
                    "SELECT entity_id::text, customer_code FROM entity_members "
                    "WHERE entity_id::text = ANY(%s::text[]) AND valid_to IS NULL",
                    (entity_ids,),
                )
                member_rows = cur.fetchall()
        finally:
            pg_conn.close()

        members_by_entity: Dict[str, List[str]] = {}
        for eid, code in member_rows:
            members_by_entity.setdefault(eid, []).append(code)
        all_codes = sorted({c for codes in members_by_entity.values() for c in codes})

        names: Dict[str, str] = {}
        dobs: Dict[str, str] = {}
        segments: Dict[str, str] = {}
        identifiers_by_code: Dict[str, Dict[str, List[str]]] = {}
        edges_raw: List[tuple] = []
        engine = None
        if session:
            engine = session.engine
            if all_codes:
                placeholders = ",".join("?" for _ in all_codes)
                if _table_exists(session, "customer_scalars"):
                    rows = session.con.execute(
                        f"SELECT customer_code, name_norm, dob_iso FROM customer_scalars WHERE customer_code IN ({placeholders})",
                        all_codes,
                    ).fetchall()
                    for code, name, dob in rows:
                        names[code] = name
                        dobs[code] = dob
                        segments[code] = classify_segment_python(name, _READ_TIME_SEGMENTATION)
                if _table_exists(session, "identifiers"):
                    id_rows = session.con.execute(
                        f"SELECT customer_code, id_type, value_norm FROM identifiers WHERE customer_code IN ({placeholders}) AND is_valid",
                        all_codes,
                    ).fetchall()
                    for code, id_type, value in id_rows:
                        identifiers_by_code.setdefault(code, {}).setdefault(id_type, []).append(value)
                if _table_exists(session, "pair_decisions"):
                    edges_raw = session.con.execute(
                        f"SELECT a_key, b_key, confidence_pct, has_veto, decision FROM pair_decisions "
                        f"WHERE a_key IN ({placeholders}) AND b_key IN ({placeholders})",
                        all_codes + all_codes,
                    ).fetchall()
    finally:
        if session:
            session.close()

    nodes: List[dict] = []
    edges: List[dict] = []
    for eid, member_count, ref, ref_state in entity_rows:
        preview = members_by_entity.get(eid, [])[:1]
        primary_name = names.get(preview[0]) if preview else None
        nodes.append({
            "id": eid, "label": primary_name or eid[:8], "type": "cluster",
            "properties": {
                "size": member_count, "size_tier": _size_tier(member_count, max_cluster_size),
                "global_ref": ref, "global_ref_state": ref_state,
            },
        })
        for code in members_by_entity.get(eid, []):
            edges.append({"source": eid, "target": code, "type": "MEMBER_OF", "weight": 1.0, "properties": {}})

    seen_codes: set = set()
    for codes in members_by_entity.values():
        for code in codes:
            if code in seen_codes:
                continue
            seen_codes.add(code)
            ids = identifiers_by_code.get(code, {})
            nodes.append({
                "id": code, "label": names.get(code) or code, "type": "record",
                "properties": {
                    "segment": segments.get(code), "dob_norm": dobs.get(code),
                    "phone_norm": (ids.get("mobile") or [None])[0],
                    "email_norm": (ids.get("email") or [None])[0],
                    "address_norm": (ids.get("address") or [None])[0],
                    "source_customer_id": code,
                },
            })

    for a, b, conf, veto, dec in edges_raw:
        # ClassicClusterGraph (forked from ClusterGraph.tsx) only special-cases
        # edge.type === 'MATCHES' (blue) and 'REVIEW' (amber); anything else
        # falls back to a neutral gray -- map AUTO_LINK to MATCHES so the
        # visual convention already established there carries over unchanged.
        edges.append({
            "source": a, "target": b,
            "type": "MATCHES" if dec == "AUTO_LINK" else ("REVIEW" if dec == "REVIEW" else "REJECT"),
            "weight": float(conf) / 100.0,
            "properties": {"confidence_pct": float(conf), "has_veto": bool(veto), "decision": dec},
        })

    return {
        "nodes": nodes, "edges": edges, "engine": engine,
        "run_id": rid, "page": page, "page_size": page_size, "total": total, "max_cluster_size": max_cluster_size,
    }


# ----------------------------------------------------------------------
# Singletons -- records with no candidate_pairs edge at all
# ----------------------------------------------------------------------

@router.get("/singletons")
async def singletons(
    run_id: Optional[str] = None, page: int = Query(1, ge=1), page_size: int = Query(60, ge=1, le=200),
    record_type: Optional[str] = None, q: Optional[str] = None,
):
    """
    The 977,045 records /graph/v2/stats already counts as "singletons"
    (no candidate_pairs row at all -- never even blocked against another
    record) but that Overview previously had no way to browse. Same
    NOT EXISTS predicate as the stats count, turned into a paginated
    SELECT with the same q/record_type filter pattern used elsewhere in
    this module.
    """
    if record_type is not None and record_type.upper() not in _VALID_RECORD_TYPES:
        raise HTTPException(status_code=400, detail=f"record_type must be one of {sorted(_VALID_RECORD_TYPES)}")

    rid = _resolve_run_id(run_id)
    session = open_run_readonly(rid)
    try:
        if not (_table_exists(session, "customer_scalars") and _table_exists(session, "candidate_pairs")):
            return {"items": [], "total": 0, "page": page, "page_size": page_size, "run_id": rid}

        where = ["NOT EXISTS (SELECT 1 FROM candidate_pairs cp WHERE cp.a_key = cs.customer_code OR cp.b_key = cs.customer_code)"]
        params: list = []
        if q:
            q_upper = q.strip().upper()
            where.append("(cs.customer_code = ? OR cs.name_norm LIKE ?)")
            params.extend([q_upper, "%" + q_upper + "%"])
        if record_type and record_type.upper() != "ALL":
            expr = segment_sql_expr("cs.name_norm", _READ_TIME_SEGMENTATION, session.dialect)
            where.append(f"({expr}) = ?")
            params.append(record_type.upper())
        where_sql = " AND ".join(where)

        total = session.con.execute(f"SELECT COUNT(*) FROM customer_scalars cs WHERE {where_sql}", params).fetchone()[0]

        offset = (page - 1) * page_size
        rows = session.con.execute(
            f"SELECT cs.customer_code, cs.name_norm FROM customer_scalars cs WHERE {where_sql} "
            f"ORDER BY cs.customer_code LIMIT ? OFFSET ?",
            params + [page_size, offset],
        ).fetchall()
        engine = session.engine
    finally:
        session.close()

    items = [
        {"customer_code": code, "name_norm": name, "record_type": classify_segment_python(name, _READ_TIME_SEGMENTATION)}
        for code, name in rows
    ]
    return {"items": items, "total": total, "page": page, "page_size": page_size, "run_id": rid, "engine": engine}


# ----------------------------------------------------------------------
# Cluster drill-down
# ----------------------------------------------------------------------

@router.get("/cluster/{entity_id}")
async def cluster_detail(entity_id: str, run_id: Optional[str] = None):
    rid = _resolve_run_id(run_id)
    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT entity_id::text, global_ref, global_ref_state, status FROM entities WHERE entity_id = %s",
            (entity_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")
        eid, global_ref, global_ref_state, status = row

        cur.execute(
            "SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL ORDER BY customer_code",
            (entity_id,),
        )
        codes = [r[0] for r in cur.fetchall()]
    finally:
        pg_conn.close()

    if not codes:
        return {"run_id": rid, "entity_id": eid, "global_ref": global_ref, "global_ref_state": global_ref_state, "status": status, "nodes": [], "edges": []}

    session = open_run_readonly(rid)
    try:
        placeholders = ",".join("?" for _ in codes)
        names: Dict[str, str] = {}
        if _table_exists(session, "customer_scalars"):
            name_rows = session.con.execute(
                f"SELECT customer_code, name_norm FROM customer_scalars WHERE customer_code IN ({placeholders})", codes,
            ).fetchall()
            names = dict(name_rows)

        edges = []
        if _table_exists(session, "pair_decisions"):
            edge_rows = session.con.execute(f"""
                SELECT a_key, b_key, confidence_pct, has_veto, decision
                FROM pair_decisions WHERE a_key IN ({placeholders}) AND b_key IN ({placeholders})
            """, codes + codes).fetchall()
            edges = [
                {"source": a, "target": b, "confidence_pct": float(c), "has_veto": bool(v), "decision": d}
                for a, b, c, v, d in edge_rows
            ]
        engine = session.engine
    finally:
        session.close()

    nodes = [
        {"id": code, "label": names.get(code) or code, "segment": classify_segment_python(names.get(code), _READ_TIME_SEGMENTATION)}
        for code in codes
    ]

    return {
        "run_id": rid, "engine": engine, "entity_id": eid, "global_ref": global_ref, "global_ref_state": global_ref_state,
        "status": status, "nodes": nodes, "edges": edges,
    }


# ----------------------------------------------------------------------
# Relationship hops -- bounded N-hop neighborhood explorer
# ----------------------------------------------------------------------

@router.get("/hops")
async def hops(
    customer_code: Optional[str] = None, entity_id: Optional[str] = None, run_id: Optional[str] = None,
    hops: int = Query(2, ge=1, le=3), max_nodes: int = Query(300, ge=10, le=1000),
):
    """
    K rounds of plain iterative joins (NOT a recursive CTE -- this
    codebase has never used WITH RECURSIVE and neither SQL dialect has a
    helper for it; live-probed instead: a 300-code IN-list on both sides
    of candidate_pairs runs in ~50-120ms per round on Doris). Same-segment
    edges come from the run's own candidate_pairs; cross-segment edges
    come from Postgres' entity_relationships, bridged through
    customers_norm since that table's a_key/b_key are UUIDs referencing
    customers_norm.customer_key, NOT the VARCHAR customer_code everything
    else here uses -- confirmed live, this join is required or cross-
    segment edges silently come back empty.

    entity_relationships is only populated for runs where segmentation
    was enabled (off by default for this dataset) -- for most runs the
    cross-segment layer legitimately contributes nothing, which is
    correct behavior, not a bug.
    """
    if not customer_code and not entity_id:
        raise HTTPException(status_code=400, detail="Provide customer_code or entity_id")
    rid = _resolve_run_id(run_id)

    seed_codes: List[str] = []
    if entity_id:
        pg_conn = _pg()
        try:
            cur = pg_conn.cursor()
            cur.execute(
                "SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (entity_id,),
            )
            seed_codes = [r[0] for r in cur.fetchall()]
        finally:
            pg_conn.close()
        if not seed_codes:
            raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found or has no current members")
    else:
        seed_codes = [customer_code]

    session = open_run_readonly(rid)
    pg_conn = _pg()
    try:
        has_candidate_pairs = _table_exists(session, "candidate_pairs")
        visited = set(seed_codes)
        frontier = set(seed_codes)
        raw_edges: List[tuple] = []  # (a, b, kind)
        truncated = False
        cur = pg_conn.cursor()

        for _ in range(hops):
            if not frontier or len(visited) >= max_nodes:
                break
            frontier_list = list(frontier)
            discovered = set()

            if has_candidate_pairs:
                placeholders = ",".join("?" for _ in frontier_list)
                rows = session.con.execute(
                    f"SELECT a_key, b_key FROM candidate_pairs WHERE a_key IN ({placeholders}) OR b_key IN ({placeholders})",
                    frontier_list + frontier_list,
                ).fetchall()
                for a, b in rows:
                    raw_edges.append((a, b, "SAME_SEGMENT"))
                    discovered.add(a)
                    discovered.add(b)

            # Cross-segment edges must ALSO expand the frontier each round --
            # a cross-segment partner is, by definition, never reachable via
            # candidate_pairs (that's the whole reason it's a relationship,
            # not a match: see engine.segments.classifier's docstring). If
            # this only ran once at the end over the same-segment-only
            # visited set, cross-segment edges would almost never appear,
            # since their other endpoint would essentially never already be
            # in `visited` -- confirmed live during Stage B testing.
            cur.execute("""
                SELECT ca.source_customer_id, cb.source_customer_id
                FROM entity_relationships r
                JOIN customers_norm ca ON ca.customer_key = r.a_key
                JOIN customers_norm cb ON cb.customer_key = r.b_key
                WHERE ca.source_customer_id = ANY(%s::text[]) OR cb.source_customer_id = ANY(%s::text[])
            """, (frontier_list, frontier_list))
            for a, b in cur.fetchall():
                raw_edges.append((a, b, "CROSS_SEGMENT"))
                discovered.add(a)
                discovered.add(b)

            new_codes = discovered - visited
            budget = max_nodes - len(visited)
            if len(new_codes) > budget:
                truncated = True
                new_codes = set(sorted(new_codes)[:budget])

            visited |= new_codes
            frontier = new_codes

        edges_by_key: Dict[tuple, str] = {}
        for a, b, kind in raw_edges:
            if a not in visited or b not in visited:
                continue
            key = tuple(sorted((a, b)))
            # A pair CAN legitimately appear in both tables -- blocking is
            # segment-agnostic (a Company and an Individual sharing a phone
            # number still become a candidate_pairs row), and scoring then
            # diverts genuinely cross-segment pairs into entity_relationships
            # instead of clustering them (see engine.segments.classifier's
            # module docstring). When both exist, CROSS_SEGMENT is the more
            # informative label: it means blocking flagged them as similar
            # but the pipeline correctly recognized they're different
            # segments and did NOT treat it as an identity match -- confirmed
            # live during Stage B testing (00085732/00023873 is exactly such
            # a pair).
            if key not in edges_by_key or kind == "CROSS_SEGMENT":
                edges_by_key[key] = kind
        edges = [{"source": a, "target": b, "kind": kind} for (a, b), kind in edges_by_key.items()]

        names: Dict[str, str] = {}
        if _table_exists(session, "customer_scalars") and visited:
            placeholders = ",".join("?" for _ in visited)
            rows = session.con.execute(
                f"SELECT customer_code, name_norm FROM customer_scalars WHERE customer_code IN ({placeholders})", list(visited),
            ).fetchall()
            names = dict(rows)
        engine = session.engine
    finally:
        session.close()
        pg_conn.close()

    nodes = [
        {"id": code, "label": names.get(code) or code,
         "segment": classify_segment_python(names.get(code), _READ_TIME_SEGMENTATION), "is_seed": code in seed_codes}
        for code in visited
    ]

    return {
        "run_id": rid, "engine": engine, "seed_codes": seed_codes, "hops": hops, "max_nodes": max_nodes,
        "nodes": nodes, "edges": edges, "truncated": truncated,
    }


# ----------------------------------------------------------------------
# Bridges -- cross-cluster connections (a shared phone/address/document
# strong enough to blocking-match but not strong enough, or wrong
# segment, to merge into one entity)
# ----------------------------------------------------------------------

@router.get("/cluster/{entity_id}/bridges")
async def cluster_bridges(entity_id: str, run_id: Optional[str] = None):
    """
    This cluster's external connections: other clusters whose members
    share real matching evidence with THIS cluster's members but never
    merged into it (REVIEW-grade same-segment pairs that stopped short
    of AUTO_LINK, or cross-segment relationships via entity_relationships).
    Same frontier-expansion shape already proven in /hops -- "the other
    side of a pair touching one of my members, that isn't also one of my
    members" -- just grouped by the external side's CURRENT entity_id
    instead of flattened into one neighborhood.
    """
    rid = _resolve_run_id(run_id)

    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL ORDER BY customer_code",
            (entity_id,),
        )
        codes = [r[0] for r in cur.fetchall()]
        if not codes:
            raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found or has no current members")
        member_set = set(codes)

        # Cross-segment: same bridge-through-customers_norm shape as /hops.
        cur.execute("""
            SELECT ca.source_customer_id, cb.source_customer_id, r.shared_evidence
            FROM entity_relationships r
            JOIN customers_norm ca ON ca.customer_key = r.a_key
            JOIN customers_norm cb ON cb.customer_key = r.b_key
            WHERE ca.source_customer_id = ANY(%s::text[]) OR cb.source_customer_id = ANY(%s::text[])
        """, (codes, codes))
        cross_rows = cur.fetchall()
    finally:
        pg_conn.close()

    # (member_code, external_code, confidence_pct, has_veto, decision, kind, evidence)
    external_pairs: List[tuple] = []
    for a, b, evidence in cross_rows:
        if a in member_set and b not in member_set:
            external_pairs.append((a, b, None, False, "CROSS_SEGMENT", "CROSS_SEGMENT", evidence))
        elif b in member_set and a not in member_set:
            external_pairs.append((b, a, None, False, "CROSS_SEGMENT", "CROSS_SEGMENT", evidence))

    session = open_run_readonly(rid)
    try:
        engine = session.engine
        if _table_exists(session, "pair_decisions"):
            placeholders = ",".join("?" for _ in codes)
            rows = session.con.execute(
                f"SELECT a_key, b_key, confidence_pct, has_veto, decision FROM pair_decisions "
                f"WHERE (a_key IN ({placeholders}) OR b_key IN ({placeholders})) AND decision != 'REJECT'",
                codes + codes,
            ).fetchall()
            for a, b, conf, veto, dec in rows:
                if a in member_set and b not in member_set:
                    external_pairs.append((a, b, float(conf), bool(veto), dec, "SAME_SEGMENT", None))
                elif b in member_set and a not in member_set:
                    external_pairs.append((b, a, float(conf), bool(veto), dec, "SAME_SEGMENT", None))

        if not external_pairs:
            return {"run_id": rid, "engine": engine, "entity_id": entity_id, "bridges": []}

        external_codes = sorted({p[1] for p in external_pairs})
        names: Dict[str, str] = {}
        all_lookup_codes = sorted(set(codes) | set(external_codes))
        if _table_exists(session, "customer_scalars") and all_lookup_codes:
            placeholders = ",".join("?" for _ in all_lookup_codes)
            rows = session.con.execute(
                f"SELECT customer_code, name_norm FROM customer_scalars WHERE customer_code IN ({placeholders})",
                all_lookup_codes,
            ).fetchall()
            names = dict(rows)
    finally:
        session.close()

    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT customer_code, entity_id::text FROM entity_members WHERE customer_code = ANY(%s::text[]) AND valid_to IS NULL",
            (external_codes,),
        )
        entity_by_code = dict(cur.fetchall())

        groups: Dict[str, dict] = {}
        for member_code, ext_code, conf, veto, dec, kind, evidence in external_pairs:
            target_entity = entity_by_code.get(ext_code)
            if not target_entity or target_entity == entity_id:
                continue
            g = groups.setdefault(target_entity, {"target_entity_id": target_entity, "connecting_pairs": []})
            g["connecting_pairs"].append({
                "code_in_cluster": member_code, "code_in_cluster_name": names.get(member_code) or member_code,
                "code_in_target": ext_code, "code_in_target_name": names.get(ext_code) or ext_code,
                "confidence_pct": conf, "has_veto": veto, "decision": dec, "kind": kind, "evidence": evidence,
            })

        target_ids = list(groups.keys())
        if target_ids:
            cur.execute("""
                SELECT e.entity_id::text, e.global_ref, e.global_ref_state, mc.member_count
                FROM entities e
                JOIN (SELECT entity_id, COUNT(*) AS member_count FROM entity_members WHERE valid_to IS NULL GROUP BY entity_id) mc
                    ON mc.entity_id = e.entity_id
                WHERE e.entity_id::text = ANY(%s::text[])
            """, (target_ids,))
            for tid, ref, ref_state, mc in cur.fetchall():
                groups[tid]["global_ref"] = ref
                groups[tid]["global_ref_state"] = ref_state
                groups[tid]["member_count"] = mc
    finally:
        pg_conn.close()

    bridges = sorted(groups.values(), key=lambda g: -len(g["connecting_pairs"]))
    for g in bridges:
        g["target_representative_name"] = g["connecting_pairs"][0]["code_in_target_name"] if g["connecting_pairs"] else None

    return {"run_id": rid, "engine": engine, "entity_id": entity_id, "bridges": bridges}


@router.get("/bridges")
async def bridges(
    run_id: Optional[str] = None, page: int = Query(1, ge=1), page_size: int = Query(30, ge=1, le=100),
    min_confidence: Optional[float] = None,
):
    """
    Global analytics: every pair of DIFFERENT clusters connected by real
    evidence -- a same-segment REVIEW/AUTO_LINK-grade pair whose two
    sides currently sit in two different entities (can legitimately
    happen: officer overrides, or a pair scored after one side already
    merged elsewhere), plus cross-segment entity_relationships bridged
    through customers_norm exactly as /hops already does. Live-verified
    before shipping: bulk-fetching all non-REJECT pair_decisions
    (270,994 rows) took 2.0-2.8s from Doris; cross-referencing against a
    bulk entity_members fetch (126,353 rows, 0.19s) and aggregating took
    0.24s -- ~3.2s total, acceptable for an on-demand analytics call with
    a loading state, not a hot-path list.

    When a pair has BOTH a same-segment pair_decisions row and a
    cross-segment entity_relationships row (blocking is segment-agnostic
    -- see engine.segments.classifier's docstring), CROSS_SEGMENT wins,
    same priority rule as /hops.
    """
    rid = _resolve_run_id(run_id)

    session = open_run_readonly(rid)
    try:
        same_segment_rows: List[tuple] = []
        if _table_exists(session, "pair_decisions"):
            q = "SELECT a_key, b_key, confidence_pct, has_veto, decision FROM pair_decisions WHERE decision != 'REJECT'"
            params: list = []
            if min_confidence is not None:
                q += " AND confidence_pct >= ?"
                params.append(min_confidence)
            same_segment_rows = session.con.execute(q, params).fetchall()
        engine = session.engine
    finally:
        session.close()

    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute("SELECT customer_code, entity_id::text FROM entity_members WHERE valid_to IS NULL")
        entity_by_code = dict(cur.fetchall())

        cur.execute("""
            SELECT ca.source_customer_id, cb.source_customer_id, r.shared_evidence
            FROM entity_relationships r
            JOIN customers_norm ca ON ca.customer_key = r.a_key
            JOIN customers_norm cb ON cb.customer_key = r.b_key
        """)
        cross_rows = cur.fetchall()

        agg: Dict[tuple, dict] = {}
        for a, b, conf, veto, dec in same_segment_rows:
            ea, eb = entity_by_code.get(a), entity_by_code.get(b)
            if not ea or not eb or ea == eb:
                continue
            key = tuple(sorted((ea, eb)))
            conf_f = float(conf)
            code_a, code_b = (a, b) if ea == key[0] else (b, a)
            existing = agg.get(key)
            if not existing or (existing["kind"] == "SAME_SEGMENT" and conf_f > existing["confidence_pct"]):
                agg[key] = {
                    "entity_id_a": key[0], "entity_id_b": key[1], "kind": "SAME_SEGMENT",
                    "code_a": code_a, "code_b": code_b,
                    "confidence_pct": conf_f, "decision": dec, "has_veto": bool(veto), "evidence": None,
                }

        for a, b, evidence in cross_rows:
            ea, eb = entity_by_code.get(a), entity_by_code.get(b)
            if not ea or not eb or ea == eb:
                continue
            key = tuple(sorted((ea, eb)))
            code_a, code_b = (a, b) if ea == key[0] else (b, a)
            existing = agg.get(key)
            if not existing or existing["kind"] != "CROSS_SEGMENT":
                agg[key] = {
                    "entity_id_a": key[0], "entity_id_b": key[1], "kind": "CROSS_SEGMENT",
                    "code_a": code_a, "code_b": code_b,
                    "confidence_pct": None, "decision": "CROSS_SEGMENT", "has_veto": False, "evidence": evidence,
                }

        bridges_list = sorted(
            agg.values(), key=lambda x: x["confidence_pct"] if x["confidence_pct"] is not None else -1, reverse=True,
        )
        total = len(bridges_list)
        offset = (page - 1) * page_size
        page_items = bridges_list[offset:offset + page_size]

        entity_ids_needed = sorted({eid for item in page_items for eid in (item["entity_id_a"], item["entity_id_b"])})
        entity_info: Dict[str, dict] = {}
        if entity_ids_needed:
            cur.execute("""
                SELECT e.entity_id::text, e.global_ref, e.global_ref_state, mc.member_count
                FROM entities e
                JOIN (SELECT entity_id, COUNT(*) AS member_count FROM entity_members WHERE valid_to IS NULL GROUP BY entity_id) mc
                    ON mc.entity_id = e.entity_id
                WHERE e.entity_id::text = ANY(%s::text[])
            """, (entity_ids_needed,))
            for eid, ref, ref_state, mc in cur.fetchall():
                entity_info[eid] = {"global_ref": ref, "global_ref_state": ref_state, "member_count": mc}
    finally:
        pg_conn.close()

    codes_needed = sorted({c for item in page_items for c in (item["code_a"], item["code_b"])})
    names: Dict[str, str] = {}
    if codes_needed:
        session = open_run_readonly(rid)
        try:
            if _table_exists(session, "customer_scalars"):
                placeholders = ",".join("?" for _ in codes_needed)
                rows = session.con.execute(
                    f"SELECT customer_code, name_norm FROM customer_scalars WHERE customer_code IN ({placeholders})",
                    codes_needed,
                ).fetchall()
                names = dict(rows)
        finally:
            session.close()

    items = []
    for item in page_items:
        info_a = entity_info.get(item["entity_id_a"], {})
        info_b = entity_info.get(item["entity_id_b"], {})
        items.append({
            "entity_id_a": item["entity_id_a"], "entity_id_b": item["entity_id_b"],
            "entity_a_name": names.get(item["code_a"]) or item["code_a"],
            "entity_b_name": names.get(item["code_b"]) or item["code_b"],
            "entity_a_size": info_a.get("member_count"), "entity_b_size": info_b.get("member_count"),
            "entity_a_global_ref": info_a.get("global_ref"), "entity_b_global_ref": info_b.get("global_ref"),
            "kind": item["kind"], "confidence_pct": item["confidence_pct"], "decision": item["decision"],
            "has_veto": item["has_veto"], "evidence": item["evidence"],
        })

    return {"run_id": rid, "engine": engine, "items": items, "total": total, "page": page, "page_size": page_size}
