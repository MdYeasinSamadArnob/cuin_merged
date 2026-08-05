"""
CUIN v2 - Entity Resolution Workbench API (Stage 4 of the entity
resolution workbench plan)

Purely additive: does not touch, redirect, or replace any /graph,
/explorer, /matches, or legacy /review endpoint -- those stay exactly
as they are (see the plan's "keep /explorer and /graph as legacy,
untouched" decision).

Read endpoints route per-run browse/search through
engine.ports.run_session.open_run_readonly -- the SAME seam
/rules/precheck, /rules/redecide, and /search already use, so this
module never has to touch a run's underlying Doris database directly.
Entity identity,
officer overrides, and audit trail come from Postgres (migrations 005
and 006) via services.workbench_service, which owns write durability
and the hash-chained audit log.
"""

import json
import logging
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import psycopg2
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from api.config import settings
from engine.ports.run_session import open_run_readonly
from engine.scoring.evidence import load_pair_evidence, evidence_to_field_evidence
from engine.segments.classifier import classify_segment_python, segment_sql_expr, SegmentationConfig
from services import workbench_service as wb
from services.run_service import get_run_service, RunStatus

logger = logging.getLogger(__name__)

router = APIRouter()

# Company-vs-individual classification is computed at READ TIME for
# workbench filters/badges, independent of whether segmentation was
# enabled for the run that produced the data (it's off by default --
# see engine.segments.classifier). This is a throwaway config for the
# UI only; it never touches the pipeline's own segmentation setting.
_READ_TIME_SEGMENTATION = SegmentationConfig(enabled=True)


def _pg():
    return psycopg2.connect(settings.DATABASE_URL)


def _resolve_run_id(run_id: Optional[str]) -> str:
    """Defaults to the latest COMPLETED run -- never an implicit cross-run union (see Stage 0's fix to /review)."""
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


def _parse_array_cell(v):
    """
    engine.scoring.evidence.load_pair_evidence's LIST-typed columns
    were originally read from a real duckdb.DuckDBPyConnection, which
    deserializes them to native Python lists. Doris (the only engine
    now) uses DorisConnection, a thin pymysql wrapper (engine/ports/
    doris_conn.py) with no array deserialization -- ARRAY<STRING>
    columns come back as their JSON-text wire representation (e.g.
    '["01713366500"]', or '[]' for an empty array) -- a non-empty
    STRING even when the array itself is empty, which silently
    corrupted has_intersection/similarity_score checks that assume
    `len(value) > 0` means "list has elements". Normalized here at the
    call site rather than inside engine.scoring.evidence itself.
    """
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        try:
            parsed = json.loads(v)
            if isinstance(parsed, list):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
    return [] if v is None else v


def _normalize_evidence_arrays(evidence: Dict[str, Any]) -> Dict[str, Any]:
    for id_ev in evidence.get("identifiers", []):
        for key in ("values_a", "values_b", "intersection"):
            id_ev[key] = _parse_array_cell(id_ev.get(key))
    nd = evidence.get("name_dob") or {}
    for key in ("tokens_a", "tokens_b", "token_intersection", "token_union"):
        if key in nd:
            nd[key] = _parse_array_cell(nd.get(key))
    return evidence


def _attach_officer_decisions(items: List[Dict[str, Any]]) -> None:
    """
    Mutates each pair item in place with officer_verdict/officer_actor/
    officer_reason/officer_decided_at/officer_entity_id when an active
    resolution_overrides row exists for it. This is the fix for "I
    approved a match and nothing changed" -- approving a pair immediately
    merges the two codes' entities (see approve_pair's _merge_entities_tx,
    which runs synchronously, not on the next pipeline run) and writes a
    durable override that's re-applied to CLUSTERING on every future run
    -- but this run's own persisted pair_decisions.decision value is never
    rewritten (that would silently falsify history). Without this, the
    list looks completely unchanged after an approval and an officer has
    no way to tell their action was recorded or find the entity it
    produced. Bounded to the current page (<=200 items), two extra queries.
    """
    if not items:
        return
    pairs = [tuple(sorted((it["a_key"], it["b_key"]))) for it in items]
    placeholders = ",".join(["(%s,%s)"] * len(pairs))
    flat_params = [x for pair in pairs for x in pair]
    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute(
            f"SELECT a_code, b_code, override_id::text, verdict, actor, reason, created_at FROM resolution_overrides "
            f"WHERE revoked_at IS NULL AND (a_code, b_code) IN ({placeholders})",
            flat_params,
        )
        by_pair = {(a, b): (oid, verdict, actor, reason, ts) for a, b, oid, verdict, actor, reason, ts in cur.fetchall()}

        codes = sorted({c for pair in pairs for c in pair})
        cur.execute(
            "SELECT customer_code, entity_id::text FROM entity_members WHERE customer_code = ANY(%s::text[]) AND valid_to IS NULL",
            (codes,),
        )
        entity_by_code = dict(cur.fetchall())
    finally:
        pg_conn.close()

    for it, key in zip(items, pairs):
        found = by_pair.get(key)
        if found:
            override_id, verdict, actor, reason, ts = found
            it["officer_verdict"] = verdict
            it["officer_actor"] = actor
            it["officer_reason"] = reason
            it["officer_decided_at"] = ts.isoformat() if ts else None
            it["officer_entity_id"] = entity_by_code.get(key[0]) or entity_by_code.get(key[1])
            it["officer_override_id"] = override_id
        else:
            it["officer_verdict"] = None


# ----------------------------------------------------------------------
# Populations
# ----------------------------------------------------------------------

def _sync_get_populations(run_id: Optional[str]):
    rid = _resolve_run_id(run_id)
    session = open_run_readonly(rid)
    try:
        counts: Dict[str, int] = {}
        if _table_exists(session, "pair_decisions"):
            rows = session.con.execute("SELECT decision, COUNT(*) FROM pair_decisions GROUP BY decision").fetchall()
            counts = {d: n for d, n in rows}
        singletons = 0
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
        cur.execute("SELECT COUNT(*) FROM entities WHERE status = 'ACTIVE'")
        n_entities = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM entities WHERE global_ref_state = 'CONFIRMED'")
        n_with_global_ref = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM entities WHERE global_ref_state = 'CONFLICT'")
        n_conflicts = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM resolution_overrides WHERE revoked_at IS NULL")
        n_overrides = cur.fetchone()[0]
    finally:
        pg_conn.close()

    return {
        "run_id": rid,
        "engine": session.engine,
        "auto_linked": counts.get("AUTO_LINK", 0),
        "needs_review": counts.get("REVIEW", 0),
        "rejected": counts.get("REJECT", 0),
        "singletons": singletons,
        "entities": n_entities,
        "entities_with_global_ref": n_with_global_ref,
        "global_ref_conflicts": n_conflicts,
        "active_officer_overrides": n_overrides,
    }


@router.get("/populations")
async def get_populations(run_id: Optional[str] = None):
    return await run_in_threadpool(_sync_get_populations, run_id)


@router.get("/runs")
async def list_runs(page: int = 1, page_size: int = 20):
    all_runs, total = get_run_service().list_runs(page=page, page_size=page_size)
    return {
        "runs": [
            {"run_id": r.run_id, "engine": r.engine, "status": r.status.value, "started_at": r.started_at.isoformat() if r.started_at else None,
             "ended_at": r.ended_at.isoformat() if r.ended_at else None, "records_in": r.counters.records_in}
            for r in all_runs
        ],
        "total": total, "page": page, "page_size": page_size,
    }


# ----------------------------------------------------------------------
# Pairs
# ----------------------------------------------------------------------

_VALID_RECORD_TYPES = {"ALL", "COMPANY", "INDIVIDUAL"}


def _sync_list_pairs(
    run_id: Optional[str], decision: Optional[str],
    min_conf: Optional[float], max_conf: Optional[float], has_veto: Optional[bool],
    q: Optional[str], record_type: Optional[str],
    page: int, page_size: int,
):
    if record_type is not None and record_type.upper() not in _VALID_RECORD_TYPES:
        raise HTTPException(status_code=400, detail=f"record_type must be one of {sorted(_VALID_RECORD_TYPES)}")

    rid = _resolve_run_id(run_id)
    session = open_run_readonly(rid)
    try:
        if not _table_exists(session, "pair_decisions"):
            raise HTTPException(status_code=409, detail=f"Run {rid} has no persisted pair_decisions")

        where = []
        params: list = []
        if decision:
            where.append("p.decision = ?")
            params.append(decision.upper())
        if min_conf is not None:
            where.append("p.confidence_pct >= ?")
            params.append(min_conf)
        if max_conf is not None:
            where.append("p.confidence_pct <= ?")
            params.append(max_conf)
        if has_veto is not None:
            where.append("p.has_veto = ?")
            params.append(has_veto)
        if q:
            q_upper = q.strip().upper()
            where.append("""(
                ca.name_norm LIKE ? OR cb.name_norm LIKE ? OR p.a_key = ? OR p.b_key = ? OR
                EXISTS (SELECT 1 FROM identifiers i WHERE i.customer_code IN (p.a_key, p.b_key) AND i.is_valid AND (i.value_norm = ? OR i.value_norm LIKE ?))
            )""")
            params.extend(["%" + q_upper + "%", "%" + q_upper + "%", q_upper, q_upper, q_upper, q_upper + "%"])
        if record_type and record_type.upper() != "ALL":
            rt = record_type.upper()
            seg_a = segment_sql_expr("ca.name_norm", _READ_TIME_SEGMENTATION, session.dialect)
            seg_b = segment_sql_expr("cb.name_norm", _READ_TIME_SEGMENTATION, session.dialect)
            where.append(f"(({seg_a}) = ? OR ({seg_b}) = ?)")
            params.extend([rt, rt])
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        # ca/cb are joined here even for the count query -- q and record_type
        # both reference them, and this file's other queries (search, records)
        # show LEFT JOIN a customer_scalars row is not guaranteed to exist for
        # every candidate pair key, so this must stay a LEFT JOIN, not INNER.
        total = session.con.execute(f"""
            SELECT COUNT(*) FROM pair_decisions p
            LEFT JOIN customer_scalars ca ON p.a_key = ca.customer_code
            LEFT JOIN customer_scalars cb ON p.b_key = cb.customer_code
            {where_sql}
        """, params).fetchone()[0]

        offset = (page - 1) * page_size
        rows = session.con.execute(f"""
            SELECT p.a_key, p.b_key, p.confidence_pct, p.has_veto, p.decision,
                   ca.name_norm, cb.name_norm
            FROM pair_decisions p
            LEFT JOIN customer_scalars ca ON p.a_key = ca.customer_code
            LEFT JOIN customer_scalars cb ON p.b_key = cb.customer_code
            {where_sql}
            ORDER BY p.confidence_pct DESC, p.a_key, p.b_key
            LIMIT ? OFFSET ?
        """, params + [page_size, offset]).fetchall()

        items = [
            {"a_key": a, "b_key": b, "confidence_pct": float(conf), "has_veto": bool(veto), "decision": dec,
             "a_name": a_name, "b_name": b_name}
            for a, b, conf, veto, dec, a_name, b_name in rows
        ]
        engine = session.engine
    finally:
        session.close()

    _attach_officer_decisions(items)
    return {"items": items, "total": total, "page": page, "page_size": page_size, "run_id": rid, "engine": engine}


@router.get("/pairs")
async def list_pairs(
    run_id: Optional[str] = None, decision: Optional[str] = None,
    min_conf: Optional[float] = None, max_conf: Optional[float] = None, has_veto: Optional[bool] = None,
    q: Optional[str] = None, record_type: Optional[str] = None,
    page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=200),
):
    return await run_in_threadpool(
        _sync_list_pairs, run_id, decision, min_conf, max_conf, has_veto, q, record_type, page, page_size,
    )


def _sync_pair_breakdown(a_key: str, b_key: str, run_id: Optional[str]):
    rid = _resolve_run_id(run_id)
    session = open_run_readonly(rid)
    try:
        if not _table_exists(session, "pair_contributions"):
            raise HTTPException(status_code=409, detail=f"Run {rid} has no persisted pair_contributions (pre-Stage-1 run, or publish failed)")

        decision_row = session.con.execute(
            "SELECT confidence_pct, has_veto, decision FROM pair_decisions WHERE a_key=? AND b_key=?", [a_key, b_key],
        ).fetchone()
        if not decision_row:
            raise HTTPException(status_code=404, detail=f"Pair {a_key}:{b_key} not found in run {rid}")

        rows = session.con.execute("""
            SELECT rule_id, ordinal, label, attribute, sub_type, matched, awarded_pct, configured_pct, is_veto, detail
            FROM pair_contributions WHERE a_key = ? AND b_key = ? ORDER BY ordinal
        """, [a_key, b_key]).fetchall()

        contributions = [
            {"rule_id": rid_, "ordinal": ordinal, "label": label, "attribute": attr, "sub_type": sub,
             "matched": bool(matched), "awarded_pct": float(awarded), "configured_pct": float(configured),
             "is_veto": bool(is_veto), "detail": detail}
            for rid_, ordinal, label, attr, sub, matched, awarded, configured, is_veto, detail in rows
        ]

        # Field-level evidence (raw value A/B, comparison algorithm, raw
        # similarity e.g. name token-Jaccard) -- purely supplementary
        # context alongside `contributions` above, NEVER a substitute for
        # it. Reuses engine.scoring.evidence's load_pair_evidence /
        # evidence_to_field_evidence UNCHANGED against
        # pair_identifier_evidence / pair_name_dob_evidence -- the exact
        # tables the real pipeline run (via engine.scoring.evidence_dialect)
        # persisted while computing THIS pair's actual, audited score. Never re-derives
        # anything from scratch (that's what the legacy /matches/{pair_id}
        # endpoint's _compute_pair_evidence_fresh does, via a DIFFERENT,
        # simpler tier classifier -- its "99%"-style score is an
        # approximation, not this run's real confidence_pct, which is why
        # it's intentionally not reused here). A rule can legitimately
        # award 0% (e.g. "Name is the same words") while the raw
        # similarity is still high (e.g. 75% token overlap that didn't
        # clear the rule's exact-match bar) -- both numbers are real and
        # worth showing side by side, not merged into one.
        field_evidence = []
        if _table_exists(session, "pair_identifier_evidence") and _table_exists(session, "pair_name_dob_evidence"):
            evidence = load_pair_evidence(session.con, a_key, b_key)
            if session.engine == "doris":
                evidence = _normalize_evidence_arrays(evidence)
            field_evidence = [asdict(fe) for fe in evidence_to_field_evidence(evidence)]
    finally:
        session.close()

    confidence_pct, has_veto, decision = decision_row
    result = {
        "run_id": rid, "a_key": a_key, "b_key": b_key,
        "confidence_pct": float(confidence_pct), "has_veto": bool(has_veto), "decision": decision,
        "contributions": contributions,
        "field_evidence": field_evidence,
    }
    _attach_officer_decisions([result])
    return result


@router.get("/pairs/{a_key}/{b_key}/breakdown")
async def pair_breakdown(a_key: str, b_key: str, run_id: Optional[str] = None):
    return await run_in_threadpool(_sync_pair_breakdown, a_key, b_key, run_id)


# ----------------------------------------------------------------------
# Records
# ----------------------------------------------------------------------

def _sync_get_record(customer_code: str, run_id: Optional[str]):
    """Real record detail -- never the fabricating get_record_profile() the legacy /graph page still uses."""
    rid = _resolve_run_id(run_id)
    session = open_run_readonly(rid)
    try:
        row = session.con.execute(
            "SELECT customer_code, name_norm, dob_iso, dob_precision FROM customer_scalars WHERE customer_code = ?",
            [customer_code],
        ).fetchone()
        if not row:
            return {"customer_code": customer_code, "resolved": False, "run_id": rid}

        id_rows = session.con.execute(
            "SELECT id_type, value_norm FROM identifiers WHERE customer_code = ? AND is_valid", [customer_code],
        ).fetchall() if _table_exists(session, "identifiers") else []
        identifiers: Dict[str, List[str]] = {}
        for id_type, value in id_rows:
            identifiers.setdefault(id_type, []).append(value)
    finally:
        session.close()

    code, name_norm, dob_iso, dob_precision = row
    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT e.entity_id::text, e.global_ref, e.global_ref_state FROM entity_members em "
            "JOIN entities e ON e.entity_id = em.entity_id WHERE em.customer_code = %s AND em.valid_to IS NULL",
            (customer_code,),
        )
        entity_row = cur.fetchone()
    finally:
        pg_conn.close()

    return {
        "customer_code": code, "resolved": True, "run_id": rid,
        "name_norm": name_norm, "dob_iso": dob_iso, "dob_precision": dob_precision,
        "identifiers": identifiers,
        "segment": classify_segment_python(name_norm, _READ_TIME_SEGMENTATION),
        "entity_id": entity_row[0] if entity_row else None,
        "global_ref": entity_row[1] if entity_row else None,
        "global_ref_state": entity_row[2] if entity_row else None,
    }


@router.get("/records/{customer_code}")
async def get_record(customer_code: str, run_id: Optional[str] = None):
    return await run_in_threadpool(_sync_get_record, customer_code, run_id)


# ----------------------------------------------------------------------
# Search
# ----------------------------------------------------------------------

def _sync_search(q: str, run_id: Optional[str], page: int, page_size: int):
    rid = _resolve_run_id(run_id)
    session = open_run_readonly(rid)
    try:
        q_upper = q.strip().upper()
        matched_codes: set = set()

        if _table_exists(session, "identifiers"):
            rows = session.con.execute(
                "SELECT DISTINCT customer_code FROM identifiers WHERE is_valid AND (value_norm = ? OR value_norm LIKE ?) LIMIT 500",
                [q_upper, q_upper + "%"],
            ).fetchall()
            matched_codes.update(r[0] for r in rows)

        if _table_exists(session, "customer_scalars"):
            rows = session.con.execute(
                "SELECT customer_code FROM customer_scalars WHERE customer_code = ? OR customer_code LIKE ? OR name_norm LIKE ? LIMIT 500",
                [q_upper, q_upper + "%", "%" + q_upper + "%"],
            ).fetchall()
            matched_codes.update(r[0] for r in rows)

        # Global Ref search -- cross-run, from Postgres, added to the same result set.
        pg_conn = _pg()
        try:
            cur = pg_conn.cursor()
            cur.execute(
                "SELECT em.customer_code FROM entities e JOIN entity_members em ON em.entity_id = e.entity_id "
                "WHERE upper(e.global_ref) LIKE upper(%s) AND em.valid_to IS NULL LIMIT 200",
                (q + "%",),
            )
            matched_codes.update(r[0] for r in cur.fetchall())
        finally:
            pg_conn.close()

        total = len(matched_codes)
        codes_page = sorted(matched_codes)[(page - 1) * page_size: page * page_size]

        results = []
        if codes_page:
            placeholders = ",".join("?" for _ in codes_page)
            rows = session.con.execute(
                f"SELECT customer_code, name_norm, dob_iso FROM customer_scalars WHERE customer_code IN ({placeholders})",
                codes_page,
            ).fetchall()
            results = [{"customer_code": c, "name_norm": n, "dob_iso": d} for c, n, d in rows]
    finally:
        session.close()

    return {"run_id": rid, "engine": session.engine, "query": q, "total": total, "page": page, "page_size": page_size, "results": results}


@router.get("/search")
async def search(q: str = Query(..., min_length=1), run_id: Optional[str] = None, page: int = 1, page_size: int = 20):
    return await run_in_threadpool(_sync_search, q, run_id, page, page_size)


# ----------------------------------------------------------------------
# Entities
# ----------------------------------------------------------------------

def _sync_list_entities(
    page: int, page_size: int,
    has_global_ref: Optional[bool], q: Optional[str],
    record_type: Optional[str], run_id: Optional[str],
):
    if record_type is not None and record_type.upper() not in _VALID_RECORD_TYPES:
        raise HTTPException(status_code=400, detail=f"record_type must be one of {sorted(_VALID_RECORD_TYPES)}")

    rid = _resolve_run_id(run_id)

    # `q` (name/identifier) matching and the record_type filter both need
    # a run session; member_names_preview does too. Open
    # one session for all three. If the run's data is unavailable (e.g. a
    # very old run whose per-run database was cleaned up), degrade to
    # Postgres-only filtering (global_ref/customer_code) and bare-code
    # previews rather than failing entity listing entirely.
    session = None
    try:
        session = open_run_readonly(rid)
    except Exception:
        logger.warning("Could not open run %s for entity search/preview -- degrading to Postgres-only", rid, exc_info=True)

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
            # RETIRED keeps global_ref's string for history (retire_global_ref
            # never clears it, only flips global_ref_state) -- but that's not
            # an ACTIVE assignment, so "has a Global ID" must exclude it or a
            # retired entity looks indistinguishable from a confirmed one in
            # this filter, and never shows up under "no Global ID" either.
            if has_global_ref is True:
                where.append("e.global_ref IS NOT NULL AND e.global_ref_state != 'RETIRED'")
            elif has_global_ref is False:
                where.append("(e.global_ref IS NULL OR e.global_ref_state = 'RETIRED')")
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
                    # "Involves a company" -- at least one member's name carries a company signal.
                    where.append(exists_company_member)
                else:
                    # "Individuals" must mean NO company signal anywhere in the
                    # cluster, not merely "has some non-matching member" -- a
                    # corporate multi-account entity (e.g. several generically
                    # labeled sub-accounts under one LTD company) has plenty of
                    # members whose own label doesn't literally contain a
                    # keyword, so "EXISTS a non-company member" would wrongly
                    # let it leak into "Individuals". Require zero company
                    # members instead.
                    where.append(f"NOT {exists_company_member}")
                params.append(company_codes)
            where_sql = " AND ".join(where)

            cur.execute(f"SELECT COUNT(*) FROM entities e WHERE {where_sql}", params)
            total = cur.fetchone()[0]

            offset = (page - 1) * page_size
            cur.execute(f"""
                SELECT e.entity_id::text, e.global_ref, e.global_ref_state, e.segment, e.created_at,
                       (SELECT COUNT(*) FROM entity_members em WHERE em.entity_id = e.entity_id AND em.valid_to IS NULL) AS member_count
                FROM entities e WHERE {where_sql}
                ORDER BY e.updated_at DESC LIMIT %s OFFSET %s
            """, params + [page_size, offset])
            page_rows = cur.fetchall()

            # Up to 3 member codes per entity, ordered by customer_code for
            # determinism across requests/pages (matches get_entity's own
            # member-fetch ordering) -- a cosmetic preview, never used to
            # decide the record_type filter above (that uses ALL members
            # via the company_codes set, not this 3-member sample).
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
            preview_names_by_code = {c: n for c, n in rows}
    finally:
        if session:
            session.close()

    items = []
    for eid, ref, state, seg, created, count in page_rows:
        preview_codes = preview_codes_by_entity.get(eid, [])
        preview_names = [preview_names_by_code.get(c) or c for c in preview_codes]
        record_type_preview = None
        if preview_names:
            record_type_preview = "COMPANY" if any(
                classify_segment_python(n, _READ_TIME_SEGMENTATION) == "COMPANY" for n in preview_names
            ) else "INDIVIDUAL"
        items.append({
            "entity_id": eid, "global_ref": ref, "global_ref_state": state, "segment": seg,
            "created_at": created.isoformat() if created else None, "member_count": count,
            "member_names_preview": preview_names, "record_type_preview": record_type_preview,
        })

    return {"items": items, "total": total, "page": page, "page_size": page_size, "run_id": rid}


@router.get("/entities")
async def list_entities(
    page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=200),
    has_global_ref: Optional[bool] = None, q: Optional[str] = None,
    record_type: Optional[str] = None, run_id: Optional[str] = None,
):
    return await run_in_threadpool(_sync_list_entities, page, page_size, has_global_ref, q, record_type, run_id)


def _sync_get_entity(entity_id: str, run_id: Optional[str]):
    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT entity_id::text, global_ref, global_ref_state, status, merged_into_entity_id::text, segment, created_at, created_by "
            "FROM entities WHERE entity_id = %s", (entity_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Entity {entity_id} not found")
        eid, ref, ref_state, status, merged_into, segment, created_at, created_by = row

        cur.execute("SELECT customer_code, source, valid_from FROM entity_members WHERE entity_id = %s AND valid_to IS NULL ORDER BY customer_code", (entity_id,))
        member_rows = cur.fetchall()

        cur.execute(
            "SELECT event, run_id::text, jaccard, member_count, from_entity_ids::text[], to_entity_ids::text[], actor, created_at "
            "FROM entity_lineage WHERE entity_id = %s ORDER BY created_at DESC LIMIT 50",
            (entity_id,),
        )
        lineage = [
            {"event": ev, "run_id": rid_, "jaccard": float(j) if j is not None else None, "member_count": mc,
             "from_entity_ids": fids, "to_entity_ids": tids, "actor": actor, "created_at": ts.isoformat()}
            for ev, rid_, j, mc, fids, tids, actor, ts in cur.fetchall()
        ]
    finally:
        pg_conn.close()

    members = [{"customer_code": c, "source": s, "joined_at": v.isoformat() if v else None} for c, s, v in member_rows]

    # Hydrate names for members from the run engine, if available.
    if run_id and members:
        try:
            session = open_run_readonly(run_id)
            try:
                codes = [m["customer_code"] for m in members]
                placeholders = ",".join("?" for _ in codes)
                rows = session.con.execute(f"SELECT customer_code, name_norm, dob_iso FROM customer_scalars WHERE customer_code IN ({placeholders})", codes).fetchall()
                by_code = {c: (n, d) for c, n, d in rows}
                for m in members:
                    n, d = by_code.get(m["customer_code"], (None, None))
                    m["name_norm"] = n
                    m["dob_iso"] = d
            finally:
                session.close()
        except Exception:
            logger.warning("Could not hydrate entity member names for run %s", run_id, exc_info=True)

    return {
        "entity_id": eid, "global_ref": ref, "global_ref_state": ref_state, "status": status,
        "merged_into_entity_id": merged_into, "segment": segment,
        "created_at": created_at.isoformat() if created_at else None, "created_by": created_by,
        "members": members, "lineage": lineage,
    }


@router.get("/entities/{entity_id}")
async def get_entity(entity_id: str, run_id: Optional[str] = None):
    return await run_in_threadpool(_sync_get_entity, entity_id, run_id)


def _sync_entity_matches(entity_id: str, run_id: Optional[str]):
    """
    Every direct, scored pairwise link among this entity's CURRENT members
    -- the actual evidence a union-find over these edges collapses into one
    cluster. Before an officer assigns a durable Global ID, this is the
    "why are all these members one entity, with what scores" view: an
    entity of N members is not proof that all N choose(2) pairs were
    directly compared -- some members can be connected only transitively
    through a third member -- so a missing pair here (not merely a
    low-scoring one) is itself meaningful, not an omission.
    """
    rid = _resolve_run_id(run_id)
    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute("SELECT customer_code FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (entity_id,))
        codes = [r[0] for r in cur.fetchall()]
    finally:
        pg_conn.close()

    if not codes:
        return {"run_id": rid, "entity_id": entity_id, "member_count": 0, "items": []}

    session = open_run_readonly(rid)
    try:
        if not _table_exists(session, "pair_decisions"):
            return {"run_id": rid, "entity_id": entity_id, "member_count": len(codes), "items": []}

        placeholders = ",".join("?" for _ in codes)
        rows = session.con.execute(f"""
            SELECT p.a_key, p.b_key, p.confidence_pct, p.has_veto, p.decision, ca.name_norm, cb.name_norm
            FROM pair_decisions p
            LEFT JOIN customer_scalars ca ON p.a_key = ca.customer_code
            LEFT JOIN customer_scalars cb ON p.b_key = cb.customer_code
            WHERE p.a_key IN ({placeholders}) AND p.b_key IN ({placeholders})
            ORDER BY p.confidence_pct DESC, p.a_key, p.b_key
        """, codes + codes).fetchall()

        items = [
            {"a_key": a, "b_key": b, "confidence_pct": float(conf), "has_veto": bool(veto), "decision": dec,
             "a_name": a_name, "b_name": b_name}
            for a, b, conf, veto, dec, a_name, b_name in rows
        ]
        engine = session.engine
    finally:
        session.close()

    possible_pairs = len(codes) * (len(codes) - 1) // 2
    return {
        "run_id": rid, "engine": engine, "entity_id": entity_id, "member_count": len(codes),
        "possible_pairs": possible_pairs, "directly_evidenced_pairs": len(items),
        "items": items,
    }


@router.get("/entities/{entity_id}/matches")
async def entity_matches(entity_id: str, run_id: Optional[str] = None):
    return await run_in_threadpool(_sync_entity_matches, entity_id, run_id)


# ----------------------------------------------------------------------
# Officer actions
# ----------------------------------------------------------------------

class PairActionRequest(BaseModel):
    run_id: Optional[str] = None
    a_code: str
    b_code: str
    reason_code: str
    reason: str
    actor: str


class MergeRequest(BaseModel):
    run_id: Optional[str] = None
    entity_id_a: str
    entity_id_b: str
    reason_code: str
    reason: str
    actor: str


class SplitRequest(BaseModel):
    run_id: Optional[str] = None
    entity_id: str
    customer_code: str
    reason_code: str
    reason: str
    actor: str


class GlobalRefRequest(BaseModel):
    global_ref: str
    state: str = "CONFIRMED"
    reason: str
    actor: str


class RetireGlobalRefRequest(BaseModel):
    reason: str
    actor: str


class RecordGlobalRefRequest(BaseModel):
    run_id: Optional[str] = None
    global_ref: str
    state: str = "CONFIRMED"
    reason: str
    actor: str


def _handle_workbench_error(fn, *args):
    try:
        return fn(*args)
    except wb.WorkbenchError as e:
        raise HTTPException(status_code=409, detail=str(e))


def _sync_action_approve(request: PairActionRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(
            wb.approve_pair, pg_conn, request.run_id, request.a_code, request.b_code, request.reason_code, request.reason, request.actor,
        )
    finally:
        pg_conn.close()


@router.post("/actions/approve")
async def action_approve(request: PairActionRequest):
    return await run_in_threadpool(_sync_action_approve, request)


def _sync_action_reject(request: PairActionRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(
            wb.reject_pair, pg_conn, request.run_id, request.a_code, request.b_code, request.reason_code, request.reason, request.actor,
        )
    finally:
        pg_conn.close()


@router.post("/actions/reject")
async def action_reject(request: PairActionRequest):
    return await run_in_threadpool(_sync_action_reject, request)


def _sync_action_merge(request: MergeRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(
            wb.merge_entities, pg_conn, request.run_id, request.entity_id_a, request.entity_id_b, request.reason_code, request.reason, request.actor,
        )
    finally:
        pg_conn.close()


@router.post("/actions/merge")
async def action_merge(request: MergeRequest):
    return await run_in_threadpool(_sync_action_merge, request)


def _sync_action_split(request: SplitRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(
            wb.split_record, pg_conn, request.run_id, request.entity_id, request.customer_code, request.reason_code, request.reason, request.actor,
        )
    finally:
        pg_conn.close()


@router.post("/actions/split")
async def action_split(request: SplitRequest):
    return await run_in_threadpool(_sync_action_split, request)


def _sync_action_assign_global_ref(entity_id: str, request: GlobalRefRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(
            wb.assign_global_ref, pg_conn, entity_id, request.global_ref, request.state, request.reason, request.actor,
        )
    finally:
        pg_conn.close()


@router.post("/entities/{entity_id}/global-ref")
async def action_assign_global_ref(entity_id: str, request: GlobalRefRequest):
    return await run_in_threadpool(_sync_action_assign_global_ref, entity_id, request)


def _sync_action_retire_global_ref(entity_id: str, request: RetireGlobalRefRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(wb.retire_global_ref, pg_conn, entity_id, request.reason, request.actor)
    finally:
        pg_conn.close()


@router.delete("/entities/{entity_id}/global-ref")
async def action_retire_global_ref(entity_id: str, request: RetireGlobalRefRequest):
    return await run_in_threadpool(_sync_action_retire_global_ref, entity_id, request)


def _sync_action_assign_global_ref_to_record(customer_code: str, request: RecordGlobalRefRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(
            wb.assign_global_ref_to_record, pg_conn, request.run_id, customer_code,
            request.global_ref, request.state, request.reason, request.actor,
        )
    finally:
        pg_conn.close()


@router.post("/records/{customer_code}/global-ref")
async def action_assign_global_ref_to_record(customer_code: str, request: RecordGlobalRefRequest):
    """
    Same as POST /entities/{entity_id}/global-ref, but keyed by
    customer_code instead of an existing entity_id -- the path a
    singleton (never clustered, no entity_id yet) needs to get a
    Global ID assigned at all. Mints a one-member entity on demand; see
    services.workbench_service.assign_global_ref_to_record.
    """
    return await run_in_threadpool(_sync_action_assign_global_ref_to_record, customer_code, request)


# ----------------------------------------------------------------------
# Rollback -- undo a merge, revert a Global Ref change, revoke an
# approve/reject override. See services.workbench_service's "Rollback"
# section docstring for why these are each a NEW forward audit event,
# never a mutation of the original one.
# ----------------------------------------------------------------------

class ReasonActorRequest(BaseModel):
    reason: str
    actor: str


def _sync_action_undo_merge(entity_id: str, request: ReasonActorRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(wb.undo_merge, pg_conn, entity_id, request.reason, request.actor)
    finally:
        pg_conn.close()


@router.post("/entities/{entity_id}/undo-merge")
async def action_undo_merge(entity_id: str, request: ReasonActorRequest):
    return await run_in_threadpool(_sync_action_undo_merge, entity_id, request)


def _sync_action_revert_global_ref(entity_id: str, request: ReasonActorRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(wb.revert_global_ref, pg_conn, entity_id, request.reason, request.actor)
    finally:
        pg_conn.close()


@router.post("/entities/{entity_id}/revert-global-ref")
async def action_revert_global_ref(entity_id: str, request: ReasonActorRequest):
    return await run_in_threadpool(_sync_action_revert_global_ref, entity_id, request)


def _sync_action_revoke_override(override_id: str, request: ReasonActorRequest):
    pg_conn = _pg()
    try:
        return _handle_workbench_error(wb.revoke_override, pg_conn, override_id, request.reason, request.actor)
    finally:
        pg_conn.close()


@router.post("/overrides/{override_id}/revoke")
async def action_revoke_override(override_id: str, request: ReasonActorRequest):
    return await run_in_threadpool(_sync_action_revoke_override, override_id, request)


# ----------------------------------------------------------------------
# Audit
# ----------------------------------------------------------------------

def _sync_audit_verify():
    pg_conn = _pg()
    try:
        is_valid, error, checked = wb.verify_audit_chain(pg_conn)
    finally:
        pg_conn.close()
    return {"valid": is_valid, "error": error, "events_checked": checked}


@router.get("/audit/verify")
async def audit_verify():
    return await run_in_threadpool(_sync_audit_verify)


def _sync_list_audit(entity_id: Optional[str], run_id: Optional[str], page: int, page_size: int):
    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        where = []
        params: list = []
        if entity_id:
            where.append("payload_json::text LIKE %s")
            params.append(f"%{entity_id}%")
        if run_id:
            where.append("run_id = %s")
            params.append(run_id)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        cur.execute(f"SELECT COUNT(*) FROM audit_events {where_sql}", params)
        total = cur.fetchone()[0]

        offset = (page - 1) * page_size
        cur.execute(
            f"SELECT audit_id::text, event_type, payload_json, actor, run_id::text, created_at "
            f"FROM audit_events {where_sql} ORDER BY created_at DESC LIMIT %s OFFSET %s",
            params + [page_size, offset],
        )
        items = [
            {"audit_id": aid, "event_type": et, "payload": payload, "actor": actor, "run_id": rid_, "created_at": ts.isoformat()}
            for aid, et, payload, actor, rid_, ts in cur.fetchall()
        ]
    finally:
        pg_conn.close()

    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.get("/audit")
async def list_audit(entity_id: Optional[str] = None, run_id: Optional[str] = None, page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500)):
    return await run_in_threadpool(_sync_list_audit, entity_id, run_id, page, page_size)


def _sync_list_overrides(page: int, page_size: int, verdict: Optional[str], run_id: Optional[str]):
    """
    Every active officer decision (MUST_LINK = approved, MUST_NOT_LINK =
    rejected) -- the "Approved" tab's traceability view. Unlike /pairs,
    this is NOT scoped to one run's own decision -- resolution_overrides
    is run-agnostic by design (an officer's verdict is re-applied on every
    future run), so this is the durable ledger of every decision ever
    made, independent of which run happens to be selected. `run_id` is
    used only to best-effort hydrate display names and the resulting
    entity_id, never to filter which decisions are returned.
    """
    if verdict is not None and verdict.upper() not in {"MUST_LINK", "MUST_NOT_LINK"}:
        raise HTTPException(status_code=400, detail="verdict must be MUST_LINK or MUST_NOT_LINK")

    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        where = ["revoked_at IS NULL"]
        params: list = []
        if verdict:
            where.append("verdict = %s")
            params.append(verdict.upper())
        where_sql = " AND ".join(where)

        cur.execute(f"SELECT COUNT(*) FROM resolution_overrides WHERE {where_sql}", params)
        total = cur.fetchone()[0]

        offset = (page - 1) * page_size
        cur.execute(
            f"SELECT override_id::text, verdict, a_code, b_code, reason_code, reason, actor, created_at "
            f"FROM resolution_overrides WHERE {where_sql} ORDER BY created_at DESC LIMIT %s OFFSET %s",
            params + [page_size, offset],
        )
        rows = cur.fetchall()

        codes = sorted({c for r in rows for c in (r[2], r[3])})
        entity_by_code: Dict[str, str] = {}
        if codes:
            cur.execute(
                "SELECT customer_code, entity_id::text FROM entity_members WHERE customer_code = ANY(%s::text[]) AND valid_to IS NULL",
                (codes,),
            )
            entity_by_code = dict(cur.fetchall())

        items = [
            {"override_id": oid, "verdict": v, "a_code": a, "b_code": b, "reason_code": rc, "reason": r, "actor": actor,
             "created_at": ts.isoformat(), "entity_id": entity_by_code.get(a) or entity_by_code.get(b)}
            for oid, v, a, b, rc, r, actor, ts in rows
        ]
    finally:
        pg_conn.close()

    rid = None
    try:
        rid = _resolve_run_id(run_id)
    except HTTPException:
        pass
    if rid and items:
        try:
            session = open_run_readonly(rid)
            try:
                all_codes = sorted({it["a_code"] for it in items} | {it["b_code"] for it in items})
                placeholders = ",".join("?" for _ in all_codes)
                name_rows = session.con.execute(
                    f"SELECT customer_code, name_norm FROM customer_scalars WHERE customer_code IN ({placeholders})", all_codes,
                ).fetchall()
                names = dict(name_rows)
                for it in items:
                    it["a_name"] = names.get(it["a_code"])
                    it["b_name"] = names.get(it["b_code"])
            finally:
                session.close()
        except Exception:
            logger.warning("Could not hydrate override names for run %s", rid, exc_info=True)

    return {"items": items, "total": total, "page": page, "page_size": page_size, "run_id": rid}


@router.get("/overrides")
async def list_overrides(
    page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500),
    verdict: Optional[str] = None, run_id: Optional[str] = None,
):
    return await run_in_threadpool(_sync_list_overrides, page, page_size, verdict, run_id)
