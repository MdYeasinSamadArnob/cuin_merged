"""
CUIN v2 - Rule Catalog & Instant Redecide/Reblock Routes

Backs the Settings UI's rule builder: CRUD over the versioned rule
catalog (engine.rules.store, policy_versions-backed), the live fan-out
precheck (engine.rules.precheck) shown next to each blocking rule, and
the instant redecide/reblock endpoints that make a threshold slider or
blocking-rule edit return in milliseconds/seconds instead of requiring
a full pipeline re-run.

redecide/reblock are NON-DESTRUCTIVE previews: they operate on a
scratch copy of the run's persisted evidence tables (see
_open_scratch_db), never mutating the run's actual Doris database.
This replaces routes_graph.py's /preview, which re-scored with the
custom probabilistic scorer and silently truncated to 500 records.
"""

import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from engine.rules.catalog import BlockingRule
from engine.rules.match_rules import MatchRule, MatchRuleset
from engine.rules import store as rule_store
from engine.rules.store import default_catalog
from engine.rules.compiler import compile_and_build
from engine.rules.confidence_compiler import compile_confidence_sql
from engine.rules.precheck import estimate_catalog_fanout, DEFAULT_WARN_PAIRS, DEFAULT_BLOCK_PAIRS
from engine.rules.comparators import registry_to_dict
from engine.scoring.evidence_dialect import build_pair_evidence
from engine.segments.classifier import SegmentationConfig, DEFAULT_COMPANY_KEYWORDS, DEFAULT_SEGMENTATION_CONFIG
from engine.ports.run_session import RunSession, open_run_scratch

router = APIRouter()


@router.get("/comparators")
async def get_comparators():
    """The portable comparator registry -- see engine.rules.comparators."""
    return registry_to_dict()


_SCRATCH_TABLES = (
    "identifiers", "customer_scalars", "identifier_frequency",
    "candidate_pairs", "pair_identifier_evidence", "pair_name_dob_evidence", "pair_decisions",
)


def _table_exists(session: RunSession, name: str) -> bool:
    # Doris's information_schema.tables spans EVERY database on the
    # cluster, not just the current connection's -- without scoping by
    # DATABASE(), a same-named table in an unrelated run's database
    # (every run has its own `identifiers`, `candidate_pairs`, etc.)
    # would register as a false positive. Verified live against Doris.
    if session.engine == "doris":
        row = session.con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = DATABASE()",
            [name],
        ).fetchone()
    else:
        row = session.con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]
        ).fetchone()
    return bool(row and row[0])


def _decision_counts(session: RunSession, table: str) -> Dict[str, int]:
    if not _table_exists(session, table):
        return {}
    rows = session.con.execute(f"SELECT decision, COUNT(*) FROM {table} GROUP BY decision").fetchall()
    return {d: n for d, n in rows}


def _delta(new_counts: Dict[str, int], baseline: Dict[str, int]) -> Dict[str, int]:
    keys = set(new_counts) | set(baseline)
    return {k: new_counts.get(k, 0) - baseline.get(k, 0) for k in keys}


# ----------------------------------------------------------------------
# Request/response models -- mirror engine.rules.catalog/scoring_rules
# field-for-field so the frontend can round-trip GET -> edit -> POST.
# ----------------------------------------------------------------------

class RuleGuardsModel(BaseModel):
    max_key_frequency: Optional[int] = None
    max_block_size: Optional[int] = None
    min_key_parts: Optional[int] = None


class BlockingRuleModel(BaseModel):
    rule_id: str
    type: str
    enabled: bool = True
    order: int = 0
    label: str = ""
    fields: List[str] = []
    params: Dict[str, Any] = {}
    guards: RuleGuardsModel = RuleGuardsModel()
    description: str = ""


class MatchRuleModel(BaseModel):
    rule_id: str
    attribute: str
    sub_type: Optional[str] = None
    comparator: str
    params: Dict[str, Any] = {}
    confidence_pct: float
    aggregation: str = "once"
    veto_kind: Optional[str] = None
    enabled: bool = True
    label: str = ""


class MatchRulesetModel(BaseModel):
    match_rules: List[MatchRuleModel]
    auto_link_min_confidence: float = 95.0
    review_min_confidence: float = 50.0
    confidence_cap: float = 100.0
    max_cluster_size: int = 12
    min_density: float = 0.35


class SegmentationConfigModel(BaseModel):
    enabled: bool = False
    mode: str = "name_patterns"
    company_keywords: List[str] = list(DEFAULT_COMPANY_KEYWORDS)
    column_map: Optional[Dict[str, Any]] = None


def _to_blocking_rule(m: BlockingRuleModel) -> BlockingRule:
    d = m.model_dump()
    return BlockingRule.from_dict(d)


def _to_match_ruleset(m: MatchRulesetModel) -> MatchRuleset:
    return MatchRuleset.from_dict(m.model_dump())


def _to_segmentation(m: Optional[SegmentationConfigModel]) -> SegmentationConfig:
    if m is None:
        return SegmentationConfig()
    return SegmentationConfig.from_dict(m.model_dump())


# ----------------------------------------------------------------------
# Rule catalog CRUD
# ----------------------------------------------------------------------

@router.get("")
async def get_active_rules():
    catalog = rule_store.get_active_catalog()
    return catalog.to_dict()


@router.get("/defaults")
async def get_default_rules():
    """
    The seeded, provably-safe starting configuration -- for a "Reset
    to defaults" action in the UI so an accidental deletion is never
    unrecoverable. Returns only the editable fields (no policy_version/
    catalog_hash/is_active, which a default preview doesn't have) --
    the frontend loads this into its local editor state and the
    banker still has to click Save & Activate, same as any other edit.
    """
    blocking_rules, match_ruleset = default_catalog()
    return {
        "blocking_rules": [r.to_dict() for r in blocking_rules],
        "match_ruleset": match_ruleset.to_dict(),
        "segmentation": DEFAULT_SEGMENTATION_CONFIG.to_dict(),
        "match_rulesets_by_segment": {},
    }


class SaveCatalogRequest(BaseModel):
    blocking_rules: List[BlockingRuleModel]
    match_ruleset: MatchRulesetModel
    created_by: str = "ui"
    activate: bool = True
    segmentation: Optional[SegmentationConfigModel] = None
    match_rulesets_by_segment: Optional[Dict[str, MatchRulesetModel]] = None


@router.post("")
async def save_rules(request: SaveCatalogRequest):
    blocking_rules = [_to_blocking_rule(r) for r in request.blocking_rules]
    match_ruleset = _to_match_ruleset(request.match_ruleset)
    segmentation = _to_segmentation(request.segmentation)
    match_rulesets_by_segment = {
        k: _to_match_ruleset(v) for k, v in (request.match_rulesets_by_segment or {}).items()
    }
    try:
        for r in blocking_rules:
            r.validate()
        for mr in match_ruleset.match_rules:
            mr.validate()
        for seg_ruleset in match_rulesets_by_segment.values():
            for mr in seg_ruleset.match_rules:
                mr.validate()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    result = rule_store.save_catalog(
        blocking_rules, match_ruleset, request.created_by, activate=request.activate,
        segmentation=segmentation, match_rulesets_by_segment=match_rulesets_by_segment,
    )
    return result.to_dict()


@router.get("/versions")
async def list_versions(limit: int = 50):
    return {"versions": rule_store.list_catalog_versions(limit=limit)}


@router.get("/versions/{policy_version}")
async def get_version(policy_version: int):
    v = rule_store.get_version(policy_version)
    if not v:
        raise HTTPException(status_code=404, detail=f"Policy version {policy_version} not found")
    return v.to_dict()


class ActivateRequest(BaseModel):
    approved_by: str = "ui"


@router.post("/versions/{policy_version}/activate")
async def activate_version(policy_version: int, request: ActivateRequest):
    if not rule_store.get_version(policy_version):
        raise HTTPException(status_code=404, detail=f"Policy version {policy_version} not found")
    rule_store.activate_version(policy_version, request.approved_by)
    return {"activated": policy_version}


# ----------------------------------------------------------------------
# Precheck -- exact fan-out estimate before a rule is saved/run
# ----------------------------------------------------------------------

class PrecheckRequest(BaseModel):
    run_id: str
    blocking_rules: List[BlockingRuleModel]
    warn_pairs: Optional[int] = None
    block_pairs: Optional[int] = None


def _sync_precheck(request: PrecheckRequest) -> dict:
    """
    Full synchronous body of precheck -- open_run_scratch/session.con
    are blocking Doris/DuckDB connection + CTAS-copy + query calls (see
    engine.ports.run_session), so the whole validate-through-close unit
    runs off the event loop as one run_in_threadpool dispatch. Same
    fix already applied to routes_matches.py's Doris-backed endpoints.
    """
    rules = [_to_blocking_rule(r) for r in request.blocking_rules]
    try:
        for r in rules:
            r.validate()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # "raw" is needed for RAW_COLUMN rules (engine.rules.catalog.
    # BlockingRuleType.RAW_COLUMN), which self-join directly on a raw
    # source column instead of identifiers/customer_scalars.
    session = open_run_scratch(request.run_id, table_names=("identifiers", "customer_scalars", "identifier_frequency", "raw"))
    try:
        kwargs = {}
        if request.warn_pairs:
            kwargs["warn_pairs"] = request.warn_pairs
        if request.block_pairs:
            kwargs["block_pairs"] = request.block_pairs
        estimates = estimate_catalog_fanout(rules, session.dialect, session.con, **kwargs)
        return {
            "run_id": request.run_id,
            "engine": session.engine,
            "estimates": [e.to_dict() for e in estimates],
            "any_blocked": any(e.severity == "BLOCK" for e in estimates),
            "total_pairs": sum(e.n_pairs for e in estimates),
        }
    finally:
        session.close()


@router.post("/precheck")
async def precheck(request: PrecheckRequest):
    return await run_in_threadpool(_sync_precheck, request)


# ----------------------------------------------------------------------
# Instant redecide (Tier 0: thresholds/tiers only, ~milliseconds)
# ----------------------------------------------------------------------

class RedecideRequest(BaseModel):
    match_ruleset: MatchRulesetModel


def _sync_redecide(run_id: str, request: RedecideRequest) -> dict:
    """Full synchronous body of redecide -- see _sync_precheck."""
    # candidate_pairs is required because compile_confidence_sql drives
    # from it (a correctness improvement over the legacy decision
    # compiler, which drove from pair_name_dob_evidence -- see
    # engine.rules.confidence_compiler's module docstring). "raw" is
    # needed for a RAW_COLUMN match rule (Stage 5.1, self-joins raw
    # directly) -- same reason /precheck and /reblock already need it
    # for RAW_COLUMN BLOCKING rules.
    session = open_run_scratch(run_id, table_names=("candidate_pairs", "pair_identifier_evidence", "pair_name_dob_evidence", "pair_decisions", "raw"))
    try:
        if not _table_exists(session, "pair_identifier_evidence") or not _table_exists(session, "pair_name_dob_evidence"):
            raise HTTPException(
                status_code=409,
                detail=f"Run {run_id} has no persisted evidence tables to redecide against.",
            )

        baseline = _decision_counts(session, "pair_decisions")

        match_ruleset = _to_match_ruleset(request.match_ruleset)
        sql = compile_confidence_sql(match_ruleset, session.dialect, table_name="pair_decisions_preview")
        t0 = time.time()
        session.con.execute(sql)
        elapsed_ms = int((time.time() - t0) * 1000)

        new_counts = _decision_counts(session, "pair_decisions_preview")
        return {
            "run_id": run_id,
            "engine": session.engine,
            "tier": "redecide",
            "decision_counts": new_counts,
            "baseline_decision_counts": baseline,
            "delta": _delta(new_counts, baseline),
            "elapsed_ms": elapsed_ms,
        }
    finally:
        session.close()


@router.post("/runs/{run_id}/redecide")
async def redecide(run_id: str, request: RedecideRequest):
    return await run_in_threadpool(_sync_redecide, run_id, request)


# ----------------------------------------------------------------------
# Reblock (Tier 1: blocking rules changed, seconds)
# ----------------------------------------------------------------------

class ReblockRequest(BaseModel):
    blocking_rules: List[BlockingRuleModel]
    match_ruleset: Optional[MatchRulesetModel] = None


def _sync_reblock(run_id: str, request: ReblockRequest) -> dict:
    """Full synchronous body of reblock -- see _sync_precheck."""
    rules = [_to_blocking_rule(r) for r in request.blocking_rules]
    try:
        for r in rules:
            r.validate()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # "raw" is needed for RAW_COLUMN rules -- see the identical comment
    # on the /precheck endpoint above.
    session = open_run_scratch(
        run_id, table_names=("identifiers", "customer_scalars", "identifier_frequency", "pair_decisions", "raw")
    )
    try:
        if not _table_exists(session, "identifiers") or not _table_exists(session, "customer_scalars"):
            raise HTTPException(
                status_code=409,
                detail=f"Run {run_id} has no persisted normalize-stage tables to reblock against.",
            )

        baseline = _decision_counts(session, "pair_decisions")

        con, dialect = session.con, session.dialect
        t0 = time.time()
        compile_and_build(con, rules, dialect)
        build_pair_evidence(con, dialect)

        match_ruleset = (
            _to_match_ruleset(request.match_ruleset)
            if request.match_ruleset
            else rule_store.get_active_catalog().match_ruleset
        )
        con.execute(compile_confidence_sql(match_ruleset, dialect, table_name="pair_decisions_preview"))
        elapsed_ms = int((time.time() - t0) * 1000)

        n_candidates = con.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]
        new_counts = _decision_counts(session, "pair_decisions_preview")

        return {
            "run_id": run_id,
            "engine": session.engine,
            "tier": "reblock",
            "candidate_pairs": n_candidates,
            "decision_counts": new_counts,
            "baseline_decision_counts": baseline,
            "delta": _delta(new_counts, baseline),
            "elapsed_ms": elapsed_ms,
        }
    finally:
        session.close()


@router.post("/runs/{run_id}/reblock")
async def reblock(run_id: str, request: ReblockRequest):
    return await run_in_threadpool(_sync_reblock, run_id, request)


# ----------------------------------------------------------------------
# Stage 5: Segments -- Company/Individual split + traceable cross-
# segment connections. Segmentation gates identity MERGING only (see
# engine.segments.classifier's module docstring); a company and the
# person who owns it are never fused into one identity, but the
# connection between them stays visible here rather than disappearing.
# ----------------------------------------------------------------------

def _sync_get_segment_stats(run_id: str) -> dict:
    """Full synchronous body of get_segment_stats -- see _sync_precheck."""
    session = open_run_scratch(run_id, table_names=("customer_segments",))
    try:
        if not _table_exists(session, "customer_segments"):
            raise HTTPException(
                status_code=409,
                detail=f"Run {run_id} has no persisted segment table (run predates Stage 5, or hasn't reached the normalize stage yet).",
            )
        rows = session.con.execute(
            "SELECT segment, COUNT(*) FROM customer_segments GROUP BY segment"
        ).fetchall()
        counts = {seg: n for seg, n in rows}
        return {
            "run_id": run_id,
            "engine": session.engine,
            "segment_counts": counts,
            "total": sum(counts.values()),
        }
    finally:
        session.close()


@router.get("/runs/{run_id}/segments")
async def get_segment_stats(run_id: str):
    """Company/Individual split for a completed run, e.g. for a Settings preview."""
    return await run_in_threadpool(_sync_get_segment_stats, run_id)


def _sync_list_relationships(run_id, customer_code, limit, offset) -> dict:
    """Full synchronous body of list_relationships -- psycopg2 connect/
    query/close is blocking network I/O, see _sync_precheck."""
    import psycopg2
    from api.config import settings

    if not run_id and not customer_code:
        raise HTTPException(status_code=400, detail="Provide run_id and/or customer_code.")

    where = []
    params: list = []
    if run_id:
        where.append("r.run_id = %s")
        params.append(run_id)
    if customer_code:
        where.append("(ca.source_customer_id = %s OR cb.source_customer_id = %s)")
        params.extend([customer_code, customer_code])
    where_sql = " AND ".join(where)

    conn = psycopg2.connect(settings.DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT r.relationship_id, r.run_id,
                       ca.source_customer_id, ca.name_norm, r.a_segment,
                       cb.source_customer_id, cb.name_norm, r.b_segment,
                       r.shared_evidence, r.created_at
                FROM entity_relationships r
                JOIN customers_norm ca ON ca.customer_key = r.a_key
                JOIN customers_norm cb ON cb.customer_key = r.b_key
                WHERE {where_sql}
                ORDER BY r.created_at DESC
                LIMIT %s OFFSET %s
            """, params + [limit, offset])
            rows = cur.fetchall()

            cur.execute(f"SELECT COUNT(*) FROM entity_relationships r JOIN customers_norm ca ON ca.customer_key = r.a_key JOIN customers_norm cb ON cb.customer_key = r.b_key WHERE {where_sql}", params)
            total = cur.fetchone()[0]
    finally:
        conn.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "relationships": [
            {
                "relationship_id": str(rel_id),
                "run_id": str(rid),
                "a": {"customer_code": a_code, "name": a_name, "segment": a_seg},
                "b": {"customer_code": b_code, "name": b_name, "segment": b_seg},
                "shared_evidence": evidence,
                "created_at": created_at.isoformat() if created_at else None,
            }
            for rel_id, rid, a_code, a_name, a_seg, b_code, b_name, b_seg, evidence, created_at in rows
        ],
    }


@router.get("/relationships")
async def list_relationships(
    run_id: Optional[str] = None,
    customer_code: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    """
    Traceable cross-segment connections (a person <-> a company they
    share a phone/email/document/address with), persisted by
    db.repository.persist_entity_relationships. Filter by run_id
    and/or customer_code (a source_customer_id -- matches either side
    of the pair). Without customer_code, use this for a run-wide
    "connections found" list; with it, for a single customer's
    "linked entities" panel.
    """
    return await run_in_threadpool(_sync_list_relationships, run_id, customer_code, limit, offset)
