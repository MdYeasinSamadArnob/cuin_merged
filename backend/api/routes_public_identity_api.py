"""
CUIN v2 - Public Identity Recognition API (bank-facing, real-time)

Answers, in real time, the question a bank must ask before opening any
new account: "does this person already exist in our resolved identity
graph, and do they already have a Global ID?" A bank submits the raw
details a new applicant hands over at the counter (name, DOB, mobile,
email, document number, address) -- fields that are NOT yet in any
CUIN dataset -- and gets back a ranked list of existing entities that
plausibly ARE this person, with the exact same explainability
(rule-by-rule contributions) the officer workbench shows for a batch
pair.

This is deliberately NOT a reimplementation of the matching logic.
Every normalization, scoring, and threshold call below reuses the
EXACT production functions the batch pipeline itself calls
(engine.normalize.identity, engine.scoring.confidence.score_pair,
engine.rules.store.get_active_catalog) -- so this endpoint and a full
pipeline re-run can never quietly disagree about whether two records
are the same person. The only new code here is: (1) building the
`evidence` dict score_pair() expects from a query record that has no
customer_code and isn't in any table, and (2) a bounded SQL candidate
pool query that mirrors engine.blocking.deterministic_blocker's exact
predicates (see _find_candidate_codes) so the pool this endpoint scores
against is neither a full-table scan nor a weaker approximation of
what the batch pipeline itself considers "worth comparing".

Mounted as its own sub-application at /api/v1 (see api/public_api.py)
so a bank integration partner's Swagger UI only ever shows this one
contract -- never the ~20 internal admin/workbench routers on the main
app. Every route requires an `Authorization: Bearer <token>` header
checked against the single global `settings.PUBLIC_API_BEARER_TOKEN`
(see require_bearer_token below) -- auto-generated at process startup
if PUBLIC_API_BEARER_TOKEN isn't set in .env, and always readable by
an admin via GET /admin/api-token (surfaced on the control plane's
/api page). One shared secret, not a per-caller issuance system --
the right amount of ceremony for now; swapping to per-partner keys or
real OAuth2 later only touches this one dependency, not the contract.
"""

import hashlib
import logging
import secrets
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, model_validator

from api.config import settings
from engine.normalize.identity import (
    norm_name, norm_mobile_bd, norm_email, parse_document, norm_dob, norm_address,
)
from engine.ports.run_session import open_run_readonly
from engine.rules.store import get_active_catalog
from engine.scoring.confidence import score_pair
from engine.scoring.evidence import evidence_to_field_evidence
from engine.segments.classifier import classify_segment_python, SegmentationConfig
from services.run_service import get_run_service, RunStatus

logger = logging.getLogger("cuin-public-api")
router = APIRouter()


def _pg():
    return psycopg2.connect(settings.DATABASE_URL)


# ----------------------------------------------------------------------
# Auth -- one global bearer token (settings.PUBLIC_API_BEARER_TOKEN),
# not a per-caller DB-issued key. auto_error=False so a missing header
# produces our own 401 with a helpful message instead of FastAPI's
# generic one. secrets.compare_digest avoids a timing side-channel on
# the comparison.
# ----------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)


def require_bearer_token(creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme)) -> None:
    if not creds or not creds.credentials:
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization: Bearer <token> header. Click 'Authorize' in the Swagger UI, or see the API page's Getting Started panel for the current token.",
        )
    if not secrets.compare_digest(creds.credentials, settings.PUBLIC_API_BEARER_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid bearer token.")
    _check_rate_limit()


# In-memory sliding-window rate limiter -- a single global bucket,
# matching the single global token. Deliberately simple: correct for
# a single backend process, NOT correct across multiple replicas
# (each process has its own counter) -- acceptable for v1's
# single-instance deployment, flagged here so a future multi-instance
# rollout knows to swap this for a shared store (Redis INCR+EXPIRE)
# rather than silently under-enforcing the limit.
_rate_lock = threading.Lock()
_rate_bucket: List[float] = []


def _check_rate_limit() -> None:
    now = time.monotonic()
    with _rate_lock:
        cutoff = now - 60.0
        while _rate_bucket and _rate_bucket[0] < cutoff:
            _rate_bucket.pop(0)
        if len(_rate_bucket) >= settings.PUBLIC_API_RATE_LIMIT_PER_MIN:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded ({settings.PUBLIC_API_RATE_LIMIT_PER_MIN} requests/min). Try again shortly.",
            )
        _rate_bucket.append(now)


# ----------------------------------------------------------------------
# Run resolution -- deliberately Doris-preferring (not "any engine's
# latest completed run"): this API's whole value proposition is
# checking against the bank's live, production-scale identity graph,
# which per this deployment's own convention (DatasourceStartRequest's
# default) IS the Doris engine. An explicit run_id always wins,
# regardless of its engine, so a caller can still pin a specific
# DuckDB snapshot on purpose.
# ----------------------------------------------------------------------

def _resolve_screening_run(run_id: Optional[str]):
    run_service = get_run_service()
    if run_id:
        run = run_service.get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
        if run.status != RunStatus.COMPLETED:
            raise HTTPException(status_code=409, detail=f"Run {run_id} is not COMPLETED (status={run.status.value})")
        return run

    all_runs, _ = run_service.list_runs(page=1, page_size=200)
    doris_completed = [r for r in all_runs if r.status == RunStatus.COMPLETED and r.engine == "doris"]
    if doris_completed:
        return doris_completed[0]  # list_runs sorts started_at DESC -- first is latest
    any_completed = [r for r in all_runs if r.status == RunStatus.COMPLETED]
    if any_completed:
        return any_completed[0]
    raise HTTPException(status_code=404, detail="No completed identity-resolution run available to screen against")


def _table_exists(session, name: str) -> bool:
    if session.engine == "doris":
        row = session.con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ? AND table_schema = DATABASE()", [name],
        ).fetchone()
    else:
        row = session.con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]).fetchone()
    return bool(row and row[0])


# ----------------------------------------------------------------------
# Request / response models
# ----------------------------------------------------------------------

class IdentityScreenRequest(BaseModel):
    full_name: str = Field(..., min_length=2, max_length=500, examples=["MD RAHIM UDDIN"])
    date_of_birth: Optional[str] = Field(
        None, examples=["1985-03-14"],
        description="Any of YYYY-MM-DD, DD-MM-YYYY, MM-DD-YYYY, DD/MM/YYYY, DD.MM.YYYY, YYYYMMDD, 'DD Mon YYYY'. Parsed the same way the batch pipeline parses a source file's BIRTH_DATE column.",
    )
    mobile: Optional[str] = Field(None, examples=["01712345678"], description="Bangladeshi mobile number, any common formatting (+880/880/leading-0 all accepted).")
    email: Optional[str] = Field(None, examples=["rahim.uddin@example.com"])
    document_type: Optional[str] = Field(None, examples=["NID"], description="e.g. NID, PASSPORT, TIN, BRC, or a bank-specific type. Required if document_number is given.")
    document_number: Optional[str] = Field(None, examples=["19855012345678901"])
    address: Optional[str] = Field(None, examples=["House 12, Road 5, Gulshan, Dhaka"], description="Informational only -- address is never used to decide a match (see 'notes' in the response), only shown as supporting evidence when other fields already establish a match.")
    segment: Optional[str] = Field(None, examples=["INDIVIDUAL"], description="INDIVIDUAL or COMPANY. Auto-classified from full_name if omitted.")
    run_id: Optional[str] = Field(
        None, examples=[None],
        description="Pin the identity snapshot to check against (a completed run_id from GET /runs). Leave as null/omit to use the latest completed Doris run -- this is what you want for almost every call.",
    )
    max_candidates: int = Field(5, ge=1, le=20)
    min_confidence_pct: float = Field(50.0, ge=0, le=100, description="Candidates scoring below this are omitted entirely. Defaults to the platform's own REVIEW-tier floor.")

    @model_validator(mode="after")
    def _require_an_identifying_signal(self):
        if not any([self.mobile, self.email, self.document_number, self.date_of_birth]):
            raise ValueError(
                "At least one of mobile, email, document_number, or date_of_birth is required -- "
                "full_name alone cannot be matched against the identity graph (name-only search has no bounded, indexed lookup path)."
            )
        if self.document_number and not self.document_type:
            raise ValueError("document_type is required when document_number is provided (e.g. 'NID', 'PASSPORT', 'TIN', 'BRC').")
        return self


class MatchedField(BaseModel):
    field: str
    matched: bool
    similarity: float
    explanation: str


class CandidateMatch(BaseModel):
    entity_id: Optional[str] = Field(None, description="Null if this record has never been resolved into a formal entity (e.g. a not-yet-clustered singleton).")
    global_id: Optional[str] = None
    global_id_state: Optional[str] = Field(None, description="UNASSIGNED | DRAFT | CONFIRMED | CONFLICT | RETIRED")
    matched_customer_code: str = Field(..., description="The existing record this query matched against, for the bank's own cross-reference.")
    representative_name: Optional[str] = None
    member_count: Optional[int] = Field(None, description="How many source records are already merged into this entity.")
    confidence_pct: float
    tier: str = Field(..., description="AUTO_LINK or REVIEW (REJECT-tier candidates are never returned).")
    matched_fields: List[MatchedField]


class IdentityScreenResponse(BaseModel):
    query_id: str
    checked_at: datetime
    run_id: str
    engine: str
    segment: str
    candidate_pool_size: int = Field(..., description="How many existing records were compared against, before filtering by min_confidence_pct.")
    already_known: bool = Field(..., description="True only when an AUTO_LINK-tier match was found -- the platform's own auto-confirm threshold.")
    has_confirmed_global_id: bool
    recommendation: str = Field(..., description="EXISTING_CUSTOMER | POSSIBLE_MATCH_NEEDS_REVIEW | NEW_CUSTOMER")
    best_match: Optional[CandidateMatch] = None
    candidates: List[CandidateMatch]
    notes: List[str] = Field(default_factory=list)


class RunSummary(BaseModel):
    run_id: str
    engine: str
    started_at: datetime
    ended_at: Optional[datetime]
    records_in: int


class GlobalIdLookupResponse(BaseModel):
    global_id: str
    found: bool
    entity_id: Optional[str] = None
    global_id_state: Optional[str] = None
    status: Optional[str] = None
    member_count: Optional[int] = None
    representative_name: Optional[str] = None


# ----------------------------------------------------------------------
# Candidate pool -- mirrors engine.blocking.deterministic_blocker's
# exact predicates for a single ad-hoc record (see that module's
# docstring): exact mobile/email/document match, or exact
# (rare-token-set, full-precision DOB) match. Address is never a
# blocking key, by design (WEAK/display-only evidence) -- see that
# module's docstring for why this is correct, not a gap.
# ----------------------------------------------------------------------

_POOL_LIMIT_PER_KEY = 200


def _find_candidate_codes(session, mobile_norm, email_norm, doc_type_norm, doc_value_norm, rare_tokens, dob_iso, dob_precision) -> set:
    codes: set = set()

    def _by_identifier(id_type: str, value_norm: str, doc_type: Optional[str] = None):
        if doc_type:
            rows = session.con.execute(
                "SELECT DISTINCT customer_code FROM identifiers "
                "WHERE is_valid AND NOT is_suppressed AND id_type = ? AND doc_type = ? AND value_norm = ? "
                f"LIMIT {_POOL_LIMIT_PER_KEY}",
                [id_type, doc_type, value_norm],
            ).fetchall()
        else:
            rows = session.con.execute(
                "SELECT DISTINCT customer_code FROM identifiers "
                "WHERE is_valid AND NOT is_suppressed AND id_type = ? AND value_norm = ? "
                f"LIMIT {_POOL_LIMIT_PER_KEY}",
                [id_type, value_norm],
            ).fetchall()
        codes.update(r[0] for r in rows)

    if mobile_norm:
        _by_identifier("mobile", mobile_norm)
    if email_norm:
        _by_identifier("email", email_norm)
    if doc_value_norm:
        _by_identifier("document", doc_value_norm, doc_type_norm)

    if rare_tokens and dob_iso and dob_precision == "FULL" and _table_exists(session, "customer_name_keys"):
        name_key = hashlib.md5("|".join(sorted(rare_tokens)).encode("utf-8")).hexdigest()
        rows = session.con.execute(
            f"SELECT customer_code FROM customer_name_keys WHERE name_key = ? AND dob_iso = ? LIMIT {_POOL_LIMIT_PER_KEY}",
            [name_key, dob_iso],
        ).fetchall()
        codes.update(r[0] for r in rows)

    return codes


def _parse_token_array(value) -> List[str]:
    """customer_scalars.name_tokens comes back as a native Python list on DuckDB but as a JSON-encoded
    string (e.g. '["SYED", "ASAD"]') on Doris -- session.con's shared .execute()/.fetchall() surface
    doesn't normalize array-typed columns across engines. Handle both; list(str) would otherwise
    silently explode the string into individual characters."""
    if not value:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except (ValueError, TypeError):
            return []
    return list(value)


def _fetch_candidate_data(session, codes: List[str]) -> Tuple[Dict[str, dict], Dict[str, List[dict]]]:
    """Bulk (single-query) fetch of customer_scalars + identifiers for the pool -- never a per-candidate loop."""
    if not codes:
        return {}, {}
    placeholders = ",".join(["?"] * len(codes))

    scalar_rows = session.con.execute(
        f"SELECT customer_code, name_norm, name_tokens, dob_iso, dob_precision FROM customer_scalars WHERE customer_code IN ({placeholders})",
        codes,
    ).fetchall()
    scalars = {
        r[0]: {"name_norm": r[1], "name_tokens": _parse_token_array(r[2]), "dob_iso": r[3], "dob_precision": r[4]}
        for r in scalar_rows
    }

    id_rows = session.con.execute(
        f"SELECT customer_code, id_type, doc_type, value_norm FROM identifiers "
        f"WHERE is_valid AND NOT is_suppressed AND customer_code IN ({placeholders})",
        codes,
    ).fetchall()
    identifiers: Dict[str, List[dict]] = {}
    for code, id_type, doc_type, value_norm in id_rows:
        identifiers.setdefault(code, []).append({"id_type": id_type, "doc_type": doc_type, "value_norm": value_norm})

    return scalars, identifiers


def _build_evidence(query_identifiers: Dict[Tuple[str, Optional[str]], List[str]], query_rare_tokens: List[str],
                     query_dob_iso: Optional[str], query_dob_precision: Optional[str],
                     cand_identifiers: List[dict], cand_scalar: dict) -> Dict:
    """Builds the exact evidence dict shape engine.scoring.confidence.score_pair() expects (see evidence.py)."""
    cand_grouped: Dict[Tuple[str, Optional[str]], List[str]] = {}
    for row in cand_identifiers:
        key = (row["id_type"], row["doc_type"])
        cand_grouped.setdefault(key, []).append(row["value_norm"])

    all_keys = set(query_identifiers) | set(cand_grouped)
    identifiers_evidence = []
    for id_type, doc_type in all_keys:
        values_a = sorted(set(query_identifiers.get((id_type, doc_type), [])))
        values_b = sorted(set(cand_grouped.get((id_type, doc_type), [])))
        identifiers_evidence.append({
            "id_type": id_type,
            "doc_type": doc_type,
            "values_a": values_a,
            "values_b": values_b,
            "intersection": sorted(set(values_a) & set(values_b)),
        })

    cand_tokens = cand_scalar.get("name_tokens") or []
    cand_dob = cand_scalar.get("dob_iso")
    cand_dob_prec = cand_scalar.get("dob_precision")
    name_dob = {
        "name_a": None, "name_b": cand_scalar.get("name_norm"),
        "tokens_a": query_rare_tokens, "tokens_b": cand_tokens,
        "token_intersection": sorted(set(query_rare_tokens) & set(cand_tokens)),
        "token_union": list(dict.fromkeys(list(query_rare_tokens) + list(cand_tokens))),
        "dob_a": query_dob_iso, "dob_b": cand_dob,
        "dob_precision_a": query_dob_precision, "dob_precision_b": cand_dob_prec,
    }

    return {"identifiers": identifiers_evidence, "name_dob": name_dob, "raw_fields": {}}


def _entity_lookup(pg_conn, codes: List[str]) -> Dict[str, dict]:
    """Bulk customer_code -> {entity_id, global_ref, global_ref_state, status, member_count} for current membership."""
    if not codes:
        return {}
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT em.customer_code, e.entity_id::text, e.global_ref, e.global_ref_state, e.status "
        "FROM entity_members em JOIN entities e ON e.entity_id = em.entity_id "
        "WHERE em.customer_code = ANY(%s::text[]) AND em.valid_to IS NULL",
        (codes,),
    )
    rows = cur.fetchall()
    by_code = {r[0]: {"entity_id": r[1], "global_ref": r[2], "global_ref_state": r[3], "status": r[4]} for r in rows}

    entity_ids = list({v["entity_id"] for v in by_code.values()})
    if entity_ids:
        cur.execute(
            "SELECT entity_id::text, COUNT(*) FROM entity_members WHERE entity_id::text = ANY(%s::text[]) AND valid_to IS NULL GROUP BY entity_id",
            (entity_ids,),
        )
        counts = dict(cur.fetchall())
        for v in by_code.values():
            v["member_count"] = counts.get(v["entity_id"], 1)
    return by_code


def _log_screening_call(pg_conn, query_id, api_key_id, run_id, engine, segment, fields_provided,
                         pool_size, returned, already_known, recommendation, best_match, response_ms):
    cur = pg_conn.cursor()
    cur.execute(
        "INSERT INTO api_screening_log (query_id, api_key_id, run_id, engine, segment, fields_provided, "
        "candidate_pool_size, candidates_returned, already_known, recommendation, best_match_entity_id, "
        "best_match_confidence_pct, response_ms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (query_id, api_key_id, run_id, engine, segment, fields_provided, pool_size, returned, already_known,
         recommendation, best_match.entity_id if best_match else None,
         best_match.confidence_pct if best_match else None, response_ms),
    )
    pg_conn.commit()


# ----------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------

@router.get("/health", tags=["Health"])
async def health() -> dict:
    return {"status": "healthy", "service": "CUIN Identity Recognition API", "version": "1.0.0"}


@router.get("/runs", response_model=List[RunSummary], tags=["Identity Recognition"])
async def list_screenable_runs(_: None = Depends(require_bearer_token)):
    """Completed identity-resolution snapshots available to pin via `run_id` on POST /identity/screen."""
    all_runs, _total = get_run_service().list_runs(page=1, page_size=50)
    return [
        RunSummary(run_id=r.run_id, engine=r.engine, started_at=r.started_at, ended_at=r.ended_at, records_in=r.counters.records_in)
        for r in all_runs if r.status == RunStatus.COMPLETED
    ]


@router.get("/identity/global-id/{global_id}", response_model=GlobalIdLookupResponse, tags=["Identity Recognition"])
async def lookup_global_id(global_id: str, _: None = Depends(require_bearer_token)):
    """Fast path for when the bank already has a candidate Global ID (e.g. from a prior screen) and just wants to confirm it's still live."""
    pg_conn = _pg()
    try:
        cur = pg_conn.cursor()
        cur.execute(
            "SELECT entity_id::text, global_ref_state, status, display_name FROM entities WHERE upper(global_ref) = upper(%s)",
            (global_id,),
        )
        row = cur.fetchone()
        if not row:
            return GlobalIdLookupResponse(global_id=global_id, found=False)
        entity_id, state, status, display_name = row
        cur.execute("SELECT COUNT(*) FROM entity_members WHERE entity_id = %s AND valid_to IS NULL", (entity_id,))
        member_count = cur.fetchone()[0]
        return GlobalIdLookupResponse(
            global_id=global_id, found=True, entity_id=entity_id, global_id_state=state,
            status=status, member_count=member_count, representative_name=display_name,
        )
    finally:
        pg_conn.close()


@router.post("/identity/screen", response_model=IdentityScreenResponse, tags=["Identity Recognition"])
async def screen_identity(request: IdentityScreenRequest, _: None = Depends(require_bearer_token)):
    """
    Screen a new applicant's details against the resolved identity
    graph before opening an account. Returns whether this person
    already exists, whether they already carry a confirmed Global ID,
    and (when relevant) ranked candidate matches with the same
    rule-by-rule explainability the officer workbench shows.

    Uses the platform's ACTIVE rule catalog (Settings > Matching &
    Confidence) at call time -- if a bank retunes matching weights or
    thresholds, this endpoint reflects the change on the very next
    call, with no redeploy.
    """
    t0 = time.monotonic()
    query_id = str(uuid.uuid4())
    notes: List[str] = []
    fields_provided = [f for f in ["mobile", "email", "document_number", "date_of_birth", "address"]
                        if getattr(request, f, None)]

    # A blank/whitespace-only run_id (e.g. left over from a client's
    # unedited example payload) means "no run_id was really given" --
    # treat it as None rather than a literal lookup that 404s.
    run_id = request.run_id.strip() if request.run_id and request.run_id.strip() else None
    run = _resolve_screening_run(run_id)
    session = open_run_readonly(run.run_id)
    try:
        if not _table_exists(session, "customer_scalars") or not _table_exists(session, "identifiers"):
            raise HTTPException(status_code=409, detail=f"Run {run.run_id} has no persisted identity tables to screen against")

        # -- Normalize the query record with the SAME functions the batch pipeline uses --
        name_norm, rare_tokens = norm_name(request.full_name)
        mobile_norm, _ = norm_mobile_bd(request.mobile) if request.mobile else (None, None)
        email_norm, _ = norm_email(request.email) if request.email else (None, None)
        doc_type_norm, doc_value_norm = None, None
        if request.document_number and request.document_type:
            doc_type_norm, doc_value_norm, doc_reject = parse_document(f"{request.document_type.strip().upper()}:{request.document_number}")
            if doc_reject:
                notes.append(f"document_number could not be validated ({doc_reject}) -- it was not used to search")
        dob_iso, dob_precision = norm_dob(request.date_of_birth) if request.date_of_birth else (None, None)
        address_norm = norm_address(request.address) if request.address else None

        if request.mobile and not mobile_norm:
            notes.append("mobile could not be validated as a Bangladeshi number -- it was not used to search")
        if request.email and not email_norm:
            notes.append("email failed format validation -- it was not used to search")
        if request.date_of_birth and not dob_iso:
            notes.append("date_of_birth could not be parsed -- it was not used to search")
        if not rare_tokens:
            notes.append("full_name reduced to no usable name tokens (e.g. honorifics only) -- name/DOB search was skipped")
        notes.append("address is never used to decide a match (informational/display evidence only), matching the batch pipeline's own rules")

        # -- Rule catalog + segment (same source of truth as the batch pipeline) --
        pg_conn = _pg()
        try:
            catalog = get_active_catalog(pg_conn)
            segmentation = catalog.segmentation or SegmentationConfig()
            segment = request.segment.upper() if request.segment else classify_segment_python(name_norm, segmentation)
            ruleset = catalog.match_ruleset_for_segment(segment)

            # -- Candidate pool (bounded, blocking-equivalent) --
            candidate_codes = _find_candidate_codes(
                session, mobile_norm, email_norm, doc_type_norm, doc_value_norm, rare_tokens, dob_iso, dob_precision,
            )
            candidate_codes.discard(None)
            pool_size = len(candidate_codes)

            best_match: Optional[CandidateMatch] = None
            candidates: List[CandidateMatch] = []

            if candidate_codes:
                codes_list = list(candidate_codes)
                scalars, id_map = _fetch_candidate_data(session, codes_list)

                # Same-segment only, mirroring "segmentation gates merging" -- a
                # cross-segment candidate is structurally never AUTO_LINK/REVIEW.
                if segmentation.enabled:
                    codes_list = [
                        c for c in codes_list
                        if classify_segment_python((scalars.get(c) or {}).get("name_norm"), segmentation) == segment
                    ]

                query_identifiers: Dict[Tuple[str, Optional[str]], List[str]] = {}
                if mobile_norm:
                    query_identifiers[("mobile", None)] = [mobile_norm]
                if email_norm:
                    query_identifiers[("email", None)] = [email_norm]
                if doc_value_norm:
                    query_identifiers[("document", doc_type_norm)] = [doc_value_norm]
                if address_norm:
                    query_identifiers[("address", None)] = [address_norm]

                scored: List[Tuple[float, str, "object"]] = []
                for code in codes_list:
                    cand_scalar = scalars.get(code) or {}
                    cand_ids = id_map.get(code, [])
                    evidence = _build_evidence(query_identifiers, rare_tokens, dob_iso, dob_precision, cand_ids, cand_scalar)
                    result = score_pair(evidence, ruleset)
                    if result.decision in ("AUTO_LINK", "REVIEW") and result.confidence_pct >= request.min_confidence_pct:
                        scored.append((result.confidence_pct, code, result))

                scored.sort(key=lambda t: t[0], reverse=True)
                top = scored[: request.max_candidates]

                entity_info = _entity_lookup(pg_conn, [code for _, code, _ in top])
                for confidence_pct, code, result in top:
                    ev = _build_evidence(query_identifiers, rare_tokens, dob_iso, dob_precision, id_map.get(code, []), scalars.get(code) or {})
                    field_evidence = evidence_to_field_evidence(ev)
                    ent = entity_info.get(code, {})
                    match = CandidateMatch(
                        entity_id=ent.get("entity_id"),
                        global_id=ent.get("global_ref"),
                        global_id_state=ent.get("global_ref_state"),
                        matched_customer_code=code,
                        representative_name=(scalars.get(code) or {}).get("name_norm"),
                        member_count=ent.get("member_count"),
                        confidence_pct=confidence_pct,
                        tier=result.decision,
                        matched_fields=[
                            MatchedField(field=fe.field_name, matched=fe.match_weight > 0, similarity=fe.similarity_score, explanation=fe.explanation)
                            for fe in field_evidence
                        ],
                    )
                    candidates.append(match)

                auto_link_matches = [c for c in candidates if c.tier == "AUTO_LINK"]
                if auto_link_matches:
                    best_match = auto_link_matches[0]

            if best_match:
                already_known = True
                recommendation = "EXISTING_CUSTOMER"
            elif candidates:
                already_known = False
                recommendation = "POSSIBLE_MATCH_NEEDS_REVIEW"
            else:
                already_known = False
                recommendation = "NEW_CUSTOMER"
                if pool_size == 0:
                    notes.append("no existing record shared any exact identifier or (name, date of birth) with this submission")

            has_confirmed_global_id = bool(best_match and best_match.global_id_state == "CONFIRMED")

            response = IdentityScreenResponse(
                query_id=query_id, checked_at=datetime.now(timezone.utc), run_id=run.run_id, engine=run.engine,
                segment=segment, candidate_pool_size=pool_size, already_known=already_known,
                has_confirmed_global_id=has_confirmed_global_id, recommendation=recommendation,
                best_match=best_match, candidates=candidates, notes=notes,
            )

            _log_screening_call(
                pg_conn, query_id, None, run.run_id, run.engine, segment, fields_provided,
                pool_size, len(candidates), already_known, recommendation, best_match,
                int((time.monotonic() - t0) * 1000),
            )
            return response
        finally:
            pg_conn.close()
    finally:
        session.close()
