"""
CUIN v2 - Doris Pipeline Orchestrator (Ruleset v2, Doris execution engine)

Same public contract as pipeline.duckdb_orchestrator.DuckDBPipelineOrchestrator
(constructor kwargs, async run(run_id, raw_records=None, mode="FULL"),
get_scores/get_decisions/get_auto_links/get_uniques/get_result_clusters/
get_fingerprints) -- so api/routes_datasource.py can pick either
orchestrator class purely by the `engine` request field, with zero
changes anywhere else. Produces the SAME decisions as the DuckDB path
for the same ruleset: engine.normalize.explode_dialect,
engine.blocking.suppression_dialect, and engine.scoring.evidence_dialect
are dialect-portable twins of the DuckDB-only modules
pipeline.duckdb_orchestrator uses, proven byte-identical to them on
DuckDB and pair-for-pair identical to the DuckDB orchestrator's actual
output on live Doris -- see tests/integration/test_doris_cross_engine_parity.py.

Where this path is DELIBERATELY better than the DuckDB one, not just
equivalent: blocking runs through engine.rules.compiler against the
UI-editable rule catalog (engine.rules.store.get_active_catalog())
rather than a hardcoded two-pass query, so a Doris-backed run
automatically respects whatever the Settings UI has saved.

Each run gets its own Doris database (`cuin_run_<run_id>`), mirroring
the DuckDB orchestrator's per-run persistent .duckdb file -- both
exist after the run completes and can be reopened (a future
extension point for redecide/reblock against Doris-backed runs, not
built in this pass; see the migration plan's noted follow-ups).
"""

import asyncio
import csv as csv_module
import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional, Callable, Any

import pymysql


def _parse_array(value) -> list:
    """
    pymysql returns Doris ARRAY<...> columns as their JSON-text
    representation (e.g. the Python str '["a","b"]'), NOT as a native
    Python list the way DuckDB's connector does for LIST columns.
    Calling list() directly on that string (an easy, silent mistake --
    it "works" without erroring) chops it into individual CHARACTERS
    instead of parsing elements, corrupting every array_size/
    intersection/jaccard computation downstream. Verified live: an
    empty Doris array round-trips as the 2-character string '[]',
    which list('[]') turns into ['[', ']'] -- a non-empty Python list
    for what was actually an empty array, which is precisely how this
    bug was first caught (spurious AUTO_LINK decisions from a jaccard
    computed over character counts instead of token counts).
    """
    if value is None:
        return []
    if isinstance(value, str):
        return json.loads(value)
    return list(value)

from pipeline.orchestrator import PipelineStage, StageProgress, PipelineResult
from engine.structures import MatchScore, MatchDecision, FieldEvidence
from engine.ports.doris_conn import DorisConnection
from engine.ports.doris_dialect import DorisDialect
from engine.ports.doris_ingest import create_raw_table_sql, load_parquet_to_doris, local_parquet_view_sql
from engine.normalize import explode_dialect
from engine.blocking import suppression_dialect
from engine.scoring.evidence_dialect import build_pair_evidence
from engine.scoring.evidence import evidence_to_field_evidence
from engine.scoring.confidence import score_pair
from engine.rules.match_rules import DEFAULT_MATCH_RULESET
from engine.rules import store as rule_store
from engine.rules.compiler import compile_and_build
from engine.rules.confidence_compiler import compile_confidence_sql
from engine.clustering import get_cluster_manager
from engine.clustering import entity_resolver
from engine.clustering.build_clusters import build_clusters, load_active_overrides
from engine.ruleset.config import get_default_ruleset
from engine.ruleset.effective import resolve_from_active_catalog
from engine.ruleset.version import RULESET_VERSION, ruleset_fingerprint
from engine.determinism import input_fingerprint, fingerprint_edges, fingerprint_clusters, output_fingerprint

logger = logging.getLogger(__name__)

PARQUET_PATH = "data_source/oracle_data.parquet"


def _sanitize_db_name(run_id: str) -> str:
    return "cuin_run_" + run_id.replace("-", "_")


class DorisPipelineOrchestrator:
    """Deterministic entity-resolution pipeline over the Oracle parquet datasource, executed on Apache Doris."""

    def __init__(
        self,
        blocking_config=None,
        scoring_config=None,
        progress_callback: Optional[Callable[[StageProgress], None]] = None,
        run_id: Optional[str] = None,
    ):
        self.blocking_config = blocking_config
        self.scoring_config = scoring_config
        self.progress_callback = progress_callback
        self.run_id = run_id

        # See pipeline.duckdb_orchestrator's identical comment --
        # EffectiveRuleset resolves decision thresholds from the active
        # catalog when one exists, YAML otherwise.
        self.ruleset = resolve_from_active_catalog()
        # One catalog fetch, reused for match_ruleset/segmentation below.
        self._catalog = self._resolve_catalog()
        self.match_ruleset = self._catalog.match_ruleset
        # Stage 5: record segmentation (Company vs Individual). Disabled
        # by default -- see engine.segments.classifier.SegmentationConfig.
        self._segmentation = self._catalog.segmentation
        self._relationships: List[dict] = []
        # Stage 3: mirrors duckdb_orchestrator's identical field.
        self._override_conflicts: List[tuple] = []
        self._con: Optional[DorisConnection] = None
        self._database: Optional[str] = None
        self._dialect = DorisDialect()

        from api.config import settings
        self._doris_host = settings.DORIS_HOST
        self._doris_mysql_port = settings.DORIS_MYSQL_PORT
        self._doris_http_port = settings.DORIS_HTTP_PORT
        self._doris_user = settings.DORIS_USER
        self._doris_password = settings.DORIS_PASSWORD

        self._scores: Dict[str, MatchScore] = {}
        self._decisions: Dict[str, MatchDecision] = {}
        self._records: Dict[str, dict] = {}
        self._blocking_rules = []

        self._input_fp = None
        self._ruleset_fp = None
        self._edges_fp = None
        self._clusters_fp = None
        self._output_fp = None

    @staticmethod
    def _resolve_catalog():
        try:
            return rule_store.get_active_catalog()
        except Exception:
            logger.warning("Could not load active rule catalog, using seeded defaults", exc_info=True)
            from engine.rules.store import RuleCatalogVersion
            from engine.rules.catalog import DEFAULT_BLOCKING_RULES
            return RuleCatalogVersion(
                policy_version=0, blocking_rules=list(DEFAULT_BLOCKING_RULES), match_ruleset=DEFAULT_MATCH_RULESET,
                is_active=True, created_by="system", created_at="", approved_by=None, catalog_hash="",
            )

    async def _emit_progress(self, progress: StageProgress) -> None:
        if self.progress_callback:
            if asyncio.iscoroutinefunction(self.progress_callback):
                await self.progress_callback(progress)
            else:
                self.progress_callback(progress)

    def _connect(self) -> DorisConnection:
        if self._con is None:
            self._database = _sanitize_db_name(self.run_id or "adhoc")
            admin = pymysql.connect(
                host=self._doris_host, port=self._doris_mysql_port,
                user=self._doris_user, password=self._doris_password, autocommit=True,
            )
            with admin.cursor() as cur:
                cur.execute(f"CREATE DATABASE IF NOT EXISTS {self._database}")
                # Self-heal the single-BE / low-resource deployment case: a
                # fresh Doris FE defaults new OLAP tables to
                # replication_num=3, which a single-BE cluster can never
                # satisfy (CREATE TABLE fails outright). Only override when
                # there's actually just one BE -- a real multi-BE cluster
                # should keep its operator-chosen replication factor.
                try:
                    cur.execute("SHOW BACKENDS")
                    if cur.rowcount == 1:
                        cur.execute(
                            'ADMIN SET FRONTEND CONFIG ("force_olap_table_replication_num" = "1")'
                        )
                except Exception:
                    logger.warning("Could not verify/set single-BE replication config", exc_info=True)
            admin.close()
            self._con = DorisConnection(
                host=self._doris_host, port=self._doris_mysql_port,
                user=self._doris_user, password=self._doris_password, database=self._database,
            )
        return self._con

    # ------------------------------------------------------------------
    # Stage 1: Ingest (read the source Parquet in place, no copy)
    # ------------------------------------------------------------------
    async def _stage_ingest(self) -> int:
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.INGEST, status="running",
            message="Reading Oracle Parquet dataset in place (LOCAL() TVF)...",
        ))

        loop = asyncio.get_event_loop()

        def _load():
            con = self._connect()
            # Preferred path: read the Parquet files directly off the
            # BE's local disk via LOCAL(), same as DuckDB's
            # read_parquet() -- no Stream Load, no NDJSON round-trip, no
            # copy of the source living inside Doris (measured at 18%
            # of a run's total Doris disk footprint before this fix).
            # Falls back to the old Stream Load copy path for
            # deployments where that mount doesn't exist (multi-BE
            # clusters, or a Doris cluster not managed by this repo's
            # docker-compose) -- LOCAL() genuinely can't work there
            # without a shared-storage lake (S3()/catalog), which is a
            # larger deployment change than an ingest-time fallback
            # should silently attempt.
            try:
                be_relative_glob = f"{PARQUET_PATH}/*.parquet"
                con.execute(local_parquet_view_sql(con, "raw", be_relative_glob))
                count = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]
                logger.info(f"Doris reading source Parquet in place via LOCAL() ({count:,} rows)")
                return count, "local"
            except Exception:
                logger.warning(
                    "LOCAL() read-in-place failed (mount missing or multi-BE cluster) -- "
                    "falling back to Stream Load copy", exc_info=True,
                )
            con.execute(create_raw_table_sql())
            scratch_path = f"/tmp/cuin_doris_ingest_{self.run_id or 'adhoc'}.json"
            load_parquet_to_doris(
                parquet_path=PARQUET_PATH,
                fe_host=self._doris_host, fe_http_port=self._doris_http_port,
                user=self._doris_user, password=self._doris_password,
                database=self._database, table="raw",
                ndjson_scratch_path=scratch_path,
            )
            return con.execute("SELECT COUNT(*) FROM raw").fetchone()[0], "stream_load"

        record_count, method = await loop.run_in_executor(None, _load)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.INGEST, status="complete",
            records_in=record_count, records_out=record_count,
            duration_ms=duration,
            message=(
                f"Read {record_count:,} customer records in place from Parquet (no copy)"
                if method == "local" else
                f"Stream-loaded {record_count:,} customer records into Doris (fallback path)"
            ),
        ))
        return record_count

    # ------------------------------------------------------------------
    # Stage 2: Normalize
    # ------------------------------------------------------------------
    async def _stage_normalize(self, record_count: int) -> int:
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.NORMALIZE, status="running",
            message="Exploding array identifiers and validating (mobile/email/document/name/dob)...",
        ))

        loop = asyncio.get_event_loop()

        def _normalize():
            from engine.segments.classifier import build_customer_segments
            con = self._connect()
            explode_dialect.build_identifiers_table(con, self._dialect, source_relation="raw")
            thresholds = {
                "mobile": self.ruleset.suppression_mobile_max,
                "email": self.ruleset.suppression_email_max,
                "document": self.ruleset.suppression_document_max,
                "address": self.ruleset.suppression_address_max,
            }
            suppression_dialect.build_frequency_table(con, self._dialect, thresholds=thresholds)
            # Stage 5: see duckdb_orchestrator's identical comment --
            # always built, elision happens at decide time.
            build_customer_segments(con, self._dialect, self._segmentation)
            return con.execute("SELECT COUNT(*) FROM identifiers WHERE is_valid").fetchone()[0]

        valid_identifier_count = await loop.run_in_executor(None, _normalize)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.NORMALIZE, status="complete",
            records_in=record_count, records_out=valid_identifier_count,
            duration_ms=duration,
            message=f"{valid_identifier_count:,} valid identifiers after normalization/suppression",
        ))
        return valid_identifier_count

    # ------------------------------------------------------------------
    # Stage 3 & 4: Block + Candidates (rule-catalog driven)
    # ------------------------------------------------------------------
    async def _stage_block_and_candidates(self) -> int:
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.BLOCK, status="running",
            message="Compiling the active blocking-rule catalog to Doris SQL...",
        ))

        loop = asyncio.get_event_loop()

        def _block():
            con = self._connect()
            self._blocking_rules = self._catalog.blocking_rules
            compile_and_build(con, self._catalog.blocking_rules, self._dialect)
            return con.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]

        candidate_count = await loop.run_in_executor(None, _block)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.BLOCK, status="complete", duration_ms=duration,
            message="Blocking rules compiled and applied",
        ))
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CANDIDATES, status="complete",
            records_out=candidate_count, duration_ms=duration,
            message=f"{candidate_count:,} candidate pairs generated",
        ))
        return candidate_count

    # ------------------------------------------------------------------
    # Stage 5 & 6: Score and Decide
    # ------------------------------------------------------------------
    async def _stage_score_and_decide(self, candidate_count: int):
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.SCORE, status="running",
            message="Building per-field evidence and applying tiered decision rule...",
        ))

        loop = asyncio.get_event_loop()

        def _score_and_decide():
            from services.review_service import get_review_service
            from engine.segments.relationships import detect_relationship_evidence
            review_service = get_review_service()

            con = self._connect()
            build_pair_evidence(con, self._dialect)

            pairs = con.execute("SELECT a_key, b_key FROM candidate_pairs").fetchall()

            # Stage 5: customer_code -> segment, mirrors duckdb_orchestrator.
            segments: Dict[str, str] = dict(
                con.execute("SELECT customer_code, segment FROM customer_segments").fetchall()
            )

            # Stage 5.1: bulk-fetch raw columns any RAW_COLUMN match
            # rule references, mirrors duckdb_orchestrator exactly --
            # except array-typed columns need _parse_array() here,
            # since pymysql returns Doris ARRAY<...> as JSON text, not
            # a native Python list (see _parse_array's docstring).
            from engine.rules.match_rules import raw_column_specs_in_catalog
            raw_column_specs = raw_column_specs_in_catalog(self._catalog)
            raw_columns = sorted(raw_column_specs)
            raw_by_code: Dict[str, dict] = {}
            if raw_columns:
                cols_sql = ", ".join(raw_columns)
                for row in con.execute(f"SELECT CUSTOMER_CODE, {cols_sql} FROM raw").fetchall():
                    code = row[0]
                    values = {}
                    for col, val in zip(raw_columns, row[1:]):
                        values[col] = _parse_array(val) if raw_column_specs[col] else val
                    raw_by_code[code] = values

            id_evidence: Dict[tuple, list] = {}
            for row in con.execute("""
                SELECT a_key, b_key, id_type, doc_type, values_a, values_b, intersection
                FROM pair_identifier_evidence
            """).fetchall():
                a_key, b_key, id_type, doc_type, values_a, values_b, intersection = row
                id_evidence.setdefault((a_key, b_key), []).append({
                    "id_type": id_type, "doc_type": doc_type,
                    "values_a": _parse_array(values_a),
                    "values_b": _parse_array(values_b),
                    "intersection": _parse_array(intersection),
                })

            name_dob_evidence: Dict[tuple, dict] = {}
            for row in con.execute("""
                SELECT a_key, b_key, name_a, name_b, tokens_a, tokens_b,
                       token_intersection, token_union,
                       dob_a, dob_b, dob_precision_a, dob_precision_b
                FROM pair_name_dob_evidence
            """).fetchall():
                (a_key, b_key, name_a, name_b, tokens_a, tokens_b,
                 token_intersection, token_union, dob_a, dob_b, prec_a, prec_b) = row
                name_dob_evidence[(a_key, b_key)] = {
                    "name_a": name_a, "name_b": name_b,
                    "tokens_a": _parse_array(tokens_a),
                    "tokens_b": _parse_array(tokens_b),
                    "token_intersection": _parse_array(token_intersection),
                    "token_union": _parse_array(token_union),
                    "dob_a": dob_a, "dob_b": dob_b,
                    "dob_precision_a": prec_a, "dob_precision_b": prec_b,
                }

            auto_links, review_items, rejected = [], [], []
            empty_name_dob = {
                "tokens_a": [], "tokens_b": [], "token_intersection": [], "token_union": [],
            }

            for a_key, b_key in pairs:
                key = (a_key, b_key)
                evidence = {
                    "identifiers": id_evidence.get(key, []),
                    "name_dob": name_dob_evidence.get(key, empty_name_dob),
                }
                if raw_columns:
                    fields_a, fields_b = raw_by_code.get(a_key, {}), raw_by_code.get(b_key, {})
                    evidence["raw_fields"] = {col: (fields_a.get(col), fields_b.get(col)) for col in raw_columns}

                seg_a = segments.get(a_key, "ALL")
                seg_b = segments.get(b_key, "ALL")
                if seg_a != seg_b:
                    # Cross-segment: not an identity decision -- record
                    # the shared-evidence connection as a traceable
                    # relationship instead. Mirrors duckdb_orchestrator.
                    rel_evidence = detect_relationship_evidence(evidence)
                    if rel_evidence:
                        self._relationships.append({
                            "a_key": a_key, "b_key": b_key,
                            "a_segment": seg_a, "b_segment": seg_b,
                            "shared_evidence": [e.to_dict() for e in rel_evidence],
                        })
                    continue

                segment_ruleset = self._catalog.match_ruleset_for_segment(seg_a)
                pair_score = score_pair(evidence, segment_ruleset)
                decision = MatchDecision(pair_score.decision)
                score_value = pair_score.confidence_pct / 100.0

                pair_id = f"{a_key}:{b_key}"

                match_score = MatchScore(
                    pair_id=pair_id, a_key=a_key, b_key=b_key, score=score_value,
                    evidence=evidence_to_field_evidence(evidence),
                    hard_conflicts=pair_score.vetoes, signals_hit=pair_score.signals_hit,
                )
                self._scores[pair_id] = match_score
                self._decisions[pair_id] = decision

                if decision == MatchDecision.AUTO_LINK:
                    auto_links.append((a_key, b_key))
                elif decision == MatchDecision.REVIEW:
                    review_items.append((a_key, b_key))
                    review_service.queue_for_review(
                        pair_id=pair_id, run_id=self.run_id, a_key=a_key, b_key=b_key,
                        score=score_value,
                        evidence=[
                            {
                                "field": ev.field_name, "value_a": ev.value_a, "value_b": ev.value_b,
                                "type": ev.comparison_type, "similarity": ev.similarity_score,
                                "explanation": ev.explanation,
                            }
                            for ev in match_score.evidence
                        ],
                        signals=pair_score.signals_hit,
                    )
                else:
                    rejected.append((a_key, b_key))

            review_service._save_queue(self.run_id)

            # Durable baseline for the Settings UI's instant redecide/
            # reblock (api/routes_rules.py, via engine.ports.run_session):
            # the SAME confidence logic compiled to SQL, run once here
            # and persisted as `pair_decisions` in this run's Doris
            # database. Without this, redecide has nothing to diff
            # against on a Doris-backed run (baseline_decision_counts
            # would always be empty) even once run_session can reach
            # the database at all. Mirrors duckdb_orchestrator's
            # identical block.
            try:
                con.execute(compile_confidence_sql(self.match_ruleset, self._dialect, table_name="pair_decisions"))
            except Exception:
                logger.warning(
                    "Could not persist SQL-compiled pair_decisions baseline "
                    "(redecide/reblock will fall back to a fresh compile)", exc_info=True,
                )

            # Stage 1 of the entity resolution workbench plan -- mirrors
            # duckdb_orchestrator's identical block. See
            # confidence_compiler.compile_contributions_sql's docstring.
            try:
                from engine.rules.confidence_compiler import compile_contributions_sql
                con.execute(compile_contributions_sql(self.match_ruleset, self._dialect, table_name="pair_contributions"))
            except Exception:
                logger.warning(
                    "Could not persist pair_contributions (workbench score breakdown unavailable for this run)",
                    exc_info=True,
                )

            return auto_links, review_items, rejected

        auto_links, review_items, rejected = await loop.run_in_executor(None, _score_and_decide)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        relationships_msg = f", {len(self._relationships):,} cross-segment relationships" if self._segmentation.enabled else ""
        await self._emit_progress(StageProgress(
            stage=PipelineStage.SCORE, status="complete",
            records_in=candidate_count, records_out=len(self._scores), duration_ms=duration,
            message=f"{len(auto_links):,} auto-link, {len(review_items):,} review, {len(rejected):,} rejected{relationships_msg}",
        ))
        await self._emit_progress(StageProgress(
            stage=PipelineStage.DECIDE, status="complete", records_out=len(auto_links),
            message=f"{len(auto_links):,} pairs meet the auto-link rule",
        ))
        return auto_links, review_items, rejected

    # ------------------------------------------------------------------
    # Stage 7: Cluster (identical to the DuckDB path -- pure Python)
    # ------------------------------------------------------------------
    async def _stage_cluster(self, auto_links):
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CLUSTER, status="running",
            message="Building cohesion-guarded identity clusters...",
        ))

        loop = asyncio.get_event_loop()

        def _cluster():
            # Stage 3 of the entity resolution workbench plan -- mirrors
            # duckdb_orchestrator's identical block. See
            # engine.clustering.build_clusters's module docstring.
            must_link, must_not_link = [], []
            try:
                import psycopg2
                from api.config import settings
                if settings.PERSIST_TO_POSTGRES:
                    pg_conn = psycopg2.connect(settings.DATABASE_URL)
                    try:
                        must_link, must_not_link = load_active_overrides(pg_conn)
                    finally:
                        pg_conn.close()
            except Exception:
                logger.warning("Could not load officer overrides for clustering (proceeding with none)", exc_info=True)

            result = build_clusters(
                auto_links,
                max_cluster_size=self.ruleset.max_cluster_size,
                min_density=self.ruleset.min_density,
                must_link=must_link,
                must_not_link=must_not_link,
            )
            self._override_conflicts = result.override_conflicts
            return result.accepted_clusters, result.demoted_to_review, result.officer_exempt_count

        accepted_clusters, demoted_to_review, officer_exempt_count = await loop.run_in_executor(None, _cluster)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        override_msg = f", {officer_exempt_count} officer-confirmed" if officer_exempt_count else ""
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CLUSTER, status="complete",
            records_out=len(accepted_clusters), duration_ms=duration,
            message=(
                f"{len(accepted_clusters):,} cohesive clusters formed "
                f"({demoted_to_review} edges demoted to review for low cohesion/oversized component{override_msg})"
            ),
            data={"cluster_stats": {"clusters_created": len(accepted_clusters)}},
        ))
        return accepted_clusters

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _load_records_for_members(self, customer_codes: set) -> Dict[str, dict]:
        if not customer_codes:
            return {}
        con = self._connect()
        codes_list = list(customer_codes)

        # Same "max 10000 children in an expression tree" limit as the
        # singleton anti-join below applies to a large IN (...) list
        # too -- chunk it. At full 1.5M-row scale, cluster membership
        # alone can exceed 100K codes, so this is not a hypothetical:
        # verified live (the unchunked form failed with exactly that
        # Doris error once cluster membership crossed 10,000 codes).
        chunk_size = 5000
        rows = []
        for i in range(0, len(codes_list), chunk_size):
            chunk = codes_list[i:i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            rows.extend(con.execute(f"""
                SELECT
                    s.customer_code, r.NAME AS name, s.name_norm,
                    any_value(i_email.value_norm) AS email_norm,
                    any_value(i_mobile.value_norm) AS phone_norm,
                    s.dob_iso,
                    any_value(i_addr.value_norm) AS address_norm,
                    any_value(i_doc.value_norm) AS natid_norm
                FROM customer_scalars s
                JOIN raw r ON r.CUSTOMER_CODE = s.customer_code
                LEFT JOIN identifiers i_email ON i_email.customer_code = s.customer_code AND i_email.id_type = 'email' AND i_email.is_valid
                LEFT JOIN identifiers i_mobile ON i_mobile.customer_code = s.customer_code AND i_mobile.id_type = 'mobile' AND i_mobile.is_valid
                LEFT JOIN identifiers i_addr ON i_addr.customer_code = s.customer_code AND i_addr.id_type = 'address' AND i_addr.is_valid
                LEFT JOIN identifiers i_doc ON i_doc.customer_code = s.customer_code AND i_doc.id_type = 'document' AND i_doc.is_valid
                WHERE s.customer_code IN ({placeholders})
                GROUP BY s.customer_code, r.NAME, s.name_norm, s.dob_iso
            """, chunk).fetchall())

        records = {}
        for row in rows:
            ckey, name, name_norm, email_norm, phone_norm, dob_iso, address_norm, natid_norm = row
            records[ckey] = {
                "customer_key": ckey, "source_customer_id": ckey,
                "name": name or "", "name_norm": name_norm or "",
                "email": email_norm or "", "email_norm": email_norm or "",
                "phone": phone_norm or "", "phone_norm": phone_norm or "",
                "dob": dob_iso or "", "dob_norm": dob_iso or "",
                "address": address_norm or "", "address_norm": address_norm or "",
                "natid": natid_norm or "", "natid_norm": natid_norm or "",
                "status": "ACT",
            }
        return records

    def _persist_run_artifacts(self, clusters: Dict[str, List[str]]) -> None:
        os.makedirs("data/runs", exist_ok=True)

        manager = get_cluster_manager()
        manager.save_snapshot(f"data/runs/{self.run_id}_clusters.json")

        all_members = set()
        for members in clusters.values():
            all_members.update(members)

        self._records = self._load_records_for_members(all_members)
        with open(f"data/runs/{self.run_id}_records.json", "w") as f:
            json.dump(self._records, f, default=str)

        con = self._connect()
        if all_members:
            # A literal `NOT IN ('a','b',...)` list with ~100K+ quoted
            # codes (realistic at full 1.5M-row scale) is megabytes of
            # SQL text -- slow to parse/plan and a real risk of hitting
            # a max-query-length limit. A staging table + LEFT JOIN
            # anti-join is the standard, scale-safe pattern (mirrors
            # pipeline.duckdb_orchestrator's `_cluster_members` table).
            con.execute("DROP TABLE IF EXISTS _cluster_members")
            con.execute("CREATE TABLE _cluster_members (customer_code VARCHAR(64))")
            member_list = list(all_members)
            batch_size = 5000
            for i in range(0, len(member_list), batch_size):
                batch = member_list[i:i + batch_size]
                placeholders = ",".join(["(?)"] * len(batch))
                con.execute(f"INSERT INTO _cluster_members VALUES {placeholders}", batch)

            singleton_rows = con.execute("""
                SELECT s.customer_code FROM customer_scalars s
                LEFT JOIN _cluster_members m ON m.customer_code = s.customer_code
                WHERE m.customer_code IS NULL
                ORDER BY s.customer_code
            """).fetchall()
        else:
            singleton_rows = con.execute(
                "SELECT customer_code FROM customer_scalars ORDER BY customer_code"
            ).fetchall()
        with open(f"data/runs/{self.run_id}_singletons.csv", "w", newline="") as f:
            writer = csv_module.writer(f)
            writer.writerow(["customer_code"])
            for (code,) in singleton_rows:
                writer.writerow([code])

        with open(f"data/runs/{self.run_id}_scores.csv", "w", newline="") as f:
            writer = csv_module.writer(f)
            writer.writerow([
                "CUSTOMER_CODE_l", "CUSTOMER_CODE_r", "match_probability",
                "decision", "signals_hit", "hard_conflicts",
            ])
            for pair_id, score in self._scores.items():
                decision = self._decisions.get(pair_id)
                writer.writerow([
                    score.a_key, score.b_key, score.score,
                    decision.value if decision else "",
                    ";".join(score.signals_hit), ";".join(score.hard_conflicts),
                ])

    def _cleanup_scratch_tables(self) -> None:
        """
        Drops every per-run staging table that's served its purpose by
        the time this runs (after candidate_pairs/pair_*_evidence/
        clusters/artifacts have all been derived from them): the
        per-rule blocking-key tables (`_rule_*`), the evidence-join
        staging table (`_agg_identifiers`), and the singleton-export
        staging table (`_cluster_members`). Measured at ~36% of a run's
        total Doris disk footprint -- see the lakehouse migration plan's
        storage breakdown. `identifiers`/`customer_scalars`/
        `identifier_frequency`/`candidate_pairs` are deliberately NOT
        dropped here: routes_rules.py's precheck/redecide/reblock reopen
        a completed run's database and read them.
        """
        from engine.rules.compiler import drop_scratch_tables
        con = self._connect()
        try:
            drop_scratch_tables(con, self._blocking_rules)
        except Exception:
            logger.warning("Failed to drop per-rule scratch tables", exc_info=True)
        for table in ("_agg_identifiers", "_cluster_members"):
            try:
                con.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                logger.warning(f"Failed to drop scratch table {table}", exc_info=True)
        # `raw` is a VIEW when ingest used LOCAL() (nothing to drop --
        # no data was ever copied) but a real materialized TABLE when
        # ingest fell back to Stream Load; DROP TABLE fails on a VIEW
        # and vice versa, so try both, ignoring whichever doesn't apply.
        for stmt in ("DROP VIEW IF EXISTS raw", "DROP TABLE IF EXISTS raw"):
            try:
                con.execute(stmt)
            except Exception:
                pass

    def _persist_to_postgres(self, clusters, auto_links, review_items, result) -> None:
        """Mirrors DuckDBPipelineOrchestrator._persist_to_postgres -- see that method's docstring."""
        try:
            from api.config import settings
            if not settings.PERSIST_TO_POSTGRES:
                return

            import psycopg2
            from db import repository

            audited_pair_ids = {f"{a}:{b}" for a, b in auto_links} | {f"{a}:{b}" for a, b in review_items}
            if not audited_pair_ids and not self._relationships:
                return

            audited_codes = set()
            for pid in audited_pair_ids:
                a, b = pid.split(":", 1)
                audited_codes.add(a)
                audited_codes.add(b)
            for rel in self._relationships:
                audited_codes.add(rel["a_key"])
                audited_codes.add(rel["b_key"])

            pg_conn = psycopg2.connect(settings.DATABASE_URL)

            def _step(name, fn, *args):
                try:
                    return fn(*args)
                except Exception as e:
                    pg_conn.rollback()
                    logger.warning(f"Postgres persistence step '{name}' failed, skipped: {e}")
                    return None

            try:
                _step("ensure_run_row", repository.ensure_run_row, pg_conn, self.run_id, result.mode, "Datasource Demo (doris)")
                code_to_uuid = _step("upsert_customers", self._upsert_customers, pg_conn, list(audited_codes)) or {}
                # repository.persist_identifier_frequency only calls
                # con.execute(...).fetchall() -- the exact subset of the
                # DuckDB connection API DorisConnection also implements
                # (see engine.ports.doris_conn), so it works unchanged
                # here. Previously never called on this path at all, so
                # suppressed-identifier audit data existed for DuckDB
                # runs but not Doris ones.
                _step("persist_identifier_frequency", repository.persist_identifier_frequency, pg_conn, self._connect(), self.run_id)

                audited_scores = {pid: s for pid, s in self._scores.items() if pid in audited_pair_ids}
                audited_decisions = {pid: d for pid, d in self._decisions.items() if pid in audited_pair_ids}

                pair_result = _step(
                    "persist_candidate_pairs_and_decisions",
                    repository.persist_candidate_pairs_and_decisions,
                    pg_conn, self.run_id, code_to_uuid, audited_scores, audited_decisions, RULESET_VERSION,
                )
                n_pairs, n_scores, n_decisions = pair_result if pair_result else (0, 0, 0)

                n_clusters = _step(
                    "persist_clusters", repository.persist_clusters,
                    pg_conn, code_to_uuid, clusters, RULESET_VERSION,
                ) or 0

                # Stage 2 of the entity resolution workbench plan --
                # mirrors duckdb_orchestrator's identical block. See
                # engine.clustering.entity_resolver's module docstring.
                entity_result = _step(
                    "resolve_entities", entity_resolver.resolve_entities,
                    pg_conn, self.run_id, clusters, set(clusters.keys()), "pipeline",
                )
                if entity_result:
                    pg_conn.commit()

                n_relationships = _step(
                    "persist_entity_relationships", repository.persist_entity_relationships,
                    pg_conn, self.run_id, code_to_uuid, self._relationships,
                ) or 0

                _step(
                    "update_run_fingerprints", repository.update_run_fingerprints,
                    pg_conn, self.run_id, self.get_fingerprints(),
                    {
                        "records_in": result.records_in, "candidates_generated": result.candidates_generated,
                        "pairs_scored": result.pairs_scored, "auto_links": result.auto_links,
                        "review_items": result.review_items, "rejected": result.rejected,
                    },
                )
                logger.info(
                    f"Persisted to Postgres: {len(code_to_uuid):,} customers, {n_pairs:,} pairs, "
                    f"{n_scores:,} scores, {n_decisions:,} decisions, {n_clusters:,} cluster memberships, "
                    f"{n_relationships:,} cross-segment relationships"
                )
            finally:
                pg_conn.close()
        except Exception as e:
            logger.warning(f"Postgres persistence failed (run still succeeds with file artifacts): {e}")

    def _upsert_customers(self, pg_conn, customer_codes: List[str]) -> Dict[str, str]:
        """Doris-sourced equivalent of db.repository.upsert_customers (which reads from a DuckDB connection)."""
        from psycopg2.extras import execute_values
        if not customer_codes:
            return {}
        con = self._connect()
        chunk_size = 5000
        rows = []
        for i in range(0, len(customer_codes), chunk_size):
            chunk = customer_codes[i:i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            rows.extend(con.execute(f"""
                SELECT customer_code, name_norm, dob_iso FROM customer_scalars
                WHERE customer_code IN ({placeholders})
            """, chunk).fetchall())

        values = [
            (code, name_norm, dob_iso, f"doris-pipeline:{code}", "ORACLE_DATASOURCE")
            for code, name_norm, dob_iso in rows
        ]
        with pg_conn.cursor() as cur:
            inserted = execute_values(cur, """
                INSERT INTO customers_norm (source_customer_id, name_norm, dob_norm, record_hash, source_system)
                VALUES %s
                ON CONFLICT (source_customer_id, source_system) DO UPDATE SET
                    name_norm = EXCLUDED.name_norm, dob_norm = EXCLUDED.dob_norm, updated_at = NOW()
                RETURNING source_customer_id, customer_key
            """, values, fetch=True, page_size=5000)
            mapping = {row[0]: str(row[1]) for row in inserted}
        pg_conn.commit()
        return mapping

    # ------------------------------------------------------------------
    # Public run()
    # ------------------------------------------------------------------
    async def run(self, run_id: str, raw_records: list = None, mode: str = "FULL") -> PipelineResult:
        self.run_id = run_id
        result = PipelineResult(run_id=run_id, success=False, mode=mode, stages=[], started_at=datetime.utcnow())

        try:
            record_count = await self._stage_ingest()
            result.records_in = record_count

            await self._stage_normalize(record_count)
            result.records_normalized = record_count

            candidate_count = await self._stage_block_and_candidates()
            result.blocks_created = candidate_count
            result.candidates_generated = candidate_count

            auto_links, review_items, rejected = await self._stage_score_and_decide(candidate_count)
            result.pairs_scored = len(self._scores)
            result.auto_links = len(auto_links)
            result.review_items = len(review_items)
            result.rejected = len(rejected)

            clusters = await self._stage_cluster(auto_links)

            persist_start = datetime.utcnow()
            await self._emit_progress(StageProgress(
                stage=PipelineStage.PERSIST, status="running",
                message="Persisting run artifacts and writing to Postgres...",
            ))

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._persist_run_artifacts, clusters)
            await loop.run_in_executor(None, self._cleanup_scratch_tables)

            self._input_fp = input_fingerprint(os.path.dirname(PARQUET_PATH))
            self._ruleset_fp = ruleset_fingerprint()
            self._edges_fp = fingerprint_edges(
                (a, b, self._decisions[f"{a}:{b}"].value, self._scores[f"{a}:{b}"].signals_hit)
                for a, b in auto_links
            )
            self._clusters_fp = fingerprint_clusters(clusters)
            self._output_fp = output_fingerprint(self._edges_fp, self._clusters_fp)

            await loop.run_in_executor(None, self._persist_to_postgres, clusters, auto_links, review_items, result)

            persist_duration = int((datetime.utcnow() - persist_start).total_seconds() * 1000)
            await self._emit_progress(StageProgress(
                stage=PipelineStage.PERSIST, status="complete",
                duration_ms=persist_duration,
                message=f"Persisted run artifacts in {persist_duration / 1000:.1f}s",
            ))

            result.success = True
            result.ended_at = datetime.utcnow()

            await self._emit_progress(StageProgress(
                stage=PipelineStage.COMPLETE, status="complete",
                message=(
                    f"Pipeline complete (Doris engine): {len(clusters):,} identity clusters resolved "
                    f"from {record_count:,} records (ruleset={RULESET_VERSION}, "
                    f"output_fingerprint={self._output_fp[:16]}...)"
                ),
            ))

        except Exception as e:
            logger.error(f"Doris pipeline failed: {e}", exc_info=True)
            result.success = False
            result.error_message = str(e)
            result.ended_at = datetime.utcnow()
            await self._emit_progress(StageProgress(stage=PipelineStage.FAILED, status="error", message=str(e)))

        finally:
            if self._con:
                try:
                    self._con.close()
                except Exception:
                    pass

        return result

    # ------------------------------------------------------------------
    # Accessors (identical contract to DuckDBPipelineOrchestrator)
    # ------------------------------------------------------------------
    def get_scores(self) -> Dict[str, MatchScore]:
        return self._scores

    def get_decisions(self) -> Dict[str, MatchDecision]:
        return self._decisions

    def get_review_queue(self) -> List[MatchScore]:
        return [self._scores[pid] for pid, d in self._decisions.items() if d == MatchDecision.REVIEW]

    def get_auto_links(self) -> List[MatchScore]:
        return [self._scores[pid] for pid, d in self._decisions.items() if d == MatchDecision.AUTO_LINK]

    def get_uniques(self) -> List[dict]:
        linked_keys = set()
        for pid, d in self._decisions.items():
            if d in (MatchDecision.AUTO_LINK, MatchDecision.REVIEW):
                score = self._scores.get(pid)
                if score:
                    linked_keys.add(score.a_key)
                    linked_keys.add(score.b_key)
        return [r for k, r in self._records.items() if k not in linked_keys]

    def get_result_clusters(self) -> List[dict]:
        manager = get_cluster_manager()
        all_clusters = manager.get_clusters()
        run_keys = set(self._records.keys())
        result_clusters = []
        for cluster_id, members in all_clusters.items():
            if any(k in run_keys for k in members):
                cluster_records = [self._records[k] for k in members if k in self._records]
                result_clusters.append({
                    "cluster_id": cluster_id, "size": len(members),
                    "members": sorted(members), "records": cluster_records,
                })
        return result_clusters

    def get_fingerprints(self) -> dict:
        return {
            "ruleset_version": RULESET_VERSION,
            "input_fingerprint": self._input_fp,
            "ruleset_fingerprint": self._ruleset_fp,
            "edges_fingerprint": self._edges_fp,
            "clusters_fingerprint": self._clusters_fp,
            "output_fingerprint": self._output_fp,
        }
