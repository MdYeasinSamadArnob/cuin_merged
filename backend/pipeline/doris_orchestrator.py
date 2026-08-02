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
import sys
from concurrent.futures import ThreadPoolExecutor
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
from engine.structures import MatchScore, MatchDecision
from engine.ports.doris_conn import DorisConnection
from engine.ports.doris_dialect import DorisDialect
from engine.ports.doris_ingest import create_raw_table_sql, load_parquet_to_doris, local_parquet_view_sql
from engine.normalize import explode_dialect
from engine.blocking import suppression_dialect
from engine.scoring.evidence_dialect import build_pair_evidence
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

# low_memory_mode's batch size for _stage_score_and_decide. Chosen as a
# balance point: large enough that per-batch overhead (staging-table
# round trips, ProcessPoolExecutor startup) stays a small fraction of
# a batch's own work, small enough that peak per-batch memory (pairs +
# evidence dicts + MatchScore objects) stays boundable on a low-resource
# node regardless of total run size. Not tuned against a real
# billion-row run (no such dataset available in dev) -- treat as a
# starting point to re-measure against, not a proven-optimal constant.
# Env-overridable so a real run can be exercised with a deliberately
# small batch size to prove the batching mechanism itself, even
# against a dev-scale dataset far below where it would matter for
# memory in production.
SCORE_BATCH_SIZE = int(os.environ.get("CUIN_SCORE_BATCH_SIZE", 500_000))


def _sanitize_db_name(run_id: str) -> str:
    return "cuin_run_" + run_id.replace("-", "_")


class DorisPipelineOrchestrator:
    """
    Deterministic entity-resolution pipeline over the Oracle parquet
    datasource, executed on Apache Doris.

    low_memory_mode (default False, opt-in): the normal path fetches
    every candidate pair's evidence into Python dicts up front and
    accumulates every scored pair's full MatchScore in self._scores/
    self._decisions for the run's entire lifetime (services.run_service
    keeps orchestrator instances alive indefinitely so api/routes_matches.py
    etc. can call get_scores()/get_decisions() after the run completes).
    That's the right, simple design at normal run sizes -- fine up to
    tens of millions of pairs -- but at a one-time multi-billion-record
    historical backfill it isn't just slow, it exhausts memory outright,
    independent of how much CPU parallelism scores pairs. low_memory_mode
    switches _stage_score_and_decide to bounded-batch processing
    (SCORE_BATCH_SIZE pairs at a time: fetch batch -> fetch only that
    batch's evidence -> score -> persist immediately -> discard) and
    stops populating self._scores/self._decisions for non-auto-link
    pairs entirely -- get_scores()/get_decisions() return an empty (or
    auto-link-only) view for a low_memory_mode run. This is a deliberate,
    documented trade-off, not a bug: nobody browses billions of match
    pairs in a live UI table, and existing routes/tests for normal-sized
    runs are completely unaffected since this whole path is opt-in.
    """

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
        self._carry_forward = True
        self._low_memory_mode = False
        # Populated instead of self._scores/self._decisions when
        # low_memory_mode is on, scoped to ONLY auto-link pairs (a small
        # fraction of all pairs) since that's all fingerprint_edges (see
        # run()) actually needs: pair_id -> (decision_value, signals_hit).
        self._auto_link_fingerprint_data: Dict[str, tuple] = {}

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

    def _fetch_chunks_parallel(self, items: list, chunk_size: int, query_fn, max_workers: int = 8) -> list:
        """
        Runs query_fn(conn, chunk) concurrently across chunks of `items`,
        each on its own DorisConnection (pymysql connections aren't
        safe to share across threads). This is I/O-bound work -- each
        chunk waits on a network round trip + Doris query execution --
        so threads are the right tool here (unlike parallel_scoring's
        CPU-bound pure-Python work, which needed processes to get past
        the GIL): the GIL is released while pymysql waits on the
        socket, so threads genuinely run these concurrently. Requires
        _connect() to have been called at least once already (so
        self._database is set).
        """
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        if not chunks:
            return []
        if len(chunks) == 1:
            return list(query_fn(self._connect(), chunks[0]))

        def _run(chunk):
            conn = DorisConnection(
                host=self._doris_host, port=self._doris_mysql_port,
                user=self._doris_user, password=self._doris_password, database=self._database,
            )
            try:
                return query_fn(conn, chunk)
            finally:
                conn.close()

        results = []
        with ThreadPoolExecutor(max_workers=min(max_workers, len(chunks))) as pool:
            for chunk_result in pool.map(_run, chunks):
                results.extend(chunk_result)
        return results

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
            from api.config import settings
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

            # Scoring/decision runs across a process pool, not a serial
            # Python loop -- see pipeline.parallel_scoring's module
            # docstring for why this was the pipeline's real throughput
            # bottleneck (measured ~757 pairs/sec on one core,
            # independent of Doris's own speed) and why splitting it
            # across cores is safe (each pair's decision is fully
            # independent of every other pair's).
            #
            # build_full_evidence=False always -- see score_pairs_parallel's
            # docstring: its ONLY consumer (routes_matches.py's
            # /{pair_id}/explain, reading MatchScore.evidence's
            # comparison_type/similarity_score fields) turned out to be
            # unreachable from production -- the frontend's "Ask Referee"
            # button calls an api.explainMatch() that was never actually
            # defined in frontend/src/lib/api.ts (verified: grep finds no
            # definition, only the `as any` call site). The live review
            # workbench's referee feature reads its OWN stored
            # explanation from referee_explanations, never MatchScore.evidence.
            # Building this per-pair explanation-string data was therefore
            # pure waste at any scale -- confirmed the single largest
            # remaining cost path in persist_candidate_pairs_and_decisions
            # (the JSONB evidence_json column).
            from pipeline.parallel_scoring import score_pairs_parallel
            auto_links, review_items, rejected, chunk_scores, chunk_decisions, chunk_relationships = score_pairs_parallel(
                pairs, id_evidence, name_dob_evidence, segments, raw_columns, raw_by_code,
                self._catalog, build_full_evidence=False,
            )
            self._scores.update(chunk_scores)
            self._decisions.update(chunk_decisions)
            self._relationships.extend(chunk_relationships)

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

        if self._low_memory_mode:
            auto_links, review_items, rejected = await loop.run_in_executor(None, self._score_and_decide_batched)
        else:
            auto_links, review_items, rejected = await loop.run_in_executor(None, _score_and_decide)

        review_count = review_items if isinstance(review_items, int) else len(review_items)
        rejected_count = rejected if isinstance(rejected, int) else len(rejected)
        scored_count = len(auto_links) + review_count + rejected_count

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        relationships_msg = f", {len(self._relationships):,} cross-segment relationships" if self._segmentation.enabled else ""
        await self._emit_progress(StageProgress(
            stage=PipelineStage.SCORE, status="complete",
            records_in=candidate_count, records_out=scored_count, duration_ms=duration,
            message=f"{len(auto_links):,} auto-link, {review_count:,} review, {rejected_count:,} rejected{relationships_msg}",
        ))
        await self._emit_progress(StageProgress(
            stage=PipelineStage.DECIDE, status="complete", records_out=len(auto_links),
            message=f"{len(auto_links):,} pairs meet the auto-link rule",
        ))
        return auto_links, review_items, rejected

    def _score_and_decide_batched(self):
        """
        low_memory_mode's replacement for the nested _score_and_decide()
        closure above: processes candidate_pairs SCORE_BATCH_SIZE rows
        at a time instead of materializing every pair's evidence and
        every pair's MatchScore for the whole run at once. See the
        class docstring for why this exists and what it deliberately
        gives up (self._scores/self._decisions stay empty for
        non-auto-link pairs).

        Returns (auto_links, review_count, rejected_count) -- note
        review/rejected are INTS here, not lists (nothing downstream
        needs the actual review/rejected pairs, only their counts;
        _stage_score_and_decide's caller-facing contract already
        tolerates either shape, see review_count/rejected_count above).
        """
        con = self._connect()
        build_pair_evidence(con, self._dialect)

        segments: Dict[str, str] = dict(
            con.execute("SELECT customer_code, segment FROM customer_segments").fetchall()
        )

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

        # Stage 6: no Postgres connection needed in this function at all
        # any more -- candidate_pairs/match_scores/match_decisions
        # (what the removed pg_conn/index-deferral machinery here used
        # to serve) are no longer written to Postgres by Doris-backed
        # runs. See the class docstring and api/doris_run_reader.py.

        os.makedirs("data/runs", exist_ok=True)
        scores_csv = open(f"data/runs/{self.run_id}_scores.csv", "w", newline="")
        scores_writer = csv_module.writer(scores_csv)
        scores_writer.writerow([
            "CUSTOMER_CODE_l", "CUSTOMER_CODE_r", "match_probability",
            "decision", "signals_hit", "hard_conflicts",
        ])

        auto_links: List[tuple] = []
        review_count = 0
        rejected_count = 0

        try:
            last_a_key, last_b_key = "", ""
            while True:
                # Keyset pagination via explicit boolean logic, not a
                # row-value tuple comparison (`(a_key, b_key) > (?, ?)`)
                # -- Doris's SQL parser doesn't support that MySQL/
                # Postgres extension (verified live: "mismatched input
                # ','... expecting {')', '.', ...}" at the tuple).
                batch_pairs = con.execute("""
                    SELECT a_key, b_key FROM candidate_pairs
                    WHERE a_key > ? OR (a_key = ? AND b_key > ?)
                    ORDER BY a_key, b_key
                    LIMIT ?
                """, (last_a_key, last_a_key, last_b_key, SCORE_BATCH_SIZE)).fetchall()
                if not batch_pairs:
                    break
                last_a_key, last_b_key = batch_pairs[-1]

                # Staging-table + JOIN, not a literal IN (...) list --
                # same reasoning as _load_records_for_members/
                # _persist_run_artifacts: a SCORE_BATCH_SIZE-row IN list
                # is megabytes of SQL text and risks Doris's expression-
                # tree limits.
                con.execute("DROP TABLE IF EXISTS _score_batch")
                con.execute("CREATE TABLE _score_batch (a_key VARCHAR(64), b_key VARCHAR(64))")
                insert_chunk = 5000
                for i in range(0, len(batch_pairs), insert_chunk):
                    chunk = batch_pairs[i:i + insert_chunk]
                    placeholders = ",".join(["(?,?)"] * len(chunk))
                    flat = [v for pair in chunk for v in pair]
                    con.execute(f"INSERT INTO _score_batch VALUES {placeholders}", flat)

                id_evidence: Dict[tuple, list] = {}
                for row in con.execute("""
                    SELECT e.a_key, e.b_key, e.id_type, e.doc_type, e.values_a, e.values_b, e.intersection
                    FROM pair_identifier_evidence e
                    JOIN _score_batch b ON b.a_key = e.a_key AND b.b_key = e.b_key
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
                    SELECT e.a_key, e.b_key, e.name_a, e.name_b, e.tokens_a, e.tokens_b,
                           e.token_intersection, e.token_union,
                           e.dob_a, e.dob_b, e.dob_precision_a, e.dob_precision_b
                    FROM pair_name_dob_evidence e
                    JOIN _score_batch b ON b.a_key = e.a_key AND b.b_key = e.b_key
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

                from pipeline.parallel_scoring import score_pairs_parallel
                b_auto, b_review, b_rejected, b_scores, b_decisions, b_relationships = score_pairs_parallel(
                    batch_pairs, id_evidence, name_dob_evidence, segments, raw_columns, raw_by_code,
                    self._catalog, build_full_evidence=False,  # see normal path's identical comment
                )
                self._relationships.extend(b_relationships)

                for a_key, b_key in b_auto:
                    pair_id = f"{a_key}:{b_key}"
                    score = b_scores[pair_id]
                    decision = b_decisions[pair_id]
                    self._auto_link_fingerprint_data[pair_id] = (decision.value, score.signals_hit)
                auto_links.extend(b_auto)
                review_count += len(b_review)
                rejected_count += len(b_rejected)

                for pair_id, score in b_scores.items():
                    decision = b_decisions.get(pair_id)
                    scores_writer.writerow([
                        score.a_key, score.b_key, score.score,
                        decision.value if decision else "",
                        ";".join(score.signals_hit), ";".join(score.hard_conflicts),
                    ])

                # Stage 6: no per-batch Postgres persist of pairs/scores/
                # decisions -- pair_decisions/pair_contributions (built
                # unconditionally below, once, after the batch loop)
                # already hold this exact data in this run's own Doris
                # database, and api/doris_run_reader.py now reads
                # directly from there. See this method's/class's
                # docstring and db/repository.py's comments for the
                # full story of what used to happen here.
                # b_scores/b_decisions/id_evidence/name_dob_evidence go
                # out of scope here -- next loop iteration's assignments
                # are this batch's only references, so they're eligible
                # for GC immediately rather than living until the whole
                # run finishes.

            con.execute("DROP TABLE IF EXISTS _score_batch")
        finally:
            scores_csv.close()

        try:
            con.execute(compile_confidence_sql(self.match_ruleset, self._dialect, table_name="pair_decisions"))
        except Exception:
            logger.warning(
                "Could not persist SQL-compiled pair_decisions baseline "
                "(redecide/reblock will fall back to a fresh compile)", exc_info=True,
            )
        try:
            from engine.rules.confidence_compiler import compile_contributions_sql
            con.execute(compile_contributions_sql(self.match_ruleset, self._dialect, table_name="pair_contributions"))
        except Exception:
            logger.warning(
                "Could not persist pair_contributions (workbench score breakdown unavailable for this run)",
                exc_info=True,
            )

        return auto_links, review_count, rejected_count

    def _score_partition(self, worker_idx: int, num_workers: int) -> dict:
        """
        Stage 4 proof-of-concept: scores exactly this worker's fixed
        partition of candidate_pairs -- crc32(a_key||b_key) mod
        num_workers = worker_idx -- rather than Stage 0/low_memory_mode's
        sequential keyset batching. The difference matters for real
        distribution: a fixed hash partition needs zero coordination
        between workers (no worker needs to know another's progress or
        last-seen key), so N of these can run as genuinely independent
        processes -- on this dev host as subprocesses (see
        pipeline/distributed_scoring_worker.py), on real infrastructure
        as separate machines, with no code difference between the two.

        Assumes the CALLER already ran ingest/normalize/block AND
        build_pair_evidence() against this run_id's Doris database
        exactly once -- re-running build_pair_evidence() per worker
        would redo the exact bulk SQL work this exists to parallelize.
        Returns a plain, JSON-serializable dict rather than mutating
        self -- this runs in a separate process from whatever collects
        the results, so there is no shared memory to mutate into.
        """
        con = self._connect()

        segments: Dict[str, str] = dict(
            con.execute("SELECT customer_code, segment FROM customer_segments").fetchall()
        )
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

        partition_pairs = con.execute(f"""
            SELECT a_key, b_key FROM candidate_pairs
            WHERE MOD(crc32(CONCAT(a_key, '|', b_key)), {num_workers}) = {worker_idx}
        """).fetchall()

        staging = f"_partition_{worker_idx}"
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        con.execute(f"CREATE TABLE {staging} (a_key VARCHAR(64), b_key VARCHAR(64))")
        insert_chunk = 5000
        for i in range(0, len(partition_pairs), insert_chunk):
            chunk = partition_pairs[i:i + insert_chunk]
            placeholders = ",".join(["(?,?)"] * len(chunk))
            flat = [v for pair in chunk for v in pair]
            con.execute(f"INSERT INTO {staging} VALUES {placeholders}", flat)

        id_evidence: Dict[tuple, list] = {}
        for row in con.execute(f"""
            SELECT e.a_key, e.b_key, e.id_type, e.doc_type, e.values_a, e.values_b, e.intersection
            FROM pair_identifier_evidence e
            JOIN {staging} p ON p.a_key = e.a_key AND p.b_key = e.b_key
        """).fetchall():
            a_key, b_key, id_type, doc_type, values_a, values_b, intersection = row
            id_evidence.setdefault((a_key, b_key), []).append({
                "id_type": id_type, "doc_type": doc_type,
                "values_a": _parse_array(values_a),
                "values_b": _parse_array(values_b),
                "intersection": _parse_array(intersection),
            })

        name_dob_evidence: Dict[tuple, dict] = {}
        for row in con.execute(f"""
            SELECT e.a_key, e.b_key, e.name_a, e.name_b, e.tokens_a, e.tokens_b,
                   e.token_intersection, e.token_union,
                   e.dob_a, e.dob_b, e.dob_precision_a, e.dob_precision_b
            FROM pair_name_dob_evidence e
            JOIN {staging} p ON p.a_key = e.a_key AND p.b_key = e.b_key
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

        from pipeline.parallel_scoring import score_pairs_parallel
        auto_links, review_items, rejected, scores, decisions, relationships = score_pairs_parallel(
            partition_pairs, id_evidence, name_dob_evidence, segments, raw_columns, raw_by_code,
            self._catalog, build_full_evidence=False,
        )

        # Stage 6: no per-partition Postgres persist of pairs/scores/
        # decisions -- pair_decisions/pair_contributions (built once by
        # the coordinator before dispatching workers) already hold this
        # data in this run's own Doris database. See the class
        # docstring and api/doris_run_reader.py.
        con.execute(f"DROP TABLE IF EXISTS {staging}")

        fingerprint_data = {}
        for a_key, b_key in auto_links:
            pid = f"{a_key}:{b_key}"
            fingerprint_data[pid] = (decisions[pid].value, scores[pid].signals_hit)

        return {
            "worker_idx": worker_idx,
            "auto_links": auto_links,
            "review_count": len(review_items),
            "rejected_count": len(rejected),
            "fingerprint_data": fingerprint_data,
            "relationships": relationships,
        }

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
        self._connect()  # ensures self._database is set before parallel fetch
        codes_list = list(customer_codes)

        # Same "max 10000 children in an expression tree" limit as the
        # singleton anti-join below applies to a large IN (...) list
        # too -- chunk it. At full 1.5M-row scale, cluster membership
        # alone can exceed 100K codes, so this is not a hypothetical:
        # verified live (the unchunked form failed with exactly that
        # Doris error once cluster membership crossed 10,000 codes).
        # Chunks are independent queries -- fetched concurrently via
        # _fetch_chunks_parallel (see its docstring for why threads,
        # not processes, are correct here).
        def _query_chunk(conn, chunk):
            placeholders = ",".join(["?"] * len(chunk))
            return conn.execute(f"""
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
            """, chunk).fetchall()

        rows = self._fetch_chunks_parallel(codes_list, 5000, _query_chunk)

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
        import time as _time
        _t = {}
        _mark = _time.perf_counter()

        def _lap(name):
            nonlocal _mark
            now = _time.perf_counter()
            _t[name] = round(now - _mark, 3)
            _mark = now

        os.makedirs("data/runs", exist_ok=True)

        manager = get_cluster_manager()
        manager.save_snapshot(f"data/runs/{self.run_id}_clusters.json")
        _lap("cluster_snapshot")

        all_members = set()
        for members in clusters.values():
            all_members.update(members)

        self._records = self._load_records_for_members(all_members)
        _lap("load_records_for_members")
        with open(f"data/runs/{self.run_id}_records.json", "w") as f:
            json.dump(self._records, f, default=str)
        _lap("records_json_dump")

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
        _lap("singleton_query")
        with open(f"data/runs/{self.run_id}_singletons.csv", "w", newline="") as f:
            writer = csv_module.writer(f)
            writer.writerow(["customer_code"])
            for (code,) in singleton_rows:
                writer.writerow([code])
        _lap("singleton_csv_write")

        if self._low_memory_mode:
            # Already streamed row-by-row per batch in
            # _score_and_decide_batched -- self._scores is empty here,
            # writing it out would just truncate that file to its
            # header row.
            _lap("scores_csv_write_skipped_low_memory_mode")
        else:
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
            _lap("scores_csv_write")
        logger.info(f"_persist_run_artifacts breakdown (s): {_t}")

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

            # Stage 6: candidate_pairs/match_scores/match_decisions are
            # never written to Postgres any more (this run's own Doris
            # database's pair_decisions/pair_contributions already hold
            # this data -- see api/doris_run_reader.py). What's left to
            # persist here is cluster-scoped, not pair-scoped:
            # persist_clusters/resolve_entities/persist_entity_relationships/
            # update_run_fingerprints, keyed off `clusters` and
            # self._relationships. low_memory_mode narrows audited_codes
            # to cluster members only (self._scores/self._decisions are
            # empty in that mode, and review_items arrives as a count,
            # not a list, so the pairs-based derivation below doesn't apply).
            if self._low_memory_mode:
                audited_codes = {code for members in clusters.values() for code in members}
                for rel in self._relationships:
                    audited_codes.add(rel["a_key"])
                    audited_codes.add(rel["b_key"])
                if not audited_codes and not self._relationships:
                    return
            else:
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

            import time as _time
            _step_timings = {}

            def _step(name, fn, *args):
                _t0 = _time.perf_counter()
                try:
                    return fn(*args)
                except Exception as e:
                    pg_conn.rollback()
                    logger.warning(f"Postgres persistence step '{name}' failed, skipped: {e}")
                    return None
                finally:
                    _step_timings[name] = round(_time.perf_counter() - _t0, 3)

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

                n_clusters = _step(
                    "persist_clusters", repository.persist_clusters,
                    pg_conn, code_to_uuid, clusters, RULESET_VERSION,
                ) or 0

                # Stage 2 of the entity resolution workbench plan --
                # mirrors duckdb_orchestrator's identical block. See
                # engine.clustering.entity_resolver's module docstring.
                entity_result = _step(
                    "resolve_entities", entity_resolver.resolve_entities,
                    pg_conn, self.run_id, clusters, set(clusters.keys()), "pipeline", self._carry_forward,
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
                    f"Persisted to Postgres: {len(code_to_uuid):,} customers, {n_clusters:,} cluster memberships, "
                    f"{n_relationships:,} cross-segment relationships (pairs/scores/decisions live only in "
                    f"this run's Doris database as of Stage 6 -- see api/doris_run_reader.py)"
                )
                logger.info(f"_persist_to_postgres step breakdown (s): {_step_timings}")
            finally:
                pg_conn.close()
        except Exception as e:
            logger.warning(f"Postgres persistence failed (run still succeeds with file artifacts): {e}")

    def _upsert_customers(self, pg_conn, customer_codes: List[str]) -> Dict[str, str]:
        """Doris-sourced equivalent of db.repository.upsert_customers (which reads from a DuckDB connection)."""
        from psycopg2.extras import execute_values
        if not customer_codes:
            return {}
        self._connect()  # ensures self._database is set before parallel fetch

        def _query_chunk(conn, chunk):
            placeholders = ",".join(["?"] * len(chunk))
            return conn.execute(f"""
                SELECT customer_code, name_norm, dob_iso FROM customer_scalars
                WHERE customer_code IN ({placeholders})
            """, chunk).fetchall()

        rows = self._fetch_chunks_parallel(customer_codes, 5000, _query_chunk)

        # Sorted by source_customer_id -- when Stage 4's distributed
        # workers run as genuinely concurrent processes, their
        # customer_codes sets legitimately overlap (partitioning is by
        # PAIR via crc32, not by code, so the same code can appear in
        # pairs assigned to different workers), and multiple concurrent
        # ON CONFLICT DO UPDATE transactions upserting overlapping rows
        # in different orders is a textbook Postgres deadlock -- caught
        # live: 3 of 4 workers failed with DeadlockDetected on
        # customers_norm before this fix. Sorting ensures every
        # concurrent transaction acquires row locks in the same order,
        # which eliminates the circular-wait condition. Harmless,
        # zero-cost for the single-writer case this function already
        # handled correctly.
        values = sorted((
            (code, name_norm, dob_iso, f"doris-pipeline:{code}", "ORACLE_DATASOURCE")
            for code, name_norm, dob_iso in rows
        ), key=lambda v: v[0])
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
    async def run(
        self, run_id: str, raw_records: list = None, mode: str = "FULL",
        carry_forward: bool = True, low_memory_mode: bool = False,
    ) -> PipelineResult:
        self.run_id = run_id
        # See pipeline.duckdb_orchestrator.run's identical comment.
        self._carry_forward = carry_forward
        self._low_memory_mode = low_memory_mode
        self._mode = mode
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
            review_count = review_items if isinstance(review_items, int) else len(review_items)
            rejected_count = rejected if isinstance(rejected, int) else len(rejected)
            result.pairs_scored = len(self._scores) if not self._low_memory_mode else (len(auto_links) + review_count + rejected_count)
            result.auto_links = len(auto_links)
            result.review_items = review_count
            result.rejected = rejected_count

            clusters = await self._stage_cluster(auto_links)

            persist_start = datetime.utcnow()
            await self._emit_progress(StageProgress(
                stage=PipelineStage.PERSIST, status="running",
                message="Persisting run artifacts and writing to Postgres...",
            ))

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._persist_run_artifacts, clusters)
            _cleanup_start = datetime.utcnow()
            await loop.run_in_executor(None, self._cleanup_scratch_tables)
            logger.info(f"_cleanup_scratch_tables took {(datetime.utcnow() - _cleanup_start).total_seconds():.3f}s")

            self._input_fp = input_fingerprint(os.path.dirname(PARQUET_PATH))
            self._ruleset_fp = ruleset_fingerprint()
            if self._low_memory_mode:
                # self._decisions/self._scores are empty in this mode --
                # the batched score stage populated the much smaller
                # self._auto_link_fingerprint_data (auto-link pairs
                # only) with exactly what fingerprint_edges needs
                # instead. See DorisPipelineOrchestrator's docstring.
                self._edges_fp = fingerprint_edges(
                    (a, b, *self._auto_link_fingerprint_data[f"{a}:{b}"])
                    for a, b in auto_links
                )
            else:
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

    async def run_distributed(
        self, run_id: str, num_workers: int = 4, mode: str = "FULL", carry_forward: bool = True,
    ) -> PipelineResult:
        """
        Stage 4 proof-of-concept coordinator. Identical to run() through
        block_and_candidates; the score stage is dispatched across
        num_workers independent subprocesses (pipeline.
        distributed_scoring_worker / _score_partition's fixed hash
        partitioning) instead of running here, sequentially or batched.
        Forces self._low_memory_mode=True for the rest of the pipeline:
        workers already persist their own partition's audited pairs to
        Postgres directly (see _score_partition), which is exactly the
        "already persisted, only cluster-level work is left" shape
        low_memory_mode's persist path already handles -- reused as-is
        rather than duplicated.
        """
        self.run_id = run_id
        self._carry_forward = carry_forward
        self._low_memory_mode = True
        self._mode = mode
        result = PipelineResult(run_id=run_id, success=False, mode=mode, stages=[], started_at=datetime.utcnow())

        try:
            record_count = await self._stage_ingest()
            result.records_in = record_count

            await self._stage_normalize(record_count)
            result.records_normalized = record_count

            candidate_count = await self._stage_block_and_candidates()
            result.blocks_created = candidate_count
            result.candidates_generated = candidate_count

            score_start = datetime.utcnow()
            await self._emit_progress(StageProgress(
                stage=PipelineStage.SCORE, status="running",
                message=f"Dispatching scoring across {num_workers} distributed workers...",
            ))

            loop = asyncio.get_event_loop()
            con = self._connect()
            # build_pair_evidence ONCE here, not per worker -- see
            # _score_partition's docstring for why that matters.
            await loop.run_in_executor(None, build_pair_evidence, con, self._dialect)

            import redis
            r = redis.from_url("redis://localhost:6381/0")
            results_key = f"cuin:dist:{run_id}:results"
            r.delete(results_key)  # clean slate in case a prior failed attempt left a stale key

            backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            procs = []
            for widx in range(num_workers):
                proc = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", "pipeline.distributed_scoring_worker",
                    run_id, str(widx), str(num_workers),
                    cwd=backend_dir,
                )
                procs.append(proc)

            exit_codes = await asyncio.gather(*(p.wait() for p in procs))
            failed = [i for i, rc in enumerate(exit_codes) if rc != 0]
            if failed:
                raise RuntimeError(f"Distributed scoring worker(s) {failed} exited non-zero: {exit_codes}")

            auto_links: List[tuple] = []
            review_count = 0
            rejected_count = 0
            for _ in range(num_workers):
                raw = r.blpop(results_key, timeout=120)
                if raw is None:
                    raise RuntimeError("Timed out waiting for a distributed scoring worker's result")
                worker_result = json.loads(raw[1])
                auto_links.extend((a, b) for a, b in worker_result["auto_links"])
                review_count += worker_result["review_count"]
                rejected_count += worker_result["rejected_count"]
                for pid, fp_data in worker_result["fingerprint_data"].items():
                    self._auto_link_fingerprint_data[pid] = tuple(fp_data)
                self._relationships.extend(worker_result["relationships"])

            score_duration = int((datetime.utcnow() - score_start).total_seconds() * 1000)
            await self._emit_progress(StageProgress(
                stage=PipelineStage.SCORE, status="complete",
                records_in=candidate_count, records_out=len(auto_links) + review_count + rejected_count,
                duration_ms=score_duration,
                message=(
                    f"{len(auto_links):,} auto-link, {review_count:,} review, {rejected_count:,} rejected "
                    f"(distributed, {num_workers} workers)"
                ),
            ))
            await self._emit_progress(StageProgress(
                stage=PipelineStage.DECIDE, status="complete", records_out=len(auto_links),
                message=f"{len(auto_links):,} pairs meet the auto-link rule",
            ))

            result.pairs_scored = len(auto_links) + review_count + rejected_count
            result.auto_links = len(auto_links)
            result.review_items = review_count
            result.rejected = rejected_count

            clusters = await self._stage_cluster(auto_links)

            persist_start = datetime.utcnow()
            await self._emit_progress(StageProgress(
                stage=PipelineStage.PERSIST, status="running",
                message="Persisting run artifacts and writing to Postgres...",
            ))

            await loop.run_in_executor(None, self._persist_run_artifacts, clusters)
            await loop.run_in_executor(None, self._cleanup_scratch_tables)

            self._input_fp = input_fingerprint(os.path.dirname(PARQUET_PATH))
            self._ruleset_fp = ruleset_fingerprint()
            self._edges_fp = fingerprint_edges(
                (a, b, *self._auto_link_fingerprint_data[f"{a}:{b}"])
                for a, b in auto_links
            )
            self._clusters_fp = fingerprint_clusters(clusters)
            self._output_fp = output_fingerprint(self._edges_fp, self._clusters_fp)

            await loop.run_in_executor(None, self._persist_to_postgres, clusters, auto_links, review_count, result)

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
                    f"Pipeline complete (Doris engine, distributed x{num_workers}): "
                    f"{len(clusters):,} identity clusters resolved from {record_count:,} records "
                    f"(ruleset={RULESET_VERSION}, output_fingerprint={self._output_fp[:16]}...)"
                ),
            ))

        except Exception as e:
            logger.error(f"Doris distributed pipeline failed: {e}", exc_info=True)
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
