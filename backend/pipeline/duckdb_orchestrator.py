"""
CUIN v2 - Deterministic DuckDB Pipeline Orchestrator (Ruleset v2)

Replaces SparkPipelineOrchestrator as the default engine for the
Datasource demo. Same constructor kwargs and async run(run_id,
raw_records=None, mode="FULL") signature, same StageProgress/
PipelineStage sequence -- so routes_datasource.py and the whole
frontend work unmodified. Also implements get_scores/get_decisions/
get_auto_links/get_uniques/get_result_clusters, which
SparkPipelineOrchestrator never did (that's why /matches/run/{id}/*
returned empty for every prior datasource run).

Why DuckDB instead of Spark: the audit found Spark's non-determinism
came from more than the unseeded Splink EM training --
spark.sql.adaptive.enabled + coalescePartitions + skewJoin change the
physical plan with runtime statistics, and run_blocking() called
`.limit()` on an unordered distributed DataFrame. DuckDB is a single
process with deterministic result sets; every intermediate is a
queryable table an auditor can inspect; no new dependencies (duckdb
already in requirements.txt).
"""

import asyncio
import json
import logging
import os
import shutil
from datetime import datetime
from typing import List, Dict, Optional, Callable, Any

import duckdb

from pipeline.orchestrator import PipelineStage, StageProgress, PipelineResult
from engine.structures import MatchScore, MatchDecision, FieldEvidence
from engine.normalize.explode import build_identifiers_table
from engine.blocking.suppression import build_frequency_table, suppression_summary
from engine.scoring.evidence import build_pair_evidence, evidence_to_field_evidence
from engine.scoring.confidence import score_pair
from engine.rules.match_rules import DEFAULT_MATCH_RULESET
from engine.clustering.cohesion import evaluate_all_components
from engine.clustering.union_find import UnionFind
from engine.clustering import get_cluster_manager
from engine.ruleset.config import get_default_ruleset
from engine.ruleset.effective import resolve_from_active_catalog
from engine.ruleset.version import RULESET_VERSION, ruleset_fingerprint
from engine.determinism import input_fingerprint, fingerprint_edges, fingerprint_clusters, output_fingerprint

logger = logging.getLogger(__name__)

PARQUET_PATH = "data_source/oracle_data.parquet"

# Every table _cleanup_scratch_tables is expected to have left behind --
# what a completed run's file should contain once compacted.
_SURVIVING_TABLES = (
    "identifiers", "customer_scalars", "identifier_frequency",
    "candidate_pairs", "pair_identifier_evidence", "pair_name_dob_evidence",
    "pair_decisions", "customer_segments",
)


def _compact_duckdb_file(run_id: str) -> None:
    """
    DuckDB's DROP TABLE does not shrink the file on disk -- verified
    live: a run whose _cleanup_scratch_tables had already dropped
    every scratch table was STILL 822 MB on disk, unchanged before and
    after an explicit VACUUM/CHECKPOINT (DuckDB has no in-place file
    compaction). The only way to actually reclaim that space is to
    rebuild the file: copy the surviving tables into a fresh file, then
    swap it in. Runs after the connection that wrote them has closed
    (DuckDB's file lock is exclusive) and only on a successful run.
    """
    path = f"data/runs/{run_id}.duckdb"
    if not os.path.exists(path):
        return
    tmp_path = f"data/runs/{run_id}.duckdb.compact.tmp"
    if os.path.exists(tmp_path):
        os.remove(tmp_path)

    con = duckdb.connect(tmp_path)
    try:
        con.execute("SET preserve_insertion_order = true")
        con.execute(f"ATTACH '{path}' AS src (READ_ONLY)")
        copied = []
        for table in _SURVIVING_TABLES:
            try:
                con.execute(f"CREATE TABLE {table} AS SELECT * FROM src.{table}")
                copied.append(table)
            except duckdb.CatalogException:
                continue
        con.execute("DETACH src")
        # `raw` is recreated as a VIEW (not copied as data) -- holds no
        # storage of its own, but engine.rules.catalog.BlockingRuleType.
        # RAW_COLUMN rules need to self-join it when a completed run is
        # reopened for precheck/reblock (api/routes_rules.py), so it
        # must still resolve after compaction.
        con.execute(f"CREATE OR REPLACE VIEW raw AS SELECT * FROM read_parquet('{PARQUET_PATH}')")
        copied.append("raw (view)")
    finally:
        con.close()

    before = os.path.getsize(path)
    os.replace(tmp_path, path)
    after = os.path.getsize(path)
    logger.info(
        f"Compacted {run_id}.duckdb: {before / 1e6:.0f} MB -> {after / 1e6:.0f} MB "
        f"({len(copied)} tables: {', '.join(copied)})"
    )


class DuckDBPipelineOrchestrator:
    """
    Deterministic entity-resolution pipeline over the Oracle parquet
    datasource, using the Ruleset v2 rules (engine.scoring.tiers) in
    place of Splink's learned probabilistic scoring.
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

        # EffectiveRuleset resolves the 5 decision-affecting thresholds
        # from the ACTIVE rule catalog when one exists, falling back to
        # policies/ruleset_v2.yaml otherwise -- closes the gap where a
        # banker's saved threshold change affected only the redecide
        # preview, not a real run. See engine/ruleset/effective.py.
        self.ruleset = resolve_from_active_catalog()
        # One catalog fetch, reused for match_ruleset/segmentation below
        # -- avoids a second Postgres round trip per run.
        self._catalog = self._resolve_catalog()
        # The active catalog's per-field confidence model (Stage 2) --
        # what _stage_score_and_decide actually scores pairs with.
        # self.ruleset above is kept for suppression thresholds and
        # cohesion guards (max_cluster_size/min_density), which
        # EffectiveRuleset also sources from this same catalog.
        self.match_ruleset = self._catalog.match_ruleset
        self._blocking_rules = []
        self._candidate_count_cached = 0
        # Stage 5: record segmentation (Company vs Individual). Disabled
        # by default -- see engine.segments.classifier.SegmentationConfig.
        self._segmentation = self._catalog.segmentation
        self._relationships: List[dict] = []
        self._con: Optional[duckdb.DuckDBPyConnection] = None

        self._scores: Dict[str, MatchScore] = {}
        self._decisions: Dict[str, MatchDecision] = {}
        self._records: Dict[str, dict] = {}

        self._input_fp = None
        self._ruleset_fp = None
        self._edges_fp = None
        self._clusters_fp = None
        self._output_fp = None

    @staticmethod
    def _resolve_catalog():
        try:
            from engine.rules.store import get_active_catalog
            return get_active_catalog()
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

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            # Persistent per-run file, not :memory: -- this is what
            # makes the Settings UI's threshold slider and blocking-
            # rule editor "instant": the normalize/block/evidence
            # tables (identifiers, customer_scalars, candidate_pairs,
            # pair_identifier_evidence, pair_name_dob_evidence) survive
            # the run and can be reopened by api/routes_rules.py's
            # /runs/{id}/redecide and /reblock endpoints, which replay
            # engine.rules.decision_compiler / engine.rules.compiler
            # against them in milliseconds instead of re-running the
            # whole pipeline. See engine/rules/store.py and the
            # migration plan's Phase 5.
            if self.run_id:
                os.makedirs("data/runs", exist_ok=True)
                db_path = f"data/runs/{self.run_id}.duckdb"
            else:
                db_path = ":memory:"
            self._con = duckdb.connect(db_path)
            self._con.execute("SET preserve_insertion_order = true")
            # This orchestrator runs a background task inside the live API
            # server process, on a host shared with other services -- cap
            # resource use rather than let DuckDB claim everything. These
            # are configurable via env vars for deployment tuning; the
            # defaults are conservative for a shared box, not a dedicated
            # batch machine. The full 1.5M-row pipeline completes in ~1
            # minute at these limits.
            memory_limit = os.environ.get("DUCKDB_MEMORY_LIMIT", "4GB")
            threads = int(os.environ.get("DUCKDB_THREADS", "4"))
            self._con.execute(f"SET memory_limit = '{memory_limit}'")
            self._con.execute(f"SET threads = {threads}")
        return self._con

    # ------------------------------------------------------------------
    # Stage 1: Ingest
    # ------------------------------------------------------------------
    async def _stage_ingest(self) -> int:
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.INGEST, status="running",
            message="Loading Oracle Parquet dataset into DuckDB...",
        ))

        loop = asyncio.get_event_loop()

        def _load():
            con = self._connect()
            con.execute(f"""
                CREATE OR REPLACE VIEW raw AS
                SELECT * FROM read_parquet('{PARQUET_PATH}')
            """)
            return con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]

        record_count = await loop.run_in_executor(None, _load)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.INGEST, status="complete",
            records_in=record_count, records_out=record_count,
            duration_ms=duration,
            message=f"Loaded {record_count:,} customer records",
        ))
        return record_count

    # ------------------------------------------------------------------
    # Stage 2: Normalize (identifier explosion + validation)
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
            from engine.ports.duckdb_dialect import DuckDbDialect
            con = self._connect()
            build_identifiers_table(con, source_relation="raw")
            build_frequency_table(con, thresholds={
                "mobile": self.ruleset.suppression_mobile_max,
                "email": self.ruleset.suppression_email_max,
                "document": self.ruleset.suppression_document_max,
                "address": self.ruleset.suppression_address_max,
            })
            # Stage 5: always builds customer_segments (even disabled,
            # where every row is "ALL") so downstream joins never need
            # to special-case its absence -- the elision that makes
            # disabled segmentation free happens at decide time instead
            # (see _stage_score_and_decide), not here.
            build_customer_segments(con, DuckDbDialect(), self._segmentation)
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
    # Stage 3 & 4: Block + Candidates
    # ------------------------------------------------------------------
    # Compiles the ACTIVE rule catalog (engine.rules.store.
    # get_active_catalog()) through engine.rules.compiler, exactly as
    # pipeline.doris_orchestrator already does -- previously this
    # called the hand-written engine.blocking.deterministic_blocker
    # instead, meaning a banker's saved blocking-rule changes (Settings
    # UI, including RAW_COLUMN rules on columns like BRANCH_CODE that
    # deterministic_blocker has no knowledge of at all) had ZERO EFFECT
    # on a real DuckDB run -- only on Doris runs and the redecide/
    # reblock PREVIEW. DEFAULT_BLOCKING_RULES (the seed a fresh
    # deployment starts with) is proven byte-identical to
    # deterministic_blocker's output by
    # tests/unit/test_rules_zero_change.py, so this is a zero-behavior-
    # change swap at the default catalog.
    async def _stage_block(self) -> int:
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.BLOCK, status="running",
            message="Compiling the active blocking-rule catalog...",
        ))

        loop = asyncio.get_event_loop()

        def _block():
            from engine.rules.compiler import compile_and_build
            from engine.ports.duckdb_dialect import DuckDbDialect
            con = self._connect()
            self._blocking_rules = self._catalog.blocking_rules
            compile_and_build(con, self._catalog.blocking_rules, DuckDbDialect())
            return con.execute("SELECT COUNT(*) FROM candidate_pairs").fetchone()[0]

        self._candidate_count_cached = await loop.run_in_executor(None, _block)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.BLOCK, status="complete",
            duration_ms=duration,
            message="Blocking rules compiled and applied",
        ))
        return self._candidate_count_cached

    async def _stage_candidates(self) -> int:
        # Blocking and candidate generation are one compiled step (see
        # _stage_block) -- this stage just reports the count computed
        # there, keeping the PipelineStage.CANDIDATES progress event
        # the frontend already expects.
        start = datetime.utcnow()
        candidate_count = self._candidate_count_cached
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
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
            build_pair_evidence(con)

            pairs = con.execute("SELECT a_key, b_key FROM candidate_pairs").fetchall()

            # Stage 5: customer_code -> segment. When segmentation is
            # disabled every value is "ALL", so seg_a == seg_b always --
            # the branch below always takes the same-segment path,
            # which is exactly the pre-Stage-5 code path. Only reached
            # when segmentation is actually enabled does the
            # cross-segment branch ever fire.
            segments: Dict[str, str] = dict(
                con.execute("SELECT customer_code, segment FROM customer_segments").fetchall()
            )

            # Stage 5.1: bulk-fetch any raw column a bank-added
            # RAW_COLUMN match rule references (across the default
            # ruleset AND every per-segment override, since segments
            # can use different custom fields), ONE query for every
            # (customer_code -> {column: value}) a pair might need --
            # not a per-pair lookup against `raw`. Zero rules
            # referencing RAW_COLUMN (the default, pre-Stage-5.1 state)
            # means this dict stays empty and every pair's raw_fields
            # is {}, a structural no-op identical to before.
            from engine.rules.match_rules import raw_columns_in_catalog
            raw_columns = raw_columns_in_catalog(self._catalog)
            raw_by_code: Dict[str, dict] = {}
            if raw_columns:
                cols_sql = ", ".join(raw_columns)
                for row in con.execute(f"SELECT CUSTOMER_CODE, {cols_sql} FROM raw").fetchall():
                    raw_by_code[row[0]] = dict(zip(raw_columns, row[1:]))

            # Bulk-fetch all evidence in two queries total (not two PER PAIR --
            # at hundreds of thousands of candidate pairs, per-pair round trips
            # would dominate runtime). Grouped into Python dicts keyed by
            # (a_key, b_key) for O(1) lookup in the loop below.
            id_evidence: Dict[tuple, list] = {}
            for row in con.execute("""
                SELECT a_key, b_key, id_type, doc_type, values_a, values_b, intersection
                FROM pair_identifier_evidence
            """).fetchall():
                a_key, b_key, id_type, doc_type, values_a, values_b, intersection = row
                id_evidence.setdefault((a_key, b_key), []).append({
                    "id_type": id_type, "doc_type": doc_type,
                    "values_a": values_a or [], "values_b": values_b or [],
                    "intersection": intersection or [],
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
                    "tokens_a": tokens_a or [], "tokens_b": tokens_b or [],
                    "token_intersection": token_intersection or [],
                    "token_union": token_union or [],
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
                    # Cross-segment: never a candidate for AUTO_LINK/
                    # REVIEW/REJECT -- score_pair() answers "how likely
                    # is this the SAME entity", which is a category
                    # error between e.g. a company and a person. The
                    # pair still exists because blocking found real
                    # shared evidence, so that connection is recorded
                    # as a traceable relationship instead of an
                    # identity decision -- see engine.segments.
                    # relationships and db/migrations/004.
                    rel_evidence = detect_relationship_evidence(evidence)
                    if rel_evidence:
                        self._relationships.append({
                            "a_key": a_key, "b_key": b_key,
                            "a_segment": seg_a, "b_segment": seg_b,
                            "shared_evidence": [e.to_dict() for e in rel_evidence],
                        })
                    continue

                # Confidence model (Stage 2) -- replaces
                # classify()/decide()'s strong/medium tier counting with
                # a per-field percentage a banker sets directly. tiers.py
                # stays in the tree as the frozen oracle
                # tests/unit/test_confidence_enumeration.py proves this
                # agrees with, exhaustively, at the seed values.
                # Stage 5: uses this segment's dedicated MatchRuleset
                # when the catalog has one, else the catalog default --
                # see RuleCatalogVersion.match_ruleset_for_segment.
                segment_ruleset = self._catalog.match_ruleset_for_segment(seg_a)
                pair_score = score_pair(evidence, segment_ruleset)
                decision = MatchDecision(pair_score.decision)
                score_value = pair_score.confidence_pct / 100.0

                pair_id = f"{a_key}:{b_key}"

                match_score = MatchScore(
                    pair_id=pair_id,
                    a_key=a_key,
                    b_key=b_key,
                    score=score_value,
                    evidence=evidence_to_field_evidence(evidence),
                    hard_conflicts=pair_score.vetoes,
                    signals_hit=pair_score.signals_hit,
                )
                self._scores[pair_id] = match_score
                self._decisions[pair_id] = decision

                if decision == MatchDecision.AUTO_LINK:
                    auto_links.append((a_key, b_key))
                elif decision == MatchDecision.REVIEW:
                    review_items.append((a_key, b_key))
                    review_service.queue_for_review(
                        pair_id=pair_id,
                        run_id=self.run_id,
                        a_key=a_key,
                        b_key=b_key,
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

            # ONE bulk save after the loop, not per-item -- review_service
            # persists this run's queue file on each save, and calling
            # that up to ~190K times inside this loop would make the
            # pipeline take far longer than the actual matching work.
            review_service._save_queue(self.run_id)

            # Durable baseline for the Settings UI's instant redecide/
            # reblock (api/routes_rules.py): the SAME confidence logic
            # compiled to SQL (engine.rules.confidence_compiler), run
            # once here and persisted as `pair_decisions` in this run's
            # now-persistent DuckDB file (see _connect()). This does
            # NOT replace the Python loop above -- auto_links/
            # review_items/rejected and the review queue still come
            # from it unchanged -- it only gives future redecide calls
            # something to diff against. Proven identical to the loop
            # above by tests/unit/test_confidence_sql_parity.py.
            try:
                from engine.rules.confidence_compiler import compile_confidence_sql
                from engine.ports.duckdb_dialect import DuckDbDialect
                con.execute(compile_confidence_sql(self.match_ruleset, DuckDbDialect(), table_name="pair_decisions"))
            except Exception as e:
                logger.warning(f"Could not persist SQL-compiled pair_decisions baseline (redecide/reblock will fall back to a fresh compile): {e}")

            return auto_links, review_items, rejected

        auto_links, review_items, rejected = await loop.run_in_executor(None, _score_and_decide)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        relationships_msg = f", {len(self._relationships):,} cross-segment relationships" if self._segmentation.enabled else ""
        await self._emit_progress(StageProgress(
            stage=PipelineStage.SCORE, status="complete",
            records_in=candidate_count, records_out=len(self._scores),
            duration_ms=duration,
            message=(
                f"{len(auto_links):,} auto-link, {len(review_items):,} review, "
                f"{len(rejected):,} rejected{relationships_msg}"
            ),
        ))

        await self._emit_progress(StageProgress(
            stage=PipelineStage.DECIDE, status="complete",
            records_out=len(auto_links),
            message=f"{len(auto_links):,} pairs meet the auto-link rule",
        ))

        return auto_links, review_items, rejected

    # ------------------------------------------------------------------
    # Stage 8: Cluster (with cohesion guard)
    # ------------------------------------------------------------------
    async def _stage_cluster(self, auto_links):
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CLUSTER, status="running",
            message="Building cohesion-guarded identity clusters...",
        ))

        loop = asyncio.get_event_loop()

        def _cluster():
            uf = UnionFind()
            edges_by_root: Dict[str, set] = {}

            for a_key, b_key in auto_links:
                uf.union(a_key, b_key)

            # Group edges by the FINAL root of their endpoints, so cohesion
            # is evaluated against the actual final component, not an
            # intermediate one.
            for a_key, b_key in auto_links:
                root = uf.find(a_key)
                edges_by_root.setdefault(root, set()).add(tuple(sorted((a_key, b_key))))

            components = uf.get_clusters()
            verdicts = evaluate_all_components(
                components, edges_by_root,
                max_cluster_size=self.ruleset.max_cluster_size,
                min_density=self.ruleset.min_density,
            )

            manager = get_cluster_manager()
            manager._uf = UnionFind()
            manager._cluster_ids = {}
            manager._members = []

            accepted_clusters: Dict[str, List[str]] = {}
            demoted_to_review = 0

            for root, members in components.items():
                verdict = verdicts[root]
                if not verdict.accepted:
                    demoted_to_review += len(edges_by_root.get(root, []))
                    continue
                if len(members) < 2:
                    continue
                for i in range(1, len(members)):
                    manager.link(members[0], members[i])
                cluster_id = manager.find(members[0])
                accepted_clusters[cluster_id] = sorted(members)

            return accepted_clusters, demoted_to_review

        accepted_clusters, demoted_to_review = await loop.run_in_executor(None, _cluster)

        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CLUSTER, status="complete",
            records_out=len(accepted_clusters), duration_ms=duration,
            message=(
                f"{len(accepted_clusters):,} cohesive clusters formed "
                f"({demoted_to_review} edges demoted to review for low cohesion/oversized component)"
            ),
            data={"cluster_stats": {"clusters_created": len(accepted_clusters)}},
        ))

        return accepted_clusters

    # ------------------------------------------------------------------
    # Persistence of run artifacts (same formats as SparkPipelineOrchestrator
    # so /graph/*, /matches/* work unmodified)
    # ------------------------------------------------------------------
    def _load_records_for_members(self, customer_codes: set) -> Dict[str, dict]:
        if not customer_codes:
            return {}

        con = self._connect()
        codes_list = list(customer_codes)
        placeholders = ",".join(["?"] * len(codes_list))
        rows = con.execute(f"""
            SELECT
                s.customer_code,
                r.NAME AS name,
                s.name_norm,
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
        """, codes_list).fetchall()

        records = {}
        for row in rows:
            ckey, name, name_norm, email_norm, phone_norm, dob_iso, address_norm, natid_norm = row
            records[ckey] = {
                "customer_key": ckey,
                "source_customer_id": ckey,
                "name": name or "",
                "name_norm": name_norm or "",
                "email": email_norm or "",
                "email_norm": email_norm or "",
                "phone": phone_norm or "",
                "phone_norm": phone_norm or "",
                "dob": dob_iso or "",
                "dob_norm": dob_iso or "",
                "address": address_norm or "",
                "address_norm": address_norm or "",
                "natid": natid_norm or "",
                "natid_norm": natid_norm or "",
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

        # Lightweight singleton manifest: customer codes present in this
        # run but NOT in any accepted cluster. Only the code list is
        # written here (not full profiles) -- with ~1.2M such codes on
        # the full dataset, materializing every singleton's full record
        # into JSON would be exactly the wasted-memory/slow-load mistake
        # the earlier audit flagged (Risk R4). api/routes_graph.py's
        # get_unique_records() pages this manifest, then looks up only
        # the current page's ~20-50 codes' profiles on demand from the
        # source parquet -- see PARQUET_PATH usage there.
        con = self._connect()
        con.execute("CREATE OR REPLACE TABLE _cluster_members (customer_code VARCHAR)")
        if all_members:
            con.executemany(
                "INSERT INTO _cluster_members VALUES (?)",
                [(m,) for m in all_members],
            )
        con.execute(f"""
            COPY (
                SELECT customer_code FROM customer_scalars
                WHERE customer_code NOT IN (SELECT customer_code FROM _cluster_members)
                ORDER BY customer_code
            ) TO 'data/runs/{self.run_id}_singletons.csv' (HEADER)
        """)

        # Written directly from self._scores/self._decisions (in-memory
        # after _stage_score_and_decide), not via a DuckDB COPY from
        # candidate_pairs -- candidate_pairs has no decision/score
        # columns at all, which is exactly why /matches/scores
        # (api/routes_matches.py, reading match_probability from this
        # CSV) silently returned empty results: pandas raised a
        # KeyError on the missing column, caught by a broad except.
        import csv as csv_module
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
                    ";".join(score.signals_hit),
                    ";".join(score.hard_conflicts),
                ])

    def _cleanup_scratch_tables(self) -> None:
        """
        Drops staging tables that have served their purpose by the time
        this runs (after candidate_pairs/pair_*_evidence/clusters/
        artifacts have all been derived from them). `identifiers`/
        `customer_scalars`/`identifier_frequency`/`candidate_pairs` are
        deliberately NOT dropped -- routes_rules.py's precheck/redecide/
        reblock reopen a completed run's .duckdb file and read them.
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

    def _persist_to_postgres(self, clusters, auto_links, review_items, result) -> None:
        """
        Closes the audit gap the earlier investigation found: the Spark
        datasource pipeline wrote nothing to Postgres or Neo4j at all
        (verified empirically as 0 rows across all 8 tables). Only
        AUTO_LINK/REVIEW pairs are persisted, not REJECT -- at full
        dataset scale REJECT is ~half of all candidate pairs and has no
        ongoing audit value once a run completes, so persisting it would
        roughly double write volume for no compliance benefit.

        Never fails the pipeline run: if Postgres is unreachable, this
        logs a warning and the run still succeeds with file-based
        artifacts (matching the graceful-degradation pattern already
        used elsewhere in this codebase for Neo4j).
        """
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
                """
                Isolates one persistence step: a Postgres error aborts
                the current transaction until rolled back, so without
                this a single bad statement (e.g. the cluster_id/UUID
                format mismatch this caught during testing) would
                silently cascade and abort every step after it in the
                same connection, not just the one that failed.
                """
                try:
                    return fn(*args)
                except Exception as e:
                    pg_conn.rollback()
                    logger.warning(f"Postgres persistence step '{name}' failed, skipped: {e}")
                    return None

            try:
                _step("ensure_run_row", repository.ensure_run_row, pg_conn, self.run_id, result.mode, "Datasource Demo (duckdb)")

                code_to_uuid = _step("upsert_customers", repository.upsert_customers, pg_conn, self._con, list(audited_codes)) or {}

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

                n_freq = _step(
                    "persist_identifier_frequency", repository.persist_identifier_frequency,
                    pg_conn, self._con, self.run_id,
                ) or 0

                n_relationships = _step(
                    "persist_entity_relationships", repository.persist_entity_relationships,
                    pg_conn, self.run_id, code_to_uuid, self._relationships,
                ) or 0

                _step(
                    "update_run_fingerprints", repository.update_run_fingerprints,
                    pg_conn, self.run_id, self.get_fingerprints(),
                    {
                        "records_in": result.records_in,
                        "candidates_generated": result.candidates_generated,
                        "pairs_scored": result.pairs_scored,
                        "auto_links": result.auto_links,
                        "review_items": result.review_items,
                        "rejected": result.rejected,
                    },
                )

                logger.info(
                    f"Persisted to Postgres: {len(code_to_uuid):,} customers, {n_pairs:,} pairs, "
                    f"{n_scores:,} scores, {n_decisions:,} decisions, {n_clusters:,} cluster memberships, "
                    f"{n_freq:,} suppressed-identifier records, {n_relationships:,} cross-segment relationships"
                )
            finally:
                pg_conn.close()

        except Exception as e:
            logger.warning(f"Postgres persistence failed (run still succeeds with file artifacts): {e}")

    # ------------------------------------------------------------------
    # Public run()
    # ------------------------------------------------------------------
    async def run(
        self,
        run_id: str,
        raw_records: list = None,
        mode: str = "FULL",
    ) -> PipelineResult:
        self.run_id = run_id
        result = PipelineResult(
            run_id=run_id, success=False, mode=mode, stages=[],
            started_at=datetime.utcnow(),
        )

        try:
            record_count = await self._stage_ingest()
            result.records_in = record_count

            valid_identifiers = await self._stage_normalize(record_count)
            result.records_normalized = record_count

            await self._stage_block()

            candidate_count = await self._stage_candidates()
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

            await loop.run_in_executor(
                None, self._persist_to_postgres, clusters, auto_links, review_items, result
            )

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
                    f"Pipeline complete: {len(clusters):,} identity clusters resolved "
                    f"from {record_count:,} records (ruleset={RULESET_VERSION}, "
                    f"output_fingerprint={self._output_fp[:16]}...)"
                ),
            ))

        except Exception as e:
            logger.error(f"DuckDB pipeline failed: {e}", exc_info=True)
            result.success = False
            result.error_message = str(e)
            result.ended_at = datetime.utcnow()

            await self._emit_progress(StageProgress(
                stage=PipelineStage.FAILED, status="error", message=str(e),
            ))

        finally:
            if self._con:
                try:
                    self._con.close()
                except Exception:
                    pass
                self._con = None

            if result.success and self.run_id:
                try:
                    _compact_duckdb_file(self.run_id)
                except Exception:
                    logger.warning("DuckDB file compaction failed (run artifacts are unaffected)", exc_info=True)

        return result

    # ------------------------------------------------------------------
    # Accessors (mirrors pipeline.orchestrator.PipelineOrchestrator's
    # contract, consumed by api/routes_matches.py)
    # ------------------------------------------------------------------
    def get_scores(self) -> Dict[str, MatchScore]:
        return self._scores

    def get_decisions(self) -> Dict[str, MatchDecision]:
        return self._decisions

    def get_review_queue(self) -> List[MatchScore]:
        return [
            self._scores[pid] for pid, d in self._decisions.items()
            if d == MatchDecision.REVIEW
        ]

    def get_auto_links(self) -> List[MatchScore]:
        return [
            self._scores[pid] for pid, d in self._decisions.items()
            if d == MatchDecision.AUTO_LINK
        ]

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
                    "cluster_id": cluster_id,
                    "size": len(members),
                    "members": sorted(members),
                    "records": cluster_records,
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
