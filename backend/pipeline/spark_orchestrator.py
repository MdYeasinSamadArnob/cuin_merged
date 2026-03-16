import asyncio
import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Optional, Callable, Any

from pipeline.orchestrator import PipelineStage, StageProgress, PipelineResult
from engine.structures import MatchScore, MatchDecision

logger = logging.getLogger(__name__)

# Match threshold for Splink (high confidence matches only)
MATCH_THRESHOLD = 0.95

# Paths
PARQUET_PATH = "data_source/oracle_data.parquet"
BLOCKING_CSV = "blocking_analysis.csv"
SCORING_CSV = "scoring_results.csv"


class SparkPipelineOrchestrator:
    """
    Orchestrates the complete ER pipeline using REAL Spark business logic.
    Properly integrates with splink_commands.py functions.
    """
    
    def __init__(
        self,
        blocking_config=None,
        scoring_config=None,
        progress_callback: Optional[Callable[[StageProgress], None]] = None,
        run_id: Optional[str] = None
    ):
        self.blocking_config = blocking_config
        self.scoring_config = scoring_config
        self.progress_callback = progress_callback
        self.run_id = run_id
        
        # In-memory storage
        self._records: Dict[str, dict] = {}
        self._scores: Dict[str, MatchScore] = {}
        self._decisions: Dict[str, MatchDecision] = {}
        
        # Spark session and data (initialized in run())
        self._spark = None
        self._df = None
        
        # Set environment variables for spark logic
        os.environ["CACHE_DIR"] = "data_source"
        os.environ["CACHE_FORMAT"] = "parquet"

    async def _emit_progress(self, progress: StageProgress) -> None:
        """Emit progress update."""
        if self.progress_callback:
            try:
                import inspect
                if inspect.iscoroutinefunction(self.progress_callback):
                    await self.progress_callback(progress)
                elif asyncio.iscoroutine(self.progress_callback):
                     await self.progress_callback
                else:
                    self.progress_callback(progress)
            except Exception as e:
                logger.error(f"Progress callback error in {self.run_id}: {e}")

    async def _stage_ingest(self) -> tuple:
        """Stage 1: Load data from Parquet into Spark DataFrame."""
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.INGEST,
            status="running",
            message="Initializing Spark and loading Oracle Parquet dataset..."
        ))
        
        loop = asyncio.get_event_loop()
        
        def load_spark_data():
            """Load data in background thread to avoid blocking."""
            try:
                # Import Spark functions
                from engine.spark_er.splink_commands import create_spark_session, load_data
                
                # Create Spark session with proper configuration
                logger.info("Creating Spark session...")
                spark = create_spark_session()
                
                # Load data from parquet
                logger.info(f"Loading data from {PARQUET_PATH}...")
                df = load_data(spark)
                
                record_count = df.count()
                logger.info(f"Loaded {record_count:,} records into Spark")
                
                return spark, df, record_count
                
            except Exception as e:
                logger.error(f"Failed to load Spark data: {e}", exc_info=True)
                raise
        
        # Run in executor to avoid blocking
        self._spark, self._df, record_count = await loop.run_in_executor(None, load_spark_data)
        self._record_count = record_count  # cache for reuse — avoids re-counting in later stages
        
        # Initialize the runs dir
        os.makedirs("data/runs", exist_ok=True)
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.INGEST,
            status="complete",
            records_in=record_count,
            records_out=record_count,
            reduction_pct=0.0,
            duration_ms=duration,
            message=f"Loaded {record_count:,} customer records from Oracle Parquet into Spark"
        ))
        return record_count

    async def _stage_normalize(self, record_count: int):
        """Stage 2: Normalize data using Spark SQL transformations."""
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.NORMALIZE,
            status="running",
            message="Normalizing fields (uppercase, trim, standardize formats)..."
        ))
        
        loop = asyncio.get_event_loop()
        
        def normalize_dataframe():
            """Apply safe normalization in a single Spark select pass (faster DAG planning)."""
            try:
                from pyspark.sql import functions as F
                from pyspark.sql.types import ArrayType

                logger.info("Applying safe Spark SQL normalization (single-pass select)...")

                df = self._df
                schema = {field.name: field.dataType for field in df.schema.fields}
                cols = set(df.columns)
                logger.info(f"Available columns: {sorted(cols)}")

                def safe_str(col_name):
                    """Return a string expression, flattening ARRAY columns to space-separated string."""
                    dtype = schema.get(col_name)
                    if isinstance(dtype, ArrayType):
                        return F.array_join(
                            F.filter(F.col(col_name).cast('array<string>'), lambda x: x.isNotNull()),
                            ' '
                        )
                    return F.col(col_name).cast('string')

                phone_col = 'MOBILE' if 'MOBILE' in cols else ('TELEPHONE' if 'TELEPHONE' in cols else None)
                dob_col   = next((c for c in ('BIRTH_DATE', 'CUSDOB') if c in cols), None)
                doc_col   = next((c for c in ('DOCUMENT', 'ID_NUMBER') if c in cols), None)
                addr_col  = next((c for c in ('FULL_ADDRESS', 'ADDRESS') if c in cols), None)

                # Column overrides — CUSTOMER_CODE is cast to plain string for Splink
                overrides = {}
                if 'CUSTOMER_CODE' in cols:
                    overrides['CUSTOMER_CODE'] = safe_str('CUSTOMER_CODE').alias('CUSTOMER_CODE')

                # New _NORM columns to append
                appended = []
                if 'NAME' in cols:
                    appended.append(
                        F.trim(F.upper(F.regexp_replace(safe_str('NAME'), r'\s+', ' '))).alias('NAME_NORM')
                    )
                if 'EMAIL' in cols:
                    appended.append(F.trim(F.lower(safe_str('EMAIL'))).alias('EMAIL_NORM'))
                if phone_col:
                    digits = F.regexp_replace(safe_str(phone_col), r'\D', '')
                    appended.append(
                        F.when(F.length(digits) > 10,
                            F.expr(f"right(regexp_replace(cast(`{phone_col}` as string), '[^0-9]', ''), 10)")
                        ).otherwise(digits).alias('PHONE_NORM')
                    )
                if dob_col:
                    appended.append(F.to_date(safe_str(dob_col)).alias('DOB_NORM'))
                if doc_col:
                    appended.append(F.trim(F.upper(safe_str(doc_col))).alias('NATID_NORM'))
                if addr_col:
                    appended.append(
                        F.trim(F.upper(F.regexp_replace(safe_str(addr_col), r'\s+', ' '))).alias('ADDRESS_NORM')
                    )

                # Single-pass select: apply overrides + append _NORM cols in one plan node
                base_cols = [overrides.get(c, F.col(c)) for c in df.columns]
                df = df.select(*base_cols, *appended)

                df.cache()
                normalized_count = df.count()  # Materialize cache

                self._df = df
                self._record_count = normalized_count  # keep consistent after normalization
                logger.info(f"Normalization complete: {normalized_count:,} records")
                return normalized_count

            except Exception as e:
                logger.error(f"Normalization failed: {e}", exc_info=True)
                raise
        
        # Run normalization in executor
        normalized_count = await loop.run_in_executor(None, normalize_dataframe)
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.NORMALIZE,
            status="complete",
            records_in=record_count,
            records_out=normalized_count,
            reduction_pct=0,
            duration_ms=duration,
            message=f"Normalized {normalized_count:,} records (uppercase names, trim spaces, standardized phone/email/dates)"
        ))

    async def _stage_block(self) -> int:
        """Stage 3: Run distributed blocking using Spark to generate candidate pairs."""
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.BLOCK,
            status="running",
            message="Running distributed blocking analysis (multipass strategy)..."
        ))
        
        loop = asyncio.get_event_loop()
        
        def run_blocking_phase():
            """Run blocking in background thread."""
            try:
                from engine.spark_er.splink_commands import run_blocking
                
                logger.info("Starting Spark-native blocking phase...")
                
                # Run blocking - generates candidate pairs
                settings, pandas_df, spark_df = run_blocking(
                    spark=self._spark,
                    df=self._df,
                    output_csv=BLOCKING_CSV,
                    record_count=self._record_count  # skip redundant df.count() inside
                )
                
                candidate_count = len(pandas_df) if pandas_df is not None else 0
                logger.info(f"Blocking complete: {candidate_count:,} candidate pairs generated")
                
                return settings, candidate_count
                
            except Exception as e:
                logger.error(f"Blocking phase failed: {e}", exc_info=True)
                raise
        
        settings, candidate_count = await loop.run_in_executor(None, run_blocking_phase)
        self._blocking_settings = settings  # Store for scoring phase
        
        # Calculate actual reduction percentage (reuse cached record count — no extra Spark job)
        n = self._record_count
        max_possible_pairs = n * (n - 1) // 2
        reduction_pct = ((max_possible_pairs - candidate_count) / max_possible_pairs * 100) if max_possible_pairs > 0 else 0
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.BLOCK,
            status="complete",
            records_in=n,
            records_out=candidate_count,
            reduction_pct=reduction_pct,
            duration_ms=duration,
            message=f"Generated {candidate_count:,} candidate pairs using distributed blocking (Exact + Phonetic + Token + LSH) - {reduction_pct:.2f}% reduction"
        ))
        
        return candidate_count

    async def _stage_candidates(self, candidate_count: int):
        """Stage 4: Candidates already generated by blocking, emit progress."""
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CANDIDATES,
            status="running",
            message="Candidate pairs ready for scoring..."
        ))
        
        await asyncio.sleep(0.3)
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        # Calculate theoretical reduction (reuse cached record count — no extra Spark job)
        n = self._record_count
        max_possible = n * (n - 1) // 2
        reduction = ((max_possible - candidate_count) / max_possible * 100) if max_possible > 0 else 0
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CANDIDATES,
            status="complete",
            records_in=max_possible,
            records_out=candidate_count,
            reduction_pct=reduction,
            duration_ms=duration,
            message=f"Candidate pairs ready: {candidate_count:,} (reduced from {max_possible:,} possible pairs = {reduction:.2f}% reduction)"
        ))

    async def _stage_score_and_decide(self, candidate_count: int) -> tuple:
        """Stage 5 & 6: Run Splink scoring on candidates and make decisions."""
        start = datetime.utcnow()

        await self._emit_progress(StageProgress(
            stage=PipelineStage.SCORE,
            status="running",
            message="Training Splink model (EM algorithm for match probabilities)...",
            data={"sub_step": "Initializing Splink linker...", "progress_pct": 5}
        ))

        loop = asyncio.get_event_loop()

        def run_scoring_phase():
            """Run scoring in background thread with progress updates."""
            try:
                from engine.spark_er.splink_commands import run_scoring
                
                logger.info("Starting Splink scoring phase...")
                
                # Run scoring - trains model and scores candidate pairs
                linker, predictions_df = run_scoring(
                    spark=self._spark,
                    df=self._df,
                    settings=self._blocking_settings,
                    blocking_csv=BLOCKING_CSV,
                    threshold=MATCH_THRESHOLD,
                    output_csv=SCORING_CSV
                )
                
                # Materialise predictions — run_scoring returns a PredictionsWrapper, not a pandas DF
                if predictions_df is not None and hasattr(predictions_df, 'as_pandas_dataframe'):
                    predictions_pd = predictions_df.as_pandas_dataframe()
                else:
                    predictions_pd = predictions_df  # already a pandas DF or None

                # Convert predictions to match list
                matches = []
                if predictions_pd is not None and not predictions_pd.empty:
                    for _, row in predictions_pd.iterrows():
                        matches.append({
                            'id1': row['CUSTOMER_CODE_l'],
                            'id2': row['CUSTOMER_CODE_r'],
                            'match_probability': row['match_probability'],
                            'match_weight': row.get('match_weight', 0)
                        })
                
                logger.info(f"Scoring complete: {len(matches):,} high-confidence matches found")
                
                return matches
                
            except Exception as e:
                logger.error(f"Scoring phase failed: {e}", exc_info=True)
                raise
        
        # Emit periodic progress during scoring
        scoring_task = loop.run_in_executor(None, run_scoring_phase)
        
        # Poll until complete with progress updates
        elapsed = 0
        while not scoring_task.done():
            await asyncio.sleep(2)
            elapsed += 2
            
            # Emit progress heartbeat
            await self._emit_progress(StageProgress(
                stage=PipelineStage.SCORE,
                status="running",
                message=f"Training Splink model and scoring pairs ({elapsed}s elapsed)...",
                data={
                    "sub_step": "Running EM iterations for probability estimation...",
                    "progress_pct": min(50 + (elapsed // 2), 90),
                    "elapsed_sec": elapsed
                }
            ))
        
        # Get result
        matches = await scoring_task
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.SCORE,
            status="complete",
            records_in=candidate_count,
            records_out=len(matches),
            reduction_pct=0,
            duration_ms=duration,
            message=f"Splink scoring complete: {len(matches):,} high-confidence matches (probability ≥ {MATCH_THRESHOLD})",
            data={
                "sub_step": "Complete",
                "progress_pct": 100,
                "sample_matches": matches[:50] if matches else []
            }
        ))

        # Emit DECIDE stage
        await self._emit_progress(StageProgress(
            stage=PipelineStage.DECIDE,
            status="running",
            message="Applying decision threshold (AUTO_LINK for matches)..."
        ))
        
        await asyncio.sleep(0.3)
        
        # Calculate clusters from matches
        clusters = self._build_clusters_from_matches(matches)
        
        await self._emit_progress(StageProgress(
            stage=PipelineStage.DECIDE,
            status="complete",
            records_in=len(matches),
            records_out=len(clusters),
            reduction_pct=0,
            duration_ms=300,
            message=f"Decisions complete: {len(matches):,} pairs auto-linked → {len(clusters):,} identity clusters"
        ))

        return clusters, matches

    def _build_clusters_from_matches(self, matches: List[dict]) -> List[dict]:
        """Build clusters from match pairs using union-find."""
        from engine.clustering.union_find import UnionFind
        
        uf = UnionFind()
        
        # Union all matched pairs
        for match in matches:
            id1 = str(match['id1'])
            id2 = str(match['id2'])
            uf.union(id1, id2)
        
        # Group by cluster
        clusters_dict = {}
        for match in matches:
            id1 = str(match['id1'])
            id2 = str(match['id2'])
            
            root = uf.find(id1)
            if root not in clusters_dict:
                clusters_dict[root] = set()
            
            clusters_dict[root].add(id1)
            clusters_dict[root].add(id2)
        
        # Convert to list format
        clusters = []
        for cluster_id, members in clusters_dict.items():
            clusters.append({
                'cluster_id': cluster_id,
                'members': list(members),
                'size': len(members)
            })
        
        return clusters

    async def _stage_cluster(self, clusters, matches):
        """Stage 8: Build identity graph from cluster results."""
        start = datetime.utcnow()
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CLUSTER,
            status="running",
            message="Building identity graph and updating ClusterManager..."
        ))
        
        loop = asyncio.get_event_loop()
        run_id = self.run_id
        
        def build_cluster_graph():
            try:
                from engine.clustering.cluster_manager import get_cluster_manager
                
                # Collect unique customer codes from matches
                customer_codes_in_clusters = set()
                for m in matches:
                    customer_codes_in_clusters.add(str(m['id1']))
                    customer_codes_in_clusters.add(str(m['id2']))
                
                logger.info(f"Building cluster graph for {len(customer_codes_in_clusters):,} entities")
                
                manager = get_cluster_manager()
                
                # Reset manager state
                from engine.clustering.union_find import UnionFind
                manager._uf = UnionFind()
                manager._cluster_ids = {}
                manager._members = []
                
                # Link matched pairs
                linked = 0
                for m in matches:
                    c1 = str(m['id1'])
                    c2 = str(m['id2'])
                    if c1 != c2:
                        manager.link(c1, c2)
                        linked += 1
                
                logger.info(f"Linked {linked:,} pairs into cluster graph")
                
                # Save cluster snapshot
                if run_id:
                    os.makedirs("data/runs", exist_ok=True)
                    manager.save_snapshot(f"data/runs/{run_id}_clusters.json")
                    
                    # Load and save cluster member records
                    member_records = self._load_records_for_clusters(customer_codes_in_clusters)
                    with open(f"data/runs/{run_id}_records.json", "w") as f:
                        json.dump(member_records, f, default=str)
                    logger.info(f"Saved {len(member_records):,} cluster-member records")
                
                return {
                    'clusters_created': len(clusters),
                    'entities_created': len(customer_codes_in_clusters),
                    'duplicate_relationships': linked
                }
            except Exception as e:
                logger.error(f"Cluster graph build failed: {e}", exc_info=True)
                # Create empty records file on failure
                if run_id:
                    with open(f"data/runs/{run_id}_records.json", "w") as f:
                        json.dump({}, f)
                return {'clusters_created': 0}
            
        stats = await loop.run_in_executor(None, build_cluster_graph)
        
        duration = int((datetime.utcnow() - start).total_seconds() * 1000)
        await self._emit_progress(StageProgress(
            stage=PipelineStage.CLUSTER,
            status="complete",
            records_in=len(matches),
            records_out=stats.get('clusters_created', 0),
            duration_ms=duration,
            message=f"Identity graph complete: {stats.get('clusters_created', 0):,} clusters with {stats.get('entities_created', 0):,} entities",
            data={"cluster_stats": stats}
        ))

    def _load_records_for_clusters(self, customer_codes: set) -> Dict[str, dict]:
        """Load ONLY records for customer codes in clusters (not all 1.5M rows)."""
        if not customer_codes:
            return {}
        
        try:
            import pandas as pd
            import numpy as np
            
            NEEDED_COLS = [
                "CUSTOMER_CODE", "NAME", "EMAIL", "MOBILE", "TELEPHONE",
                "FULL_ADDRESS", "BIRTH_DATE", "DOCUMENT"
            ]
            
            # Read only needed columns
            df = pd.read_parquet(PARQUET_PATH, columns=NEEDED_COLS, engine='pyarrow')
            
            # Filter to cluster members
            codes_str = {str(c) for c in customer_codes}
            df['CUSTOMER_CODE'] = df['CUSTOMER_CODE'].astype(str)
            df = df[df['CUSTOMER_CODE'].isin(codes_str)]
            
            records = {}
            for _, row in df.iterrows():
                ckey = str(row['CUSTOMER_CODE'])
                
                def safe_get(col):
                    val = row.get(col)
                    if isinstance(val, (list, dict, bytes)):
                        return ""
                    if isinstance(val, (np.ndarray, np.generic)):
                         if hasattr(val, 'size') and val.size > 0:
                             val = val.flat[0]
                         else:
                             return ""
                    if pd.isna(val):
                        return ""
                    return str(val).strip()

                name = safe_get('NAME')
                email = safe_get('EMAIL')
                mobile = safe_get('MOBILE')
                phone = safe_get('TELEPHONE')
                address = safe_get('FULL_ADDRESS')
                dob = safe_get('BIRTH_DATE')
                doc = safe_get('DOCUMENT')
                
                records[ckey] = {
                    "customer_key": ckey,
                    "source_customer_id": ckey,
                    "name": name,
                    "name_norm": name.upper() if name else "",
                    "email": email,
                    "email_norm": email.lower() if email else "",
                    "phone": mobile or phone,
                    "phone_norm": "".join(filter(str.isdigit, mobile or phone or "")),
                    "dob": dob,
                    "address": address,
                    "natid": doc,
                    "status": "ACT",
                }
            
            logger.info(f"Loaded {len(records):,} cluster-member records")
            return records
            
        except Exception as e:
            logger.error(f"Failed to load cluster records: {e}", exc_info=True)
            return {}

    async def run(
        self,
        run_id: str,
        raw_records: list = None,
        mode: str = "FULL"
    ) -> PipelineResult:
        """Execute the complete Spark-based ER pipeline."""
        result = PipelineResult(
            run_id=run_id,
            success=False,
            mode=mode,
            stages=[],
            started_at=datetime.utcnow()
        )
        
        try:
            # Stage 1: Ingest - Load data into Spark
            record_count = await self._stage_ingest()
            result.records_in = record_count
            
            # Stage 2: Normalize - Prepare data
            await self._stage_normalize(record_count)
            result.records_normalized = record_count
            
            # Stage 3: Block - Generate candidate pairs
            candidate_count = await self._stage_block()
            result.blocks_created = candidate_count
            
            # Stage 4: Candidates - Already generated
            await self._stage_candidates(candidate_count)
            result.candidates_generated = candidate_count
            
            # Stage 5 & 6: Score and Decide
            clusters, matches = await self._stage_score_and_decide(candidate_count)
            result.pairs_scored = len(matches)
            result.auto_links = len(matches)
            result.review_items = 0
            result.rejected = 0
            
            # Stage 7: Explain (skipped in Spark pipeline - all auto-linked)
            
            # Stage 8: Cluster - Build identity graph
            await self._stage_cluster(clusters, matches)
            
            result.success = True
            result.ended_at = datetime.utcnow()
            
            await self._emit_progress(StageProgress(
                stage=PipelineStage.COMPLETE,
                status="complete",
                message=f"Pipeline complete: {len(clusters):,} identity clusters resolved from {record_count:,} records"
            ))
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            result.success = False
            result.error_message = str(e)
            result.ended_at = datetime.utcnow()
            
            await self._emit_progress(StageProgress(
                stage=PipelineStage.FAILED,
                status="error",
                message=str(e)
            ))
        
        finally:
            # Clean up Spark session
            if self._spark:
                try:
                    from engine.spark_er.splink_commands import stop_spark_gracefully
                    stop_spark_gracefully(self._spark)
                except Exception as e:
                    logger.warning(f"Spark cleanup error (non-critical): {e}")
            
        return result
