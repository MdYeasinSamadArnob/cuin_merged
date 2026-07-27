from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from services.run_service import get_run_service, RunStatus
from pipeline.duckdb_orchestrator import DuckDBPipelineOrchestrator
from api.ws_events import ws_manager, EventType
from datetime import datetime

router = APIRouter()

class DatasourceStartRequest(BaseModel):
    mode: str = "FULL"
    # "duckdb" is the deterministic Ruleset v2 engine and the default.
    # "spark" is kept available for comparison/rollback but is no longer
    # the default -- it retrains Splink's m/u probabilities on every run
    # with no fixed seed, so identical input can produce different
    # clusters between runs (see engine.ruleset for the replacement).
    engine: str = "duckdb"

@router.post("/demo")
async def start_datasource_demo(
    background_tasks: BackgroundTasks,
    request: DatasourceStartRequest
):
    """
    Trigger the deterministic ingestion and clustering pipeline.
    Reads from backend/data_source/oracle_data.parquet.
    Returns the full Run object so the frontend can track it by run_id.
    """
    run_service = get_run_service()

    try:
        run = run_service.create_run(
            mode=request.mode,
            description=f"Datasource Demo ({request.engine})",
            policy_version=1
        )

        async def execute_pipeline():
            try:
                async def progress_callback(progress):
                    # Persist current_stage (and live counters) so API polling always reflects reality
                    live_run = run_service.get_run(run.run_id)
                    if live_run:
                        live_run.current_stage = progress.stage.value
                        if progress.stage.value == 'ingest' and progress.records_out:
                            live_run.counters.records_in = progress.records_out
                        if progress.stage.value == 'candidates' and progress.records_out:
                            live_run.counters.candidates_generated = progress.records_out
                        if progress.stage.value == 'cluster' and progress.status == 'complete':
                            cc = (progress.data or {}).get('cluster_stats', {}).get('clusters_created', 0)
                            if cc:
                                live_run.counters.clusters_created = cc
                        run_service._save_runs()

                    await ws_manager.broadcast_stage_progress(
                        run_id=run.run_id,
                        stage=progress.stage.value,
                        status=progress.status,
                        message=progress.message,
                        records_in=progress.records_in,
                        records_out=progress.records_out,
                        reduction_pct=progress.reduction_pct,
                        duration_ms=progress.duration_ms,
                        data=progress.data
                    )

                if request.engine == "duckdb":
                    orchestrator_cls = DuckDBPipelineOrchestrator
                else:
                    # Imported lazily, not at module load time: pyspark/
                    # splink are excluded from the Docker image's dependency
                    # set (requirements-docker.txt) since only this legacy/
                    # rollback path needs them -- the app must still boot
                    # fine when they aren't installed, and only requesting
                    # engine=spark should fail, with a clear error, not the
                    # whole API refusing to start.
                    from pipeline.spark_orchestrator import SparkPipelineOrchestrator
                    orchestrator_cls = SparkPipelineOrchestrator
                orchestrator = orchestrator_cls(
                    progress_callback=progress_callback,
                    run_id=run.run_id
                )

                # Register the orchestrator so /matches/run/{id}/* (get_scores,
                # get_decisions, get_auto_links, get_uniques, get_result_clusters)
                # can find it -- the Spark path never did this, which is why
                # those endpoints always returned empty for datasource runs.
                run_service._orchestrators[run.run_id] = orchestrator

                run_obj = run_service.get_run(run.run_id)
                if run_obj:
                    run_obj.status = RunStatus.RUNNING
                    run_service._save_runs()
                await ws_manager.broadcast(EventType.RUN_STARTED, {
                    'run_id': run.run_id,
                    'mode': run.mode.value
                })

                result = await orchestrator.run(run.run_id, mode=request.mode)

                run_obj = run_service.get_run(run.run_id)
                if run_obj:
                    if result.success:
                        run_obj.status = RunStatus.COMPLETED
                        run_obj.counters.records_in = result.records_in
                        run_obj.counters.records_normalized = result.records_normalized
                        run_obj.counters.blocks_created = result.blocks_created
                        run_obj.counters.candidates_generated = result.candidates_generated
                        run_obj.counters.pairs_scored = result.pairs_scored
                        run_obj.counters.auto_links = result.auto_links
                        run_obj.counters.review_items = result.review_items
                        run_obj.counters.rejected = result.rejected

                        if hasattr(orchestrator, "get_fingerprints"):
                            fps = orchestrator.get_fingerprints()
                            run_obj.ruleset_version = fps.get("ruleset_version")
                            run_obj.output_fingerprint = fps.get("output_fingerprint")
                    else:
                        run_obj.status = RunStatus.FAILED
                        run_obj.error_message = result.error_message

                    run_obj.ended_at = datetime.utcnow()
                    run_obj.duration_seconds = (run_obj.ended_at - run_obj.started_at).total_seconds()
                    run_service._save_runs()

                if run_obj and result.success:
                    await ws_manager.broadcast_run_complete(
                        run_id=run.run_id,
                        success=True,
                        counters={
                            'records_in': result.records_in,
                            'auto_links': result.auto_links,
                            'review_items': result.review_items,
                            'rejected': result.rejected,
                            'candidates_generated': result.candidates_generated,
                            'clusters_created': run_obj.counters.clusters_created,
                        }
                    )
                else:
                    await ws_manager.broadcast(EventType.RUN_FAILED, {
                        'run_id': run.run_id,
                        'error': result.error_message or "Unknown error"
                    })

            except Exception as e:
                print(f"Pipeline error for run {run.run_id}: {e}")
                run_obj = run_service.get_run(run.run_id)
                if run_obj:
                    run_obj.status = RunStatus.FAILED
                    run_obj.error_message = str(e)
                    run_obj.ended_at = datetime.utcnow()
                    run_obj.duration_seconds = (run_obj.ended_at - run_obj.started_at).total_seconds()
                    run_service._save_runs()
                await ws_manager.broadcast(EventType.RUN_FAILED, {
                    'run_id': run.run_id,
                    'error': str(e)
                })

        background_tasks.add_task(execute_pipeline)

        return run.to_dict()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start pipeline: {str(e)}")
