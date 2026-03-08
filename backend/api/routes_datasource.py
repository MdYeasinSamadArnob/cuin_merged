from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel

from services.run_service import get_run_service, RunStatus
from pipeline.spark_orchestrator import SparkPipelineOrchestrator
from api.ws_events import ws_manager, EventType
from datetime import datetime

router = APIRouter()

class DatasourceStartRequest(BaseModel):
    mode: str = "FULL"

@router.post("/demo")
async def start_datasource_demo(
    background_tasks: BackgroundTasks,
    request: DatasourceStartRequest
):
    """
    Trigger the Spark-based realtime ingestion and clustering pipeline.
    Reads from backend/data_source/oracle_data.parquet.
    Returns the full Run object so the frontend can track it by run_id.
    """
    run_service = get_run_service()
    
    try:
        run = run_service.create_run(
            mode=request.mode,
            description="Datasource Spark Demo",
            policy_version=1
        )
        
        async def execute_spark_pipeline():
            try:
                # Progress callback to emit WS events AND persist current_stage to the run
                async def progress_callback(progress):
                    # Persist current_stage (and live counters) so API polling always reflects reality
                    live_run = run_service.get_run(run.run_id)
                    if live_run:
                        live_run.current_stage = progress.stage.value
                        if progress.stage.value == 'ingest' and progress.records_out:
                            live_run.counters.records_in = progress.records_out
                        if progress.stage.value == 'candidates' and progress.records_out:
                            live_run.counters.candidates_generated = progress.records_out
                        if progress.stage.value == 'score' and progress.records_out:
                            live_run.counters.auto_links = progress.records_out
                        if progress.stage.value == 'decide' and progress.records_out:
                            live_run.counters.auto_links = progress.records_out
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
                
                orchestrator = SparkPipelineOrchestrator(
                    progress_callback=progress_callback,
                    run_id=run.run_id
                )
                
                # Mark run as running
                run_obj = run_service.get_run(run.run_id)
                if run_obj:
                    run_obj.status = RunStatus.RUNNING
                    run_service._save_runs()
                await ws_manager.broadcast(EventType.RUN_STARTED, {
                    'run_id': run.run_id,
                    'mode': run.mode.value
                })
                
                # Execute Pipeline
                result = await orchestrator.run(run.run_id, mode=request.mode)
                
                # Update run in DB
                run_obj = run_service.get_run(run.run_id)
                if run_obj:
                    if result.success:
                        run_obj.status = RunStatus.COMPLETED
                        run_obj.counters.records_in = result.records_in
                        run_obj.counters.auto_links = result.auto_links
                        run_obj.counters.review_items = result.review_items
                        run_obj.counters.candidates_generated = result.candidates_generated
                    else:
                        run_obj.status = RunStatus.FAILED
                        run_obj.error_message = result.error_message
                    
                    run_obj.ended_at = datetime.utcnow()
                    run_obj.duration_seconds = (run_obj.ended_at - run_obj.started_at).total_seconds()
                    run_service._save_runs()
                
                # Broadcast completion
                if run_obj and result.success:
                    await ws_manager.broadcast_run_complete(
                        run_id=run.run_id,
                        success=True,
                        counters={
                            'records_in': result.records_in,
                            'auto_links': result.auto_links,
                            'review_items': result.review_items,
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
                print(f"Spark Pipeline error for run {run.run_id}: {e}")
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

        background_tasks.add_task(execute_spark_pipeline)
        
        # Return the full run dict so the frontend can track it by run_id
        return run.to_dict()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start pipeline: {str(e)}")
