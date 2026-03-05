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
                # Progress callback to emit WS events
                async def progress_callback(progress):
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
                            'candidates_generated': result.candidates_generated
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
        
        return {
            "message": "Spark Pipeline started successfully",
            "run_id": run.run_id
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start pipeline: {str(e)}")
