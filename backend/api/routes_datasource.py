"""
CUIN v2 Control Plane - Data Source API Routes

Endpoints for ingesting data from pre-defined data sources (e.g. Oracle Parquet).
"""

import os
import pandas as pd
from fastapi import APIRouter, HTTPException, BackgroundTasks
from uuid import uuid4

from services.run_service import get_run_service
from api.ws_events import ws_manager, EventType

router = APIRouter()

PARQUET_PATH = "/home/arnob/Projects/cuin/cuin-controlplane/backend/data_source/oracle_data.parquet"

@router.post("/ingest")
async def ingest_datasource(
    background_tasks: BackgroundTasks,
    run_mode: str = "AUTO"
):
    """
    Trigger ingestion from the Oracle Parquet data source.
    """
    if not os.path.exists(PARQUET_PATH):
        raise HTTPException(status_code=404, detail=f"Data source file not found at {PARQUET_PATH}")
    
    try:
        # Read parquet
        df = pd.read_parquet(PARQUET_PATH)
        
        if df.empty:
            raise HTTPException(status_code=400, detail="The data source is empty.")
            
        # Convert NaN to None for JSON compatibility
        records = df.where(pd.notnull(df), None).to_dict(orient='records')
        
        mapped_records = []
        for r in records:
            new_record = {}
            
            # Explicit mapping for Oracle Data Source
            new_record['branch'] = str(r.get('BRANCH_CODE')) if r.get('BRANCH_CODE') is not None else None
            new_record['source_customer_id'] = str(r.get('CUSTOMER_CODE')) if r.get('CUSTOMER_CODE') is not None else None
            new_record['name'] = str(r.get('NAME')) if r.get('NAME') is not None else None
            new_record['sponsor'] = str(r.get('SPONSOR_NAME')) if r.get('SPONSOR_NAME') is not None else None
            new_record['dob'] = r.get('BIRTH_DATE')
            new_record['email'] = str(r.get('EMAIL')) if r.get('EMAIL') is not None else None
            new_record['address'] = str(r.get('FULL_ADDRESS')) if r.get('FULL_ADDRESS') is not None else None
            
            # Phone logic: priority to MOBILE, fallback to TELEPHONE
            mobile = r.get('MOBILE')
            telephone = r.get('TELEPHONE')
            
            mob_str = str(mobile).strip() if mobile is not None else ""
            tel_str = str(telephone).strip() if telephone is not None else ""
            
            if mob_str and mob_str.lower() != 'none':
                new_record['phone'] = mob_str
            elif tel_str and tel_str.lower() != 'none':
                new_record['phone'] = tel_str
            
            # National ID mapping from DOCUMENT field
            doc = r.get('DOCUMENT')
            if doc is not None:
                # Handle list or array-like
                if hasattr(doc, '__iter__') and not isinstance(doc, str):
                    for d in doc:
                        d_str = str(d)
                        if d_str.startswith('NAI:'):
                            new_record['natid'] = d_str.replace('NAI:', '')
                            break
                else:
                    doc_str = str(doc)
                    if doc_str.startswith('NAI:'):
                        new_record['natid'] = doc_str.replace('NAI:', '')
                    elif doc_str.startswith('[') and 'NAI:' in doc_str:
                        import re
                        match = re.search(r'NAI:([^,\]\s]+)', doc_str)
                        if match:
                            new_record['natid'] = match.group(1)

            # Pass through other fields as metadata
            for k, v in r.items():
                k_lower = k.lower()
                if k_lower not in ['branch_code', 'customer_code', 'name', 'sponsor_name', 'birth_date', 'email', 'full_address', 'telephone', 'mobile', 'document']:
                    new_record[k_lower] = v
            
            # Ensure name exists (mimicking routes_upload.py safety)
            if not new_record.get('name'):
                 continue
                 
            mapped_records.append(new_record)
            
        # Trigger Pipeline
        run_service = get_run_service()
        
        # Create run
        mode_enum = "AUTO"
        if run_mode.upper() == "FULL":
            mode_enum = "FULL"
        elif run_mode.upper() == "DELTA":
            mode_enum = "DELTA"
            
        run = run_service.create_run(
            mode=mode_enum,
            description="Oracle Data Source Ingestion",
            policy_version=1
        )
        
        # Execute in background
        async def execute_pipeline():
            try:
                await run_service.execute_run(run.run_id, mapped_records)
                
                # Broadcast completion
                updated_run = run_service.get_run(run.run_id)
                if updated_run:
                    await ws_manager.broadcast(EventType.RUN_COMPLETE, {
                        'run_id': updated_run.run_id,
                        'status': updated_run.status.value,
                        'counters': {
                            'records_in': updated_run.counters.records_in,
                            'auto_links': updated_run.counters.auto_links,
                            'review_items': updated_run.counters.review_items,
                        }
                    })
            except Exception as e:
                print(f"Pipeline error for run {run.run_id}: {e}")
        
        background_tasks.add_task(execute_pipeline)
        
        return {
            "message": "Data source ingestion started successfully",
            "run_id": run.run_id,
            "records_count": len(mapped_records)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing data source: {str(e)}")
