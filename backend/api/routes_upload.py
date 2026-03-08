"""
CUIN v2 Control Plane - Upload API Routes

Endpoints for uploading data files (Excel, CSV).
"""

from typing import Optional, List
import io
import pandas as pd
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from uuid import uuid4

from services.run_service import get_run_service, RunMode
from pipeline.orchestrator import PipelineOrchestrator, PipelineResult
from api.ws_events import ws_manager, EventType

router = APIRouter()

@router.post("/file")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    run_mode: str = "AUTO"
):
    """
    Upload a data file (Excel or CSV) and trigger the matching pipeline.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is missing.")
        
    ext = file.filename.lower().split('.')[-1]
    if ext not in ['xls', 'xlsx', 'csv']:
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel (.xls, .xlsx) or CSV (.csv) file.")
    
    try:
        content = await file.read()
        
        if ext == 'csv':
            # Try parsing with default settings, then fallback if needed
            try:
                df = pd.read_csv(io.BytesIO(content))
            except UnicodeDecodeError:
                # Try latin1 if utf-8 fails
                df = pd.read_csv(io.BytesIO(content), encoding='latin1')
        else:
            df = pd.read_excel(io.BytesIO(content))
        
        # Basic validation: ensure we have some data
        if df.empty:
            raise HTTPException(status_code=400, detail="The uploaded file is empty.")
            
        # Convert NaN to None for JSON compatibility
        records = df.where(pd.notnull(df), None).to_dict(orient='records')
        
        # Map common headers to internal schema
        # This is a robust heuristic mapping with fallbacks
        def is_empty(val):
            """Check if a value is effectively empty."""
            if val is None:
                return True
            if isinstance(val, str):
                return val.strip() == '' or val.strip().upper() in ['N/A', 'NA', 'NULL', 'NONE', '-', '—']
            return False

        mapped_records = []
        for r in records:
            new_record = {}
            
            # Helper to safely get values from specific known columns if heuristic fails
            # or to combine them (e.g. First + Last Name)
            
            # 1. Name Strategy
            # Try to find specific name parts first
            fname = None
            lname = None
            full_name = None
            
            # First pass: Look for exact/strong matches
            for k, v in r.items():
                if k is None: continue
                k_norm = str(k).lower().strip().replace('_', '').replace(' ', '')
                
                # First Name
                if k_norm in ['cusnmf', 'firstname', 'fname', 'givenname', 'mn', 'first', 'firstnm']:
                     fname = v
                # Last Name
                elif k_norm in ['cusnml', 'lastname', 'lname', 'surname', 'familyname', 'last', 'lastnm', 'familynm']:
                     lname = v
                # Full Name
                elif k_norm in ['name', 'fullname', 'cusname', 'customername', 'entity', 'company', 'organization', 'entityname', 'partyname', 'clientname', 'companyname']:
                     full_name = v
            
            # Second pass: Fuzzy matches if we still lack data
            if is_empty(fname) and is_empty(lname) and is_empty(full_name):
                for k, v in r.items():
                    if k is None: continue
                    k_norm = str(k).lower().strip().replace('_', '').replace(' ', '')
                    
                    if 'name' in k_norm and not any(x in k_norm for x in ['file', 'date', 'user', 'branch']):
                        # Use as full name if it seems like a name field
                        full_name = v
                        break

            # Smart name handling: Check if fname contains a full name (has spaces) and lname is empty
            if not is_empty(fname) and is_empty(lname):
                fname_str = str(fname).strip()
                # If fname contains spaces, it's likely a full name
                if ' ' in fname_str:
                    new_record['name'] = fname_str
                elif fname_str:
                    new_record['name'] = fname_str
            elif not is_empty(fname) or not is_empty(lname):
                # Combine first and last name
                f_part = str(fname).strip() if not is_empty(fname) else ""
                l_part = str(lname).strip() if not is_empty(lname) else ""
                combined = f"{f_part} {l_part}".strip()
                if combined:
                    new_record['name'] = combined
            
            # Use full_name if we still don't have a name
            if not new_record.get('name') and not is_empty(full_name):
                new_record['name'] = str(full_name).strip()
                
            # Fallback: If still no name, try to use a "Description" or "Label" field
            if not new_record.get('name'):
                 for k, v in r.items():
                    if k is None or is_empty(v): continue
                    k_norm = str(k).lower().strip().replace('_', '').replace(' ', '')
                    if k_norm in ['description', 'desc', 'label', 'title', 'remarks', 'notes']:
                        new_record['name'] = str(v).strip()
                        break
            
            # Final Fallback: Log warning but don't crash
            if not new_record.get('name'):
                # Try to use Email or Phone as name proxy if available (better than nothing)
                pass
                
            # 2. Iterate for other fields
            for k, v in r.items():
                if k is None: continue
                k_clean = str(k).lower().strip().replace(' ', '_')
                k_norm = k_clean.replace('_', '') # For stricter matching
                
                # Skip if we already handled name and this is a name field
                if k_norm in ['cusnmf', 'cusnml', 'firstname', 'lastname', 'fname', 'lname', 'givenname', 'surname', 'name', 'fullname']:
                    continue

                # Phone/Mobile
                elif any(x in k_norm for x in ['phone', 'mobile', 'teleno', 'moblno', 'cell', 'ph_', 'telxno']):
                    # Prioritize Mobile if available
                    if 'mob' in k_norm: 
                         new_record['phone'] = v # Overwrite/Set mobile
                    elif not new_record.get('phone'):
                         new_record['phone'] = v # Set if empty
                
                # Email
                elif any(x in k_norm for x in ['email', 'mail', 'maili', 'mailid']):
                    new_record['email'] = v
                    
                # DOB
                elif any(x in k_norm for x in ['dob', 'birth', 'cusdob', 'dateofbirth']):
                    new_record['dob'] = v
                    
                # Address (concatenate parts if needed, or take best)
                elif any(x in k_norm for x in ['addr', 'adrs', 'street', 'addrs1', 'addrs2', 'addrs3', 'addrs4']):
                    # Check for parts
                    current_addr = new_record.get('address', '')
                    # If it's a primary address field or we haven't set it
                    if '1' in k_norm or 'addrs1' in k_norm:
                        if v: new_record['address'] = str(v) + (f" {current_addr}" if current_addr else "")
                    # Append secondary parts
                    elif any(s in k_norm for s in ['2', '3', '4']):
                        if v and current_addr:
                             new_record['address'] = f"{current_addr} {v}".strip()
                        elif v:
                             new_record['address'] = str(v)
                    elif not current_addr:
                        new_record['address'] = v
                        
                # City
                elif any(x in k_norm for x in ['city', 'town', 'citynm']):
                    new_record['city'] = v
                    
                # National ID
                elif any(x in k_norm for x in ['natid', 'ssid', 'nationalid', 'idnum', 'nid', 'natlid']):
                    new_record['natid'] = v
                    
                # Customer ID
                elif any(x in k_norm for x in ['custid', 'cuscod', 'customerid', 'accountno', 'uscod']):
                    new_record['source_customer_id'] = v
                
                # Metadata Fields (Pass through)
                elif k_norm in ['custyp']: new_record['cust_type'] = v
                elif k_norm in ['cussts']: new_record['status'] = v
                elif k_norm in ['gender']: new_record['gender'] = v
                elif k_norm in ['oprbra']: new_record['branch'] = v
                elif k_norm in ['sponam']: new_record['sponsor'] = v
                elif k_norm in ['timstamp']: new_record['timestamp'] = v
                
                else:
                    new_record[k_clean] = v
            
            # 3. Post-Processing Fallbacks
            if not new_record.get('name'):
                if new_record.get('email'):
                    new_record['name'] = new_record['email'].split('@')[0]
                elif new_record.get('phone'):
                    new_record['name'] = f"Phone: {new_record['phone']}"
                elif new_record.get('source_customer_id'):
                    new_record['name'] = f"ID: {new_record['source_customer_id']}"
                elif new_record.get('natid'):
                    new_record['name'] = f"NID: {new_record['natid']}"
                else:
                    # Last resort: try to find ANY string value
                    for val in new_record.values():
                        if isinstance(val, str) and len(val) > 2:
                            new_record['name'] = val
                            break
                            
            if not new_record.get('name'):
                 print(f"Warning: Record dropped due to missing name and no fallback found: {new_record}")
                 continue # Skip this record if we absolutely can't identify it

            mapped_records.append(new_record)
            
        # Trigger Pipeline
        run_service = get_run_service()
        try:
            # Create run first
            mode_enum = "AUTO"
            if run_mode.upper() == "FULL":
                mode_enum = "FULL"
            elif run_mode.upper() == "DELTA":
                mode_enum = "DELTA"
                
            run = run_service.create_run(
                mode=mode_enum,
                description=f"File Upload: {file.filename}",
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
                "message": "File processed and pipeline started successfully",
                "run_id": run.run_id,
                "records_count": len(mapped_records)
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to start pipeline: {str(e)}")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")
