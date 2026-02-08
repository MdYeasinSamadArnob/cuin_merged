"""
Read Staging Module.
Reads raw data from CSV or Database staging area.
"""

import pandas as pd
import os
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

# Default path as per USER specification
DEFAULT_CSV_PATH = "/mnt/data/challenging_er_200.csv"
# Fallback local path if user uploaded it to project root
LOCAL_CSV_PATH = "challenging_er_200.csv"

def read_staging_data(file_path: str = None) -> List[Dict]:
    """
    Read data from staging CSV.
    Tries configured path, then default paths.
    """
    paths_to_try = []
    if file_path:
        paths_to_try.append(file_path)
    paths_to_try.append(DEFAULT_CSV_PATH)
    paths_to_try.append(LOCAL_CSV_PATH)
    paths_to_try.append(os.path.join(os.getcwd(), LOCAL_CSV_PATH))
    
    selected_path = None
    for p in paths_to_try:
        if os.path.exists(p):
            selected_path = p
            break
            
    if not selected_path:
        logger.warning("Staging CSV not found in any standard location.")
        return []
    
    try:
        logger.info(f"Reading staging data from {selected_path}")
        df = pd.read_csv(selected_path)
        # Convert to list of dicts, handling NaNs
        records = df.where(pd.notnull(df), None).to_dict(orient='records')
        logger.info(f"Loaded {len(records)} records from {selected_path}")
        return records
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        return []

def get_staging_stats() -> dict:
    """Get statistics about staging data."""
    return {
        "new_records": 0,
        "total_records": 0
    }
