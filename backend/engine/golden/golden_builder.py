"""
CUIN v2 - Golden Record Builder

Merges member records into a single golden record.
"""

from typing import List, Dict, Optional
from datetime import datetime
from engine.structures import GoldenRecord

class GoldenBuilder:
    """
    Builder for golden records.
    Uses a strategy to merge fields from multiple source records.
    """
    
    def generate_golden_record(
        self,
        cluster_id: str,
        records: List[dict],
        version: int,
        created_by: str = "SYSTEM"
    ) -> GoldenRecord:
        """
        Generate a golden record by merging member records.
        
        Uses a simple "most recent non-null" merge strategy.
        """
        merged = {}
        
        # Fields to merge
        fields = [
            'name_norm', 'phone_norm', 'email_norm',
            'dob_norm', 'address_norm', 'city_norm', 'natid_norm'
        ]
        
        # Merge strategy: iterate records in reverse (assuming input is sorted by time?) 
        # Actually logic depends on caller providing sorted records or trusting implicit order.
        # Original code reversed the list, assuming last is newest.
        
        for field_name in fields:
            # Take most recent non-null value
            for record in reversed(records):
                value = record.get(field_name)
                if value:
                    merged[field_name] = value
                    break
        
        # Add member count
        merged['member_count'] = len(records)
        merged['source_ids'] = [r.get('source_customer_id') for r in records if r.get('source_customer_id')]
        
        return GoldenRecord(
            cluster_id=cluster_id,
            version=version,
            payload=merged,
            created_at=datetime.utcnow(),
            created_by=created_by
        )
