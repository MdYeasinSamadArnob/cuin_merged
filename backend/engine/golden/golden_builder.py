"""
CUIN v2 - Golden Record Builder (Ruleset v2)

Merges member records into a single golden record.
"""

from typing import List, Dict, Optional
from datetime import datetime
from engine.structures import GoldenRecord

_MERGE_FIELDS = [
    'name_norm', 'phone_norm', 'email_norm',
    'dob_norm', 'address_norm', 'city_norm', 'natid_norm'
]


def _completeness(record: dict) -> int:
    """Number of non-empty merge fields -- more complete records win ties."""
    return sum(1 for f in _MERGE_FIELDS if record.get(f))


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

        Deterministic "most complete, then lowest source_customer_id"
        merge order -- NOT `reversed(records)`. The old code assumed
        the caller passed records sorted by recency, but callers build
        this list from an unordered set (e.g. UnionFind component
        members), so "most recent" was really "whatever order happened
        to come out of a set() this run" -- a real source of
        non-reproducible golden records across identical runs.
        """
        merged = {}

        ordered = sorted(
            records,
            key=lambda r: (-_completeness(r), str(r.get('source_customer_id') or '')),
        )

        for field_name in _MERGE_FIELDS:
            for record in ordered:
                value = record.get(field_name)
                if value:
                    merged[field_name] = value
                    break

        merged['member_count'] = len(records)
        merged['source_ids'] = sorted(
            r.get('source_customer_id') for r in records if r.get('source_customer_id')
        )

        return GoldenRecord(
            cluster_id=cluster_id,
            version=version,
            payload=merged,
            created_at=datetime.utcnow(),
            created_by=created_by
        )
