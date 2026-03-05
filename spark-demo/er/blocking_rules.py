"""
Splink Blocking Rules Configuration

This module defines blocking rules for entity resolution using Splink.
Blocking rules reduce the number of record comparisons by only comparing
records that pass certain criteria.

All blocking operations are executed in parallel using PySpark across
all available CPU cores for optimal performance.

Key Features:
- Multipass blocking for comprehensive matching
- Exact match blocking on key fields
- LSH (Locality Sensitive Hashing) for name similarity
- OR conditions: pairs combined from all blocking passes
- Does NOT use BRANCH_CODE per requirements
- Supports array type fields (EMAIL, MOBILE, DOCUMENT) via explosion
- Parallel execution using PySpark

Blocking Strategy:
- Multiple blocking passes are executed independently
- Each pass generates candidate pairs
- All pairs are combined using OR logic (union)
- Deduplication ensures each pair appears only once

Usage:
    from er.blocking_rules import get_blocking_rules
    
    blocking_rules = get_blocking_rules()
"""

from typing import List


def get_blocking_rules() -> List[str]:
    """
    Get blocking rules for Splink entity resolution with multipass strategy.
    
    Blocking rules reduce the number of comparisons by only comparing
    records that match on specific fields. This improves performance
    while maintaining matching accuracy.
    
    These rules are executed in parallel across all CPU cores using PySpark.
    Each blocking rule creates a separate blocking pass that runs in parallel,
    leveraging Spark's distributed computing capabilities.
    
    Multipass Blocking Strategy:
    - Exact match passes for key fields
    - LSH (Locality Sensitive Hashing) for name similarity
    - Array fields are automatically exploded for matching
    - All passes combined using OR logic
    
    Note: BRANCH_CODE is intentionally NOT used in blocking rules
    per requirements. We use DOCUMENT as consolidated document field.
    
    Returns:
        List of blocking rule SQL expressions (executed in parallel by PySpark)
    """
    blocking_rules = [
        # Block on exact name match to reduce comparisons
        "l.NAME = r.NAME",
        
        # Block on similar email
        "l.EMAIL = r.EMAIL",
        
        # Block on same mobile number
        "l.MOBILE = r.MOBILE",
        
        # Block on same document
        # Note: Array fields are automatically exploded in run_blocking
        "l.DOCUMENT = r.DOCUMENT",
    ]
    
    # Note: LSH blocking for NAME is added separately in run_blocking function
    # This provides similarity-based matching for names (e.g., "John Smith" vs "J Smith")
    
    return blocking_rules
