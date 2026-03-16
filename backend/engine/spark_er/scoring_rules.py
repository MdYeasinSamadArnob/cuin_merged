"""
Splink Scoring Rules Configuration

This module defines comparison/scoring rules for entity resolution using Splink.
These rules determine how record fields are compared and scored to calculate
match probabilities.

Key Features:
- Multiple comparison methods (exact match, fuzzy matching)
- Levenshtein distance for text similarity
- Does NOT use BRANCH_CODE per requirements
- Uses DOCUMENT_CODE and DOCUMENT_NUMBER for document matching

Usage:
    from engine.spark_er.scoring_rules import get_comparison_rules
    
    comparisons = get_comparison_rules(cl)
"""

from typing import List, Any

try:
    from splink.spark.spark_comparison_library import SparkComparisonLibrary as cl
except ImportError:
    # Will be imported when actually used
    cl = None


def get_comparison_rules(comparison_library: Any = None) -> List[Any]:
    """
    Get comparison rules for Splink entity resolution.
    
    These rules define how different fields are compared to calculate
    match probabilities. Each comparison contributes to the overall
    match weight.
    
    Note: BRANCH_CODE is intentionally NOT used in comparisons
    per requirements. We use DOCUMENT_CODE and DOCUMENT_NUMBER
    for document-based matching.
    
    Args:
        comparison_library: Splink comparison library (e.g., SparkComparisonLibrary)
                           If None, uses the default imported cl
    
    Returns:
        List of comparison configurations
    """
    if comparison_library is None:
        if cl is None:
            raise ImportError(
                "Splink must be installed. Run: pip install splink"
            )
        comparison_library = cl
    
    comparisons = [
        # Compare names using multiple similarity thresholds
        comparison_library.levenshtein_at_thresholds("NAME", [0.9, 0.8, 0.7]),
        
        {
            "output_column_name": "EMAIL",
            "comparison_levels": [
                {"sql_condition": "EMAIL_l IS NULL OR EMAIL_r IS NULL", "label_for_charts": "Null", "is_null_level": True},
                {"sql_condition": "arrays_overlap(EMAIL_l, EMAIL_r)", "label_for_charts": "Intersection"},
                {"sql_condition": "ELSE", "label_for_charts": "No Intersection"}
            ]
        },
        {
            "output_column_name": "MOBILE",
            "comparison_levels": [
                {"sql_condition": "MOBILE_l IS NULL OR MOBILE_r IS NULL", "label_for_charts": "Null", "is_null_level": True},
                {"sql_condition": "arrays_overlap(MOBILE_l, MOBILE_r)", "label_for_charts": "Intersection"},
                {"sql_condition": "ELSE", "label_for_charts": "No Intersection"}
            ]
        },
        {
            "output_column_name": "TELEPHONE",
            "comparison_levels": [
                {"sql_condition": "TELEPHONE_l IS NULL OR TELEPHONE_r IS NULL", "label_for_charts": "Null", "is_null_level": True},
                {"sql_condition": "arrays_overlap(TELEPHONE_l, TELEPHONE_r)", "label_for_charts": "Intersection"},
                {"sql_condition": "ELSE", "label_for_charts": "No Intersection"}
            ]
        },
        {
            "output_column_name": "FULL_ADDRESS",
            "comparison_levels": [
                {"sql_condition": "FULL_ADDRESS_l IS NULL OR FULL_ADDRESS_r IS NULL", "label_for_charts": "Null", "is_null_level": True},
                {"sql_condition": "arrays_overlap(FULL_ADDRESS_l, FULL_ADDRESS_r)", "label_for_charts": "Intersection"},
                {"sql_condition": "ELSE", "label_for_charts": "No Intersection"}
            ]
        },
        {
            "output_column_name": "DOCUMENT",
            "comparison_levels": [
                {"sql_condition": "DOCUMENT_l IS NULL OR DOCUMENT_r IS NULL", "label_for_charts": "Null", "is_null_level": True},
                {"sql_condition": "arrays_overlap(DOCUMENT_l, DOCUMENT_r)", "label_for_charts": "Intersection"},
                {"sql_condition": "ELSE", "label_for_charts": "No Intersection"}
            ]
        },
        
        # Compare birth dates (exact match only)
        comparison_library.exact_match("BIRTH_DATE"),
    ]
    
    return comparisons
