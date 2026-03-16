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
    from er.scoring_rules import get_comparison_rules
    
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
        # Higher thresholds mean more similar strings
        comparison_library.LevenshteinAtThresholds("NAME", [0.9, 0.8, 0.7]),
        
        # Compare email addresses (exact match only)
        comparison_library.ExactMatch("EMAIL"),
        
        # Compare mobile numbers (exact match only)
        comparison_library.ExactMatch("MOBILE"),
        
        # Compare telephone numbers (exact match only)
        comparison_library.ExactMatch("TELEPHONE"),
        
        # Compare full addresses with fuzzy matching
        comparison_library.LevenshteinAtThresholds("FULL_ADDRESS", [0.9, 0.8]),
        
        # Compare document code (exact match only)
        # Used together with DOCUMENT_NUMBER per requirements
        comparison_library.ExactMatch("DOCUMENT_CODE"),
        
        # Compare document numbers (exact match only)
        # Used together with DOCUMENT_CODE per requirements
        comparison_library.ExactMatch("DOCUMENT_NUMBER"),
        
        # Compare birth dates (exact match only)
        comparison_library.ExactMatch("BIRTH_DATE"),
    ]
    
    return comparisons
