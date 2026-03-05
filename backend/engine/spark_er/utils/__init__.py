"""
Utility modules for data normalization and similarity calculations
"""

from engine.spark_er.utils.normalize import preprocess_record
from engine.spark_er.utils.entity_resolution import compute_similarity

__all__ = ['preprocess_record', 'compute_similarity']
