"""
CUIN - Customer Unified Identification Network
Entity Resolution System for identifying duplicate entities

This package provides tools for entity resolution including:
- Generic entity resolution engine
- Multi-pass blocking strategies
- Similarity algorithms
- Neo4j integration
- Distributed processing (Flink-ER)
- XGBoost machine learning classifier
"""

__version__ = "0.1.0"
__author__ = "CUIN Team"

# Import main classes for convenience
from er.core.generic_entity_resolution import GenericEntityResolver
from er.core.neo4j_entity_resolution import Neo4jEntityResolver
from er.core.flink_er import FlinkEntityResolver, FlinkERConfig
from er.blocking.multi_pass_blocking import MultiPassBlocker
from er.utils.normalize import preprocess_record
from er.utils.entity_resolution import compute_similarity
from er.ml.xgboost_classifier import XGBoostEntityClassifier

__all__ = [
    'GenericEntityResolver',
    'Neo4jEntityResolver',
    'FlinkEntityResolver',
    'FlinkERConfig',
    'MultiPassBlocker',
    'preprocess_record',
    'compute_similarity',
    'XGBoostEntityClassifier',
]
