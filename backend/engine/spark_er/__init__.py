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

# Lazy imports — only expose what's available, don't crash if optional deps are missing
try:
    from engine.spark_er.core.generic_entity_resolution import GenericEntityResolver
except Exception:
    GenericEntityResolver = None  # type: ignore

try:
    from engine.spark_er.core.neo4j_entity_resolution import Neo4jEntityResolver
except Exception:
    Neo4jEntityResolver = None  # type: ignore

try:
    from engine.spark_er.core.flink_er import FlinkEntityResolver, FlinkERConfig
except Exception:
    FlinkEntityResolver = None  # type: ignore
    FlinkERConfig = None  # type: ignore

try:
    from engine.spark_er.blocking.multi_pass_blocking import MultiPassBlocker
except Exception:
    MultiPassBlocker = None  # type: ignore

try:
    from engine.spark_er.utils.normalize import preprocess_record
except Exception:
    preprocess_record = None  # type: ignore

try:
    from engine.spark_er.utils.entity_resolution import compute_similarity
except Exception:
    compute_similarity = None  # type: ignore

try:
    from engine.spark_er.ml.xgboost_classifier import XGBoostEntityClassifier
except Exception:
    XGBoostEntityClassifier = None  # type: ignore

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
