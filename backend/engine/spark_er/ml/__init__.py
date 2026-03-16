"""
Machine Learning Module for Entity Resolution

This module provides ML-based classifiers for entity resolution,
including XGBoost-based duplicate detection.
"""

from engine.spark_er.ml.xgboost_classifier import XGBoostEntityClassifier

__all__ = ['XGBoostEntityClassifier']
