"""
CUIN - Customer Unified Identification Network
Entity Resolution System for identifying duplicate entities

The live path through this package is pipeline.spark_orchestrator ->
engine.spark_er.splink_commands + engine.spark_er.blocking.multi_pass_blocking
(selected via api/routes_datasource.py's engine="spark"). An earlier,
broader generic/Neo4j/Flink/XGBoost entity-resolution surface (core/,
ml/, utils/, plus several orphaned top-level modules) was never wired
into that live path and has been removed as unused (YAGNI cleanup) --
see git history if any of it needs to be recovered for reference.
"""

__version__ = "0.1.0"
__author__ = "CUIN Team"

# Lazy import -- only expose what's available, don't crash if optional
# deps are missing.
try:
    from engine.spark_er.blocking.multi_pass_blocking import MultiPassBlocker
except Exception:
    MultiPassBlocker = None  # type: ignore

__all__ = [
    'MultiPassBlocker',
]
