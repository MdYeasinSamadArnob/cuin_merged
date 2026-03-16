"""
Core entity resolution modules
"""

from engine.spark_er.core.generic_entity_resolution import GenericEntityResolver
from engine.spark_er.core.neo4j_entity_resolution import Neo4jEntityResolver
from engine.spark_er.core.flink_er import FlinkEntityResolver, FlinkERConfig

__all__ = [
    'GenericEntityResolver',
    'Neo4jEntityResolver',
    'FlinkEntityResolver',
    'FlinkERConfig',
]
