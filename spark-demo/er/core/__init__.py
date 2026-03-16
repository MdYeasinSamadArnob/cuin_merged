"""
Core entity resolution modules
"""

from er.core.generic_entity_resolution import GenericEntityResolver
from er.core.neo4j_entity_resolution import Neo4jEntityResolver
from er.core.flink_er import FlinkEntityResolver, FlinkERConfig

__all__ = [
    'GenericEntityResolver',
    'Neo4jEntityResolver',
    'FlinkEntityResolver',
    'FlinkERConfig',
]
