"""
Blocking strategies for entity resolution
"""

from engine.spark_er.blocking.multi_pass_blocking import MultiPassBlocker

__all__ = ['MultiPassBlocker']
