"""
CUIN v2 - Cluster Cohesion Guard (Ruleset v2, Layer 5)

The old pipeline built clusters as pure connected-components /
union-find over AUTO_LINK edges with NO size cap and NO cohesion
check -- so a chain of pairwise edges (A-B, B-C, C-D) collapses into
one component even when A and D (or A and C) were never compared
directly, or would fail the match rule outright. This is exactly how
the reported bug produced a 10-member cluster of distinct people.

This module evaluates each connected component's internal edge
density and size, and routes ones that fail either check to REVIEW
(with the full component preserved so a reviewer can approve it in one
action) instead of silently emitting it as a cluster.
"""

from dataclasses import dataclass
from typing import Dict, List, Set, Tuple


@dataclass
class ComponentVerdict:
    members: List[str]
    edge_count: int
    density: float
    accepted: bool
    reason_code: str = None  # OVERSIZED_COMPONENT | LOW_COHESION | None


def evaluate_component(
    members: List[str],
    edges: Set[Tuple[str, str]],
    max_cluster_size: int,
    min_density: float,
) -> ComponentVerdict:
    """
    density = 2|E| / (|V|(|V|-1)) -- the fraction of all possible pairs
    within the component that are actually AUTO_LINK edges. A component
    formed by a long chain of edges (each individually valid) but with
    few of the possible pairwise edges present has low density and is
    exactly the failure mode being guarded against.
    """
    n = len(members)
    if n <= 1:
        return ComponentVerdict(members=members, edge_count=0, density=1.0, accepted=True)

    max_possible_edges = n * (n - 1) / 2
    density = len(edges) / max_possible_edges if max_possible_edges > 0 else 0.0

    if n > max_cluster_size:
        return ComponentVerdict(
            members=members, edge_count=len(edges), density=density,
            accepted=False, reason_code="OVERSIZED_COMPONENT",
        )

    if density < min_density:
        return ComponentVerdict(
            members=members, edge_count=len(edges), density=density,
            accepted=False, reason_code="LOW_COHESION",
        )

    return ComponentVerdict(members=members, edge_count=len(edges), density=density, accepted=True)


def evaluate_all_components(
    components: Dict[str, List[str]],
    edges_by_root: Dict[str, Set[Tuple[str, str]]],
    max_cluster_size: int,
    min_density: float,
) -> Dict[str, ComponentVerdict]:
    """
    components: root -> sorted member list (from UnionFind.get_clusters()).
    edges_by_root: root -> set of (a_key, b_key) AUTO_LINK edges that
        contributed to that component.
    """
    return {
        root: evaluate_component(
            members, edges_by_root.get(root, set()), max_cluster_size, min_density
        )
        for root, members in components.items()
    }
