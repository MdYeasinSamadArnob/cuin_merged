"""
CUIN v2 - Shared cluster-building logic (Stage 3 of the entity
resolution workbench plan)

Extracted from doris_orchestrator.py's (and, historically, the
removed DuckDB orchestrator's byte-identical) inner _cluster()
function specifically so officer overrides ("officer decision is law"
-- see db/migrations/005_entity_registry.sql) are implemented in one
place, not duplicated with a real risk of engines silently drifting
apart.

With must_link=[] and must_not_link=[] (the default, and the only
state possible before any officer has acted), build_clusters() is
BYTE-IDENTICAL to the pre-Stage-3 inline logic -- every override hook
below is additive and only engages when the caller actually passes
override rows, which is exactly what keeps the baseline gate green
with an empty resolution_overrides table.
"""

from dataclasses import dataclass, replace
from typing import Dict, List, Optional, Set, Tuple

from engine.clustering.union_find import UnionFind
from engine.clustering.cohesion import evaluate_all_components
from engine.clustering import get_cluster_manager


def load_active_overrides(pg_conn) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    Returns (must_link_pairs, must_not_link_pairs) -- active
    (non-revoked) resolution_overrides. Read ONCE at the start of
    clustering (not re-read mid-run), so an officer editing overrides
    while a pipeline is running affects the NEXT run, never a
    partially-applied current one -- see the plan's risk register.
    Fails soft (empty lists) if Postgres is unreachable, matching this
    codebase's established graceful-degradation pattern: a run must
    never fail because the officer-override store is briefly down.
    """
    if pg_conn is None:
        return [], []
    try:
        cur = pg_conn.cursor()
        cur.execute("SELECT a_code, b_code, verdict FROM resolution_overrides WHERE revoked_at IS NULL")
        must_link: List[Tuple[str, str]] = []
        must_not_link: List[Tuple[str, str]] = []
        for a_code, b_code, verdict in cur.fetchall():
            (must_link if verdict == "MUST_LINK" else must_not_link).append((a_code, b_code))
        return must_link, must_not_link
    except Exception:
        return [], []


@dataclass
class ClusterBuildResult:
    accepted_clusters: Dict[str, List[str]]
    demoted_to_review: int
    # MUST_NOT_LINK pairs that were nonetheless found transitively
    # connected -- their WHOLE component was excluded from
    # accepted_clusters (see module docstring) rather than any single
    # edge being silently cut. Surfaced so the caller can flag these
    # for explicit officer resolution instead of the officer's
    # MUST_NOT_LINK verdict just silently not holding.
    override_conflicts: List[Tuple[str, str]]
    officer_exempt_count: int


def build_clusters(
    auto_links: List[Tuple[str, str]],
    max_cluster_size: int,
    min_density: float,
    must_link: Optional[List[Tuple[str, str]]] = None,
    must_not_link: Optional[List[Tuple[str, str]]] = None,
) -> ClusterBuildResult:
    """
    must_link: officer-confirmed pairs, unioned alongside auto_links
        and counted as genuine edges for cohesion density (a confirmed
        link IS real evidence, not a bypass). A component that would
        otherwise be rejected ONLY for exceeding max_cluster_size, but
        contains a must_link edge, is exempted from that cap -- an
        officer's explicit merge must never be silently dropped by a
        size threshold (density is never exempted: a confirmed edge
        only helps density, it doesn't need a waiver from it).
    must_not_link: officer-confirmed non-matches. NOT simply "don't
        union" -- if A and C are still connected transitively (e.g.
        via A-B and B-C auto-links, with the officer only ruling on
        A vs C), that whole component is pulled out of
        accepted_clusters as a conflict rather than guessing which
        edge to cut. See ClusterBuildResult.override_conflicts.
    """
    must_link = must_link or []
    must_not_link = must_not_link or []

    uf = UnionFind()
    edges_by_root: Dict[str, set] = {}
    officer_edges: Set[Tuple[str, str]] = set()

    for a_key, b_key in auto_links:
        uf.union(a_key, b_key)
    for a_key, b_key in must_link:
        uf.union(a_key, b_key)
        officer_edges.add(tuple(sorted((a_key, b_key))))

    # Group edges by the FINAL root of their endpoints, so cohesion is
    # evaluated against the actual final component, not an
    # intermediate one -- unchanged from the original inline logic,
    # now also covering must_link edges.
    for a_key, b_key in list(auto_links) + must_link:
        root = uf.find(a_key)
        edges_by_root.setdefault(root, set()).add(tuple(sorted((a_key, b_key))))

    components = uf.get_clusters()
    verdicts = evaluate_all_components(
        components, edges_by_root, max_cluster_size=max_cluster_size, min_density=min_density,
    )

    officer_exempt_count = 0
    for root, verdict in list(verdicts.items()):
        if not verdict.accepted and verdict.reason_code == "OVERSIZED_COMPONENT":
            if edges_by_root.get(root, set()) & officer_edges:
                verdicts[root] = replace(verdict, accepted=True, reason_code="OFFICER_OVERRIDE_EXEMPT")
                officer_exempt_count += 1

    manager = get_cluster_manager()
    manager._uf = UnionFind()
    manager._cluster_ids = {}
    manager._members = []

    accepted_clusters: Dict[str, List[str]] = {}
    demoted_to_review = 0
    root_to_cluster_id: Dict[str, str] = {}
    member_to_root: Dict[str, str] = {}

    for root, members in components.items():
        verdict = verdicts[root]
        if not verdict.accepted:
            demoted_to_review += len(edges_by_root.get(root, []))
            continue
        if len(members) < 2:
            continue
        for member in members:
            member_to_root[member] = root
        for i in range(1, len(members)):
            manager.link(members[0], members[i])
        cluster_id = manager.find(members[0])
        accepted_clusters[cluster_id] = sorted(members)
        root_to_cluster_id[root] = cluster_id

    override_conflicts: List[Tuple[str, str]] = []
    conflicted_roots: Set[str] = set()
    for a_code, b_code in must_not_link:
        root_a = member_to_root.get(a_code)
        root_b = member_to_root.get(b_code)
        if root_a is not None and root_a == root_b:
            override_conflicts.append((a_code, b_code))
            conflicted_roots.add(root_a)

    for root in conflicted_roots:
        cluster_id = root_to_cluster_id.get(root)
        removed = accepted_clusters.pop(cluster_id, None) if cluster_id else None
        if removed is not None:
            demoted_to_review += len(edges_by_root.get(root, []))

    return ClusterBuildResult(accepted_clusters, demoted_to_review, override_conflicts, officer_exempt_count)
