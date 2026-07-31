"""
Stage 3 of the entity resolution workbench plan:
engine.clustering.build_clusters -- the shared clustering logic both
orchestrators now call instead of duplicating an inline _cluster().

  1. No-op equivalence: with empty must_link/must_not_link (the state
     before any officer has ever acted), the result must be
     structurally identical to running the auto_links alone through
     engine.clustering.cohesion directly -- proving Stage 3 cannot
     move the ER baseline for a run with zero overrides.
  2. MUST_LINK exemption: a component that would be rejected ONLY for
     exceeding max_cluster_size, but contains an officer-confirmed
     edge, must be accepted.
  3. MUST_NOT_LINK conflict: a transitively-connected pair the officer
     ruled apart must never end up in the same accepted cluster --
     the whole component is pulled, not one edge silently cut.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from engine.clustering.build_clusters import build_clusters
from engine.clustering.union_find import UnionFind
from engine.clustering.cohesion import evaluate_all_components
from engine.clustering import get_cluster_manager


def _reference_build(auto_links, max_cluster_size, min_density):
    """The ORIGINAL inline logic (pre-Stage-3), for the no-op equivalence check."""
    uf = UnionFind()
    edges_by_root = {}
    for a, b in auto_links:
        uf.union(a, b)
    for a, b in auto_links:
        root = uf.find(a)
        edges_by_root.setdefault(root, set()).add(tuple(sorted((a, b))))
    components = uf.get_clusters()
    verdicts = evaluate_all_components(components, edges_by_root, max_cluster_size=max_cluster_size, min_density=min_density)

    manager = get_cluster_manager()
    manager._uf = UnionFind()
    manager._cluster_ids = {}
    manager._members = []

    accepted = {}
    demoted = 0
    for root, members in components.items():
        verdict = verdicts[root]
        if not verdict.accepted:
            demoted += len(edges_by_root.get(root, []))
            continue
        if len(members) < 2:
            continue
        for i in range(1, len(members)):
            manager.link(members[0], members[i])
        cluster_id = manager.find(members[0])
        accepted[cluster_id] = sorted(members)
    return accepted, demoted


def test_empty_overrides_is_byte_identical_to_original_logic():
    auto_links = [
        ("A", "B"), ("B", "C"),          # small chain, should form one cluster
        ("D", "E"),                       # simple pair
        ("F", "G"), ("G", "H"), ("H", "I"), ("I", "J"), ("J", "K"), ("K", "L"),
        ("L", "M"), ("M", "N"), ("N", "O"), ("O", "P"), ("P", "Q"), ("Q", "R"),  # oversized chain -> rejected
    ]
    ref_accepted, ref_demoted = _reference_build(auto_links, max_cluster_size=12, min_density=0.35)
    result = build_clusters(auto_links, max_cluster_size=12, min_density=0.35, must_link=[], must_not_link=[])

    assert result.accepted_clusters == ref_accepted
    assert result.demoted_to_review == ref_demoted
    assert result.override_conflicts == []
    assert result.officer_exempt_count == 0


def test_must_link_exempts_an_oversized_component():
    # 13-member chain exceeds max_cluster_size=12 on auto_links alone -> rejected without an override.
    members = [f"N{i}" for i in range(13)]
    auto_links = [(members[i], members[i + 1]) for i in range(12)]

    baseline = build_clusters(auto_links, max_cluster_size=12, min_density=0.0)
    assert baseline.accepted_clusters == {}, "expected the oversized component to be rejected with no override"
    assert baseline.demoted_to_review == 12

    # Officer confirms one of the existing edges -- doesn't change membership, but the
    # exemption should now apply since an officer edge is present in this component.
    result = build_clusters(auto_links, max_cluster_size=12, min_density=0.0, must_link=[(members[0], members[1])])
    assert len(result.accepted_clusters) == 1
    assert result.officer_exempt_count == 1
    (only_cluster,) = result.accepted_clusters.values()
    assert sorted(only_cluster) == sorted(members)


def test_must_not_link_pulls_the_whole_transitively_connected_component():
    # A-B and B-C are auto-links; officer rules A and C are NOT the same entity.
    # A, B, C must NOT all end up in one accepted cluster.
    auto_links = [("A", "B"), ("B", "C")]
    result = build_clusters(auto_links, max_cluster_size=12, min_density=0.0, must_not_link=[("A", "C")])

    assert result.accepted_clusters == {}, "the conflicted component must be pulled, not silently kept"
    assert result.override_conflicts == [("A", "C")]
    assert result.demoted_to_review == 2  # both edges (A-B, B-C) demoted


def test_must_not_link_with_no_transitive_connection_is_a_no_op():
    # A-B is a real cluster; officer separately rules some unrelated pair X/Y apart -- must not affect A-B.
    auto_links = [("A", "B")]
    result = build_clusters(auto_links, max_cluster_size=12, min_density=0.0, must_not_link=[("X", "Y")])

    assert list(result.accepted_clusters.values()) == [["A", "B"]]
    assert result.override_conflicts == []


if __name__ == "__main__":
    test_empty_overrides_is_byte_identical_to_original_logic()
    test_must_link_exempts_an_oversized_component()
    test_must_not_link_pulls_the_whole_transitively_connected_component()
    test_must_not_link_with_no_transitive_connection_is_a_no_op()
    print("build_clusters tests: OK")
