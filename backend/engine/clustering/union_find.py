"""
CUIN v2 - Union Find Module (Ruleset v2)

Union-Find with path compression. Root selection is deterministic
(always the lexicographically smaller element), not rank-based --
this makes the resulting partition's root/representative independent
of the ORDER union() calls happen in, which matters because DuckDB
query result ordering, while stable within a single run, must not be
allowed to change which element becomes "the root" between runs.

`find` is iterative, not recursive: the old recursive implementation
raises RecursionError on a long union chain, which is a real risk at
the ~1.5M record scale this pipeline processes (Python's default
recursion limit is 1000).

Member sets are tracked incrementally (small-into-large merge on every
union, O(1) amortized) rather than recomputed by scanning `_parent`
on demand. This matters for engine.clustering.cluster_manager: a
component's membership grows across MANY incremental union() calls
before it's "done", and cluster_id must reflect the FINAL membership,
not whatever partial snapshot existed when the cluster_id was first
requested. Cheap incremental tracking lets the caller re-derive the
current authoritative membership on every call without an O(N) scan.
"""

from typing import Dict, List, Set


class UnionFind:
    def __init__(self):
        self._parent: Dict[str, str] = {}
        self._members: Dict[str, Set[str]] = {}  # root -> current member set

    def find(self, x: str) -> str:
        """Find the root of element x with path compression (iterative)."""
        if x not in self._parent:
            self._parent[x] = x
            self._members[x] = {x}
            return x

        root = x
        while self._parent[root] != root:
            root = self._parent[root]

        node = x
        while self._parent[node] != root:
            next_node = self._parent[node]
            self._parent[node] = root
            node = next_node

        return root

    def union(self, x: str, y: str) -> str:
        """
        Union two elements. Returns the new root, chosen deterministically
        as the lexicographically smaller of the two prior roots -- so the
        final partition's root does not depend on call order.
        """
        root_x = self.find(x)
        root_y = self.find(y)

        if root_x == root_y:
            return root_x

        new_root, old_root = (root_x, root_y) if root_x < root_y else (root_y, root_x)
        self._parent[old_root] = new_root

        # Merge smaller member set into larger for amortized efficiency,
        # then re-key under new_root.
        members_new = self._members.pop(new_root, {new_root})
        members_old = self._members.pop(old_root, {old_root})
        if len(members_new) < len(members_old):
            members_new, members_old = members_old, members_new
        members_new |= members_old
        self._members[new_root] = members_new

        return new_root

    def get_members(self, root_or_element: str) -> Set[str]:
        """Current authoritative member set for the component containing this element."""
        root = self.find(root_or_element)
        return self._members.get(root, {root})

    def connected(self, x: str, y: str) -> bool:
        return self.find(x) == self.find(y)

    def get_clusters(self) -> Dict[str, List[str]]:
        """
        Get all clusters as root -> sorted member list. Returns SORTED
        LISTS, not sets -- a bare Python set's iteration order depends
        on string hashing, which is randomized per-process unless
        PYTHONHASHSEED is pinned. Any consumer that picks "the first
        member" (representative selection, merge root, etc.) must see a
        stable order across runs and across processes.
        """
        clusters: Dict[str, List[str]] = {}
        for element in self._parent:
            root = self.find(element)
            clusters.setdefault(root, []).append(element)

        return {root: sorted(members) for root, members in clusters.items()}

    def to_dict(self) -> Dict:
        return {
            "parent": self._parent,
            "members": {root: sorted(m) for root, m in self._members.items()},
        }

    def from_dict(self, data: Dict):
        self._parent = data.get("parent", {})
        members_data = data.get("members")
        if members_data:
            self._members = {root: set(m) for root, m in members_data.items()}
        else:
            # Backward-compat with snapshots saved before member tracking
            # existed: rebuild member sets from `parent` via a full scan
            # (one-time cost on load, not on the hot union() path).
            self._members = {}
            for element in self._parent:
                root = self.find(element)
                self._members.setdefault(root, set()).add(element)
