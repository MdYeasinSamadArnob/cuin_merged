"""
CUIN v2 - Union Find Module

Union-Find data structure with path compression
and union by rank for efficient clustering.
"""

from typing import Dict, Set

class UnionFind:
    """
    Union-Find data structure with path compression
    and union by rank for efficient clustering.
    """
    
    def __init__(self):
        self._parent: Dict[str, str] = {}
        self._rank: Dict[str, int] = {}
    
    def find(self, x: str) -> str:
        """Find the root of element x with path compression."""
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
        
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])  # Path compression
        
        return self._parent[x]
    
    def union(self, x: str, y: str) -> str:
        """
        Union two elements. Returns the new root.
        Uses union by rank for efficiency.
        """
        root_x = self.find(x)
        root_y = self.find(y)
        
        if root_x == root_y:
            return root_x
        
        # Union by rank
        if self._rank[root_x] < self._rank[root_y]:
            self._parent[root_x] = root_y
            return root_y
        elif self._rank[root_x] > self._rank[root_y]:
            self._parent[root_y] = root_x
            return root_x
        else:
            self._parent[root_y] = root_x
            self._rank[root_x] += 1
            return root_x
    
    def connected(self, x: str, y: str) -> bool:
        """Check if two elements are in the same cluster."""
        return self.find(x) == self.find(y)
    
    def get_clusters(self) -> Dict[str, Set[str]]:
        """Get all clusters as root -> members mapping."""
        clusters: Dict[str, Set[str]] = {}
        
        for element in self._parent:
            root = self.find(element)
            if root not in clusters:
                clusters[root] = set()
            clusters[root].add(element)
        
        return clusters
