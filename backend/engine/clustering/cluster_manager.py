"""
CUIN v2 - Cluster Manager (Ruleset v2)

Manages identity clusters with versioning and golden record generation.

cluster_id is now a deterministic sha256 of the ruleset version plus
the sorted member set, NOT uuid4() -- the old str(uuid4()) meant the
same set of members got a brand-new random cluster_id on every single
pipeline run, even on byte-identical input data, which broke the
"same run twice -> same cluster IDs" reproducibility requirement.
"""

from typing import Dict, List, Set, Optional, Tuple
from datetime import datetime
import hashlib
import json
import os
import logging
from dataclasses import asdict

from engine.clustering.union_find import UnionFind
from engine.golden.golden_builder import GoldenBuilder
from engine.structures import ClusterMember, GoldenRecord
from engine.ruleset.version import RULESET_VERSION

logger = logging.getLogger(__name__)

class ClusterManager:
    """
    Manages identity clusters with versioning.
    
    Supports:
    - Creating clusters from auto-linked pairs
    - Merging clusters based on review decisions
    - Generating golden records
    - Temporal queries on cluster membership
    - Persistence (save/load snapshot)
    """
    
    def __init__(self):
        self._uf = UnionFind()
        self._cluster_ids: Dict[str, str] = {}  # root -> cluster_id
        self._members: List[ClusterMember] = []
        self._golden_records: Dict[str, List[GoldenRecord]] = {}
        self._current_version = 0
        self._golden_builder = GoldenBuilder()
        self.loaded_run_id: Optional[str] = None
    
    def save_snapshot(self, path: str = 'data/cluster_snapshot.json'):
        """Persist state to disk."""
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            state = {
                "uf": self._uf.to_dict(),
                "cluster_ids": self._cluster_ids,
                "members": [asdict(m) for m in self._members],
                "current_version": self._current_version
            }
            # Custom encoder for datetime
            def default(o):
                if isinstance(o, (datetime,)):
                    return o.isoformat()
                return str(o)
                
            with open(path, 'w') as f:
                json.dump(state, f, default=default)
            logger.info(f"Saved cluster snapshot to {path}")
        except Exception as e:
            logger.error(f"Failed to save cluster snapshot: {e}")

    def load_snapshot(self, path: str = 'data/cluster_snapshot.json', run_id: Optional[str] = None):
        """Load state from disk."""
        try:
            if not os.path.exists(path):
                return
            
            with open(path, 'r') as f:
                state = json.load(f)
            
            self._uf.from_dict(state["uf"])
            self._cluster_ids = state["cluster_ids"]
            self._current_version = state.get("current_version", 0)
            
            # Reconstruct members
            self._members = []
            for m_data in state["members"]:
                if 'valid_from' in m_data and isinstance(m_data['valid_from'], str):
                     m_data['valid_from'] = datetime.fromisoformat(m_data['valid_from'])
                if 'valid_to' in m_data and m_data['valid_to'] and isinstance(m_data['valid_to'], str):
                     m_data['valid_to'] = datetime.fromisoformat(m_data['valid_to'])
                self._members.append(ClusterMember(**m_data))
            
            self.loaded_run_id = run_id
            logger.info(f"Loaded cluster snapshot from {path} (run_id={run_id})")
        except Exception as e:
            logger.error(f"Failed to load cluster snapshot: {e}")
    
    def _get_or_create_cluster_id(self, root: str) -> str:
        """
        Deterministic cluster ID: sha256(RULESET_VERSION + sorted(members)).

        Deliberately NOT cached by root string. A component's membership
        grows across many incremental union() calls before it settles
        (spark_orchestrator._stage_cluster links members into a shared
        root one pair at a time), and the deterministic root chosen by
        UnionFind.union() can stay the SAME string across several of
        those calls while membership keeps growing underneath it -- so
        caching by root alone would freeze the ID at whatever partial
        membership existed on first request. UnionFind.get_members() is
        O(current component size) via incremental tracking, not an O(N)
        full-table scan, so recomputing on every call is cheap.
        """
        members = sorted(self._uf.get_members(root))
        digest = hashlib.sha256(
            (RULESET_VERSION + "|" + "|".join(members)).encode("utf-8")
        ).hexdigest()
        cluster_id = f"CL_{digest[:32]}"
        self._cluster_ids[root] = cluster_id  # kept for get_clusters()'s id lookup
        return cluster_id
    
    def find(self, key: str) -> str:
        """Find the cluster ID for a record key."""
        root = self._uf.find(key)
        return self._get_or_create_cluster_id(root)
    
    def register_record(self, key: str) -> str:
        """
        Register a record in the cluster manager.
        If it's new, it starts as a singleton.
        Returns cluster ID.
        """
        # Ensure it exists in UF
        self._uf.find(key)
        
        cluster_id = self.find(key)
        
        # Check if already a member
        # Note: This is checking if we know about this membership
        # In a real DB backing, we'd check the table
        known_members = set(m.customer_key for m in self._members if m.valid_to is None)
        
        if key not in known_members:
            now = datetime.utcnow()
            self._members.append(ClusterMember(
                customer_key=key,
                cluster_id=cluster_id,
                version=self._current_version,
                valid_from=now
            ))
            
        return cluster_id
    
    def link(self, a_key: str, b_key: str) -> str:
        """
        Link two records into the same cluster.
        Returns the cluster ID.
        """
        new_root = self._uf.union(a_key, b_key)
        cluster_id = self._get_or_create_cluster_id(new_root)
        
        # Record membership for both
        now = datetime.utcnow()
        
        self._members.append(ClusterMember(
            customer_key=a_key,
            cluster_id=cluster_id,
            version=self._current_version,
            valid_from=now
        ))
        
        self._members.append(ClusterMember(
            customer_key=b_key,
            cluster_id=cluster_id,
            version=self._current_version,
            valid_from=now
        ))
        
        return cluster_id
    
    def process_auto_links(self, pairs: List[Tuple[str, str]]) -> Dict[str, str]:
        """
        Process a batch of auto-linked pairs.
        Returns mapping of customer_key -> cluster_id.
        """
        self._current_version += 1

        for a_key, b_key in pairs:
            self.link(a_key, b_key)

        result = {}
        for member in self._members:
            if member.valid_to is None:
                result[member.customer_key] = self.find(member.customer_key)

        return result
    
    def get_cluster_members(self, cluster_id: str) -> List[str]:
        """Get all members of a cluster."""
        return [
            m.customer_key
            for m in self._members
            if m.cluster_id == cluster_id and m.valid_to is None
        ]
    
    def get_clusters(self) -> Dict[str, List[str]]:
        """Get all clusters with their members."""
        clusters = self._uf.get_clusters()
        
        result = {}
        for root, members in clusters.items():
            cluster_id = self._get_or_create_cluster_id(root)
            result[cluster_id] = list(members)
        
        return result
    
    def generate_golden_record(
        self,
        cluster_id: str,
        records: List[dict],
        created_by: str = "SYSTEM"
    ) -> GoldenRecord:
        """
        Generate a golden record for a cluster.
        Delegates to GoldenBuilder.
        """
        if cluster_id not in self._golden_records:
            self._golden_records[cluster_id] = []
            
        version = len(self._golden_records[cluster_id]) + 1
        
        golden = self._golden_builder.generate_golden_record(
            cluster_id, records, version, created_by
        )
        
        self._golden_records[cluster_id].append(golden)
        return golden
    
    def get_golden_record(
        self,
        cluster_id: str,
        version: Optional[int] = None
    ) -> Optional[GoldenRecord]:
        """Get a golden record, optionally at a specific version."""
        records = self._golden_records.get(cluster_id, [])
        
        if not records:
            return None
        
        if version is None:
            return records[-1]  # Latest version
        
        for record in records:
            if record.version == version:
                return record
        
        return None
    
    def get_stats(self) -> dict:
        """Get clustering statistics."""
        clusters = self.get_clusters()
        
        if not clusters:
            return {
                'total_clusters': 0,
                'total_members': 0,
                'avg_cluster_size': 0,
                'max_cluster_size': 0,
                'singleton_clusters': 0,
            }
        
        sizes = [len(members) for members in clusters.values()]
        
        return {
            'total_clusters': len(clusters),
            'total_members': sum(sizes),
            'avg_cluster_size': sum(sizes) / len(sizes),
            'max_cluster_size': max(sizes),
            'singleton_clusters': sum(1 for s in sizes if s == 1),
            'golden_records_count': sum(len(records) for records in self._golden_records.values()),
            'size_distribution': {
                '1': sum(1 for s in sizes if s == 1),
                '2-5': sum(1 for s in sizes if 2 <= s <= 5),
                '6-10': sum(1 for s in sizes if 6 <= s <= 10),
                '10+': sum(1 for s in sizes if s > 10)
            }
        }


# Singleton instance
_cluster_manager = ClusterManager()


def get_cluster_manager() -> ClusterManager:
    """Get the global cluster manager instance."""
    return _cluster_manager
