"""
Neo4j Writer module.
Handles projection of Identity Graph to Neo4j.
"""

import logging
import os
from typing import List, Dict, Any
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

class Neo4jWriter:
    def __init__(self, uri: str = None, auth: tuple = None):
        self.uri = uri or os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password123")
        self.auth = auth or (user, password)
        self.driver = None
        self.enabled = False 

        # Try to connect immediately if checking health, or lazy load?
        # For now, we assume enabled if config present
        self.enabled = False
        # try:
        #     self.driver = GraphDatabase.driver(self.uri, auth=self.auth)
        #     # Verify connectivity
        #     self.driver.verify_connectivity()
        #     self.enabled = True
        #     logger.info(f"Connected to Neo4j at {self.uri}")
        # except Exception as e:
        #     logger.warning(f"Could not connect to Neo4j: {e}. Graph projection disabled.")
        #     self.enabled = False

    def write_graph(self, clusters: Dict[str, List[Dict]], golden_records: Dict[str, Dict]):
        """
        Write or update the complete graph state for the given clusters.
        
        Args:
            clusters: Dict of {cluster_id: [list of member records]}
            golden_records: Dict of {cluster_id: golden_record_payload}
        """
        if not self.enabled or not self.driver:
            return

        with self.driver.session() as session:
            for cluster_id, members in clusters.items():
                golden = golden_records.get(cluster_id, {})
                
                # Create Transaction to update this cluster
                session.execute_write(self._update_cluster_tx, cluster_id, members, golden)

    def project_cluster(self, cluster_id: str, members: List[Dict], golden_record: Dict):
        """
        Project a single cluster to the graph.
        """
        if not self.enabled or not self.driver:
            return
            
        with self.driver.session() as session:
            session.execute_write(self._update_cluster_tx, cluster_id, members, golden_record)
                
    def _update_cluster_tx(self, tx, cluster_id: str, members: List[Dict], golden: Dict):
        """
        Neo4j Transaction to merge Cluster and Members.
        """
        # 1. Merge the Golden Record (Entity)
        # Use simple properties from the golden record
        props = {}
        for k in ['name_norm', 'email_norm', 'phone_norm', 'city_norm']:
            if golden.get(k):
                props[k] = str(golden[k])
                
        # Cypher to merge Entity
        cypher_entity = """
        MERGE (e:Entity {id: $cluster_id})
        SET e += $props, e.updated_at = timestamp()
        """
        tx.run(cypher_entity, cluster_id=cluster_id, props=props)
        
        # 2. Merge Member Records and Link
        for member in members:
            # We assume member has 'source_customer_id' and 'source_system' as unique keys
            source = member.get('source_system', 'UNKNOWN')
            cid = member.get('source_customer_id', 'UNKNOWN')
            
            # Use record_hash as unique ID if available, else composite
            # Actually, let's use the 'customer_key' we generated internally if possible
            # But here we might just receive raw mapped records or normalized records
            # Let's rely on source + id.
            
            # Prepare properties
            m_props = {}
            for k in ['name_norm', 'email_norm', 'phone_norm', 'dob_norm', 'natid_norm']:
                if member.get(k):
                    m_props[k] = str(member[k])
            
            # Add metadata if available
            meta = member.get('metadata', {})
            if meta:
                for mk, mv in meta.items():
                    if mv: m_props[f"meta_{mk}"] = str(mv)

            cypher_member = """
            MERGE (r:Record {source_system: $source, source_id: $cid})
            SET r += $props
            WITH r
            MATCH (e:Entity {id: $cluster_id})
            MERGE (r)-[:BELONGS_TO]->(e)
            """
            tx.run(cypher_member, source=source, cid=cid, props=m_props, cluster_id=cluster_id)

    def get_record(self, record_id: str) -> Dict[str, Any]:
        """
        Fetch a record's properties from Neo4j.
        """
        if not self.enabled or not self.driver:
            return None
            
        with self.driver.session() as session:
            result = session.run(
                "MATCH (r:Record {source_id: $id}) RETURN r",
                id=record_id
            )
            record = result.single()
            if record:
                # Convert Neo4j node to dict
                node = record['r']
                props = dict(node)
                # Map back to API expected format
                return {
                    "name": props.get('name_norm', 'Unknown'),
                    "email": props.get('email_norm', 'N/A'),
                    "phone": props.get('phone_norm', 'N/A'),
                    "address": props.get('address_norm', ''),
                    "riskLevel": "High" if "SUSP" in str(props) else "Medium" if "INACT" in str(props) else "Low",
                    "balance": "৳" + props.get('meta_balance', '0.00'), # Placeholder if not mapped
                    # Add raw props for debug
                    **props
                }
            return None

    def close(self):
        if self.driver:
            self.driver.close()

# Singleton
try:
    _writer = Neo4jWriter()
except:
    _writer = None # Will instantiate on demand or fail gracefully

def get_neo4j_writer():
    global _writer
    if _writer is None:
        _writer = Neo4jWriter()
    return _writer
