"""
CUIN v2 - Candidate Builder

Generates candidate pairs from blocking keys.
"""

from typing import List, Dict, Set, Tuple
from collections import defaultdict
from dataclasses import dataclass

from engine.blocking.multipass_blocker import MultiPassBlocker


@dataclass
class CandidatePair:
    """A candidate pair with provenance."""
    a_key: str
    b_key: str
    blocking_reasons: List[str]
    block_key_sample: str


class CandidateBuilder:
    """
    Builder for generating candidate pairs from blocks.
    """
    
    def __init__(self, blocker: MultiPassBlocker):
        self.blocker = blocker
        
    def _extract_method_from_key(self, key: str) -> str:
        """Extract blocking method name from key prefix."""
        prefix = key.split(':')[0]
        method_map = {
            'PHONE': 'phone_last10',
            'EMAIL': 'email_exact',
            'DOMAIN': 'email_domain',
            'NATID': 'natid_exact',
            'DOB': 'dob_exact',
            'SDX': 'soundex_full',
            'SDX1': 'soundex_first',
            'TOK': 'name_token',
            'CITY': 'city_exact',
            'DI': 'dob_initial',
        }
        return method_map.get(prefix, prefix)

    def generate_candidate_pairs(
        self,
        blocks: Dict[str, List[str]],
        max_pairs: int = 100000
    ) -> List[CandidatePair]:
        """
        Generate candidate pairs from blocking key index.
        """
        pair_reasons: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
        pair_key_sample: Dict[Tuple[str, str], str] = {}
        
        count = 0
        
        for key, record_keys in blocks.items():
            # Skip suppressed keys
            if self.blocker.should_suppress_key(key):
                continue
            
            # Skip large blocks
            if len(record_keys) > self.blocker.config.max_block_size:
                continue
            
            # Skip singleton blocks
            if len(record_keys) < 2:
                continue
            
            method = self._extract_method_from_key(key)
            
            # Generate pairs
            for i, a_key in enumerate(record_keys):
                for b_key in record_keys[i + 1:]:
                    if a_key > b_key:
                        a_key, b_key = b_key, a_key
                    
                    pair = (a_key, b_key)
                    pair_reasons[pair].add(method)
                    
                    if pair not in pair_key_sample:
                        pair_key_sample[pair] = key
                        count += 1
            
            # Rough check to break early if way over limit (optimization)
            if count >= max_pairs * 2:
                break
        
        # Convert to objects
        candidates = []
        for (a_key, b_key), reasons in pair_reasons.items():
            candidates.append(CandidatePair(
                a_key=a_key,
                b_key=b_key,
                blocking_reasons=list(reasons),
                block_key_sample=pair_key_sample.get((a_key, b_key), '')
            ))
            
            if len(candidates) >= max_pairs:
                break
                
        return candidates
