"""
CUIN v2 - Multi-Pass Blocking Module

Implements various blocking strategies to generate candidate pairs
efficiently while maintaining explainability.
"""

import re
import hashlib
from typing import List, Dict, Set, Tuple, Optional, Any
from collections import defaultdict
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

# Optional imports for advanced blocking
try:
    from datasketch import MinHash, MinHashLSH
    HAS_DATASKETCH = True
except ImportError:
    HAS_DATASKETCH = False

try:
    import jellyfish
    HAS_JELLYFISH = True
except ImportError:
    HAS_JELLYFISH = False


@dataclass
class BlockingConfig:
    """Configuration for blocking operations."""
    max_block_size: int = 200
    max_keys_per_record: int = 50
    suppress_frequency_pct: float = 50.0  # Default to 50% - only suppress very common keys
    min_token_length: int = 2
    lsh_num_perm: int = 128
    lsh_threshold: float = 0.5


@dataclass
class BlockingResult:
    """Result of blocking operation for a single record."""
    record_key: str
    blocking_keys: Dict[str, List[str]] = field(default_factory=dict)


class MultiPassBlocker:
    """
    Multi-pass blocking implementation.
    
    Generates blocking keys using multiple strategies:
    1. Exact match keys (phone, email, natid)
    2. Phonetic keys (Soundex of name)
    3. Token-based keys (name tokens)
    4. LSH keys (MinHash for fuzzy matching)
    """
    
    def __init__(self, config: Optional[BlockingConfig] = None):
        self.config = config or BlockingConfig()
        self._key_frequency: Dict[str, int] = defaultdict(int)
        self._total_records = 0
        self._lsh: Optional[Any] = None
        
        if HAS_DATASKETCH:
            self._lsh = MinHashLSH(
                threshold=self.config.lsh_threshold,
                num_perm=self.config.lsh_num_perm
            )
    
    def _get_soundex(self, text: str) -> Optional[str]:
        """Get Soundex code for a string."""
        if not HAS_JELLYFISH or not text:
            return None
        try:
            return jellyfish.soundex(text)
        except Exception:
            return None
    
    def _tokenize_name(self, name: Optional[str]) -> List[str]:
        """Split name into tokens for blocking."""
        if not name:
            return []
        
        tokens = name.split()
        return [t for t in tokens if len(t) >= self.config.min_token_length]
    
    def _get_stable_hash(self, key_string: str) -> str:
        """Generate a stable MD5 hash for a blocking key string."""
        return hashlib.md5(key_string.encode('utf-8')).hexdigest()

    def _generate_lsh_keys(self, name: str) -> List[str]:
        """Generate LSH bucket keys for a name."""
        if not self._lsh or not name:
            return []
            
        # Create MinHash
        m = MinHash(num_perm=self.config.lsh_num_perm)
        
        # Shingling (3-grams)
        input_str = name.lower().replace(" ", "")
        if len(input_str) < 3:
            return []
            
        for i in range(len(input_str) - 2):
            m.update(input_str[i:i+3].encode('utf8'))
            
        # Get bucket keys (using internal _lsh logic or manual banding)
        # Note: Datasketch MinHashLSH doesn't easily expose "get buckets for this minhash" 
        # without inserting, but we can simulate or use the band hashes.
        # For simplicity in this custom engine, we'll use band hashes directly.
        
        # Extract digest
        digest = m.hashvalues
        
        # Create 4 bands of 32 hashes (for 128 perms) - simplified approach
        # We just return the first few band hashes as blocking keys
        keys = []
        band_size = 4
        for i in range(0, 16, band_size): # Use first 4 bands
            band = tuple(digest[i:i+band_size])
            # Hash the band to make a key
            band_hash = hashlib.md5(str(band).encode('utf8')).hexdigest()
            keys.append(f"LSH:{i}:{band_hash}")
            
        return keys

    def generate_blocking_keys(self, record: dict) -> BlockingResult:
        """
        Generate all blocking keys for a single record.
        """
        result = BlockingResult(
            record_key=record.get('customer_key') or record.get('source_customer_id', ''),
            blocking_keys={}
        )
        
        # Helper to add hashed keys
        def add_key(category: str, raw_val: str):
            if raw_val:
                key_str = f"{category}:{raw_val}"
                hashed = self._get_stable_hash(key_str)
                # Store readable prefix for debug, but hashed for blocking
                # To save space, we just use the hash. 
                # Yet for explainability, we might want the prefix? 
                # Let's use the HASH as the key.
                if category not in result.blocking_keys:
                    result.blocking_keys[category] = []
                result.blocking_keys[category].append(hashed)

        # 1. Phone blocking
        phone = record.get('phone_norm')
        if phone:
            add_key('phone_last10', phone)
        
        # 2. Email exact blocking
        email = record.get('email_norm')
        if email:
            add_key('email_exact', email)
            domain = email.split('@')[-1] if '@' in email else None
            if domain:
                add_key('email_domain', domain)
        
        # 3. National ID exact blocking
        natid = record.get('natid_norm')
        if natid:
            add_key('natid_exact', natid)
        
        # 4. DOB exact blocking
        dob = record.get('dob_norm')
        if dob:
            add_key('dob_exact', dob)
        
        # 5. Soundex blocking
        name = record.get('name_norm', '')
        if name:
            soundex = self._get_soundex(name)
            if soundex:
                add_key('soundex_full', soundex)
            
            tokens = self._tokenize_name(name)
            if tokens:
                first_soundex = self._get_soundex(tokens[0])
                if first_soundex:
                    add_key('soundex_first', first_soundex)
                
                # 6. Name token blocking
                for t in tokens[:5]:
                    add_key('name_token', t)
                    
        # 7. City blocking
        city = record.get('city_norm')
        if city:
            add_key('city_exact', city)
            
        # 8. Composite key
        if dob and name:
            first_initial = name[0] if name else ''
            if first_initial:
                add_key('dob_initial', f"{dob}:{first_initial}")

        # 9. LSH Blocking (New)
        if HAS_DATASKETCH and name:
            lsh_keys = self._generate_lsh_keys(name)
            if lsh_keys:
                result.blocking_keys['lsh_minhash'] = lsh_keys
        
        return result
    
    def build_blocks(
        self,
        records: List[dict]
    ) -> Dict[str, List[str]]:
        """
        Build inverted index of blocking keys to record IDs.
        """
        blocks: Dict[str, List[str]] = defaultdict(list)
        self._total_records = len(records)
        
        for record in records:
            result = self.generate_blocking_keys(record)
            record_key = result.record_key
            
            for method, keys in result.blocking_keys.items():
                for key in keys:
                    blocks[key].append(record_key)
                    self._key_frequency[key] += 1
        
        return dict(blocks)

    def should_suppress_key(self, key: str) -> bool:
        """Check if a key is too frequent and should be suppressed."""
        if self._total_records == 0:
            return False
        
        frequency = self._key_frequency.get(key, 0)
        frequency_pct = (frequency / self._total_records) * 100
        
        return frequency_pct > self.config.suppress_frequency_pct

    def get_stats(self) -> dict:
         return {
            'total_records': self._total_records,
            'total_keys': len(self._key_frequency),
            'avg_keys_per_record': sum(self._key_frequency.values()) / self._total_records if self._total_records else 0,
        }
