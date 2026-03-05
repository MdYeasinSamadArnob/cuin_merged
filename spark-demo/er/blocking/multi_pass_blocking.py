"""
Multi-pass Blocking for Entity Resolution

This module implements the MultiPassBlocker for large-scale entity resolution.
Optimized for datasets of 100K+ records with configurable blocking strategies.
"""

import re
from collections import defaultdict
from typing import List, Dict, Set, Tuple, Optional
import json


class LSHBlocker:
    """
    Locality Sensitive Hashing blocker using MinHash.
    
    LSH groups similar items together by hashing them to the same buckets,
    enabling efficient similarity search without comparing all pairs.
    """
    
    def __init__(self, num_hashes: int = 100, num_bands: int = 20):
        """
        Initialize LSH blocker.
        
        Args:
            num_hashes: Number of hash functions for MinHash signature
            num_bands: Number of bands for LSH (more bands = more recall, less precision)
        """
        self.num_hashes = num_hashes
        self.num_bands = num_bands
        self.rows_per_band = num_hashes // num_bands
        
        # Pre-generate hash seeds for consistent hashing
        self.hash_seeds = [i * 2654435761 for i in range(num_hashes)]
    
    def _shingle(self, text: str, k: int = 3) -> Set[str]:
        """
        Create k-shingles (k-grams) from text.
        
        Args:
            text: Input text
            k: Shingle size
            
        Returns:
            Set of k-shingles
        """
        if not text or len(text) < k:
            return {text} if text else set()
        
        text = text.upper()
        return {text[i:i+k] for i in range(len(text) - k + 1)}
    
    def _minhash(self, shingles: Set[str]) -> List[int]:
        """
        Compute MinHash signature for a set of shingles.
        
        Args:
            shingles: Set of shingles
            
        Returns:
            MinHash signature as list of integers
        """
        if not shingles:
            return [0] * self.num_hashes
        
        signature = []
        
        for seed in self.hash_seeds:
            min_hash = float('inf')
            
            for shingle in shingles:
                # Hash each shingle with the seed
                hash_val = hash((shingle, seed)) & 0x7FFFFFFF
                min_hash = min(min_hash, hash_val)
            
            signature.append(min_hash)
        
        return signature
    
    def _lsh_buckets(self, signature: List[int]) -> List[str]:
        """
        Generate LSH bucket keys from MinHash signature.
        
        Args:
            signature: MinHash signature
            
        Returns:
            List of bucket keys
        """
        buckets = []
        
        for band in range(self.num_bands):
            start = band * self.rows_per_band
            end = start + self.rows_per_band
            band_values = signature[start:end]
            
            # Create bucket key from band values
            bucket_key = f"lsh_{band}_{hash(tuple(band_values))}"
            buckets.append(bucket_key)
        
        return buckets
    
    def generate_blocks(self, text: str) -> List[str]:
        """
        Generate LSH blocking keys for a text field.
        
        Args:
            text: Input text
            
        Returns:
            List of blocking keys
        """
        if not text:
            return []
        
        # Create shingles
        shingles = self._shingle(text)
        
        # Compute MinHash signature
        signature = self._minhash(shingles)
        
        # Generate LSH buckets
        buckets = self._lsh_buckets(signature)
        
        return buckets


class SoundexBlocker:
    """
    Soundex phonetic blocking for name matching.
    
    Soundex is a phonetic algorithm for indexing names by sound.
    Names that sound similar will have the same Soundex code.
    """
    
    @staticmethod
    def soundex(name: str) -> str:
        """
        Generate Soundex code for a name.
        
        Args:
            name: Input name
            
        Returns:
            Soundex code (e.g., 'S530' for 'Smith')
        """
        if not name or not isinstance(name, str):
            return "0000"
        name = name.upper()
        name = re.sub(r'[^A-Z]', '', name)
        if not name:
            return "0000"
        
        first_letter = name[0]
        soundex_mapping = {
            'B': '1', 'F': '1', 'P': '1', 'V': '1',
            'C': '2', 'G': '2', 'J': '2', 'K': '2', 'Q': '2', 'S': '2', 'X': '2', 'Z': '2',
            'D': '3', 'T': '3',
            'L': '4',
            'M': '5', 'N': '5',
            'R': '6'
        }
        
        encoded = ""
        prev_code = soundex_mapping.get(first_letter, '0')
        for char in name[1:]:
            code = soundex_mapping.get(char, '0')
            if code == '0':
                prev_code = '0'
                continue
            if code != prev_code:
                encoded += code
                prev_code = code
            if len(encoded) >= 3:
                break
        
        encoded = encoded.ljust(3, '0')
        return first_letter + encoded[:3]
    
    @staticmethod
    def generate_blocks(name: str) -> List[str]:
        if not name:
            return []
        parts = name.upper().split()
        blocks = []
        for part in parts:
            if len(part) >= 2:
                soundex_code = SoundexBlocker.soundex(part)
                blocks.append(f"soundex_{soundex_code}")
        full_soundex = SoundexBlocker.soundex(name.replace(" ", ""))
        blocks.append(f"soundex_full_{full_soundex}")
        return blocks


class RuleBasedBlocker:
    """
    Rule-based blocking using exact and fuzzy matching on key fields.
    
    This blocker generates deterministic blocking keys based on exact matches,
    prefixes, numeric values, dates, and text tokens.
    """
    
    @staticmethod
    def exact_block(value: str, prefix: str = "exact") -> str:
        """
        Generate exact match blocking key.
        
        Args:
            value: Input value
            prefix: Key prefix for namespacing
            
        Returns:
            Blocking key or None if value is empty
        """
        if not value:
            return None
        normalized = value.upper().strip()
        normalized = re.sub(r'\s+', '_', normalized)
        return f"{prefix}_{normalized}"
    
    @staticmethod
    def prefix_block(value: str, length: int = 3, prefix: str = "prefix") -> str:
        if not value or len(value) < length:
            return None
        normalized = value.upper().strip()
        return f"{prefix}_{normalized[:length]}"
    
    @staticmethod
    def numeric_block(value: str, prefix: str = "num") -> str:
        if not value:
            return None
        digits = re.sub(r'\D', '', value)
        if len(digits) < 4:
            return None
        return f"{prefix}_{digits[-10:]}"
    
    @staticmethod
    def date_block(date_str: str, prefix: str = "date") -> List[str]:
        if not date_str:
            return []
        parts = date_str.split('-')
        if len(parts) < 3:
            return []
        year, month, day = parts[:3]
        return [
            f"{prefix}_ymd_{year}{month}{day}",
            f"{prefix}_ym_{year}{month}",
            f"{prefix}_y_{year}"
        ]
    
    @staticmethod
    def token_block(text: str, prefix: str = "token") -> List[str]:
        if not text:
            return []
        text = text.upper()
        tokens = re.findall(r'\w+', text)
        tokens = [t for t in tokens if len(t) >= 3]
        return [f"{prefix}_{token}" for token in tokens]


class GeoHasher:
    """
    Geohashing for location-based blocking.
    
    Geohash encodes geographic coordinates (latitude/longitude) into short strings.
    Nearby locations share common prefixes, making it useful for blocking.
    """
    
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    
    @staticmethod
    def encode(latitude: float, longitude: float, precision: int = 6) -> str:
        lat_range = [-90.0, 90.0]
        lon_range = [-180.0, 180.0]
        geohash = []
        bits = 0
        bit = 0
        even = True
        
        while len(geohash) < precision:
            if even:
                mid = (lon_range[0] + lon_range[1]) / 2
                if longitude > mid:
                    bit |= (1 << (4 - bits))
                    lon_range[0] = mid
                else:
                    lon_range[1] = mid
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if latitude > mid:
                    bit |= (1 << (4 - bits))
                    lat_range[0] = mid
                else:
                    lat_range[1] = mid
            
            even = not even
            bits += 1
            
            if bits == 5:
                geohash.append(GeoHasher.BASE32[bit])
                bits = 0
                bit = 0
        
        return ''.join(geohash)
    
    @staticmethod
    def generate_blocks(latitude: Optional[float], longitude: Optional[float], 
                       precisions: List[int] = None) -> List[str]:
        if latitude is None or longitude is None:
            return []
        if precisions is None:
            precisions = [4, 5, 6]
        blocks = []
        for precision in precisions:
            geohash = GeoHasher.encode(latitude, longitude, precision)
            blocks.append(f"geo_{precision}_{geohash}")
        return blocks


class MultiPassBlocker:
    """
    Multi-pass blocking for large-scale entity resolution.
    
    Optimized for datasets of 100K+ records. For 1.7M records, use:
        use_lsh=False, use_soundex=False, use_selective_rules=True, max_block_size=50
    """
    
    def __init__(self, 
                 use_lsh: bool = False,
                 use_soundex: bool = False,
                 use_geohash: bool = False,
                 use_rules: bool = True,
                 lsh_num_hashes: int = 100,
                 lsh_num_bands: int = 20,
                 max_block_size: int = 50,
                 use_selective_rules: bool = True):
        """
        Args:
            use_lsh: Enable LSH (slow for large datasets)
            use_soundex: Enable Soundex (generates many pairs)
            use_geohash: Enable geohashing
            use_rules: Enable rule-based blocking
            lsh_num_hashes: Number of MinHash functions
            lsh_num_bands: Number of LSH bands
            max_block_size: Max records per block (prevents OOM)
            use_selective_rules: Only phone/email/dob exact matches (recommended for 1M+ records)
        """
        self.use_lsh = use_lsh
        self.use_soundex = use_soundex
        self.use_geohash = use_geohash
        self.use_rules = use_rules
        self.max_block_size = max_block_size
        self.use_selective_rules = use_selective_rules
        
        if self.use_lsh:
            self.lsh_blocker = LSHBlocker(lsh_num_hashes, lsh_num_bands)
        
        self.soundex_blocker = SoundexBlocker()
        self.rule_blocker = RuleBasedBlocker()
        self.geo_hasher = GeoHasher()
        
        self.stats = {
            'total_records': 0,
            'total_blocks': 0,
            'avg_blocks_per_record': 0.0,
            'blocking_keys': defaultdict(int),
            'skipped_blocks': 0,
            'skipped_pairs': 0
        }
    
    def generate_blocking_keys(self, record: dict) -> List[str]:
        """
        Generate all blocking keys for a record using multiple strategies.
        
        Args:
            record: Dictionary containing record fields (CUSNMF, TELENO, MAILID, CUSDOB, etc.)
            
        Returns:
            List of unique blocking keys
            
        Note:
            - In selective mode: Only generates exact matches on phone/email/dob
            - In full mode: Uses LSH, Soundex, and all rule-based strategies
        """
        keys = []
        
        # Extract fields
        name = record.get('name') or record.get('CUSNMF') or record.get('CUSNML')
        phone = record.get('phone') or record.get('TELENO') or record.get('mobile') or record.get('MOBLNO')
        email = record.get('email') or record.get('MAILID')
        dob = record.get('dob') or record.get('CUSDOB')
        city = record.get('city') or record.get('CITYNM')
        national_id = record.get('nationalId') or record.get('NATLID')
        
        if not name:
            first = record.get('CUSNMF', '')
            last = record.get('CUSNML', '')
            name = f"{first} {last}".strip()
        
        # Selective mode: only exact matches on discriminating fields
        if self.use_selective_rules:
            if phone:
                keys.append(self.rule_blocker.numeric_block(phone, "phone"))
            if email:
                keys.append(self.rule_blocker.exact_block(email, "email"))
            if dob:
                keys.append(self.rule_blocker.exact_block(dob, "dob"))
            if national_id:
                keys.append(self.rule_blocker.exact_block(national_id, "nid"))
            return [k for k in keys if k is not None]
        
        # Full mode: all blocking strategies 
        if self.use_lsh and name:
            keys.extend(self.lsh_blocker.generate_blocks(name))
        
        if self.use_soundex and name:
            keys.extend(self.soundex_blocker.generate_blocks(name))
        
        if self.use_rules:
            if name:
                keys.append(self.rule_blocker.exact_block(name, "name_exact"))
                keys.append(self.rule_blocker.prefix_block(name, 3, "name_prefix"))
                keys.extend(self.rule_blocker.token_block(name, "name_token"))
            if phone:
                keys.append(self.rule_blocker.numeric_block(phone, "phone"))
            if email:
                keys.append(self.rule_blocker.exact_block(email, "email"))
                if '@' in email:
                    domain = email.split('@')[-1]
                    keys.append(self.rule_blocker.exact_block(domain, "email_domain"))
            if dob:
                keys.extend(self.rule_blocker.date_block(str(dob), "dob"))
            if national_id:
                keys.append(self.rule_blocker.exact_block(str(national_id), "natid"))
            if city:
                keys.append(self.rule_blocker.exact_block(city, "city"))
        
        if self.use_geohash:
            lat = record.get('latitude')
            lon = record.get('longitude')
            if lat is not None and lon is not None:
                keys.extend(self.geo_hasher.generate_blocks(lat, lon))
        
        return list(set([k for k in keys if k is not None]))
    
    def create_blocks(self, records: List[dict]) -> Dict[str, List[int]]:
        """
        Create blocks (candidate pairs) from a list of records.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Dictionary mapping blocking keys to list of record indices
            
        Example:
            blocks = {
                'phone_1234567890': [0, 5, 12],  # Records 0, 5, 12 share this phone
                'email_john@example.com': [2, 8],  # Records 2, 8 share this email
            }
        """
        blocks = defaultdict(list)
        
        for idx, record in enumerate(records):
            keys = self.generate_blocking_keys(record)
            for key in keys:
                blocks[key].append(idx)
                self.stats['blocking_keys'][key] += 1
        
        self.stats['total_records'] = len(records)
        self.stats['total_blocks'] = len(blocks)
        if len(records) > 0:
            total_keys = sum(len(self.generate_blocking_keys(r)) for r in records)
            self.stats['avg_blocks_per_record'] = total_keys / len(records)
        
        return dict(blocks)
    
    def get_candidate_pairs(self, blocks: Dict[str, List[int]], 
                           filter_large_blocks: bool = True) -> Set[Tuple[int, int]]:
        """
        Extract candidate pairs from blocks with optional filtering of large blocks.
        
        Args:
            blocks: Dictionary of blocking keys to record indices
            filter_large_blocks: If True, skip blocks larger than max_block_size
            
        Returns:
            Set of candidate pairs (i, j) where i < j
            
        Note:
            Large blocks (>max_block_size) are skipped to prevent combinatorial explosion.
            For a block of size N, there are N*(N-1)/2 pairs. A block of 1000 records
            would generate 499,500 pairs!
        """
        candidates = set()
        skipped_blocks = 0
        skipped_pairs = 0
        
        for key, indices in blocks.items():
            block_size = len(indices)
            
            if filter_large_blocks and block_size > self.max_block_size:
                potential_pairs = (block_size * (block_size - 1)) // 2
                skipped_blocks += 1
                skipped_pairs += potential_pairs
                continue
            
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    idx1, idx2 = indices[i], indices[j]
                    pair = (min(idx1, idx2), max(idx1, idx2))
                    candidates.add(pair)
        
        self.stats['skipped_blocks'] = skipped_blocks
        self.stats['skipped_pairs'] = skipped_pairs
        
        return candidates
    
    def print_statistics(self):
        """Print blocking statistics."""
        stats = self.stats
        
        print("\n" + "=" * 60)
        print("MULTI-PASS BLOCKING STATISTICS")
        print("=" * 60)
        print(f"Total Records: {stats['total_records']}")
        print(f"Total Blocks: {stats['total_blocks']}")
        print(f"Avg Blocks per Record: {stats['avg_blocks_per_record']:.2f}")
        
        if stats.get('skipped_blocks', 0) > 0:
            print("\n⚠️  Large Block Filtering:")
            print(f"   Skipped Blocks: {stats['skipped_blocks']}")
            print(f"   Skipped Pairs: {stats['skipped_pairs']:,}")
        
        print("\nTop 10 Blocking Keys by Frequency:")
        sorted_keys = sorted(stats['blocking_keys'].items(), 
                           key=lambda x: x[1], reverse=True)[:10]
        for key, count in sorted_keys:
            print(f"  {key}: {count} records")
        
        print("=" * 60 + "\n")


def calculate_reduction(total_records: int, candidate_pairs: int) -> float:
    """Calculate reduction in comparisons achieved by blocking."""
    if total_records <= 1:
        return 0.0
    total_possible = (total_records * (total_records - 1)) // 2
    if total_possible == 0:
        return 0.0
    return ((total_possible - candidate_pairs) / total_possible) * 100


# Example usage
if __name__ == "__main__":
    import os
    
    # Load sample records
    script_dir = os.path.dirname(os.path.abspath(__file__))
    records_path = os.path.join(script_dir, "records.json")
    
    try:
        with open(records_path, "r") as f:
            sample_records = json.load(f)
    except FileNotFoundError:
        print(f"Sample file not found: {records_path}")
        exit(1)
    
    print("=" * 80)
    print("MULTI-PASS BLOCKER - SELECTIVE MODE (for 1.7M records)")
    print("=" * 80)
    
    # Configuration for large datasets
    blocker = MultiPassBlocker(
        use_lsh=True,               # Too slow for 1.7M
        use_soundex=False,           # Generates too many pairs
        use_rules=False,
        use_geohash=False,
        max_block_size=50,           # Skip blocks >50
        use_selective_rules=False     # Only phone/email/dob
    )
    
    # Create blocks and get candidates
    blocks = blocker.create_blocks(sample_records)
    candidates = blocker.get_candidate_pairs(blocks, filter_large_blocks=True)
    
    # Print results
    total_possible = (len(sample_records) * (len(sample_records) - 1)) // 2
    reduction = calculate_reduction(len(sample_records), len(candidates))
    
    print(f"\nTotal records: {len(sample_records)}")
    print(f"Total blocks: {len(blocks)}")
    print(f"Candidate pairs: {len(candidates)}")
    print(f"Total possible pairs: {total_possible:,}")
    print(f"Reduction: {reduction:.2f}%")
    
    if blocker.stats.get('skipped_blocks', 0) > 0:
        print("\n⚠️  Large Block Filtering:")
        print(f"   Skipped blocks: {blocker.stats['skipped_blocks']}")
        print(f"   Skipped pairs: {blocker.stats['skipped_pairs']:,}")
    
    # Display candidate pairs with record details
    print("\n" + "=" * 80)
    print("CANDIDATE PAIRS")
    print("=" * 80)
    
    if len(candidates) == 0:
        print("No candidate pairs found.")
    else:
        for idx1, idx2 in sorted(candidates)[:20]:  # Show first 20 pairs
            rec1 = sample_records[idx1]
            rec2 = sample_records[idx2]
            
            print(f"\nPair ({idx1}, {idx2}):")
            print(f"  Record {idx1}: {rec1.get('CUSNMF', '')} {rec1.get('CUSNML', '')}")
            print(f"             Phone: {rec1.get('TELENO', 'N/A')}")
            print(f"             Email: {rec1.get('MAILID', 'N/A')}")
            print(f"             DOB:   {rec1.get('CUSDOB', 'N/A')}")
            print(f"             City:  {rec1.get('CITYNM', 'N/A')}")
            
            print(f"  Record {idx2}: {rec2.get('CUSNMF', '')} {rec2.get('CUSNML', '')}")
            print(f"             Phone: {rec2.get('TELENO', 'N/A')}")
            print(f"             Email: {rec2.get('MAILID', 'N/A')}")
            print(f"             DOB:   {rec2.get('CUSDOB', 'N/A')}")
            print(f"             City:  {rec2.get('CITYNM', 'N/A')}")
        
        if len(candidates) > 20:
            print(f"\n... and {len(candidates) - 20} more pairs")
    
    print("\n" + "=" * 80)
    print("CONFIGURATION BY DATASET SIZE")
    print("=" * 80)
    print(f"{'Dataset':<15} | {'Config':<60}")
    print("-" * 80)
    print(f"{'<100K':<15} | use_selective_rules=False, max_block_size=1000")
    print(f"{'100K-500K':<15} | use_selective_rules=True, max_block_size=100")
    print(f"{'500K-2M':<15} | use_selective_rules=True, max_block_size=50 ⭐")
    print(f"{'>2M':<15} | Batch processing, max_block_size=20")
    print("=" * 80)
    
    blocker.print_statistics()
