
import sys
import os
sys.path.append('.')

from engine.normalize.standardize import normalize_name, normalize_address
from engine.blocking.multipass_blocker import MultiPassBlocker
from engine.matching.splink_engine import SplinkScorer
from engine.structures import ScoringConfig

# Test cases
pairs = [
    # 1. Exact Name + Exact Email overrides NatID (Previous Fix)
    (
        {
            "name": "MD MOHI UDDIN", 
            "id": "7", 
            "natid": "9085839", 
            "email": "mohi.uddin@example.com"
        },
        {
            "name": "MD MOHI UDDIN", 
            "id": "8", 
            "natid": "8775153", 
            "email": "mohi.uddin@example.com"
        }
    ),
    # 2. Fuzzy Name + Fuzzy Address overrides NatID (New Fix)
    # "MD MOHI UDDIN" vs "MOHAMMAD MOHI UDDIN" (Fuzzy 86%)
    # "489 ERIC TRACK..." vs "489 ERIC TRACK..." (Fuzzy 94%)
    (
        {
            "name": "MD MOHI UDDIN", 
            "id": "9", 
            "natid": "6855920",
            "address": "489 ERIC TRACK APT 888 LAKE CRYSTALBURY OHIO" 
        },
        {
            "name": "MOHAMMAD MOHI UDDIN", 
            "id": "10", 
            "natid": "9858906", # Mismatch
            "address": "489 ERIC TRACK APT 877 LAKE CRYSTALBURY MONTANA" # Similar address
        }
    )
]

print("--- Matching Test ---")
scorer = SplinkScorer()
for p1, p2 in pairs:
    n1 = {
        "name_norm": normalize_name(p1["name"]), 
        "source_customer_id": p1["id"],
        "natid_norm": p1.get("natid"),
        "email_norm": p1.get("email"),
        "address_norm": normalize_address(p1.get("address"))
    }
    n2 = {
        "name_norm": normalize_name(p2["name"]), 
        "source_customer_id": p2["id"],
        "natid_norm": p2.get("natid"),
        "email_norm": p2.get("email"),
        "address_norm": normalize_address(p2.get("address"))
    }
    
    score = scorer.score_pair("test", n1, n2)
    print(f"Pair: {n1['name_norm']} vs {n2['name_norm']}")
    if n1.get('address_norm'):
        print(f"  Addr: {n1['address_norm'][:20]}... vs {n2['address_norm'][:20]}...")
    
    print(f"  Score: {score.score:.4f}")
    if score.hard_conflicts:
        print(f"  HARD CONFLICTS: {score.hard_conflicts}")
    else:
        print(f"  No Hard Conflicts (Overridden!)")
    print("")
