import sys
import os

# Add backend to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from engine.matching.splink_engine import SplinkScorer
from engine.structures import ScoringConfig

# Mock Records: "MD MOHI UDDIN" vs "MOHAMMAD MOHI UDDIN"
# Assuming shared other attributes (Phone, Email) for a strong match
record_a = {
    "source_customer_id": "9002",
    "name_norm": "MD MOHI UDDIN",
    "email_norm": "mohi.uddin@example.com",
    "phone_norm": "5642170805",
    "dob_norm": "1971-12-30",
    "natid_norm": "ID_SHARED",
    "address_norm": "489 ERIC TRACK LAKE CRYSTALBURY",
}

record_b = {
    "source_customer_id": "00050001",
    "name_norm": "MOHAMMAD MOHI UDDIN", # Variant
    "email_norm": "mohi.uddin@example.com", # Exact match
    "phone_norm": "6512161559",            # Different phone
    "dob_norm": "1971-12-30",              # Exact match
    "natid_norm": None,                    # Missing NID
    "address_norm": "489 ERIC TRACK LAKE CRYSTALBURY", # Exact match
}

config = ScoringConfig(auto_link_threshold=0.92) # Default
scorer = SplinkScorer(config)

score = scorer.score_pair("test-pair", record_a, record_b)

print(f"Final Score: {score.score:.4f}")
print("Evidence:")
for ev in score.evidence:
    print(f"  - {ev.field_name}: {ev.explanation} (Score: {ev.similarity_score:.2f}, Weight: {ev.match_weight:.2f})")

print(f"\nAuto Link Threshold: {config.auto_link_threshold}")
print(f"Would Link? {'YES' if score.score >= config.auto_link_threshold else 'NO'}")
