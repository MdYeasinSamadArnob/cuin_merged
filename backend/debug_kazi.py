import sys
import os

# Add backend to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from engine.matching.splink_engine import SplinkScorer
from engine.structures import ScoringConfig

# Mock Records: Kazi variations
# Case 1: Same DOB, Same Address, Different Phone Ext
rec_a = {
    "source_customer_id": "00050005",
    "name_norm": "KAZI MASIHUR RAHMAN",
    "email_norm": "kazi.masihur@example.com",
    "phone_norm": "5642170805", # Adjusted manually to simulate normalized
    "dob_norm": "1956-06-11",
    "address_norm": "901 TAYLOR MOUNTAIN GARCIASTAD",
}

rec_b = {
    "source_customer_id": "00050006",
    "name_norm": "KAZI MASIHUR RAHMAN", # Exact Name
    "email_norm": "kazi.masihur@example.com",
    "phone_norm": "3159430391", # Totally different phone (as per CSV sample 00050006 alt phone)
    "dob_norm": "1956-06-11", # Exact DOB
    "address_norm": "901 TAYLOR MOUNTAIN GARCIASTAD", # Exact Address
}

# Case 2: Slightly different Name (typo), Same DOB
rec_c = {
    "source_customer_id": "999",
    "name_norm": "KAZI MASHIRUL RAHMAN", # Typo
    "email_norm": "unknown@example.com",
    "phone_norm": "0000000000",
    "dob_norm": "1956-06-11",
    "address_norm": "901 TAYLOR MOUNTAIN GARCIASTAD",
}

config = ScoringConfig(auto_link_threshold=0.92)
scorer = SplinkScorer(config)

print("--- Case 1: Exact Name, Diff Phone, Same DOB/Addr ---")
score1 = scorer.score_pair("pair1", rec_a, rec_b)
print(f"Score: {score1.score:.4f}")
print(f"Signals: {score1.signals_hit}")


print("\n--- Case 2: Typo Name, Diff Phone, Same DOB/Addr ---")
score2 = scorer.score_pair("pair2", rec_a, rec_c)
print(f"Score: {score2.score:.4f}")
print(f"Signals: {score2.signals_hit}")

# Case 3: Kazi vs Mohi (The Problem Case)
rec_mohi = {
    "source_customer_id": "00050000",
    "name_norm": "MD MOHI UDDIN",
    "email_norm": "mohi.uddin@example.com",
    "phone_norm": "4306391171",
    "dob_norm": "1971-12-30",
    "address_norm": "489 ERIC TRACK LAKE CRYSTALBURY",
}
print("\n--- Case 3: Kazi vs Mohi ---")
score3 = scorer.score_pair("pair3", rec_a, rec_mohi)
print(f"Score: {score3.score:.4f}")
print(f"Signals: {score3.signals_hit}")
print(f"Hard Conflicts: {score3.hard_conflicts}")

print(f"Evidence: {[e.explanation for e in score3.evidence]}")

# Case 4: Mohi vs Mohi (Fragmentation Check)
# 00050000: MD MOHI UDDIN, 489 Eric Track...
# 00050001: MOHAMMAD MOHI UDDIN, 489 Eric Track... (Same Addr!)
# Wait, grep showed "489 Eric Track" for both!
# Let's verify if they link.
rec_mohi_1 = {
    "source_customer_id": "00050000",
    "name_norm": "MD MOHI UDDIN",
    "email_norm": "mohi.uddin@example.com",
    "phone_norm": "4306391171",
    "dob_norm": "1971-12-30",
    "address_norm": "489 ERIC TRACK LAKE CRYSTALBURY",
}
rec_mohi_2 = {
    "source_customer_id": "00050001",
    "name_norm": "MOHAMMAD MOHI UDDIN",
    "email_norm": "mohi.uddin@example.com",
    "phone_norm": "4306391171", # Same phone (normalized)
    "dob_norm": "1971-12-30",
    "address_norm": "489 ERIC TRACK LAKE CRYSTALBURY",
}
# These SHOULD link easily because exact Phone/Email match!
# Why did user say "many mohiuddin"?
# Maybe checking a pair WITHOUT shared phone?
# 00050024: MOHAMMAD MOHI UDDIN... mohi.uddin@gmail.com (Different Email!)
rec_mohi_3 = {
    "source_customer_id": "00050024",
    "name_norm": "MOHAMMAD MOHI UDDIN",
    "email_norm": "mohi.uddin@gmail.com",
    "phone_norm": "4306391171", # Matches phone!
    "dob_norm": "1971-12-30",
    "address_norm": "489 ERIC TRACK LAKE CRYSTALBURY",
}
# If Phone matches, they should link.

print("\n--- Case 4: Mohi 1 vs Mohi 2 (Exact Strong ID) ---")
score4 = scorer.score_pair("pair4", rec_mohi_1, rec_mohi_2)
print(f"Score: {score4.score:.4f}")


print("\n--- Case 5: Mohi 1 vs Mohi 3 (Diff Email, Shared Phone) ---")
score5 = scorer.score_pair("pair5", rec_mohi_1, rec_mohi_3)
print(f"Score: {score5.score:.4f}")

# Case 6: Mohi vs Mohi (Name Only Match - No shared IDs)
rec_mohi_4 = {
    "source_customer_id": "9999",
    "name_norm": "MD MOHI UDDIN", # Exact Name (0.81 vs Mohammad Mohi Uddin)
    "email_norm": "different@example.com",
    "phone_norm": "0000000000",
    "dob_norm": "1971-12-30", # Same DOB to avoid conflict
    "address_norm": "TOTALLY DIFFERENT ADDRESS",
}
# Sim(MD MOHI UDDIN, MOHAMMAD MOHI UDDIN) = 0.81
# Should trigger name_power boost -> 0.95
print("\n--- Case 6: Mohi 2 vs Mohi 4 (Name Only, Sim ~0.81) ---")
score6 = scorer.score_pair("pair6", rec_mohi_2, rec_mohi_4)
print(f"Score: {score6.score:.4f}")
print(f"Signals: {score6.signals_hit}")



