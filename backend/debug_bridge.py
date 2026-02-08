import csv
import sys
import os

# Add backend to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from engine.matching.splink_engine import SplinkScorer
from engine.structures import ScoringConfig

# Load Data
records = []
try:
    with open('backend/data/challenging_er_200.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize basic fields for comparison
            raw_phone = (row.get('MOBLNO') or row.get('TELENO') or '').lower()
            if 'x' in raw_phone: raw_phone = raw_phone.split('x')[0]
            phone_digits = "".join(filter(str.isdigit, raw_phone))
            if len(phone_digits) == 11 and phone_digits.startswith('1'): phone_digits = phone_digits[1:]

            records.append({
                "id": row.get('CUSCOD'),
                "name": f"{row.get('CUSNMF', '')} {row.get('CUSNML', '')}".strip().upper(),
                "phone": phone_digits,
                "email": (row.get('MAILID') or '').strip().lower(),
                "natid": row.get('NATLID'),
                "raw": row
            })
except FileNotFoundError:
    print("File not found")
    sys.exit(1)

kazis = [r for r in records if "KAZI" in r['name']]
mohis = [r for r in records if "MOHI" in r['name']]

print(f"Found {len(kazis)} Kazis and {len(mohis)} Mohis")

# Check for ANY shared identifiers
found_bridge = False
for k in kazis:
    for m in mohis:
        reasons = []
        if k['phone'] and k['phone'] == m['phone']: reasons.append(f"Phone {k['phone']}")
        if k['email'] and k['email'] == m['email']: reasons.append(f"Email {k['email']}")
        if k['natid'] and k['natid'] == m['natid']: reasons.append(f"NID {k['natid']}")
        
        if reasons:
            print(f"BRIDGE FOUND! {k['id']} ({k['name']}) <-> {m['id']} ({m['name']})")
            print(f"  Reasons: {reasons}")
            found_bridge = True

if not found_bridge:
    print("No direct bridge found via simple ID match.")
    
    # Check for Score-based Bridge?
    # Maybe use Scorer?
    scorer = SplinkScorer(ScoringConfig(auto_link_threshold=0.75)) # Low threshold
    
    print("Checking for Score-based High Matches...")
    for k in kazis:
        for m in mohis:
            # Construct mock records for scorer
            ra = {
                "source_customer_id": k['id'],
                "name_norm": k['name'],
                "email_norm": k['email'],
                "phone_norm": k['phone'],
                "dob_norm": k['raw'].get('CUSDOB'),
                "natid_norm": k['natid'],
                "address_norm": f"{k['raw'].get('ADDRS1')} {k['raw'].get('CITYNM')}".upper(),
            }
            rb = {
                "source_customer_id": m['id'],
                "name_norm": m['name'],
                "email_norm": m['email'],
                "phone_norm": m['phone'],
                "dob_norm": m['raw'].get('CUSDOB'),
                "natid_norm": m['natid'],
                "address_norm": f"{m['raw'].get('ADDRS1')} {m['raw'].get('CITYNM')}".upper(),
            }
            
            score = scorer.score_pair("test", ra, rb)
            if score.score > 0.6: # Check even low matches
                 print(f"HIGH SCORE: {score.score:.4f} | {k['name']} <-> {m['name']}")
                 print(f"  Signals: {score.signals_hit}")
