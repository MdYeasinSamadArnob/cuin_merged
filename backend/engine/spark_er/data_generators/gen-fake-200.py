import pandas as pd
import random
from faker import Faker

fake = Faker()
Faker.seed(42)

fields = [
    "CUSCOD","CUSTYP","CUSNMF","CUSNML","CUSDOB",
    "ADDRS1","ADDRS2","ADDRS3","ADDRS4","CITYNM",
    "TELENO","MOBLNO","TELXNO","FAXNO","MAILID",
    "SPONAM","GENDER","CUSSTS","NATLID","TIMSTAMP","OPRBRA"
]

rows = []

# Base person records for creating duplicates
base_persons = [
    {
        "first": "MD MOHI UDDIN", "last": "", "dob": "1971-12-30",
        "addr1": "489 Eric Track", "addr2": "Apt. 880", "city": "Lake Crystalbury",
        "phone": "430-639-1171", "email": "mohi.uddin@example.com"
    },
    {
        "first": "GOLAM MOHD ZUBAYED A", "last": "SHRAF", "dob": "1987-09-06",
        "addr1": "433 Jill Springs", "addr2": "Apt. 960", "city": "Franciscostad",
        "phone": "283-486-3794", "email": "golam.zubayed@example.com"
    },
    {
        "first": "SK MAHBUBLLAH", "last": "KAISA", "dob": "1965-10-02",
        "addr1": "192 Frank Light", "addr2": "Suite 835", "city": "Teresaburgh",
        "phone": "653-876-7242", "email": "sk.mahbub@example.com"
    },
    {
        "first": "KAZI MASIHUR", "last": "RAHMAN", "dob": "1956-06-11",
        "addr1": "901 Taylor Mountain", "addr2": "Apt. 046", "city": "Garciastad",
        "phone": "564-217-0805", "email": "kazi.masihur@example.com"
    },
]

# Variation distribution pattern for creating realistic duplicates
# Pattern: exact, minor, moderate, major, moderate
# This creates a good mix for testing entity resolution with varying similarity scores
VARIATION_PATTERN = [0, 1, 2, 3, 2]

# Prime number for deterministic person selection to ensure even distribution
PERSON_SELECTION_MULTIPLIER = 7

def create_duplicate_with_variations(base_person, variation_level=0):
    """
    Create a record that's a duplicate of base_person with controlled variations.
    variation_level: 0=exact, 1=minor, 2=moderate, 3=major variations
    """
    first_name = base_person["first"]
    last_name = base_person["last"]
    dob = base_person["dob"]
    addr1 = base_person["addr1"]
    addr2 = base_person["addr2"]
    city = base_person["city"]
    phone = base_person["phone"]
    email = base_person["email"]
    
    # Apply variations based on level
    if variation_level >= 1:
        # Minor variations: abbreviations, formatting
        # Use word boundary to avoid replacing MD/MOHD within other words
        if first_name.startswith("MD ") and random.random() < 0.5:
            first_name = first_name.replace("MD ", "MOHAMMAD ", 1)
        if "MOHD " in first_name and random.random() < 0.3:
            first_name = first_name.replace("MOHD ", "MOHAMMED ", 1)
        
        # Phone formatting variations
        phone_rand = random.random()
        if phone_rand < 0.5:
            phone = f"+1-{phone}x{random.randint(1000,9999)}"
        elif phone_rand < 0.75:
            phone = f"001-{phone}x{random.randint(100,999)}"
    
    if variation_level >= 2:
        # Moderate variations: typos, different apt numbers
        if addr2 and random.random() < 0.4:
            # Change apartment number slightly
            if "Apt" in addr2:
                new_apt = random.randint(100, 999)
                addr2 = f"Apt. {new_apt}"
            elif "Suite" in addr2:
                new_suite = random.randint(100, 999)
                addr2 = f"Suite {new_suite}"
        
        # Email variations
        email_rand = random.random()
        if email_rand < 0.3:
            email = email.replace("@example.com", "@gmail.com")
        elif email_rand < 0.5:
            email = ""  # Missing email
    
    if variation_level >= 3:
        # Major variations: missing fields
        missing_rand = random.random()
        if missing_rand < 0.3 and dob == base_person["dob"]:
            dob = ""  # Missing DOB
        elif missing_rand < 0.6:
            # Only clear email if it hasn't been cleared by level 2
            # Check both original and gmail variations
            if email and (email == base_person["email"] or "@gmail.com" in email):
                email = ""
    
    return {
        "first": first_name,
        "last": last_name,
        "dob": dob,
        "addr1": addr1,
        "addr2": addr2,
        "city": city,
        "phone": phone,
        "email": email
    }

# Generate 200 records with controlled duplicates
# Create 40 groups of 5 records each (where each group has duplicates)
record_count = 0
for group_idx in range(40):
    # Pick a base person using deterministic distribution
    # Using prime multiplier ensures even distribution across all base persons
    base_person = base_persons[(group_idx * PERSON_SELECTION_MULTIPLIER) % len(base_persons)]
    
    # Create 5 variations of this person
    for var_idx in range(5):
        person = create_duplicate_with_variations(base_person, variation_level=VARIATION_PATTERN[var_idx])
        
        # Generate other random fields
        custyp = random.choice(["REG", "COR", "STF"])
        sponsor = fake.name() if random.random() < 0.3 else ""
        gender = random.choice(["M", "F", "C"])
        cussts = random.choice(["ACT", "INACT", "SUSP"])
        natlid = str(random.randint(1000000, 9999999))
        timestamp = fake.date_time_between(start_date="-20y", end_date="now")
        branch = str(random.randint(1, 50)).zfill(3)
        fax = fake.phone_number() if random.random() < 0.5 else ""
        state = fake.state()
        
        rows.append([
            str(50000 + record_count).zfill(8),
            custyp,
            person["first"],
            person["last"],
            person["dob"],
            person["addr1"],
            person["addr2"],
            person["city"],  # Use controlled city for better matching
            state,
            person["city"],
            person["phone"],
            fake.phone_number(),  # Mobile can be different
            person["phone"],  # Telex same as phone
            fax,
            person["email"],
            sponsor,
            gender,
            cussts,
            natlid,
            timestamp,
            branch
        ])
        
        record_count += 1

df = pd.DataFrame(rows, columns=fields)
df.to_csv("csv/challenging_er_200.csv", index=False, quoting=1)

print(f"CSV with {len(rows)} challenging ER rows generated: csv/challenging_er_200.csv")
print(f"Generated {len(rows)//5} groups of ~5 potential duplicates each")
