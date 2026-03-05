import oracledb
import pandas as pd
import csv  # ✅ needed for quoting

# ---------- Configuration ----------
ORACLE_USER = "stlbas"
ORACLE_PASSWORD = "STLBAS"
ORACLE_DSN  = "10.11.200.21:1527/STLBAS"
csv_file    = "cust_100.csv"

fields = [
    "CUSCOD", "CUSTYP", "CUSNMF", "CUSNML", "CUSDOB",
    "ADDRS1", "ADDRS2", "ADDRS3", "ADDRS4",
    "CITYNM", "TELENO", "MOBLNO", "TELXNO", "FAXNO", "MAILID",
    "SPONAM", "GENDER", "CUSSTS", "NATLID",
    "TIMSTAMP", "OPRBRA"
]

# ---------- Connect to Oracle ----------
print("Connecting to Oracle...")
conn = oracledb.connect(
    user=ORACLE_USER,
    password=ORACLE_PASSWORD,
    dsn=ORACLE_DSN,
    mode=oracledb.DEFAULT_AUTH
)

cursor = conn.cursor()

# ---------- Build SQL query ----------
query = f"""
SELECT {', '.join(fields)}
FROM stcusmas
WHERE ROWNUM <= 100
"""
cursor.execute(query)

# Fetch all rows
rows = cursor.fetchall()
columns = [col[0] for col in cursor.description]

# Create DataFrame
df = pd.DataFrame(rows, columns=columns)

# ---------- Clean data ----------
df = df.fillna('')  # Replace NULLs with empty string
df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# ---------- Save as CSV ----------
df.to_csv(csv_file, index=False, quoting=csv.QUOTE_ALL)

print(f"CSV exported successfully: {csv_file}")

# ---------- Close connection ----------
cursor.close()
conn.close()
