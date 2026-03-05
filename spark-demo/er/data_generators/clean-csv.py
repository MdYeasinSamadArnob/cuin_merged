import pandas as pd

# Read CSV (pandas handles messy quotes)
df = pd.read_csv("/mnt/c/Users/MdRajibHossainPavel/cuin/cust_100.csv",
                 dtype=str,
                 keep_default_na=False)

# Remove problematic quotes and trim spaces
df = df.apply(lambda col: col.astype(str).str.replace('"','').str.strip())

# Save cleaned CSV
df.to_csv("/mnt/c/Users/MdRajibHossainPavel/cuin/cust_100_clean.csv",
          index=False,
          quoting=1)  # quoting=1 -> quote all
