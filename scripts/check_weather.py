import duckdb
import glob

con = duckdb.connect()

files = glob.glob("data/processed/weather/*.parquet")

print("=== Parquet files ===")
for f in files:
    print(f)

print()
print("=== Row count per file ===")

for f in files:
    result = con.execute(
        "SELECT COUNT(*) FROM read_parquet(?)",
        [f]
    ).fetchone()

    print(f"{f}: {result[0]} rows")

print()
print("=== Sample data ===")

for f in files:
    print(f"\nFile: {f}")

    con.execute(
        "SELECT * FROM read_parquet(?) LIMIT 5",
        [f]
    ).show()
