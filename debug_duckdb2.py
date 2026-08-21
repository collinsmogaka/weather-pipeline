import duckdb
con = duckdb.connect()

file_path = "data/processed/weather/part-00000-d451e0dd-0faf-44ad-9abf-6f94bc541857-c000.snappy.parquet"

print("=== Reading exact file ===")
try:
    con.sql(f"SELECT COUNT(*) FROM read_parquet('{file_path}')").show()
except Exception as e:
    print("Error:", e)

print()
print("=== Sample data ===")
try:
    con.sql(f"SELECT * FROM read_parquet('{file_path}') LIMIT 5").show()
except Exception as e:
    print("Error:", e)
