import duckdb
con = duckdb.connect()
print('=== Testing direct read ===')
try:
    result = con.sql("SELECT COUNT(*) FROM read_parquet('data/processed/weather/*.parquet')")
    result.show()
except Exception as e:
    print('Error:', e)

print()
print('=== Listing files DuckDB can see ===')
con.sql("SELECT * FROM glob('data/processed/weather/*')").show()
