from snowflake.connector import connect

conn = connect(
    account="db89749.eu-west-2.aws",
    user="BARNAP",
    password="Halo89Qalo29Ko",
    role="TRANSFORMER",
    warehouse="TRANSFORMING",
)
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM ANALYTICS.BRONZE.fact_sessions")
rows = cur.fetchone()[0]
cur.close()
conn.close()

if rows < 100:
    raise Exception(f"FAIL: fact_sessions has only {rows} rows (expected >= 100)")
print(f"PASS: fact_sessions has {rows} rows")   
