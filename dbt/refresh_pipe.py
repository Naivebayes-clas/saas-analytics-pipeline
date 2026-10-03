from snowflake.connector import connect

conn = connect(
    account="db89749.eu-west-2.aws",
    user="BARNAP",
    password="Halo89Qalo29Ko",
    role="TRANSFORMER",
    warehouse="TRANSFORMING",
)
cur = conn.cursor()
cur.execute("ALTER PIPE ANALYTICS.BRONZE.bronze_pipe REFRESH")
print("Snowpipe refreshed")
cur.close()
conn.close()   
