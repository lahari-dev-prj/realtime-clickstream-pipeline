"""
Quick sanity-check script: reads the aggregated results table from Postgres
and prints the top products by event volume in the most recent windows.

Usage:
    python analysis/query_results.py
"""
import pandas as pd
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="clickstream",
    user="de_user",
    password="de_pass",
)

query = """
    SELECT window_start, product_id, event_type, event_count, unique_users
    FROM product_activity_5min
    ORDER BY window_start DESC, event_count DESC
    LIMIT 25;
"""

df = pd.read_sql(query, conn)
print(df.to_string(index=False))
conn.close()
