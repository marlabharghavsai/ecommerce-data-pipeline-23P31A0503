import os
import time
import json
import psycopg2
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
SQL_FILE = os.path.join(BASE_DIR, "sql", "queries", "analytical_queries.sql")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed", "analytics")

os.makedirs(OUTPUT_DIR, exist_ok=True)

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "ecommerce_db",
    "user": "admin",
    "password": "password",
}

def execute_query(conn, query, filename):
    start = time.time()
    df = pd.read_sql(query, conn)
    df.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "execution_time_ms": round((time.time() - start) * 1000, 2)
    }

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    with open(SQL_FILE) as f:
        queries = f.read().split(";")

    summary = {
        "generation_timestamp": datetime.utcnow().isoformat(),
        "queries_executed": 10,
        "query_results": {},
        "total_execution_time_seconds": 0
    }

    start_all = time.time()

    for i, query in enumerate(queries):
        query = query.strip()
        if not query:
            continue
        result = execute_query(
            conn,
            query,
            f"query{i+1}.csv"
        )
        summary["query_results"][f"query{i+1}"] = result

    summary["total_execution_time_seconds"] = round(time.time() - start_all, 2)

    with open(os.path.join(OUTPUT_DIR, "analytics_summary.json"), "w") as f:
        json.dump(summary, f, indent=4)

    conn.close()
    print("✅ Analytics generated successfully")

if __name__ == "__main__":
    main()
