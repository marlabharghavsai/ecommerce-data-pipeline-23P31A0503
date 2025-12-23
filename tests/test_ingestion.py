import os
import psycopg2

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "admin"),
        password=os.getenv("DB_PASSWORD", "password"),
    )

def test_staging_tables_exist():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging'
    """)
    tables = {row[0] for row in cur.fetchall()}
    expected = {"customers", "products", "transactions", "transaction_items"}
    assert expected.issubset(tables)
    conn.close()
