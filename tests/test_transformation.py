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

def test_production_tables_populated():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM production.customers")
    count = cur.fetchone()[0]
    assert count > 0
    conn.close()

def test_email_lowercase():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM production.customers
        WHERE email <> LOWER(email)
    """)
    assert cur.fetchone()[0] == 0
    conn.close()
