import psycopg2

def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="ecommerce_db",
        user="admin",
        password="password"
    )

def test_fact_sales_exists():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM warehouse.fact_sales
    """)
    assert cur.fetchone()[0] > 0
    conn.close()

def test_surrogate_keys_used():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT customer_key FROM warehouse.fact_sales LIMIT 1
    """)
    assert cur.fetchone()[0] is not None
    conn.close()
