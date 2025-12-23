import psycopg2

def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="ecommerce_db",
        user="admin",
        password="password"
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
