import psycopg2
from datetime import date, timedelta

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="ecommerce_db",
    user="admin",
    password="password"
)
cur = conn.cursor()

print("Loading warehouse...")

# -------------------------
# DIM DATE
# -------------------------
start = date(2024,1,1)
end = date(2024,12,31)
d = start

while d <= end:
    cur.execute("""
        INSERT INTO warehouse.dim_date
        (date_key, full_date, year, quarter, month, day,
         month_name, day_name, week_of_year, is_weekend)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (date_key) DO NOTHING
    """, (
        int(d.strftime("%Y%m%d")),
        d,
        d.year,
        (d.month-1)//3+1,
        d.month,
        d.day,
        d.strftime("%B"),
        d.strftime("%A"),
        int(d.strftime("%W")),
        d.weekday() >= 5
    ))
    d += timedelta(days=1)

# -------------------------
# DIM PAYMENT METHOD
# -------------------------
cur.execute("""
INSERT INTO warehouse.dim_payment_method (payment_method_name, payment_type)
SELECT DISTINCT payment_method,
CASE WHEN payment_method='Cash on Delivery'
     THEN 'Offline' ELSE 'Online' END
FROM production.transactions
ON CONFLICT DO NOTHING
""")

# -------------------------
# DIM CUSTOMERS (SCD2 SIMPLE)
# -------------------------
cur.execute("""
INSERT INTO warehouse.dim_customers
(customer_id, full_name, email, city, state, country, age_group,
 customer_segment, registration_date, effective_date, end_date, is_current)
SELECT
c.customer_id,
c.first_name||' '||c.last_name,
c.email,
c.city,
c.state,
c.country,
c.age_group,
'Regular',
c.registration_date,
CURRENT_DATE,
NULL,
TRUE
FROM production.customers c
""")

# -------------------------
# DIM PRODUCTS (SCD2 SIMPLE)
# -------------------------
cur.execute("""
INSERT INTO warehouse.dim_products
(product_id, product_name, category, sub_category, brand,
 price_range, effective_date, end_date, is_current)
SELECT
p.product_id,
p.product_name,
p.category,
p.sub_category,
p.brand,
p.price_category,
CURRENT_DATE,
NULL,
TRUE
FROM production.products p
""")

# -------------------------
# FACT SALES
# -------------------------
cur.execute("""
INSERT INTO warehouse.fact_sales
(date_key, customer_key, product_key, payment_method_key,
 transaction_id, quantity, unit_price,
 discount_amount, line_total, profit)
SELECT
dd.date_key,
dc.customer_key,
dp.product_key,
pm.payment_method_key,
ti.transaction_id,
ti.quantity,
ti.unit_price,
(ti.unit_price*ti.quantity*(ti.discount_percentage/100)),
ti.line_total,
ti.line_total - (p.cost*ti.quantity)
FROM production.transaction_items ti
JOIN production.transactions t ON ti.transaction_id=t.transaction_id
JOIN production.products p ON ti.product_id=p.product_id
JOIN warehouse.dim_date dd ON dd.full_date=t.transaction_date
JOIN warehouse.dim_customers dc ON dc.customer_id=t.customer_id AND dc.is_current=TRUE
JOIN warehouse.dim_products dp ON dp.product_id=ti.product_id AND dp.is_current=TRUE
JOIN warehouse.dim_payment_method pm ON pm.payment_method_name=t.payment_method
""")

# -------------------------
# AGGREGATES
# -------------------------
cur.execute("""
INSERT INTO warehouse.agg_daily_sales
SELECT date_key,
COUNT(DISTINCT transaction_id),
SUM(line_total),
SUM(profit),
COUNT(DISTINCT customer_key)
FROM warehouse.fact_sales
GROUP BY date_key
""")

conn.commit()
cur.close()
conn.close()

print("Warehouse load completed")
