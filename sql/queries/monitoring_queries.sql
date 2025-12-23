-- =========================================
-- QUERY 1: DATA FRESHNESS
-- =========================================
SELECT
    MAX(loaded_at) AS staging_latest
FROM staging.customers;

SELECT
    MAX(created_at) AS production_latest
FROM production.transactions;

SELECT
    MAX(created_at) AS warehouse_latest
FROM warehouse.fact_sales;

-- =========================================
-- QUERY 2: DAILY VOLUME (LAST 30 DAYS)
-- =========================================
SELECT
    dd.full_date,
    COUNT(fs.sales_key) AS daily_transactions
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date dd ON fs.date_key = dd.date_key
WHERE dd.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY dd.full_date
ORDER BY dd.full_date;

-- =========================================
-- QUERY 3: DATA QUALITY
-- =========================================
SELECT COUNT(*) AS orphan_items
FROM warehouse.fact_sales fs
LEFT JOIN warehouse.dim_customers dc
ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;

SELECT COUNT(*) AS null_revenue
FROM warehouse.fact_sales
WHERE line_total IS NULL;

-- =========================================
-- QUERY 4: DATABASE HEALTH
-- =========================================
SELECT COUNT(*) AS active_connections
FROM pg_stat_activity;

-- =========================================
-- QUERY 5: TABLE ROW COUNTS
-- =========================================
SELECT
    schemaname,
    relname,
    n_live_tup
FROM pg_stat_user_tables;
