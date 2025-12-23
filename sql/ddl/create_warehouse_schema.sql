CREATE SCHEMA IF NOT EXISTS warehouse;

-- =========================
-- DIM DATE
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    day INT,
    month_name VARCHAR(20),
    day_name VARCHAR(20),
    week_of_year INT,
    is_weekend BOOLEAN
);

-- =========================
-- DIM PAYMENT METHOD
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.dim_payment_method (
    payment_method_key SERIAL PRIMARY KEY,
    payment_method_name VARCHAR(50),
    payment_type VARCHAR(20)
);

-- =========================
-- DIM CUSTOMERS (SCD TYPE 2)
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20),
    full_name VARCHAR(120),
    email VARCHAR(100),
    city VARCHAR(60),
    state VARCHAR(60),
    country VARCHAR(60),
    age_group VARCHAR(20),
    customer_segment VARCHAR(20),
    registration_date DATE,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- =========================
-- DIM PRODUCTS (SCD TYPE 2)
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(60),
    brand VARCHAR(100),
    price_range VARCHAR(20),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

-- =========================
-- FACT SALES
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    date_key INT REFERENCES warehouse.dim_date(date_key),
    customer_key INT REFERENCES warehouse.dim_customers(customer_key),
    product_key INT REFERENCES warehouse.dim_products(product_key),
    payment_method_key INT REFERENCES warehouse.dim_payment_method(payment_method_key),
    transaction_id VARCHAR(20),
    quantity INT,
    unit_price DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    line_total DECIMAL(12,2),
    profit DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- AGG TABLES
-- =========================
CREATE TABLE IF NOT EXISTS warehouse.agg_daily_sales (
    date_key INT PRIMARY KEY,
    total_transactions INT,
    total_revenue DECIMAL(14,2),
    total_profit DECIMAL(14,2),
    unique_customers INT
);

CREATE TABLE IF NOT EXISTS warehouse.agg_product_performance (
    product_key INT PRIMARY KEY,
    total_quantity_sold INT,
    total_revenue DECIMAL(14,2),
    total_profit DECIMAL(14,2),
    avg_discount_percentage DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS warehouse.agg_customer_metrics (
    customer_key INT PRIMARY KEY,
    total_transactions INT,
    total_spent DECIMAL(14,2),
    avg_order_value DECIMAL(14,2),
    last_purchase_date DATE
);
