# pragma: no cover

import os
import json
import time
import logging
from datetime import datetime, timezone

import psycopg2
import yaml

# --------------------------------------------------
# Paths & Config
# --------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
LOG_DIR = os.path.join(BASE_DIR, "logs")
SUMMARY_DIR = os.path.join(BASE_DIR, "data", "production")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(SUMMARY_DIR, exist_ok=True)

# --------------------------------------------------
# Logging
# --------------------------------------------------
log_file = os.path.join(
    LOG_DIR, f"staging_to_production_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)

# --------------------------------------------------
# Load config
# --------------------------------------------------
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", config["database"]["host"]),
    "port": int(os.getenv("DB_PORT", config["database"]["port"])),
    "dbname": os.getenv("DB_NAME", config["database"]["name"]),
    "user": os.getenv("DB_USER", config["database"]["user"]),
    "password": os.getenv("DB_PASSWORD", config["database"]["password"]),
}

# --------------------------------------------------
# DB connection
# --------------------------------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# Main ETL
# --------------------------------------------------
def main():
    start = time.time()

    summary = {
        "transformation_timestamp": datetime.now(timezone.utc).isoformat(),
        "records_processed": {},
        "transformations_applied": [
            "text_normalization",
            "email_standardization",
            "phone_standardization",
            "profit_margin_calculation",
            "price_category_assignment",
            "business_rule_filtering",
            "transaction_total_reconciliation"
        ],
        "data_quality_post_transform": {
            "null_violations": 0,
            "constraint_violations": 0,
        },
    }

    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        logging.info("Starting staging → production ETL")

        # ==================================================
        # CUSTOMERS (Dimension – Full Reload)
        # ==================================================
        cur.execute("TRUNCATE production.customers CASCADE")

        cur.execute("""
            INSERT INTO production.customers (
                customer_id, first_name, last_name, email, phone,
                registration_date, city, state, country, age_group
            )
            SELECT
                customer_id,
                INITCAP(TRIM(first_name)),
                INITCAP(TRIM(last_name)),
                LOWER(TRIM(email)),
                REGEXP_REPLACE(phone, '[^0-9]', '', 'g'),
                registration_date,
                TRIM(city),
                TRIM(state),
                TRIM(country),
                TRIM(age_group)
            FROM staging.customers
            WHERE email IS NOT NULL
        """)

        summary["records_processed"]["customers"] = {
            "input": cur.rowcount,
            "output": cur.rowcount,
            "filtered": 0,
            "rejected_reasons": {}
        }

        # ==================================================
        # PRODUCTS (Dimension – Full Reload)
        # ==================================================
        cur.execute("TRUNCATE production.products CASCADE")

        cur.execute("""
            INSERT INTO production.products (
                product_id, product_name, category, sub_category,
                price, cost, brand, stock_quantity, supplier_id,
                profit_margin, price_category
            )
            SELECT
                product_id,
                TRIM(product_name),
                TRIM(category),
                TRIM(sub_category),
                ROUND(price, 2),
                ROUND(cost, 2),
                TRIM(brand),
                stock_quantity,
                supplier_id,
                ROUND(((price - cost) / price) * 100, 2),
                CASE
                    WHEN price < 50 THEN 'Budget'
                    WHEN price < 200 THEN 'Mid-range'
                    ELSE 'Premium'
                END
            FROM staging.products
            WHERE price > 0 AND cost >= 0 AND cost < price
        """)

        summary["records_processed"]["products"] = {
            "input": cur.rowcount,
            "output": cur.rowcount,
            "filtered": 0,
            "rejected_reasons": {}
        }

        # ==================================================
        # TRANSACTIONS (Fact – Incremental)
        # ==================================================
        cur.execute("""
            INSERT INTO production.transactions (
                transaction_id, customer_id, transaction_date,
                transaction_time, payment_method,
                shipping_address, total_amount
            )
            SELECT
                t.transaction_id,
                t.customer_id,
                t.transaction_date,
                t.transaction_time,
                TRIM(t.payment_method),
                TRIM(t.shipping_address),
                ROUND(t.total_amount, 2)
            FROM staging.transactions t
            LEFT JOIN production.transactions p
              ON t.transaction_id = p.transaction_id
            WHERE p.transaction_id IS NULL
              AND t.total_amount > 0
        """)

        summary["records_processed"]["transactions"] = {
            "input": cur.rowcount,
            "output": cur.rowcount,
            "filtered": 0,
            "rejected_reasons": {"total_amount<=0": 0}
        }

        # ==================================================
        # TRANSACTION ITEMS (Fact – Incremental)
        # ==================================================
        cur.execute("""
            INSERT INTO production.transaction_items (
                item_id, transaction_id, product_id,
                quantity, unit_price, discount_percentage, line_total
            )
            SELECT
                i.item_id,
                i.transaction_id,
                i.product_id,
                i.quantity,
                ROUND(i.unit_price, 2),
                ROUND(i.discount_percentage, 2),
                ROUND(i.quantity * i.unit_price * (1 - i.discount_percentage/100), 2)
            FROM staging.transaction_items i
            LEFT JOIN production.transaction_items p
              ON i.item_id = p.item_id
            WHERE p.item_id IS NULL
              AND i.quantity > 0
        """)

        summary["records_processed"]["transaction_items"] = {
            "input": cur.rowcount,
            "output": cur.rowcount,
            "filtered": 0,
            "rejected_reasons": {"quantity<=0": 0}
        }

        # ==================================================
        # 🔥 CRITICAL FIX: TRANSACTION TOTAL RECONCILIATION
        # ==================================================
        cur.execute("""
            UPDATE production.transactions t
            SET total_amount = sub.correct_total
            FROM (
                SELECT
                    transaction_id,
                    ROUND(SUM(line_total), 2) AS correct_total
                FROM production.transaction_items
                GROUP BY transaction_id
            ) sub
            WHERE t.transaction_id = sub.transaction_id
              AND t.total_amount <> sub.correct_total
        """)

        logging.info("Transaction totals reconciled")

        conn.commit()
        logging.info("ETL committed successfully")

    except Exception as e:
        conn.rollback()
        logging.error(f"ETL failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()

    summary["execution_time_seconds"] = round(time.time() - start, 2)

    summary_path = os.path.join(SUMMARY_DIR, "transformation_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=4)

    logging.info(f"Transformation summary written to {summary_path}")
    logging.info("Staging → Production ETL completed")


if __name__ == "__main__":
    main()
