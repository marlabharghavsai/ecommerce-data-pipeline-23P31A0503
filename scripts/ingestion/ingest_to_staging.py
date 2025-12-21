import os
import json
import time
import logging
from datetime import datetime, timezone

import psycopg2
import yaml
from psycopg2 import sql

# --------------------------------------------------
# Paths & Configuration
# --------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
RAW_DATA_DIR = os.path.join(BASE_DIR, "data", "raw")
STAGING_DIR = os.path.join(BASE_DIR, "data", "staging")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# --------------------------------------------------
# Logging setup
# --------------------------------------------------
log_file = os.path.join(
    LOG_DIR, f"staging_ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# --------------------------------------------------
# Load configuration
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

TABLE_FILE_MAP = {
    "staging.customers": "customers.csv",
    "staging.products": "products.csv",
    "staging.transactions": "transactions.csv",
    "staging.transaction_items": "transaction_items.csv",
}

# --------------------------------------------------
# COPY column mapping (CRITICAL)
# --------------------------------------------------
COPY_COLUMNS = {
    "staging.customers": [
        "customer_id", "first_name", "last_name", "email", "phone",
        "registration_date", "city", "state", "country", "age_group"
    ],
    "staging.products": [
        "product_id", "product_name", "category", "sub_category",
        "price", "cost", "brand", "stock_quantity", "supplier_id"
    ],
    "staging.transactions": [
        "transaction_id", "customer_id", "transaction_date",
        "transaction_time", "payment_method", "shipping_address", "total_amount"
    ],
    "staging.transaction_items": [
        "item_id", "transaction_id", "product_id",
        "quantity", "unit_price", "discount_percentage", "line_total"
    ],
}

# --------------------------------------------------
# Database connection
# --------------------------------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# Bulk COPY loader
# --------------------------------------------------
def copy_csv_to_table(cursor, table_name, csv_path):
    columns = COPY_COLUMNS[table_name]

    with open(csv_path, "r", encoding="utf-8") as f:
        next(f)  # skip CSV header
        cursor.copy_expert(
            sql.SQL("COPY {} ({}) FROM STDIN WITH CSV").format(
                sql.Identifier(*table_name.split(".")),
                sql.SQL(", ").join(map(sql.Identifier, columns))
            ),
            f
        )

# --------------------------------------------------
# Validation
# --------------------------------------------------
def validate_staging_load(cursor):
    results = {}

    for table, csv_file in TABLE_FILE_MAP.items():
        csv_path = os.path.join(RAW_DATA_DIR, csv_file)
        csv_count = sum(1 for _ in open(csv_path, encoding="utf-8")) - 1

        cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(*table.split("."))
            )
        )
        db_count = cursor.fetchone()[0]

        results[table] = {
            "csv_rows": csv_count,
            "db_rows": db_count,
            "status": "success" if csv_count == db_count else "mismatch"
        }

    return results

# --------------------------------------------------
# Main ingestion logic
# --------------------------------------------------
def main():
    start_time = time.time()
    summary = {
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "tables_loaded": {},
        "total_execution_time_seconds": 0.0
    }

    conn = None
    cursor = None

    try:
        conn = get_connection()
        conn.autocommit = False
        cursor = conn.cursor()

        logging.info("Connected to PostgreSQL")

        # -------------------------------
        # Truncate staging tables
        # -------------------------------
        for table in TABLE_FILE_MAP:
            logging.info(f"Truncating {table}")
            cursor.execute(
                sql.SQL("TRUNCATE {}").format(
                    sql.Identifier(*table.split("."))
                )
            )

        # -------------------------------
        # Bulk load CSVs
        # -------------------------------
        for table, csv_file in TABLE_FILE_MAP.items():
            csv_path = os.path.join(RAW_DATA_DIR, csv_file)

            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"Missing CSV file: {csv_file}")

            logging.info(f"Loading {csv_file} into {table}")
            copy_csv_to_table(cursor, table, csv_path)

            summary["tables_loaded"][table] = {
                "rows_loaded": sum(1 for _ in open(csv_path, encoding="utf-8")) - 1,
                "status": "success",
                "error_message": None
            }

        # -------------------------------
        # Validation
        # -------------------------------
        validation_results = validate_staging_load(cursor)
        for table, result in validation_results.items():
            if result["status"] != "success":
                raise ValueError(f"Row count mismatch in {table}")

        conn.commit()
        logging.info("Transaction committed successfully")

    except Exception as e:
        logging.error(f"Ingestion failed: {e}")

        if conn:
            conn.rollback()
            logging.error("Transaction rolled back")

        for table in TABLE_FILE_MAP:
            summary["tables_loaded"].setdefault(table, {
                "rows_loaded": 0,
                "status": "failed",
                "error_message": str(e)
            })

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    summary["total_execution_time_seconds"] = round(time.time() - start_time, 2)

    summary_path = os.path.join(STAGING_DIR, "ingestion_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=4)

    logging.info(f"Ingestion summary written to {summary_path}")
    logging.info("Staging ingestion completed")

# --------------------------------------------------
if __name__ == "__main__":
    main()
