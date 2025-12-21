import os
import json
import psycopg2
import yaml
from datetime import datetime, timezone

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
REPORT_DIR = os.path.join(BASE_DIR, "data", "quality")
os.makedirs(REPORT_DIR, exist_ok=True)

# -------------------------------
# Load config
# -------------------------------
with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", config["database"]["host"]),
    "port": int(os.getenv("DB_PORT", config["database"]["port"])),
    "dbname": os.getenv("DB_NAME", config["database"]["name"]),
    "user": os.getenv("DB_USER", config["database"]["user"]),
    "password": os.getenv("DB_PASSWORD", config["database"]["password"]),
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# -------------------------------
# Helper
# -------------------------------
def fetch_count(cursor, query):
    cursor.execute(query)
    return cursor.fetchone()[0]

# -------------------------------
# Quality Checks
# -------------------------------
def run_checks(cursor):
    results = {}

    # 1. Completeness
    nulls = {
        "customers.email": fetch_count(cursor, "SELECT COUNT(*) FROM production.customers WHERE email IS NULL"),
        "products.price": fetch_count(cursor, "SELECT COUNT(*) FROM production.products WHERE price IS NULL"),
    }
    results["null_checks"] = {
        "status": "passed" if sum(nulls.values()) == 0 else "failed",
        "tables_checked": list(nulls.keys()),
        "null_violations": sum(nulls.values()),
        "details": nulls,
    }

    # 2. Duplicates
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT email FROM production.customers GROUP BY email HAVING COUNT(*) > 1
        ) t;
    """)
    dup_emails = cursor.fetchone()[0]

    results["duplicate_checks"] = {
        "status": "passed" if dup_emails == 0 else "failed",
        "duplicates_found": dup_emails,
        "details": {"customers.email": dup_emails},
    }

    # 3. Referential integrity
    orphan_txn = fetch_count(cursor, """
        SELECT COUNT(*) FROM production.transactions t
        LEFT JOIN production.customers c ON t.customer_id=c.customer_id
        WHERE c.customer_id IS NULL
    """)

    orphan_items_txn = fetch_count(cursor, """
        SELECT COUNT(*) FROM production.transaction_items ti
        LEFT JOIN production.transactions t ON ti.transaction_id=t.transaction_id
        WHERE t.transaction_id IS NULL
    """)

    orphan_items_prod = fetch_count(cursor, """
        SELECT COUNT(*) FROM production.transaction_items ti
        LEFT JOIN production.products p ON ti.product_id=p.product_id
        WHERE p.product_id IS NULL
    """)

    total_orphans = orphan_txn + orphan_items_txn + orphan_items_prod

    results["referential_integrity"] = {
        "status": "passed" if total_orphans == 0 else "failed",
        "orphan_records": total_orphans,
        "details": {
            "transactions.customer_id": orphan_txn,
            "items.transaction_id": orphan_items_txn,
            "items.product_id": orphan_items_prod,
        },
    }

    # 4. Consistency
    line_mismatch = fetch_count(cursor, """
        SELECT COUNT(*) FROM production.transaction_items
        WHERE ABS(line_total - (quantity*unit_price*(1-discount_percentage/100))) > 0.01
    """)

    txn_mismatch = fetch_count(cursor, """
        SELECT COUNT(*) FROM (
            SELECT t.transaction_id
            FROM production.transactions t
            JOIN production.transaction_items ti ON t.transaction_id=ti.transaction_id
            GROUP BY t.transaction_id, t.total_amount
            HAVING ABS(t.total_amount - SUM(ti.line_total)) > 0.01
        ) x
    """)

    results["data_consistency"] = {
        "status": "passed" if (line_mismatch + txn_mismatch) == 0 else "failed",
        "mismatches": line_mismatch + txn_mismatch,
        "details": {
            "line_total": line_mismatch,
            "transaction_total": txn_mismatch,
        },
    }

    # 5. Business rules
    invalid_cost = fetch_count(cursor, "SELECT COUNT(*) FROM production.products WHERE cost >= price")
    invalid_discount = fetch_count(cursor, """
        SELECT COUNT(*) FROM production.transaction_items
        WHERE discount_percentage < 0 OR discount_percentage > 100
    """)
    future_txn = fetch_count(cursor, """
        SELECT COUNT(*) FROM production.transactions
        WHERE transaction_date > CURRENT_DATE
    """)

    results["range_checks"] = {
        "status": "passed" if (invalid_cost + invalid_discount + future_txn) == 0 else "failed",
        "violations": invalid_cost + invalid_discount + future_txn,
        "details": {
            "cost_vs_price": invalid_cost,
            "discount_range": invalid_discount,
            "future_transactions": future_txn,
        },
    }

    return results

# -------------------------------
# Scoring
# -------------------------------
def calculate_score(results):
    weights = {
        "null_checks": 0.20,
        "duplicate_checks": 0.15,
        "referential_integrity": 0.30,
        "data_consistency": 0.20,
        "range_checks": 0.15,
    }

    score = 100
    for k, w in weights.items():
        if results[k]["status"] != "passed":
            score -= w * 100

    return max(0, round(score, 2))

# -------------------------------
# Main
# -------------------------------
def main():
    conn = get_conn()
    cursor = conn.cursor()

    checks = run_checks(cursor)
    score = calculate_score(checks)

    grade = (
        "A" if score >= 90 else
        "B" if score >= 80 else
        "C" if score >= 70 else
        "D" if score >= 60 else "F"
    )

    report = {
        "check_timestamp": datetime.now(timezone.utc).isoformat(),
        "checks_performed": checks,
        "overall_quality_score": score,
        "quality_grade": grade,
    }

    report_path = os.path.join(REPORT_DIR, "data_quality_report.json")
    with open(report_path, "w") as f:
        json.dump(report, f, indent=4)

    print("✅ Data Quality Checks Completed")
    print(json.dumps(report, indent=2))

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
