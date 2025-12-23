import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
import psycopg2
import yaml
import statistics

# --------------------------------------------------
# Paths & Config
# --------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed", "monitoring_report.json")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

# --------------------------------------------------
# Logging
# --------------------------------------------------
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "monitoring.log"),
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# --------------------------------------------------
# Load Config
# --------------------------------------------------
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

DB_CONFIG = {
    "host": config["database"]["host"],
    "port": config["database"]["port"],
    "dbname": config["database"]["name"],
    "user": config["database"]["user"],
    "password": config["database"]["password"],
}

# --------------------------------------------------
# DB Connection
# --------------------------------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# --------------------------------------------------
# Helper
# --------------------------------------------------
def make_utc(dt):
    """Ensure datetime is timezone-aware UTC"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

# --------------------------------------------------
# Monitoring Logic
# --------------------------------------------------
def main():
    report = {
        "monitoring_timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_health": "healthy",
        "checks": {},
        "alerts": [],
        "overall_health_score": 100
    }

    conn = get_connection()
    cur = conn.cursor()

    # ==================================================
    # 1. PIPELINE EXECUTION HEALTH
    # ==================================================
    pipeline_report_path = os.path.join(
        BASE_DIR, "data", "processed", "pipeline_execution_report.json"
    )

    if os.path.exists(pipeline_report_path):
        with open(pipeline_report_path) as f:
            pipeline_report = json.load(f)

        last_run = make_utc(datetime.fromisoformat(pipeline_report["end_time"]))
        hours_since = (datetime.now(timezone.utc) - last_run).total_seconds() / 3600

        status = "ok"
        if hours_since > 25:
            status = "critical"
            report["alerts"].append({
                "severity": "critical",
                "check": "pipeline_execution",
                "message": "Pipeline has not run in last 25 hours",
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            report["pipeline_health"] = "critical"
            report["overall_health_score"] -= 30

        report["checks"]["last_execution"] = {
            "status": status,
            "last_run": last_run.isoformat(),
            "hours_since_last_run": round(hours_since, 2),
            "threshold_hours": 25
        }
    else:
        report["pipeline_health"] = "critical"
        report["overall_health_score"] -= 40

    # ==================================================
    # 2. DATA FRESHNESS
    # ==================================================
    cur.execute("SELECT MAX(loaded_at) FROM staging.customers")
    staging_latest = make_utc(cur.fetchone()[0])

    cur.execute("SELECT MAX(created_at) FROM production.transactions")
    production_latest = make_utc(cur.fetchone()[0])

    cur.execute("SELECT MAX(created_at) FROM warehouse.fact_sales")
    warehouse_latest = make_utc(cur.fetchone()[0])

    max_lag = 0
    if staging_latest:
        max_lag = max(max_lag, (datetime.now(timezone.utc) - staging_latest).total_seconds() / 3600)
    if production_latest:
        max_lag = max(max_lag, (datetime.now(timezone.utc) - production_latest).total_seconds() / 3600)
    if warehouse_latest:
        max_lag = max(max_lag, (datetime.now(timezone.utc) - warehouse_latest).total_seconds() / 3600)

    freshness_status = "ok"
    if max_lag > 24:
        freshness_status = "critical"
        report["pipeline_health"] = "critical"
        report["overall_health_score"] -= 25

    report["checks"]["data_freshness"] = {
        "status": freshness_status,
        "staging_latest_record": staging_latest.isoformat() if staging_latest else None,
        "production_latest_record": production_latest.isoformat() if production_latest else None,
        "warehouse_latest_record": warehouse_latest.isoformat() if warehouse_latest else None,
        "max_lag_hours": round(max_lag, 2)
    }

    # ==================================================
    # 3. DATA VOLUME ANOMALIES
    # ==================================================
    cur.execute("""
        SELECT transaction_date, COUNT(*)
        FROM production.transactions
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY transaction_date
        ORDER BY transaction_date
    """)

    rows = cur.fetchall()
    counts = [r[1] for r in rows]

    anomaly = False
    anomaly_type = None

    if len(counts) >= 7:
        mean = statistics.mean(counts)
        std = statistics.stdev(counts)
        today_count = counts[-1]

        if today_count > mean + 3 * std:
            anomaly = True
            anomaly_type = "spike"
        elif today_count < mean - 3 * std:
            anomaly = True
            anomaly_type = "drop"

    report["checks"]["data_volume_anomalies"] = {
        "status": "anomaly_detected" if anomaly else "ok",
        "expected_range": f"{int(mean - 3*std)}-{int(mean + 3*std)}" if counts else None,
        "actual_count": today_count if counts else None,
        "anomaly_detected": anomaly,
        "anomaly_type": anomaly_type
    }

    if anomaly:
        report["alerts"].append({
            "severity": "warning",
            "check": "data_volume",
            "message": f"Transaction volume anomaly detected: {anomaly_type}",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        report["overall_health_score"] -= 15

    # ==================================================
    # 4. DATA QUALITY MONITORING
    # ==================================================
    quality_path = os.path.join(
        BASE_DIR, "data", "quality", "data_quality_report.json"
    )

    if os.path.exists(quality_path):
        with open(quality_path) as f:
            quality = json.load(f)

        score = quality.get("overall_quality_score", 100)
        report["checks"]["data_quality"] = {
            "status": "ok" if score >= 95 else "degraded",
            "quality_score": score,
            "orphan_records": quality["checks_performed"]["referential_integrity"]["orphan_records"],
            "null_violations": quality["checks_performed"]["null_checks"]["null_violations"]
        }

        if score < 95:
            report["overall_health_score"] -= 20

    # ==================================================
    # 5. DATABASE CONNECTIVITY
    # ==================================================
    start = time.time()
    cur.execute("SELECT 1")
    response_ms = (time.time() - start) * 1000

    cur.execute("""
        SELECT COUNT(*) FROM pg_stat_activity
        WHERE state = 'active'
    """)
    active_connections = cur.fetchone()[0]

    report["checks"]["database_connectivity"] = {
        "status": "ok",
        "response_time_ms": round(response_ms, 2),
        "connections_active": active_connections
    }

    # --------------------------------------------------
    # Finalize
    # --------------------------------------------------
    conn.close()

    with open(OUTPUT_PATH, "w") as f:
        json.dump(report, f, indent=4)

    logging.info("Monitoring report generated successfully")
    print("✅ Monitoring report generated successfully")


if __name__ == "__main__":
    main()
