# pragma: no cover

import subprocess
import time
import json
import logging
from datetime import datetime
from pathlib import Path

# --------------------------------------------------
# Paths
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "logs"
REPORT_DIR = BASE_DIR / "data" / "processed"

LOG_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(exist_ok=True)

# --------------------------------------------------
# Logging
# --------------------------------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"pipeline_orchestrator_{timestamp}.log"),
        logging.FileHandler(LOG_DIR / "pipeline_errors.log"),
        logging.StreamHandler()
    ]
)

# --------------------------------------------------
# Pipeline steps (ORDER MATTERS)
# --------------------------------------------------
PIPELINE_STEPS = [
    ("ingestion", ["python", "scripts/ingestion/ingest_to_staging.py"]),
    ("quality_checks", ["python", "scripts/quality_checks/validate_data.py"]),
    ("production_etl", ["python", "scripts/transformation/staging_to_production.py"]),
    ("warehouse_load", ["python", "scripts/transformation/load_warehouse.py"]),
    ("analytics", ["python", "scripts/transformation/generate_analytics.py"])
]

MAX_RETRIES = 3

# --------------------------------------------------
# Run a step with retry + backoff
# --------------------------------------------------
def run_step(step_name, command):
    retries = 0
    start = time.time()

    while retries < MAX_RETRIES:
        try:
            logging.info(f"Starting step: {step_name} (attempt {retries+1})")
            subprocess.run(command, check=True)
            duration = round(time.time() - start, 2)

            logging.info(f"Step completed: {step_name} in {duration}s")
            return {
                "status": "success",
                "duration_seconds": duration,
                "retry_attempts": retries,
                "error_message": None
            }

        except Exception as e:
            retries += 1
            logging.error(f"Step failed: {step_name} | {str(e)}")

            if retries >= MAX_RETRIES:
                return {
                    "status": "failed",
                    "duration_seconds": round(time.time() - start, 2),
                    "retry_attempts": retries,
                    "error_message": str(e)
                }

            wait_time = 2 ** (retries - 1)
            logging.info(f"Retrying {step_name} after {wait_time}s...")
            time.sleep(wait_time)

# --------------------------------------------------
# Main Orchestrator
# --------------------------------------------------
def main():
    pipeline_start = time.time()
    execution_id = f"PIPE_{timestamp}"

    report = {
        "pipeline_execution_id": execution_id,
        "start_time": datetime.utcnow().isoformat(),
        "end_time": None,
        "total_duration_seconds": None,
        "status": "success",
        "steps_executed": {},
        "errors": [],
        "warnings": []
    }

    for step_name, command in PIPELINE_STEPS:
        result = run_step(step_name, command)
        report["steps_executed"][step_name] = result

        if result["status"] == "failed":
            report["status"] = "failed"
            report["errors"].append(f"{step_name} failed")
            break

    report["end_time"] = datetime.utcnow().isoformat()
    report["total_duration_seconds"] = round(time.time() - pipeline_start, 2)

    report_path = REPORT_DIR / "pipeline_execution_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=4)

    logging.info(f"Pipeline execution finished with status: {report['status']}")
    logging.info(f"Report written to {report_path}")

if __name__ == "__main__":
    main()
