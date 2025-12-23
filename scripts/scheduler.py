import time
import subprocess
import logging
import yaml
from datetime import datetime
import schedule
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")
LOCK_FILE = os.path.join(BASE_DIR, "pipeline.lock")

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

RUN_TIME = config["scheduler"]["pipeline_run_time"]

LOG_FILE = os.path.join(BASE_DIR, "logs", "scheduler_activity.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def is_pipeline_running():
    return os.path.exists(LOCK_FILE)

def run_pipeline():
    if is_pipeline_running():
        logging.warning("Pipeline already running. Skipping this execution.")
        return

    try:
        open(LOCK_FILE, "w").close()
        logging.info("Starting scheduled pipeline execution")

        subprocess.run(
            ["python", "scripts/pipeline_orchestrator.py"],
            cwd=BASE_DIR,
            check=True
        )

        subprocess.run(
            ["python", "scripts/cleanup_old_data.py"],
            cwd=BASE_DIR,
            check=True
        )

        logging.info("Pipeline and cleanup completed successfully")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")

    finally:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)

schedule.every().day.at(RUN_TIME).do(run_pipeline)

logging.info(f"Scheduler started. Pipeline scheduled at {RUN_TIME}")

while True:
    schedule.run_pending()
    time.sleep(60)
