import os
import time
import logging
from datetime import datetime, timedelta
import yaml

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "config.yaml")

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

RETENTION_DAYS = config.get("retention", {}).get("raw_data_days", 7)

TARGET_DIRS = [
    os.path.join(BASE_DIR, "data", "raw"),
    os.path.join(BASE_DIR, "data", "staging"),
    os.path.join(BASE_DIR, "logs"),
]

LOG_FILE = os.path.join(BASE_DIR, "logs", "scheduler_activity.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def should_preserve(filename):
    preserve_keywords = ["summary", "report", "metadata"]
    return any(k in filename.lower() for k in preserve_keywords)

def cleanup():
    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    logging.info("Starting cleanup job")

    for directory in TARGET_DIRS:
        if not os.path.exists(directory):
            continue

        for file in os.listdir(directory):
            path = os.path.join(directory, file)

            if not os.path.isfile(path):
                continue

            if should_preserve(file):
                continue

            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff:
                os.remove(path)
                logging.info(f"Deleted old file: {path}")

    logging.info("Cleanup completed successfully")

if __name__ == "__main__":
    cleanup()
