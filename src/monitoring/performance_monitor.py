
import json
import time
import duckdb

from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

DB_PATH = ROOT / "data" / "warehouse" / "weather.duckdb"
LOG_PATH = ROOT / "logs" / "performance_metrics.jsonl"


def record_metric(metric):
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(LOG_PATH, "a", encoding="utf-8") as f:
        json.dump(metric, f)
        f.write("\n")   