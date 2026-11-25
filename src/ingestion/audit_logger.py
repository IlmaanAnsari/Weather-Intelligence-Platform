
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AUDIT_DIR = ROOT / "logs"
AUDIT_FILE = AUDIT_DIR / "weather_audit.jsonl"


def write_audit_record(record):
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    required_fields = [
        "batch_id",
        "city",
        "status",
        "start_time",
        "end_time",
        "records_extracted",
        "duration_seconds",
    ]

    missing = [
        field for field in required_fields
        if field not in record
    ]

    if missing:
        raise ValueError(
            f"Missing audit fields: {missing}"
        )

    record["audit_created_at"] = (
        datetime.now(timezone.utc).isoformat()
    )

    with AUDIT_FILE.open(
        "a", encoding="utf-8"
    ) as file:
        file.write(json.dumps(record) + "\n")


def read_audit_records():
    if not AUDIT_FILE.exists():
        return []

    with AUDIT_FILE.open(
        "r", encoding="utf-8"
    ) as file:
        return [
            json.loads(line)
            for line in file
            if line.strip()
        ]
