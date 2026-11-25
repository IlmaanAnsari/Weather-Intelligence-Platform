
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AUDIT_FILE = ROOT / "logs" / "pipeline_audit.jsonl"


def read_pipeline_audit():
    if not AUDIT_FILE.exists():
        print("No pipeline audit file found")
        return []

    with AUDIT_FILE.open(encoding="utf-8") as file:
        return [
            json.loads(line)
            for line in file
            if line.strip()
        ]


def show_recent_runs(limit=10):
    records = read_pipeline_audit()

    if not records:
        return

    print("\nRecent pipeline stages")
    print("-" * 80)

    for record in records[-limit:]:
        print(
            f"Batch: {record['batch_id']} | "
            f"Stage: {record['stage']} | "
            f"Status: {record['status']} | "
            f"Duration: {record['duration_seconds']}s"
        )

        if record.get("error_message"):
            print(
                f"  Error: {record['error_message']}"
            )


if __name__ == "__main__":
    show_recent_runs()
