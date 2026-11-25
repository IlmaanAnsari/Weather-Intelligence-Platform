
import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
AUDIT_FILE = ROOT / "logs" / "pipeline_audit.jsonl"

INGEST_SCRIPT = (
    ROOT / "src" / "ingestion" / "weather_recovery.py"
)
UPLOAD_SCRIPT = (
    ROOT / "src" / "ingestion" / "local_uploader.py"
)


def utc_now():
    return datetime.now(timezone.utc)


def get_window(value=None):
    if value:
        window = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
        if window.tzinfo is None:
            raise ValueError("Window requires a timezone")
        window = window.astimezone(timezone.utc)
        if (window.minute, window.second,
                window.microsecond) != (0, 0, 0):
            raise ValueError("Window must be hour-aligned")
        return window

    return utc_now().replace(
        minute=0, second=0, microsecond=0
    )


def write_audit(record):
    AUDIT_FILE.parent.mkdir(
        parents=True, exist_ok=True
    )
    with AUDIT_FILE.open(
        "a", encoding="utf-8"
    ) as file:
        file.write(json.dumps(record) + "\n")


def run_stage(name, command, run_id, batch_id):
    start = utc_now()
    timer = perf_counter()

    print(f"\nSTARTING: {name}", flush=True)

    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        status = (
            "SUCCESS"
            if result.returncode == 0
            else "FAILED"
        )
        error = (
            None if status == "SUCCESS"
            else f"Exit code: {result.returncode}"
        )

    except (subprocess.TimeoutExpired, OSError) as exc:
        status = "FAILED"
        error = str(exc)
        print(f"Stage error: {error}")

    duration = round(perf_counter() - timer, 3)

    write_audit({
        "run_id": run_id,
        "batch_id": batch_id,
        "stage": name,
        "status": status,
        "start_time": start.isoformat(),
        "end_time": utc_now().isoformat(),
        "duration_seconds": duration,
        "error_message": error,
    })

    print(f"{name}: {status}")
    return status == "SUCCESS"


def run_pipeline(window):
    batch_id = window.strftime("%Y%m%dT%HZ")
    run_id = uuid4().hex

    print(f"Pipeline run: {run_id}")
    print(f"Logical batch: {batch_id}")

    stages = [
        (
            "WEATHER_INGESTION",
            [
                sys.executable,
                str(INGEST_SCRIPT),
                "--window",
                window.isoformat(),
            ],
        ),
        (
            "LAKEHOUSE_UPLOAD",
            [
                sys.executable,
                str(UPLOAD_SCRIPT),
            ],
        ),
    ]

    for name, command in stages:
        success = run_stage(
            name, command, run_id, batch_id
        )

        if not success:
            print(f"PIPELINE FAILED at {name}")
            return False

    print("\nPIPELINE COMPLETED SUCCESSFULLY")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--window",
        help="UTC hour, e.g. 2026-11-18T15:00:00Z"
    )
    args = parser.parse_args()

    window = get_window(args.window)
    success = run_pipeline(window)

    sys.exit(0 if success else 1)

