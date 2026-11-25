
import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import requests

from weather_api import (
    CITIES, BRONZE, create_session, fetch_weather
)
from audit_logger import write_audit_record
from checkpoint_manager import (
    initialize_checkpoints,
    get_checkpoint,
    update_checkpoint,
)


def parse_window(value):
    window = datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )

    if window.tzinfo is None:
        raise ValueError("Window must include a timezone")

    window = window.astimezone(timezone.utc)

    if window.minute or window.second or window.microsecond:
        raise ValueError("Window must start on an exact hour")

    return window


def save_batch(city, data, batch_id, window):
    folder = (
        BRONZE
        / f"city={city.lower()}"
        / f"ingestion_date={window:%Y-%m-%d}"
    )
    folder.mkdir(parents=True, exist_ok=True)

    destination = folder / f"{batch_id}.json"
    temporary = folder / f".{batch_id}.{uuid4().hex}.tmp"

    payload = {
        "batch_id": batch_id,
        "city": city,
        "window_start_utc": window.isoformat(),
        "ingested_at_utc": (
            datetime.now(timezone.utc).isoformat()
        ),
        "data_type": "forecast",
        "source": "open-meteo",
        "weather_data": data,
    }

    try:
        with temporary.open("x", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)

    return destination


def run_pipeline(window, fetch_fn=fetch_weather):
    initialize_checkpoints()
    batch_id = window.strftime("%Y%m%dT%HZ")
    failures = []

    with create_session() as session:
        for city, (latitude, longitude) in CITIES.items():
            checkpoint = get_checkpoint(batch_id, city)

            if checkpoint and checkpoint["status"] == "SUCCESS":
                print(f"SKIPPED: {city} already SUCCESS")
                continue

            started = datetime.now(timezone.utc)
            timer = perf_counter()
            status = "SUCCESS"
            error = None
            path = None
            records = 0

            update_checkpoint(batch_id, city, "RUNNING")

            try:
                data = fetch_fn(
                    session, latitude, longitude
                )
                records = len(data["hourly"]["time"])

                path = str(
                    save_batch(city, data, batch_id, window)
                )

                update_checkpoint(
                    batch_id, city, "SUCCESS",
                    file_path=path
                )
                print(f"SUCCESS: {city} Records: {records}")

            except (
                requests.RequestException,
                ValueError,
                OSError,
                sqlite3.Error,
            ) as exc:
                status = "FAILED"
                error = str(exc)
                failures.append(city)

                update_checkpoint(
                    batch_id, city, "FAILED",
                    error_message=error
                )
                print(f"FAILED: {city}: {error}")

            finally:
                write_audit_record({
                    "batch_id": batch_id,
                    "city": city,
                    "status": status,
                    "start_time": started.isoformat(),
                    "end_time": (
                        datetime.now(timezone.utc).isoformat()
                    ),
                    "records_extracted": records,
                    "file_path": path,
                    "error_message": error,
                    "duration_seconds": round(
                        perf_counter() - timer, 3
                    ),
                })

    if failures:
        raise RuntimeError(
            f"Failed cities: {', '.join(failures)}"
        )

    print("Batch completed successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--window",
        help="UTC hour, e.g. 2026-11-17T18:00:00Z"
    )
    args = parser.parse_args()

    if args.window:
        window = parse_window(args.window)
    else:
        now = datetime.now(timezone.utc)
        window = now.replace(
            minute=0, second=0, microsecond=0
        )

    run_pipeline(window)
