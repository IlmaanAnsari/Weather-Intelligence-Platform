
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from time import perf_counter
from audit_logger import write_audit_record



import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.open-meteo.com/v1/forecast"

CITIES = {
    "Mumbai": (19.0760, 72.8777),
    "Delhi": (28.6139, 77.2090),
    "Bengaluru": (12.9716, 77.5946),
}

ROOT = Path(__file__).resolve().parents[2]
BRONZE = ROOT / "data" / "bronze" / "weather"
LOGS = ROOT / "logs"

BRONZE.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOGS / "weather_ingestion.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def create_session():
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount(
        "https://",
        HTTPAdapter(max_retries=retry),
    )
    return session


def fetch_weather(session, latitude, longitude):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "precipitation"
        ),
        "forecast_days": 2,
        "timezone": "UTC",
    }

    response = session.get(
        BASE_URL,
        params=params,
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()

    hourly = data.get("hourly", {})
    required = (
        "time",
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
    )

    if not all(key in hourly for key in required):
        raise ValueError("Missing required hourly fields")

    if len({len(hourly[k]) for k in required}) != 1:
        raise ValueError("Hourly arrays have different lengths")

    return data


def save_raw(city, data, batch_id):
    now = datetime.now(timezone.utc)
    folder = (
        BRONZE
        / f"city={city.lower()}"
        / f"ingestion_date={now:%Y-%m-%d}"
    )
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / f"{batch_id}.json"

    payload = {
        "batch_id": batch_id,
        "city": city,
        "ingested_at_utc": now.isoformat(),
        "source": "open-meteo",
        "data_type": "forecast",
        "weather_data": data,
    }

    with file_path.open("x", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)

    return file_path



def main():
    batch_id = (
        datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
        + "_" + uuid4().hex[:8]
    )

    failures = []

    with create_session() as session:
        for city, (latitude, longitude) in CITIES.items():
            start_time = datetime.now(timezone.utc)
            timer = perf_counter()

            status = "SUCCESS"
            records_extracted = 0
            file_path = None
            error_message = None

            try:
                data = fetch_weather(
                    session, latitude, longitude
                )

                records_extracted = len(
                    data["hourly"]["time"]
                )

                file_path = str(
                    save_raw(city, data, batch_id)
                )

                logging.info(
                    "batch_id=%s city=%s status=SUCCESS "
                    "records=%s",
                    batch_id, city, records_extracted
                )

                print(
                    f"SUCCESS: {city} "
                    f"Records: {records_extracted}"
                )

            except (
                requests.RequestException,
                ValueError,
                OSError,
            ) as exc:
                status = "FAILED"
                error_message = str(exc)
                failures.append(city)

                logging.exception(
                    "batch_id=%s city=%s status=FAILED",
                    batch_id, city
                )

                print(f"FAILED: {city}: {exc}")

            finally:
                end_time = datetime.now(timezone.utc)
                duration = round(
                    perf_counter() - timer, 3
                )

                audit_record = {
                    "batch_id": batch_id,
                    "city": city,
                    "status": status,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "records_extracted": records_extracted,
                    "file_path": file_path,
                    "error_message": error_message,
                    "duration_seconds": duration,
                }

                write_audit_record(audit_record)

    if failures:
        raise RuntimeError(
            f"Ingestion failed for: {', '.join(failures)}"
        )

    print("Batch completed successfully")


if __name__ == "__main__":
    main()
