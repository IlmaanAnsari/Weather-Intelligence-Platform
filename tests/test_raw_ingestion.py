
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "bronze" / "weather"


def test_raw_weather_files():
    for city in ["mumbai", "delhi", "bengaluru"]:
        files = list(
            (BRONZE / f"city={city}").rglob("*.json")
        )

        assert files, f"Missing raw data for {city}"

        with files[-1].open(encoding="utf-8") as file:
            payload = json.load(file)

        hourly = payload["weather_data"]["hourly"]

        assert payload["data_type"] == "forecast"
        assert len(hourly["time"]) > 0
        assert len(hourly["temperature_2m"]) == len(
            hourly["time"]
        )
