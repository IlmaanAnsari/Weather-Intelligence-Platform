
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
TARGET = (
    ROOT / "data" / "lakehouse"
    / "bronze" / "weather_structured"
)


def test_bronze_output():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("TestWeatherBronze")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(str(TARGET))

        assert df.count() > 0

        required = {
            "batch_id",
            "city",
            "forecast_valid_at_utc",
            "temperature_c",
            "humidity_pct",
            "precipitation_mm",
            "source_file",
        }

        assert required.issubset(set(df.columns))
        assert df.select("city").distinct().count() == 3

    finally:
        spark.stop()
