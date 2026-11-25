
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]

SOURCE = (
    ROOT / "data" / "lakehouse"
    / "bronze" / "weather"
)

TARGET = (
    ROOT / "data" / "lakehouse"
    / "bronze" / "weather_structured"
)


def create_spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("WeatherBronzeProcessing")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def transform_weather(raw_df):
    """
    Convert nested Open-Meteo hourly arrays
    into one structured record per forecast hour.
    """

    df = raw_df.withColumn(
        "source_file", F.input_file_name()
    )

    df = df.withColumn(
        "hourly_combined",
        F.arrays_zip(
            F.col("weather_data.hourly.time"),
            F.col("weather_data.hourly.temperature_2m"),
            F.col("weather_data.hourly.relative_humidity_2m"),
            F.col("weather_data.hourly.precipitation")
        )
    )

    df = df.withColumn(
        "hourly_record",
        F.explode("hourly_combined")
    )

    return df.select(
        F.col("batch_id"),
        F.col("city"),
        F.col("source"),
        F.col("data_type"),

        F.to_timestamp(
            "ingested_at_utc"
        ).alias("ingested_at_utc"),

        F.to_timestamp(
            "hourly_record.time",
            "yyyy-MM-dd'T'HH:mm"
        ).alias("forecast_valid_at_utc"),

        F.col(
            "hourly_record.temperature_2m"
        ).cast("double").alias("temperature_c"),

        F.col(
            "hourly_record.relative_humidity_2m"
        ).cast("double").alias("humidity_pct"),

        F.col(
            "hourly_record.precipitation"
        ).cast("double").alias("precipitation_mm"),

        F.col("source_file")
    )


def process_bronze():
    files = sorted(SOURCE.rglob("*.json"))

    if not files:
        raise FileNotFoundError(
            "No weather JSON files found in local lakehouse"
        )

    spark = create_spark()

    try:
        raw_df = (
            spark.read
            .option("multiLine", "true")
            .json([str(path) for path in files])
        )

        bronze_df = transform_weather(raw_df)

        count = bronze_df.count()

        if count == 0:
            raise ValueError("Bronze transformation produced no rows")

        bronze_df.printSchema()
        bronze_df.show(10, truncate=False)

        print(f"Bronze records: {count}")

        (
            bronze_df.write
            .mode("overwrite")
            .parquet(str(TARGET))
        )

        print("Bronze Parquet created successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    process_bronze()
