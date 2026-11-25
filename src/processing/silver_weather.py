
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from bronze_weather import create_spark

ROOT = Path(__file__).resolve().parents[2]

LAKEHOUSE = ROOT / "data" / "lakehouse"

SOURCE = LAKEHOUSE / "bronze" / "weather_structured"
TARGET = LAKEHOUSE / "silver" / "weather"
QUARANTINE = LAKEHOUSE / "quarantine" / "weather"

AUDIT_FILE = ROOT / "logs" / "silver_audit.jsonl"

ALLOWED_CITIES = [
    "mumbai",
    "delhi",
    "bengaluru"
]


def validate_weather(df):

    df = df.withColumn(
        "city",
        F.lower(F.trim(F.col("city")))
    )

    rules = {

        "INVALID_BATCH_ID": (
            F.col("batch_id").isNull()
            | (F.trim(F.col("batch_id")) == "")
        ),

        "INVALID_CITY": (
            F.col("city").isNull()
            | ~F.col("city").isin(ALLOWED_CITIES)
        ),

        "INVALID_TEMPERATURE": (
            F.col("temperature_c").isNull()
            | F.isnan("temperature_c")
            | ~F.col("temperature_c").between(-90, 60)
        ),

        "INVALID_HUMIDITY": (
            F.col("humidity_pct").isNull()
            | F.isnan("humidity_pct")
            | ~F.col("humidity_pct").between(0, 100)
        ),

        "INVALID_PRECIPITATION": (
            F.col("precipitation_mm").isNull()
            | F.isnan("precipitation_mm")
            | (F.col("precipitation_mm") < 0)
            | (F.col("precipitation_mm") == float("inf"))
        ),

        "INVALID_FORECAST_TIME": (
            F.col("forecast_valid_at_utc").isNull()
        ),

        "INVALID_INGESTION_TIME": (
            F.col("ingested_at_utc").isNull()
        )
    }

    validated_df = df.withColumn(
        "error_reason",
        F.concat_ws(
            "|",
            *[
                F.when(condition, F.lit(reason))
                for reason, condition in rules.items()
            ]
        )
    )

    valid_df = (
        validated_df
        .filter(F.col("error_reason") == "")
        .drop("error_reason")
    )

    rejected_df = (
        validated_df
        .filter(F.col("error_reason") != "")
    )

    return valid_df, rejected_df


def deduplicate_weather(df):

    payload_columns = [
        "temperature_c",
        "humidity_pct",
        "precipitation_mm"
    ]

    df = df.withColumn(
        "_payload_hash",
        F.sha2(
            F.to_json(
                F.struct(*payload_columns)
            ),
            256
        )
    )

    window = (
        Window.partitionBy(
            "batch_id",
            "city",
            "forecast_valid_at_utc"
        )
        .orderBy(
            F.col("ingested_at_utc").desc(),
            F.col("source_file").desc_nulls_last(),
            F.col("_payload_hash").desc()
        )
    )

    ranked_df = df.withColumn(
        "rn",
        F.row_number().over(window)
    )

    silver_df = (
        ranked_df
        .filter(F.col("rn") == 1)
        .drop("rn", "_payload_hash")
    )

    duplicate_df = (
        ranked_df
        .filter(F.col("rn") > 1)
        .drop("rn", "_payload_hash")
    )

    return silver_df, duplicate_df


def process_silver_weather(bronze_df):

    valid_df, rejected_df = validate_weather(bronze_df)

    silver_df, duplicate_df = deduplicate_weather(
        valid_df
    )

    metrics = {
        "input_count": bronze_df.count(),
        "valid_count": valid_df.count(),
        "rejected_count": rejected_df.count(),
        "silver_count": silver_df.count(),
        "duplicate_count": duplicate_df.count()
    }

    reconciled_count = (
        metrics["silver_count"]
        + metrics["rejected_count"]
        + metrics["duplicate_count"]
    )

    if metrics["input_count"] != reconciled_count:
        raise RuntimeError(
            "Silver data reconciliation failed"
        )

    return silver_df, rejected_df, metrics


def write_audit(metrics):

    AUDIT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    record = {
        "pipeline_name": "silver_weather",
        "execution_time": (
            datetime.now(timezone.utc).isoformat()
        ),
        "status": "SUCCESS",
        **metrics
    }

    with AUDIT_FILE.open(
        "a", encoding="utf-8"
    ) as file:
        file.write(json.dumps(record) + "\n")


def main():

    spark = create_spark()

    try:

        bronze_df = spark.read.parquet(
            str(SOURCE)
        )

        silver_df, rejected_df, metrics = (
            process_silver_weather(bronze_df)
        )

        silver_df.show(10, truncate=False)

        (
            silver_df.write
            .mode("overwrite")
            .parquet(str(TARGET))
        )

        (
            rejected_df.write
            .mode("overwrite")
            .parquet(str(QUARANTINE))
        )

        write_audit(metrics)

        print("\nSilver processing metrics")

        for key, value in metrics.items():
            print(f"{key}: {value}")

        print("Data reconciliation: PASSED")
        print("Silver processing completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
