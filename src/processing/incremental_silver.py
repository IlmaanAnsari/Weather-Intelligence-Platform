
from pathlib import Path

from pyspark.sql import functions as F

from spark_config import create_delta_spark
from silver_weather import process_silver_weather
from bronze_weather import transform_weather

from delta_utils import (
    merge_weather,
    merge_quarantine
)

from processing_checkpoint import (
    initialize,
    get_processed_files,
    mark_success
)

ROOT = Path(__file__).resolve().parents[2]
LAKEHOUSE = ROOT / "data" / "lakehouse"

SOURCE = LAKEHOUSE / "bronze" / "weather"

SILVER_TARGET = (
    LAKEHOUSE / "silver" / "weather_delta"
)

QUARANTINE_TARGET = (
    LAKEHOUSE / "quarantine" / "weather_delta"
)


def discover_new_files():

    processed = get_processed_files()

    return [
        file
        for file in sorted(SOURCE.rglob("*.json"))
        if file.relative_to(SOURCE).as_posix()
        not in processed
    ]


def run_incremental_silver(
    spark,
    simulate_failure=False
):

    initialize()

    files = discover_new_files()

    if not files:
        print("No new files to process")
        return

    print(f"New files discovered: {len(files)}")

    # Read newly discovered immutable JSON files.
    raw_df = (
        spark.read
        .option("multiLine", "true")
        .json([str(file) for file in files])
    )

    bronze_df = transform_weather(raw_df)

    silver_df, rejected_df, metrics = (
        process_silver_weather(bronze_df)
    )

    # Create a stable identifier for rejected records.
    rejected_df = rejected_df.withColumn(
        "rejection_id",
        F.sha2(
            F.to_json(
                F.struct(
                    *[
                        F.col(column)
                        for column in sorted(
                            rejected_df.columns
                        )
                    ]
                ),
                {"ignoreNullFields": "false"}
            ),
            256
        )
    )

    merge_weather(
        spark,
        silver_df,
        SILVER_TARGET
    )

    if simulate_failure:
        raise RuntimeError(
            "Simulated failure after Silver MERGE"
        )

    merge_quarantine(
        spark,
        rejected_df,
        QUARANTINE_TARGET
    )

    # Update checkpoints only after both writes succeed.
    mark_success([
        file.relative_to(SOURCE).as_posix()
        for file in files
    ])

    print("\nIncremental processing metrics")

    for key, value in metrics.items():
        print(f"{key}: {value}")

    print("Processing checkpoint updated")
    print("Incremental Silver completed successfully")


if __name__ == "__main__":

    spark = create_delta_spark()

    try:
        run_incremental_silver(spark)
    finally:
        spark.stop()
