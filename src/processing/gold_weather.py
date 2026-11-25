
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[2]

LAKEHOUSE = ROOT / "data" / "lakehouse"

SILVER_SOURCE = (
    LAKEHOUSE / "silver" / "weather_delta"
)

GOLD_TARGET = LAKEHOUSE / "gold"

AUDIT_FILE = ROOT / "logs" / "gold_audit.jsonl"


def get_latest_forecast(silver_df):

    window = (
        Window.partitionBy(
            "city",
            "forecast_valid_at_utc"
        )
        .orderBy(
            F.col("ingested_at_utc").desc(),
            F.col("batch_id").desc()
        )
    )

    latest_df = (
        silver_df
        .withColumn(
            "rn",
            F.row_number().over(window)
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    latest_df = latest_df.withColumn(
        "forecast_local_time",
        F.from_utc_timestamp(
            "forecast_valid_at_utc",
            "Asia/Kolkata"
        )
    )

    latest_df = latest_df.withColumn(
        "forecast_date",
        F.to_date("forecast_local_time")
    )

    return latest_df


def build_daily_weather_summary(latest_df):

    daily_df = (
        latest_df
        .groupBy(
            "city",
            "forecast_date"
        )
        .agg(
            F.round(
                F.avg("temperature_c"), 2
            ).alias("avg_temperature_c"),

            F.max(
                "temperature_c"
            ).alias("max_temperature_c"),

            F.min(
                "temperature_c"
            ).alias("min_temperature_c"),

            F.round(
                F.avg("humidity_pct"), 2
            ).alias("avg_humidity_pct"),

            F.round(
                F.sum("precipitation_mm"), 2
            ).alias("total_precipitation_mm"),

            F.max(
                "precipitation_mm"
            ).alias("max_hourly_precipitation_mm"),

            F.sum(
                F.when(
                    F.col("precipitation_mm") > 0,
                    1
                ).otherwise(0)
            ).alias("rainy_hours"),

            F.count("*").alias("forecast_hours")
        )
        .withColumn(
            "is_complete_day",
            F.col("forecast_hours") == 24
        )
    )

    return daily_df


def build_delivery_risk_report(latest_df):

    risk_df = latest_df.withColumn(
        "rain_score",
        F.when(
            F.col("precipitation_mm") >= 10, 50
        ).when(
            F.col("precipitation_mm") >= 2, 25
        ).otherwise(0)
    )

    risk_df = risk_df.withColumn(
        "temperature_score",
        F.when(
            (F.col("temperature_c") >= 40)
            | (F.col("temperature_c") <= 5),
            30
        ).otherwise(0)
    )

    risk_df = risk_df.withColumn(
        "humidity_score",
        F.when(
            F.col("humidity_pct") >= 85, 20
        ).otherwise(0)
    )

    risk_df = risk_df.withColumn(
        "risk_score",
        F.col("rain_score")
        + F.col("temperature_score")
        + F.col("humidity_score")
    )

    risk_df = risk_df.withColumn(
        "risk_level",

        F.when(
            F.col("risk_score") >= 75,
            "CRITICAL"
        ).when(
            F.col("risk_score") >= 50,
            "HIGH"
        ).when(
            F.col("risk_score") >= 25,
            "MEDIUM"
        ).otherwise("LOW")
    )

    risk_df = risk_df.withColumn(
        "risk_reason",
        F.concat_ws(
            "|",

            F.when(
                F.col("rain_score") > 0,
                F.lit("RAINFALL")
            ),

            F.when(
                F.col("temperature_score") > 0,
                F.lit("EXTREME_TEMPERATURE")
            ),

            F.when(
                F.col("humidity_score") > 0,
                F.lit("HIGH_HUMIDITY")
            )
        )
    )

    risk_df = risk_df.withColumn(
        "delivery_recommendation",

        F.when(
            F.col("risk_level") == "CRITICAL",
            "ESCALATE_FOR_REVIEW"
        ).when(
            F.col("risk_level") == "HIGH",
            "REVIEW_DELIVERY_SCHEDULE"
        ).when(
            F.col("risk_level") == "MEDIUM",
            "ADDITIONAL_PRECAUTIONS"
        ).otherwise("NORMAL_OPERATION")
    )

    return risk_df


def build_forecast_revision_report(silver_df):

    window = (
        Window.partitionBy(
            "city",
            "forecast_valid_at_utc"
        )
        .orderBy(
            F.col("ingested_at_utc"),
            F.col("batch_id")
        )
    )

    revision_df = (
        silver_df
        .withColumn(
            "previous_batch_id",
            F.lag("batch_id").over(window)
        )
        .withColumn(
            "previous_temperature_c",
            F.lag("temperature_c").over(window)
        )
        .withColumn(
            "previous_precipitation_mm",
            F.lag("precipitation_mm").over(window)
        )
    )

    revision_df = revision_df.withColumn(
        "temperature_revision_c",
        F.round(
            F.col("temperature_c")
            - F.col("previous_temperature_c"),
            2
        )
    )

    revision_df = revision_df.withColumn(
        "precipitation_revision_mm",
        F.round(
            F.col("precipitation_mm")
            - F.col("previous_precipitation_mm"),
            2
        )
    )

    return revision_df


def build_gold_tables(silver_df):

    latest_df = get_latest_forecast(
        silver_df
    )

    daily_df = build_daily_weather_summary(
        latest_df
    )

    risk_df = build_delivery_risk_report(
        latest_df
    )

    revision_df = build_forecast_revision_report(
        silver_df
    )

    return {
        "latest_weather_forecast": latest_df,
        "daily_weather_summary": daily_df,
        "delivery_risk_report": risk_df,
        "forecast_revision_report": revision_df
    }


def write_audit(metrics, status, error=None):

    AUDIT_FILE.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    record = {
        "pipeline_name": "gold_weather",
        "execution_time": (
            datetime.now(timezone.utc).isoformat()
        ),
        "status": status,
        "error_message": error,
        **metrics
    }

    with AUDIT_FILE.open(
        "a", encoding="utf-8"
    ) as file:

        file.write(json.dumps(record) + "\n")


def process_gold(spark):

    metrics = {}

    try:

        silver_df = (
            spark.read
            .format("delta")
            .load(str(SILVER_SOURCE))
            .cache()
        )

        try:

            metrics["silver_count"] = (
                silver_df.count()
            )

            if metrics["silver_count"] == 0:
                raise ValueError(
                    "Silver table contains no records"
                )

            tables = build_gold_tables(
                silver_df
            )

            for table_name, df in tables.items():
                metrics[table_name] = df.count()

            if (
                metrics["latest_weather_forecast"]
                != metrics["delivery_risk_report"]
            ):
                raise RuntimeError(
                    "Risk report reconciliation failed"
                )

            if (
                metrics["forecast_revision_report"]
                != metrics["silver_count"]
            ):
                raise RuntimeError(
                    "Revision report reconciliation failed"
                )

            for table_name, df in tables.items():

                target = GOLD_TARGET / table_name

                (
                    df.write
                    .format("delta")
                    .mode("overwrite")
                    .save(str(target))
                )

                print(f"{table_name}: SUCCESS")

            write_audit(metrics, "SUCCESS")

        finally:
            silver_df.unpersist()

        print("\nGold processing metrics")

        for key, value in metrics.items():
            print(f"{key}: {value}")

        print("Gold processing completed successfully")

    except Exception as exc:

        write_audit(
            metrics,
            "FAILED",
            str(exc)
        )

        raise


if __name__ == "__main__":

    from spark_config import create_delta_spark

    spark = create_delta_spark()

    try:
        process_gold(spark)
    finally:
        spark.stop()
