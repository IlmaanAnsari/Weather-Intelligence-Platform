
import sys
import duckdb
from pathlib import Path
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(
    0, str(ROOT / "src" / "processing")
)

from spark_config import create_delta_spark

LAKEHOUSE = ROOT / "data" / "lakehouse"
WAREHOUSE = ROOT / "data" / "warehouse" / "weather.duckdb"


def validate_pipeline():
    spark = create_delta_spark()

    try:
        silver = spark.read.format("delta").load(
            str(LAKEHOUSE / "silver" / "weather_delta")
        )

        latest = spark.read.format("delta").load(
            str(LAKEHOUSE / "gold" / "latest_weather_forecast")
        )

        daily = spark.read.format("delta").load(
            str(LAKEHOUSE / "gold" / "daily_weather_summary")
        )

        risk = spark.read.format("delta").load(
            str(LAKEHOUSE / "gold" / "delivery_risk_report")
        )

        revisions = spark.read.format("delta").load(
            str(LAKEHOUSE / "gold" / "forecast_revision_report")
        )

        counts = {
            "silver": silver.count(),
            "latest": latest.count(),
            "daily": daily.count(),
            "risk": risk.count(),
            "revisions": revisions.count()
        }

        # Gold reconciliation
        assert counts["latest"] == counts["risk"]
        assert counts["silver"] == counts["revisions"]

        # Unique Silver business keys
        silver_duplicates = (
            silver.groupBy(
                "batch_id",
                "city",
                "forecast_valid_at_utc"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert silver_duplicates == 0

        # Unique latest-forecast business keys
        latest_duplicates = (
            latest.groupBy(
                "city",
                "forecast_valid_at_utc"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert latest_duplicates == 0

        # Risk constraints
        invalid_risk = risk.filter(
            F.col("risk_score").isNull()
            | ~F.col("risk_score").between(0, 100)
            | ~F.col("risk_level").isin(
                "LOW", "MEDIUM", "HIGH", "CRITICAL"
            )
        ).count()

        assert invalid_risk == 0

        print("Delta Layer Validation: PASSED")
        print("Delta counts:", counts)

    finally:
        spark.stop()

    # Validate the DuckDB warehouse
    with duckdb.connect(
        str(WAREHOUSE), read_only=True
    ) as conn:

        hourly_count = conn.execute("""
            SELECT COUNT(*)
            FROM fact_weather_hourly
        """).fetchone()[0]

        daily_count = conn.execute("""
            SELECT COUNT(*)
            FROM fact_weather_daily
        """).fetchone()[0]

        city_count = conn.execute("""
            SELECT COUNT(*)
            FROM dim_city
        """).fetchone()[0]

        assert hourly_count == counts["risk"]
        assert daily_count == counts["daily"]
        assert city_count == 3

        # Verify warehouse business keys
        duplicates = conn.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT city_key, forecast_valid_at_utc
                FROM fact_weather_hourly
                GROUP BY 1, 2
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        assert duplicates == 0

        print("Warehouse Validation: PASSED")

    print("\nEND-TO-END VALIDATION PASSED")


if __name__ == "__main__":
    validate_pipeline()
