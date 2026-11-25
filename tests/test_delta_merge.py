
import sys
from pathlib import Path
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(
    0, str(ROOT / "src" / "processing")
)

from spark_config import create_delta_spark
from delta_utils import merge_weather


def test_delta_merge_idempotency(tmp_path):

    spark = create_delta_spark()

    try:

        target = tmp_path / "weather_delta"

        source_df = spark.sql("""
            SELECT
                'B1' AS batch_id,
                'mumbai' AS city,
                TIMESTAMP '2026-11-19 15:00:00'
                    AS forecast_valid_at_utc,
                TIMESTAMP '2026-11-19 10:00:00'
                    AS ingested_at_utc,
                30.0D AS temperature_c
        """)

        merge_weather(spark, source_df, target)

        merge_weather(spark, source_df, target)

        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        assert result.count() == 1

        updated_df = spark.sql("""
            SELECT
                'B1' AS batch_id,
                'mumbai' AS city,
                TIMESTAMP '2026-11-19 15:00:00'
                    AS forecast_valid_at_utc,
                TIMESTAMP '2026-11-19 10:05:00'
                    AS ingested_at_utc,
                31.0D AS temperature_c
        """)

        merge_weather(spark, updated_df, target)

        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        assert result.count() == 1
        assert result.first().temperature_c == 31.0

        # An older snapshot must not overwrite newer data.
        merge_weather(spark, source_df, target)

        result = (
            spark.read
            .format("delta")
            .load(str(target))
        )

        assert result.count() == 1

        updated_count = (
            result
            .filter(F.col("temperature_c") == 31.0)
            .count()
        )

        assert updated_count == 1, (
            "Delta MERGE did not update temperature to 31.0"
        )

        print("Delta MERGE Idempotency Test PASSED")

    finally:
        spark.stop()
