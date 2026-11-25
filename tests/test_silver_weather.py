
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(
    0, str(ROOT / "src" / "processing")
)

from bronze_weather import create_spark
from silver_weather import process_silver_weather


@pytest.fixture(scope="module")
def spark():

    session = create_spark()

    yield session

    session.stop()


def test_silver_quality_and_reconciliation(spark):

    bronze_df = spark.sql("""
        SELECT * FROM VALUES

        (
            'B1', 'Mumbai', 30.0D, 75.0D, 0.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 10:00:00',
            'file1.json'
        ),

        (
            'B1', 'Mumbai', 31.0D, 76.0D, 0.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 10:05:00',
            'file2.json'
        ),

        (
            'B2', 'Mumbai', 29.0D, 70.0D, 1.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 11:00:00',
            'file3.json'
        ),

        (
            'B1', 'Delhi', 28.0D, 150.0D, 0.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 10:00:00',
            'file4.json'
        ),

        (
            'B1', 'Bengaluru', 100.0D, 70.0D, -5.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 10:00:00',
            'file5.json'
        )

        AS weather(
            batch_id,
            city,
            temperature_c,
            humidity_pct,
            precipitation_mm,
            forecast_valid_at_utc,
            ingested_at_utc,
            source_file
        )
    """)

    silver_df, rejected_df, metrics = (
        process_silver_weather(bronze_df)
    )

    assert metrics == {
        "input_count": 5,
        "valid_count": 3,
        "rejected_count": 2,
        "silver_count": 2,
        "duplicate_count": 1
    }

    latest = (
        silver_df
        .filter("batch_id = 'B1'")
        .first()
    )

    assert latest.temperature_c == 31.0

    rejected = {
        row.city: row.error_reason
        for row in rejected_df.collect()
    }

    assert "INVALID_HUMIDITY" in rejected["delhi"]

    assert (
        "INVALID_TEMPERATURE"
        in rejected["bengaluru"]
    )

    assert (
        "INVALID_PRECIPITATION"
        in rejected["bengaluru"]
    )

    assert silver_df.select(
        "batch_id"
    ).distinct().count() == 2
