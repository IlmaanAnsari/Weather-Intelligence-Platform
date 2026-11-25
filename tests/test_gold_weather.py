
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(
    0, str(ROOT / "src" / "processing")
)

from bronze_weather import create_spark

from gold_weather import (
    get_latest_forecast,
    build_daily_weather_summary,
    build_delivery_risk_report,
    build_forecast_revision_report
)


@pytest.fixture(scope="module")
def spark():

    session = create_spark()

    yield session

    session.stop()


@pytest.fixture(scope="module")
def silver_df(spark):

    return spark.sql("""
        SELECT * FROM VALUES

        (
            'B1', 'mumbai', 30.0D, 70.0D, 0.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 10:00:00'
        ),

        (
            'B2', 'mumbai', 42.0D, 90.0D, 12.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 11:00:00'
        ),

        (
            'B2', 'mumbai', 28.0D, 70.0D, 3.0D,
            TIMESTAMP '2026-11-19 16:00:00',
            TIMESTAMP '2026-11-19 11:00:00'
        ),

        (
            'B2', 'delhi', 41.0D, 60.0D, 0.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 11:00:00'
        ),

        (
            'B2', 'bengaluru', 25.0D, 60.0D, 0.0D,
            TIMESTAMP '2026-11-19 15:00:00',
            TIMESTAMP '2026-11-19 11:00:00'
        )

        AS weather(
            batch_id,
            city,
            temperature_c,
            humidity_pct,
            precipitation_mm,
            forecast_valid_at_utc,
            ingested_at_utc
        )
    """)


def test_latest_weather_forecast(silver_df):

    latest = get_latest_forecast(silver_df)

    assert latest.count() == 4

    assert latest.filter(
        "batch_id = 'B1'"
    ).count() == 0


def test_daily_weather_summary(silver_df):

    latest = get_latest_forecast(silver_df)

    daily = build_daily_weather_summary(
        latest
    )

    mumbai = daily.filter(
        "city = 'mumbai'"
    ).first()

    assert mumbai.avg_temperature_c == 35.0

    assert mumbai.max_temperature_c == 42.0

    assert mumbai.min_temperature_c == 28.0

    assert mumbai.total_precipitation_mm == 15.0

    assert mumbai.forecast_hours == 2

    assert mumbai.rainy_hours == 2

    assert mumbai.is_complete_day is False


def test_delivery_risk_report(silver_df):

    latest = get_latest_forecast(silver_df)

    risk_df = build_delivery_risk_report(
        latest
    )

    scores = {
        row.temperature_c: (
            row.risk_score,
            row.risk_level
        )
        for row in risk_df.collect()
    }

    assert scores[42.0] == (100, "CRITICAL")

    assert scores[28.0] == (25, "MEDIUM")

    assert scores[41.0] == (30, "MEDIUM")

    assert scores[25.0] == (0, "LOW")

    assert risk_df.count() == latest.count()


def test_forecast_revision_report(silver_df):

    revision_df = (
        build_forecast_revision_report(silver_df)
    )

    record = (
        revision_df
        .filter("""
            city = 'mumbai'
            AND batch_id = 'B2'
            AND temperature_c = 42
        """)
        .first()
    )

    assert record.previous_temperature_c == 30.0

    assert record.temperature_revision_c == 12.0

    assert record.precipitation_revision_mm == 12.0

    assert revision_df.count() == silver_df.count()
