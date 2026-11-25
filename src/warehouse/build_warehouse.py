
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]

DB_PATH = ROOT / "data" / "warehouse" / "weather.duckdb"
EXPORT = ROOT / "data" / "exports"


def parquet_glob(table):
    folder = EXPORT / table
    if not list(folder.glob("*.parquet")):
        raise FileNotFoundError(
            f"No exported Parquet files: {folder}"
        )

    return (folder / "*.parquet").as_posix()


def build_warehouse():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    hourly_path = parquet_glob("delivery_risk_report")
    daily_path = parquet_glob("daily_weather_summary")

    conn = duckdb.connect(str(DB_PATH))

    try:
        conn.execute("BEGIN TRANSACTION")

        conn.execute("""
            CREATE OR REPLACE TABLE dim_city AS
            SELECT *
            FROM (VALUES
                (1, 'mumbai'),
                (2, 'delhi'),
                (3, 'bengaluru')
            ) AS cities(city_key, city_name)
        """)

        conn.execute("""
            CREATE OR REPLACE TABLE fact_weather_hourly AS
            SELECT
                c.city_key,
                w.city,
                w.batch_id,
                w.forecast_valid_at_utc,
                w.forecast_date,
                w.temperature_c,
                w.humidity_pct,
                w.precipitation_mm,
                w.risk_score,
                w.risk_level,
                w.delivery_recommendation
            FROM read_parquet(?) w
            JOIN dim_city c
              ON lower(w.city) = c.city_name
        """, [hourly_path])

        conn.execute("""
            CREATE OR REPLACE TABLE fact_weather_daily AS
            SELECT
                c.city_key,
                d.forecast_date,
                d.avg_temperature_c,
                d.max_temperature_c,
                d.min_temperature_c,
                d.avg_humidity_pct,
                d.total_precipitation_mm,
                d.rainy_hours,
                d.forecast_hours,
                d.is_complete_day
            FROM read_parquet(?) d
            JOIN dim_city c
              ON lower(d.city) = c.city_name
        """, [daily_path])

        # Check that no source rows were dropped by city joins.
        expected_hourly = conn.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [hourly_path]
        ).fetchone()[0]

        actual_hourly = conn.execute(
            "SELECT count(*) FROM fact_weather_hourly"
        ).fetchone()[0]

        expected_daily = conn.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [daily_path]
        ).fetchone()[0]

        actual_daily = conn.execute(
            "SELECT count(*) FROM fact_weather_daily"
        ).fetchone()[0]

        if (
            expected_hourly != actual_hourly
            or expected_daily != actual_daily
        ):
            raise RuntimeError(
                "Warehouse count reconciliation failed"
            )

        # Check the natural grain of both fact tables.
        hourly_dups = conn.execute("""
            SELECT count(*) FROM (
                SELECT city_key, forecast_valid_at_utc
                FROM fact_weather_hourly
                GROUP BY 1, 2
                HAVING count(*) > 1
            )
        """).fetchone()[0]

        daily_dups = conn.execute("""
            SELECT count(*) FROM (
                SELECT city_key, forecast_date
                FROM fact_weather_daily
                GROUP BY 1, 2
                HAVING count(*) > 1
            )
        """).fetchone()[0]

        if hourly_dups or daily_dups:
            raise RuntimeError(
                "Duplicate warehouse business keys"
            )

        conn.execute("COMMIT")

        for table in [
            "dim_city",
            "fact_weather_hourly",
            "fact_weather_daily"
        ]:
            count = conn.execute(
                f"SELECT count(*) FROM {table}"
            ).fetchone()[0]

            print(f"{table}: {count} records")

        print("Warehouse reconciliation PASSED")

    except Exception:
        conn.execute("ROLLBACK")
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    build_warehouse()
