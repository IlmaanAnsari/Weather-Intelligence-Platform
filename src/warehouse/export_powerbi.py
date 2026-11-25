
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parents[2]

DB_PATH = ROOT / "data" / "warehouse" / "weather.duckdb"
OUTPUT = ROOT / "data" / "powerbi"

TABLES = [
    "dim_city",
    "fact_weather_hourly",
    "fact_weather_daily"
]


def export_powerbi_data():
    OUTPUT.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(
        str(DB_PATH), read_only=True
    ) as conn:

        for table in TABLES:
            output_file = OUTPUT / f"{table}.csv"

            # Table names are from a fixed trusted list.
            conn.execute(
                f"COPY (SELECT * FROM {table}) "
                "TO ? (FORMAT CSV, HEADER TRUE)",
                [str(output_file)]
            )

            count = conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]

            print(
                f"EXPORTED: {table} "
                f"({count} records)"
            )


if __name__ == "__main__":
    export_powerbi_data()
