
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(
    0, str(ROOT / "src" / "processing")
)

from spark_config import create_delta_spark

GOLD = ROOT / "data" / "lakehouse" / "gold"
EXPORT = ROOT / "data" / "exports"

TABLES = [
    "delivery_risk_report",
    "daily_weather_summary",
]


def export_gold():
    spark = create_delta_spark()

    try:
        for name in TABLES:
            source = GOLD / name
            target = EXPORT / name

            df = (
                spark.read
                .format("delta")
                .load(str(source))
            )

            count = df.count()

            (
                df.write
                .mode("overwrite")
                .parquet(str(target))
            )

            print(f"{name}: {count} records exported")
    finally:
        spark.stop()


if __name__ == "__main__":
    export_gold()
