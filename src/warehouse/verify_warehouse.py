
import duckdb
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "warehouse" / "weather.duckdb"

with duckdb.connect(str(DB_PATH), read_only=True) as conn:
    result = conn.execute("""
        SELECT
            c.city_name,
            COUNT(*) AS forecast_hours,
            ROUND(AVG(f.temperature_c), 2)
                AS avg_temperature,
            SUM(
                CASE WHEN f.risk_level = 'CRITICAL'
                THEN 1 ELSE 0 END
            ) AS critical_risk_hours
        FROM fact_weather_hourly f
        JOIN dim_city c
            ON f.city_key = c.city_key
        GROUP BY c.city_name
        ORDER BY c.city_name
    """).fetchall()

    for row in result:
        print(row)
