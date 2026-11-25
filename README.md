
# Weather Intelligence Platform

An end-to-end Data Engineering project that ingests
weather forecasts, processes them through a Medallion
Architecture, and generates business insights for
logistics and delivery planning.

## Tech Stack

- Python
- REST APIs (Open-Meteo)
- PySpark
- Delta Lake
- Parquet
- DuckDB
- SQL
- Power BI
- Pytest

## Architecture

Weather API
  -> Python Ingestion
  -> Bronze (JSON / Parquet)
  -> Silver (PySpark / Delta Lake)
  -> Gold (Analytical Delta Tables)
  -> DuckDB Warehouse
  -> Power BI

## Features

- Automated weather ingestion for three cities
- Retry and failure recovery
- Incremental processing with checkpoints
- Data quality validation and quarantine
- Idempotent Delta MERGE operations
- Historical forecast tracking
- Delivery risk classification
- Analytical star schema
- Interactive Power BI dashboard
- Automated testing

## Data Quality

The Silver pipeline performs:
- Null validation
- Temperature range checks
- Humidity range checks
- Invalid precipitation detection
- Duplicate detection
- Source-to-target count reconciliation

## Testing

Run the complete test suite:

python run_all_tests.py

Validated result:
15 tests passed.

## Sample Pipeline Results

Bronze: 864
Silver: 864
Latest Forecast: 216
Daily Summary: 12
Delivery Risk: 216

These counts represent a verified development run.

## Dashboard

The Power BI dashboard includes:
- Weather KPI cards
- Hourly temperature trends
- Forecast rainfall analysis
- Delivery risk distribution
- City and date filtering

## Environment

Developed locally using native Windows Spark,
Delta Lake, DuckDB and Power BI Desktop.
No Azure subscription required.
