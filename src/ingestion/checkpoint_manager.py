
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "weather_checkpoints.db"


def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def initialize_checkpoints():
    with connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_checkpoint (
                batch_id TEXT NOT NULL,
                city TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK(status IN ('RUNNING','SUCCESS','FAILED')),
                attempts INTEGER NOT NULL DEFAULT 0,
                file_path TEXT,
                error_message TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (batch_id, city)
            )
        """)


def get_checkpoint(batch_id, city):
    with connect() as conn:
        row = conn.execute("""
            SELECT status, attempts, file_path
            FROM ingestion_checkpoint
            WHERE batch_id = ? AND city = ?
        """, (batch_id, city)).fetchone()

    if row is None:
        return None

    return {
        "status": row[0],
        "attempts": row[1],
        "file_path": row[2],
    }


def update_checkpoint(
    batch_id, city, status,
    file_path=None, error_message=None
):
    now = datetime.now(timezone.utc).isoformat()

    with connect() as conn:
        conn.execute("""
            INSERT INTO ingestion_checkpoint (
                batch_id, city, status, attempts,
                file_path, error_message, updated_at
            )
            VALUES (?, ?, ?, 1, ?, ?, ?)
            ON CONFLICT(batch_id, city)
            DO UPDATE SET
                status = excluded.status,
                attempts = CASE
                    WHEN excluded.status = 'RUNNING'
                    THEN ingestion_checkpoint.attempts + 1
                    ELSE ingestion_checkpoint.attempts
                END,
                file_path = excluded.file_path,
                error_message = excluded.error_message,
                updated_at = excluded.updated_at
        """, (
            batch_id, city, status,
            file_path, error_message, now
        ))
