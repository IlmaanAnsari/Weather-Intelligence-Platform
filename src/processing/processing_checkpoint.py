
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "processing_checkpoints.db"


def initialize():

    DB_PATH.parent.mkdir(
        parents=True, exist_ok=True
    )

    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS
                processing_checkpoint (
                    source_file TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    processed_at TEXT NOT NULL
                )
            """)


def get_processed_files():

    with closing(sqlite3.connect(DB_PATH)) as conn:
        rows = conn.execute("""
            SELECT source_file
            FROM processing_checkpoint
            WHERE status = 'SUCCESS'
        """).fetchall()

    return {row[0] for row in rows}


def mark_success(source_files):

    timestamp = datetime.now(
        timezone.utc
    ).isoformat()

    records = [
        (file, "SUCCESS", timestamp)
        for file in source_files
    ]

    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.executemany("""
                INSERT INTO processing_checkpoint (
                    source_file,
                    status,
                    processed_at
                )
                VALUES (?, ?, ?)
                ON CONFLICT(source_file)
                DO UPDATE SET
                    status = excluded.status,
                    processed_at = excluded.processed_at
            """, records)
