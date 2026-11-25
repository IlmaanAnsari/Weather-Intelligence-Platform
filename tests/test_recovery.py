
import sys
import json
from pathlib import Path
from contextlib import nullcontext
from datetime import datetime, timezone

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "ingestion"))

import checkpoint_manager as checkpoints
import audit_logger
import weather_recovery as recovery


def test_partial_failure_and_recovery(tmp_path, monkeypatch):
    monkeypatch.setattr(
        checkpoints, "DB_PATH",
        tmp_path / "checkpoints.db"
    )
    monkeypatch.setattr(
        recovery, "BRONZE",
        tmp_path / "bronze"
    )
    monkeypatch.setattr(
        audit_logger, "AUDIT_FILE",
        tmp_path / "audit.jsonl"
    )
    monkeypatch.setattr(
        recovery, "create_session",
        lambda: nullcontext(None)
    )

    window = datetime(
        2026, 11, 17, 18, tzinfo=timezone.utc
    )
    batch_id = "20261117T18Z"
    calls = []

    def fake_fetch(session, latitude, longitude):
        city = next(
            name for name, coords in recovery.CITIES.items()
            if coords == (latitude, longitude)
        )
        calls.append(city)

        if city == "Delhi" and calls.count("Delhi") == 1:
            raise ValueError("Simulated Delhi API failure")

        return {
            "hourly": {
                "time": ["2026-11-17T18:00"],
                "temperature_2m": [25.0],
                "relative_humidity_2m": [65],
                "precipitation": [0.0]
            }
        }

    # First attempt: Delhi fails.
    with pytest.raises(RuntimeError):
        recovery.run_pipeline(window, fake_fetch)

    assert checkpoints.get_checkpoint(
        batch_id, "Mumbai"
    )["status"] == "SUCCESS"

    assert checkpoints.get_checkpoint(
        batch_id, "Delhi"
    )["status"] == "FAILED"

    assert checkpoints.get_checkpoint(
        batch_id, "Bengaluru"
    )["status"] == "SUCCESS"

    # Second attempt: only Delhi is fetched.
    recovery.run_pipeline(window, fake_fetch)

    assert calls == [
        "Mumbai", "Delhi", "Bengaluru", "Delhi"
    ]

    assert checkpoints.get_checkpoint(
        batch_id, "Delhi"
    )["status"] == "SUCCESS"

    # Third attempt: nothing is fetched.
    recovery.run_pipeline(window, fake_fetch)

    assert len(calls) == 4

    # Exactly three finalized files.
    files = list((tmp_path / "bronze").rglob("*.json"))
    assert len(files) == 3

    # Every processing attempt is audited.
    records = audit_logger.read_audit_records()
    assert len(records) == 4
    assert sum(
        r["status"] == "FAILED" for r in records
    ) == 1
