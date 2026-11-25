
import sys
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "ingestion"))

import audit_logger


def test_audit_success(tmp_path, monkeypatch):
    audit_file = tmp_path / "test_audit.jsonl"
    monkeypatch.setattr(
        audit_logger, "AUDIT_FILE", audit_file
    )

    record = {
        "batch_id": "batch_001",
        "city": "Mumbai",
        "status": "SUCCESS",
        "start_time": "2026-11-17T10:00:00Z",
        "end_time": "2026-11-17T10:00:01Z",
        "records_extracted": 48,
        "duration_seconds": 1.0,
        "file_path": "sample.json",
        "error_message": None,
    }

    audit_logger.write_audit_record(record)

    saved = audit_logger.read_audit_records()

    assert len(saved) == 1
    assert saved[0]["status"] == "SUCCESS"
    assert saved[0]["records_extracted"] == 48
    assert saved[0]["city"] == "Mumbai"


def test_missing_required_fields(
    tmp_path, monkeypatch
):
    import pytest

    monkeypatch.setattr(
        audit_logger,
        "AUDIT_FILE",
        tmp_path / "test_audit.jsonl",
    )

    with pytest.raises(ValueError):
        audit_logger.write_audit_record({
            "batch_id": "batch_002",
            "city": "Delhi",
        })
