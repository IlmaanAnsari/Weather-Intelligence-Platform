
import sys
from pathlib import Path
from datetime import datetime, timezone
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(
    0, str(ROOT / "src" / "orchestration")
)

import pipeline_runner as runner


def test_pipeline_stops_on_ingestion_failure(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        runner,
        "AUDIT_FILE",
        tmp_path / "pipeline_audit.jsonl"
    )

    commands = []

    def fake_run(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="Simulated ingestion failure"
        )

    monkeypatch.setattr(
        runner.subprocess, "run", fake_run
    )

    window = datetime(
        2026, 11, 18, 15,
        tzinfo=timezone.utc
    )

    result = runner.run_pipeline(window)

    assert result is False
    assert len(commands) == 1

    records = runner.AUDIT_FILE.read_text(
        encoding="utf-8"
    ).splitlines()

    assert len(records) == 1
    assert '"status": "FAILED"' in records[0]
    assert '"stage": "WEATHER_INGESTION"' in records[0]
