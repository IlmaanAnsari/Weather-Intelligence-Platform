
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(
    0, str(ROOT / "src" / "processing")
)

import processing_checkpoint as checkpoint


def test_processing_checkpoint(
    tmp_path, monkeypatch
):

    monkeypatch.setattr(
        checkpoint,
        "DB_PATH",
        tmp_path / "checkpoint.db"
    )

    checkpoint.initialize()

    assert checkpoint.get_processed_files() == set()

    checkpoint.mark_success([
        "mumbai/file1.json",
        "delhi/file2.json"
    ])

    assert checkpoint.get_processed_files() == {
        "mumbai/file1.json",
        "delhi/file2.json"
    }

    # Retrying must not create duplicate checkpoints.
    checkpoint.mark_success([
        "mumbai/file1.json"
    ])

    assert len(
        checkpoint.get_processed_files()
    ) == 2
