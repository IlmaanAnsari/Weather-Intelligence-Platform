
import hashlib
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

SOURCE = ROOT / "data" / "bronze" / "weather"
TARGET = ROOT / "data" / "lakehouse" / "bronze" / "weather"


def calculate_hash(path):
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024), b""
        ):
            digest.update(chunk)

    return digest.hexdigest()


def upload_to_lakehouse():
    uploaded = 0
    skipped = 0

    files = sorted(SOURCE.rglob("*.json"))

    if not files:
        raise FileNotFoundError(
            "No Bronze weather files found"
        )

    for source_file in files:
        relative_path = source_file.relative_to(SOURCE)
        destination = TARGET / relative_path
        destination.parent.mkdir(
            parents=True, exist_ok=True
        )

        if destination.exists():
            if (
                calculate_hash(source_file)
                != calculate_hash(destination)
            ):
                raise ValueError(
                    f"Conflicting file: {destination}"
                )

            print(f"SKIPPED: {relative_path}")
            skipped += 1
            continue

        # Copy to a temporary file first.
        temp_path = destination.with_suffix(
            destination.suffix + ".tmp"
        )

        try:
            shutil.copy2(source_file, temp_path)

            if (
                calculate_hash(source_file)
                != calculate_hash(temp_path)
            ):
                raise ValueError("Copy verification failed")

            temp_path.replace(destination)
        finally:
            temp_path.unlink(missing_ok=True)

        print(f"UPLOADED: {relative_path}")
        uploaded += 1

    print(
        f"Completed: {uploaded} uploaded, "
        f"{skipped} skipped"
    )


if __name__ == "__main__":
    upload_to_lakehouse()
