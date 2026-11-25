
import argparse
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = (
    ROOT / "src" / "orchestration" / "pipeline_runner.py"
)

MAX_ATTEMPTS = 4  # Initial run + 3 retries
RETRY_DELAY_SECONDS = 10


def run_with_retry(window, delay=RETRY_DELAY_SECONDS):
    command = [
        sys.executable,
        str(PIPELINE),
        "--window",
        window,
    ]

    for attempt in range(1, MAX_ATTEMPTS + 1):
        print(
            f"\nPipeline attempt {attempt}/{MAX_ATTEMPTS}",
            flush=True,
        )

        try:
            result = subprocess.run(
                command,
                cwd=ROOT,
                timeout=900,
                check=False,
            )
            success = result.returncode == 0

        except (subprocess.TimeoutExpired, OSError) as exc:
            print(f"Execution error: {exc}", flush=True)
            success = False

        if success:
            print(
                "Pipeline completed successfully",
                flush=True,
            )
            return True

        if attempt < MAX_ATTEMPTS:
            print(
                f"Retrying in {delay} seconds...",
                flush=True,
            )
            time.sleep(delay)

    print(
        "Pipeline failed after all attempts",
        flush=True,
    )
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window")
    args = parser.parse_args()

    if args.window:
        # Use the pipeline's normal window validation.
        from pipeline_runner import get_window
        window = get_window(args.window)
    else:
        wipipeline_monitorndow = datetime.now(timezone.utc).replace(
            minute=0,
            second=0,
            microsecond=0,
        )

    success = run_with_retry(window.isoformat())
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
