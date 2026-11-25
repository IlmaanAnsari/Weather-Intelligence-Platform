
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(
    0, str(ROOT / "src" / "orchestration")
)

import scheduled_runner as runner


def test_retry_succeeds_on_third_attempt(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        code = 0 if len(calls) == 3 else 1
        return SimpleNamespace(returncode=code)

    monkeypatch.setattr(
        runner.subprocess, "run", fake_run
    )

    result = runner.run_with_retry(
        "2026-11-18T15:00:00+00:00",
        delay=0
    )

    assert result is True
    assert len(calls) == 3


def test_retry_exhaustion(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(
        runner.subprocess, "run", fake_run
    )

    result = runner.run_with_retry(
        "2026-11-18T15:00:00+00:00",
        delay=0
    )

    assert result is False
    assert len(calls) == 4
# ```

# Run:

# ```powershell
# python -m pytest tests -v
# ```

# <box border radius="lg" padding={3} gap={1}>
#   <text color="secondary" weight="medium" size="xs">EXPECTED TEST RESULT</text>
#   <row align="baseline" gap={2}>
#     <title size="2xl" color="success">7 passed</title>
#     <text color="secondary" size="sm">5 previous + 2 new tests</text>
#   </row>
# </box>

# ## Day 6 assignment checklist

# {@body const tasks=["Created scheduled_runner.py","Executed retry wrapper successfully","Configured hourly Windows Task Scheduler task","Configured non-overlapping scheduled executions","Created pipeline_monitor.py and checked audit output","Created automated retry tests","All seven tests passed"]}
# {@body const [done,setDone] = DIL.useState([])}
# <box border radius="lg" padding={3} gap={3}>
#   <row align="center" justify="between">
#     **Day 6 tasks**
#     <text color="secondary" size="sm" tabularNums>{done.length}/{tasks.length}</text>
#   </row>
#   <box background="surface-tertiary" radius="full" height="6px" clip>
#     <box background="rgba(28,140,95,0.85)" width={`${done.length/tasks.length*100}%`} height="100%" radius="full"/>
#   </box>
#   {#each tasks as task,i}
#     <checkbox key={i} checked={done.includes(i)} onChange={v=>setDone(old=>v?[...old,i]:old.filter(x=>x!==i))}>{task}</checkbox>
#   {/each}
#   <button block disabled={done.length!==tasks.length} onClick={()=>GenUI.issueNewTurn("Weather Intelligence Platform Day 6 completed. Seven automated tests passed, hourly scheduling configured, and monitoring verified. Proceed to Day 7 PySpark Bronze ingestion.")}>Proceed to Day 7 <icon name="arrow-right" inline/></button>
# </box>

# ## Interview question

# Suppose your hourly pipeline starts at 10:00 AM but takes 75 minutes to finish. What happens when the next scheduled execution is triggered at 11:00 AM?

# How would you avoid overlapping runs while ensuring no hourly ingestion windows are missed?

# This is an important distinction: preventing overlapping executions doesn't automatically guarantee that every scheduled hour is processed. We'll address missed-window handling as we harden the pipeline.

# **Next milestone — Day 7:** We'll begin processing the collected JSON data using PySpark and build our first structured Bronze table.