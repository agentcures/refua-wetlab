from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

from refua_wetlab.runner import RunBackgroundRunner
from refua_wetlab.storage import RunStore


def test_priority_queue_runs_higher_priority_first() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = RunStore(Path(tmp_dir) / "runs.sqlite3")
        runner = RunBackgroundRunner(store, max_workers=1)

        started = threading.Event()
        release = threading.Event()
        order: list[str] = []
        order_lock = threading.Lock()

        def blocking_task() -> dict[str, bool]:
            started.set()
            release.wait(timeout=5)
            return {"ok": True}

        def low_priority_task() -> dict[str, bool]:
            with order_lock:
                order.append("low")
            return {"ok": True}

        def high_priority_task() -> dict[str, bool]:
            with order_lock:
                order.append("high")
            return {"ok": True}

        first = runner.submit(
            provider="opentrons",
            request={"provider": "opentrons", "protocol": {"name": "blocking"}},
            fn=blocking_task,
            priority=50,
        )
        assert first["status"] in {"queued", "running"}
        assert started.wait(timeout=2)

        low = runner.submit(
            provider="opentrons",
            request={"provider": "opentrons", "protocol": {"name": "low"}},
            fn=low_priority_task,
            priority=10,
        )
        high = runner.submit(
            provider="opentrons",
            request={"provider": "opentrons", "protocol": {"name": "high"}},
            fn=high_priority_task,
            priority=90,
        )
        assert low["status"] == "queued"
        assert high["status"] == "queued"

        release.set()

        deadline = time.time() + 5
        while time.time() < deadline:
            low_run = store.get_run(low["run_id"])
            high_run = store.get_run(high["run_id"])
            if (
                low_run
                and high_run
                and low_run["status"] == "completed"
                and high_run["status"] == "completed"
            ):
                break
            time.sleep(0.05)

        assert order == ["high", "low"]

        runner.shutdown()


def test_running_run_can_be_cancel_requested() -> None:
    with tempfile.TemporaryDirectory() as tmp_dir:
        store = RunStore(Path(tmp_dir) / "runs.sqlite3")
        runner = RunBackgroundRunner(store, max_workers=1)

        started = threading.Event()

        def cancellable_task(*, cancel_event: threading.Event) -> dict[str, bool]:
            started.set()
            deadline = time.time() + 5
            while time.time() < deadline:
                if cancel_event.is_set():
                    return {"cancelled": True}
                time.sleep(0.02)
            return {"ok": True}

        run = runner.submit(
            provider="opentrons",
            request={"provider": "opentrons", "protocol": {"name": "running"}},
            fn=cancellable_task,
            priority=50,
        )
        assert run["status"] in {"queued", "running"}
        assert started.wait(timeout=2)

        cancelled = runner.cancel(run["run_id"])
        assert cancelled["cancelled"] is True

        deadline = time.time() + 5
        latest = store.get_run(run["run_id"])
        while (
            latest is not None
            and latest["status"] in {"queued", "running"}
            and time.time() < deadline
        ):
            time.sleep(0.05)
            latest = store.get_run(run["run_id"])

        assert latest is not None
        assert latest["status"] == "cancelled"
        assert latest["cancel_requested"] is True

        runner.shutdown()
