from __future__ import annotations

import inspect
import queue
import threading
import time
from typing import Any, Callable

from refua_wetlab.storage import RunStore


class RunBackgroundRunner:
    """Executes queued run tasks with explicit priority ordering."""

    def __init__(self, store: RunStore, *, max_workers: int = 2) -> None:
        self._store = store
        self._max_workers = max(1, max_workers)
        self._queue: queue.PriorityQueue[
            tuple[int, int, str, Callable[..., dict[str, Any]] | None]
        ] = queue.PriorityQueue()
        self._lock = threading.Lock()
        self._sequence = 0
        self._threads: list[threading.Thread] = []
        self._shutdown = False
        self._cancel_events: dict[str, threading.Event] = {}

        for index in range(self._max_workers):
            thread = threading.Thread(
                target=self._worker_loop,
                name=f"refua-wetlab-runner-{index}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def submit(
        self,
        *,
        provider: str,
        request: dict[str, Any],
        fn: Callable[..., dict[str, Any]],
        priority: int = 50,
    ) -> dict[str, Any]:
        run = self._store.create_run(provider=provider, request=request)
        run_id = run["run_id"]
        cancel_event = threading.Event()

        with self._lock:
            if self._shutdown:
                raise RuntimeError("runner is shut down")
            sequence = self._sequence
            self._sequence += 1
            self._cancel_events[run_id] = cancel_event

        # Lower numeric key is pulled first, so negate to make larger priorities run earlier.
        self._queue.put((-priority, sequence, run_id, fn))

        refreshed = self._wait_for_visible_transition(run_id)
        if refreshed is None:
            raise RuntimeError("Submitted run cannot be found.")
        return refreshed

    def cancel(self, run_id: str) -> dict[str, Any]:
        run = self._store.get_run(run_id)
        if run is None:
            raise KeyError(run_id)

        if run["status"] == "queued":
            cancelled = self._store.set_cancelled(
                run_id, "Cancelled by user before execution."
            )
            latest = self._store.get_run(run_id) or run
            if cancelled:
                return {
                    "run_id": run_id,
                    "cancelled": True,
                    "status": latest["status"],
                    "message": "Run cancelled.",
                }
            return {
                "run_id": run_id,
                "cancelled": False,
                "status": latest["status"],
                "message": "Run is already running and cannot be cancelled safely.",
            }

        if run["status"] == "running":
            with self._lock:
                cancel_event = self._cancel_events.get(run_id)
            if cancel_event is not None:
                cancel_event.set()
            self._store.request_cancel(
                run_id, reason="Cancellation requested by user while running."
            )
            latest = self._store.get_run(run_id) or run
            return {
                "run_id": run_id,
                "cancelled": True,
                "status": latest["status"],
                "message": "Cancellation requested for running run.",
            }

        return {
            "run_id": run_id,
            "cancelled": False,
            "status": run["status"],
            "message": "Run is not cancellable in its current state.",
        }

    def shutdown(self) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True

        for _ in self._threads:
            self._queue.put((10**9, 10**9, "", None))

        for thread in self._threads:
            thread.join(timeout=2)

    def _worker_loop(self) -> None:
        while True:
            _, _, run_id, fn = self._queue.get()
            try:
                if fn is None:
                    return

                with self._lock:
                    cancel_event = self._cancel_events.get(run_id)
                if cancel_event is None:
                    cancel_event = threading.Event()

                if not self._store.set_running(run_id):
                    continue
                if cancel_event.is_set() or self._store.is_cancel_requested(run_id):
                    self._store.set_cancelled(
                        run_id, "Cancelled by user before execution."
                    )
                    continue

                try:
                    result = _invoke_run_fn(fn, cancel_event=cancel_event)
                except Exception as exc:  # noqa: BLE001
                    if cancel_event.is_set() or self._store.is_cancel_requested(run_id):
                        self._store.set_cancelled(
                            run_id, "Cancelled by user during execution."
                        )
                    else:
                        self._store.set_failed(run_id, str(exc))
                    continue

                if cancel_event.is_set() or self._store.is_cancel_requested(run_id):
                    self._store.set_cancelled(
                        run_id, "Cancelled by user during execution."
                    )
                else:
                    self._store.set_completed(run_id, result)
            finally:
                with self._lock:
                    self._cancel_events.pop(run_id, None)
                self._queue.task_done()

    def _wait_for_visible_transition(self, run_id: str) -> dict[str, Any] | None:
        deadline = time.monotonic() + 0.12
        latest = self._store.get_run(run_id)
        while (
            latest is not None
            and latest["status"] == "queued"
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
            latest = self._store.get_run(run_id)
        return latest


def _invoke_run_fn(
    fn: Callable[..., dict[str, Any]],
    *,
    cancel_event: threading.Event,
) -> dict[str, Any]:
    signature = inspect.signature(fn)
    if "cancel_event" in signature.parameters:
        return fn(cancel_event=cancel_event)
    return fn()
