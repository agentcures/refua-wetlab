from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _duration_ms(start_iso: str, end_iso: str) -> int | None:
    try:
        start = datetime.fromisoformat(start_iso)
        end = datetime.fromisoformat(end_iso)
    except ValueError:
        return None
    delta = end - start
    return max(int(delta.total_seconds() * 1000), 0)


class RunStore:
    """SQLite-backed metadata store for wet-lab runs."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    status TEXT NOT NULL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    result_json TEXT,
                    error_text TEXT
                )
                """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_updated ON runs(updated_at DESC)"
            )
            columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(runs)").fetchall()
            }
            if "cancel_requested" not in columns:
                conn.execute(
                    "ALTER TABLE runs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0"
                )
            conn.commit()

    def create_run(self, *, provider: str, request: dict[str, Any]) -> dict[str, Any]:
        run_id = str(uuid.uuid4())
        now = _utc_now_iso()
        with self._lock, closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO runs(
                    run_id, provider, status, cancel_requested, created_at, updated_at,
                    request_json, result_json, error_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    provider,
                    "queued",
                    0,
                    now,
                    now,
                    json.dumps(request, ensure_ascii=True),
                    None,
                    None,
                ),
            )
            conn.commit()
        created = self.get_run(run_id)
        if created is None:
            raise RuntimeError("Failed to create run.")
        return created

    def set_running(self, run_id: str) -> bool:
        return self._set_status(run_id, status="running", allow_from=("queued",))

    def set_completed(self, run_id: str, result: dict[str, Any]) -> bool:
        return self._set_status(
            run_id,
            status="completed",
            result=result,
            error=None,
            cancel_requested=False,
            allow_from=("running",),
        )

    def set_failed(self, run_id: str, error: str) -> bool:
        return self._set_status(
            run_id,
            status="failed",
            result=None,
            error=error,
            cancel_requested=False,
            allow_from=("running",),
        )

    def set_cancelled(self, run_id: str, reason: str = "Cancelled by user.") -> bool:
        return self._set_status(
            run_id,
            status="cancelled",
            result=None,
            error=reason,
            cancel_requested=True,
            allow_from=("queued", "running"),
        )

    def request_cancel(
        self,
        run_id: str,
        *,
        reason: str = "Cancellation requested by user.",
    ) -> bool:
        now = _utc_now_iso()
        with self._lock, closing(self._connect()) as conn:
            cursor = conn.execute(
                """
                UPDATE runs
                SET cancel_requested = 1,
                    updated_at = ?,
                    error_text = COALESCE(error_text, ?)
                WHERE run_id = ? AND status = 'running'
                """,
                (now, reason, run_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    def is_cancel_requested(self, run_id: str) -> bool:
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT cancel_requested FROM runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if row is None:
            return False
        return bool(int(row["cancel_requested"]))

    def _set_status(
        self,
        run_id: str,
        *,
        status: str,
        result: dict[str, Any] | None = None,
        error: str | None = None,
        cancel_requested: bool | None = None,
        allow_from: tuple[str, ...] | None = None,
    ) -> bool:
        now = _utc_now_iso()
        result_json = (
            json.dumps(result, ensure_ascii=True) if result is not None else None
        )
        cancel_requested_value = (
            1 if cancel_requested else 0 if cancel_requested is not None else None
        )
        with self._lock, closing(self._connect()) as conn:
            if cancel_requested_value is None:
                set_clause = (
                    "status = ?, updated_at = ?, result_json = ?, error_text = ?"
                )
                values: tuple[Any, ...] = (status, now, result_json, error)
            else:
                set_clause = "status = ?, updated_at = ?, result_json = ?, error_text = ?, cancel_requested = ?"
                values = (
                    status,
                    now,
                    result_json,
                    error,
                    cancel_requested_value,
                )
            if allow_from:
                placeholders = ",".join("?" for _ in allow_from)
                cursor = conn.execute(
                    f"""
                    UPDATE runs
                    SET {set_clause}
                    WHERE run_id = ? AND status IN ({placeholders})
                    """,
                    (*values, run_id, *allow_from),
                )
            else:
                cursor = conn.execute(
                    f"""
                    UPDATE runs
                    SET {set_clause}
                    WHERE run_id = ?
                    """,
                    (*values, run_id),
                )
            conn.commit()
            return cursor.rowcount > 0

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock, closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT run_id, provider, status, created_at, updated_at,
                       cancel_requested, request_json, result_json, error_text
                FROM runs
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_run(row)

    def list_runs(
        self,
        *,
        limit: int = 100,
        statuses: tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        safe_limit = min(max(limit, 1), 1000)
        with self._lock, closing(self._connect()) as conn:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                rows = conn.execute(
                    f"""
                    SELECT run_id, provider, status, created_at, updated_at,
                           cancel_requested, request_json, result_json, error_text
                    FROM runs
                    WHERE status IN ({placeholders})
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (*statuses, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT run_id, provider, status, created_at, updated_at,
                           cancel_requested, request_json, result_json, error_text
                    FROM runs
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
        return [self._row_to_run(row) for row in rows]

    def status_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) AS count FROM runs GROUP BY status"
            ).fetchall()
        for row in rows:
            counts[str(row["status"])] = int(row["count"])
        return counts

    @staticmethod
    def _row_to_run(row: sqlite3.Row) -> dict[str, Any]:
        request_json = row["request_json"]
        result_json = row["result_json"]
        created_at = row["created_at"]
        updated_at = row["updated_at"]
        request = json.loads(request_json) if isinstance(request_json, str) else {}
        result = json.loads(result_json) if isinstance(result_json, str) else None
        return {
            "run_id": row["run_id"],
            "provider": row["provider"],
            "status": row["status"],
            "cancel_requested": bool(int(row["cancel_requested"])),
            "created_at": created_at,
            "updated_at": updated_at,
            "duration_ms": _duration_ms(created_at, updated_at),
            "request": request,
            "result": result,
            "error": row["error_text"],
        }
