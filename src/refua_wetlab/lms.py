from __future__ import annotations

import json
import re
import sqlite3
import threading
import uuid
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

PROJECT_STATUSES = frozenset({"planned", "active", "on_hold", "completed", "archived"})
SAMPLE_STATUSES = frozenset(
    {
        "registered",
        "in_use",
        "consumed",
        "failed_qc",
        "archived",
    }
)
PLATE_STATUSES = frozenset({"ready", "in_use", "completed", "archived"})
INVENTORY_STATUSES = frozenset({"active", "quarantined", "depleted", "expired"})
EXPERIMENT_STATUSES = frozenset(
    {
        "draft",
        "scheduled",
        "running",
        "completed",
        "cancelled",
        "failed",
    }
)

_WELL_RE = re.compile(r"^[A-Za-z]{1,2}[0-9]{1,2}$")


class LmsError(ValueError):
    """Base class for LMS store exceptions."""


class LmsValidationError(LmsError):
    """Raised when payloads fail LMS validation."""


class LmsConflictError(LmsError):
    """Raised when a valid request cannot be fulfilled due to state conflict."""


class LmsNotFoundError(KeyError):
    """Raised when an LMS entity cannot be found."""


class LmsStore:
    """SQLite-backed LMS metadata store for wet-lab operations."""

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
                CREATE TABLE IF NOT EXISTS projects (
                    project_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    owner TEXT,
                    description TEXT,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS samples (
                    sample_id TEXT PRIMARY KEY,
                    project_id TEXT,
                    name TEXT NOT NULL,
                    sample_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    concentration_ng_ul REAL,
                    volume_ul REAL,
                    storage_location TEXT,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sample_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sample_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    notes TEXT,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS plates (
                    plate_id TEXT PRIMARY KEY,
                    project_id TEXT,
                    plate_type TEXT NOT NULL,
                    label TEXT NOT NULL,
                    status TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS plate_wells (
                    plate_id TEXT NOT NULL,
                    well TEXT NOT NULL,
                    sample_id TEXT NOT NULL,
                    volume_ul REAL NOT NULL,
                    assigned_at TEXT NOT NULL,
                    PRIMARY KEY (plate_id, well)
                )
                """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inventory_items (
                    item_id TEXT PRIMARY KEY,
                    sku TEXT,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    reorder_threshold REAL NOT NULL,
                    lot_number TEXT,
                    expiration_date TEXT,
                    storage_location TEXT,
                    status TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inventory_transactions (
                    tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id TEXT NOT NULL,
                    delta REAL NOT NULL,
                    reason TEXT NOT NULL,
                    actor TEXT,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS experiments (
                    experiment_id TEXT PRIMARY KEY,
                    project_id TEXT,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    provider TEXT,
                    protocol_json TEXT NOT NULL,
                    sample_ids_json TEXT NOT NULL,
                    run_id TEXT,
                    metadata_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """)

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_projects_updated ON projects(updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_samples_updated ON samples(updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_samples_project ON samples(project_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_sample_events_sample ON sample_events(sample_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_plates_updated ON plates(updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_inventory_updated ON inventory_items(updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_inventory_txs_item ON inventory_transactions(item_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_experiments_updated ON experiments(updated_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_events(created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_events(entity_type, entity_id, created_at DESC)"
            )
            conn.commit()

    def core_counts(self) -> dict[str, int]:
        with self._lock, closing(self._connect()) as conn:
            return {
                "projects": _table_count(conn, "projects"),
                "samples": _table_count(conn, "samples"),
                "plates": _table_count(conn, "plates"),
                "inventory_items": _table_count(conn, "inventory_items"),
                "experiments": _table_count(conn, "experiments"),
                "audit_events": _table_count(conn, "audit_events"),
            }

    def summary(self, *, expiring_within_days: int = 14) -> dict[str, Any]:
        window_days = min(max(expiring_within_days, 1), 365)
        today = date.today()
        max_expiration = (today + timedelta(days=window_days)).isoformat()

        with self._lock, closing(self._connect()) as conn:
            projects_by_status = _status_counts(conn, "projects")
            samples_by_status = _status_counts(conn, "samples")
            plates_by_status = _status_counts(conn, "plates")
            experiments_by_status = _status_counts(conn, "experiments")

            inventory_status_rows = conn.execute(
                "SELECT status, COUNT(*) AS count FROM inventory_items GROUP BY status"
            ).fetchall()
            inventory_by_status = {
                str(row["status"]): int(row["count"]) for row in inventory_status_rows
            }

            low_stock_rows = conn.execute("""
                SELECT item_id, name, quantity, unit, reorder_threshold
                FROM inventory_items
                WHERE status IN ('active', 'quarantined', 'depleted')
                  AND quantity <= reorder_threshold
                ORDER BY (reorder_threshold - quantity) DESC, updated_at DESC
                LIMIT 20
                """).fetchall()

            expiring_rows = conn.execute(
                """
                SELECT item_id, name, expiration_date, quantity, unit
                FROM inventory_items
                WHERE expiration_date IS NOT NULL
                  AND expiration_date != ''
                  AND expiration_date <= ?
                ORDER BY expiration_date ASC, updated_at DESC
                LIMIT 20
                """,
                (max_expiration,),
            ).fetchall()

            recent_audit = self._list_audit_events_locked(conn, limit=20)

            run_counts: dict[str, int] = {}
            try:
                run_rows = conn.execute(
                    "SELECT status, COUNT(*) AS count FROM runs GROUP BY status"
                ).fetchall()
                run_counts = {str(row["status"]): int(row["count"]) for row in run_rows}
            except sqlite3.OperationalError:
                run_counts = {}

        return {
            "generated_at": _utc_now_iso(),
            "counts": {
                "projects": {
                    "total": sum(projects_by_status.values()),
                    "by_status": projects_by_status,
                },
                "samples": {
                    "total": sum(samples_by_status.values()),
                    "by_status": samples_by_status,
                },
                "plates": {
                    "total": sum(plates_by_status.values()),
                    "by_status": plates_by_status,
                },
                "inventory_items": {
                    "total": sum(inventory_by_status.values()),
                    "by_status": inventory_by_status,
                },
                "experiments": {
                    "total": sum(experiments_by_status.values()),
                    "by_status": experiments_by_status,
                },
                "runs": run_counts,
            },
            "inventory_alerts": {
                "low_stock": [
                    {
                        "item_id": str(row["item_id"]),
                        "name": str(row["name"]),
                        "quantity": float(row["quantity"]),
                        "unit": str(row["unit"]),
                        "reorder_threshold": float(row["reorder_threshold"]),
                    }
                    for row in low_stock_rows
                ],
                "expiring_soon": [
                    {
                        "item_id": str(row["item_id"]),
                        "name": str(row["name"]),
                        "expiration_date": str(row["expiration_date"]),
                        "quantity": float(row["quantity"]),
                        "unit": str(row["unit"]),
                    }
                    for row in expiring_rows
                ],
                "window_days": window_days,
            },
            "recent_audit_events": recent_audit,
        }

    def create_project(
        self, payload: Mapping[str, Any], *, actor: str | None = None
    ) -> dict[str, Any]:
        name = _require_nonempty_string(payload.get("name"), "name")
        project_id = _optional_nonempty_string(payload.get("project_id")) or str(
            uuid.uuid4()
        )
        owner = _optional_nonempty_string(payload.get("owner"))
        description = _optional_nonempty_string(payload.get("description"))
        status = _normalize_status(
            payload.get("status", "active"),
            field_name="status",
            allowed=PROJECT_STATUSES,
        )
        priority = _require_int(
            payload.get("priority", 50), "priority", minimum=0, maximum=100
        )
        metadata = _normalize_metadata(payload.get("metadata"))
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            self._ensure_unique_id(
                conn, "projects", "project_id", project_id, entity="project"
            )
            conn.execute(
                """
                INSERT INTO projects(
                    project_id, name, owner, description, status, priority,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    name,
                    owner,
                    description,
                    status,
                    priority,
                    json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                    now,
                    now,
                ),
            )
            self._write_audit_locked(
                conn,
                entity_type="project",
                entity_id=project_id,
                action="created",
                actor=actor,
                payload={
                    "name": name,
                    "owner": owner,
                    "status": status,
                    "priority": priority,
                },
                created_at=now,
            )
            conn.commit()
            created = self._get_project_locked(conn, project_id)

        if created is None:
            raise RuntimeError("failed to create project")
        return created

    def list_projects(
        self,
        *,
        limit: int = 100,
        statuses: tuple[str, ...] | None = None,
        owner: str | None = None,
    ) -> list[dict[str, Any]]:
        safe_limit = _safe_limit(limit)
        where: list[str] = []
        params: list[Any] = []

        normalized_statuses = _normalize_statuses(
            statuses, allowed=PROJECT_STATUSES, field_name="status"
        )
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            where.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)

        owner_value = _optional_nonempty_string(owner)
        if owner_value is not None:
            where.append("owner = ?")
            params.append(owner_value)

        query = (
            "SELECT project_id, name, owner, description, status, priority, "
            "metadata_json, created_at, updated_at FROM projects"
        )
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(safe_limit)

        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [self._project_row_to_payload(row) for row in rows]

    def get_project(self, project_id: str) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(project_id, "project_id")
        with self._lock, closing(self._connect()) as conn:
            project = self._get_project_locked(conn, normalized_id)
        if project is None:
            raise LmsNotFoundError(f"Unknown project_id: {normalized_id}")
        return project

    def update_project_status(
        self,
        project_id: str,
        *,
        status: str,
        actor: str | None = None,
        notes: str | None = None,
    ) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(project_id, "project_id")
        normalized_status = _normalize_status(
            status,
            field_name="status",
            allowed=PROJECT_STATUSES,
        )
        notes_value = _optional_nonempty_string(notes)
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            current = self._get_project_locked(conn, normalized_id)
            if current is None:
                raise LmsNotFoundError(f"Unknown project_id: {normalized_id}")
            conn.execute(
                "UPDATE projects SET status = ?, updated_at = ? WHERE project_id = ?",
                (normalized_status, now, normalized_id),
            )
            self._write_audit_locked(
                conn,
                entity_type="project",
                entity_id=normalized_id,
                action="status_updated",
                actor=actor,
                payload={
                    "from": current["status"],
                    "to": normalized_status,
                    "notes": notes_value,
                },
                created_at=now,
            )
            conn.commit()
            project = self._get_project_locked(conn, normalized_id)

        if project is None:
            raise RuntimeError("failed to update project")
        return project

    def create_sample(
        self, payload: Mapping[str, Any], *, actor: str | None = None
    ) -> dict[str, Any]:
        sample_id = _optional_nonempty_string(payload.get("sample_id")) or str(
            uuid.uuid4()
        )
        project_id = _optional_nonempty_string(payload.get("project_id"))
        name = _require_nonempty_string(payload.get("name"), "name")
        sample_type = _require_nonempty_string(
            payload.get("sample_type"), "sample_type"
        )
        status = _normalize_status(
            payload.get("status", "registered"),
            field_name="status",
            allowed=SAMPLE_STATUSES,
        )
        concentration_ng_ul = _optional_non_negative_number(
            payload.get("concentration_ng_ul"),
            "concentration_ng_ul",
        )
        volume_ul = _optional_non_negative_number(payload.get("volume_ul"), "volume_ul")
        storage_location = _optional_nonempty_string(payload.get("storage_location"))
        metadata = _normalize_metadata(payload.get("metadata"))
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            self._ensure_unique_id(
                conn, "samples", "sample_id", sample_id, entity="sample"
            )
            if project_id is not None:
                self._ensure_project_exists_locked(conn, project_id)

            conn.execute(
                """
                INSERT INTO samples(
                    sample_id, project_id, name, sample_type, status,
                    concentration_ng_ul, volume_ul, storage_location,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sample_id,
                    project_id,
                    name,
                    sample_type,
                    status,
                    concentration_ng_ul,
                    volume_ul,
                    storage_location,
                    json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                    now,
                    now,
                ),
            )
            self._insert_sample_event_locked(
                conn,
                sample_id=sample_id,
                event_type="registered",
                actor=actor,
                notes="Sample registered",
                metadata={
                    "status": status,
                    "volume_ul": volume_ul,
                },
                created_at=now,
            )
            self._write_audit_locked(
                conn,
                entity_type="sample",
                entity_id=sample_id,
                action="created",
                actor=actor,
                payload={
                    "project_id": project_id,
                    "sample_type": sample_type,
                    "status": status,
                },
                created_at=now,
            )
            conn.commit()
            sample = self._get_sample_locked(conn, sample_id)

        if sample is None:
            raise RuntimeError("failed to create sample")
        return sample

    def list_samples(
        self,
        *,
        limit: int = 100,
        statuses: tuple[str, ...] | None = None,
        project_id: str | None = None,
        sample_type: str | None = None,
    ) -> list[dict[str, Any]]:
        safe_limit = _safe_limit(limit)
        where: list[str] = []
        params: list[Any] = []

        normalized_statuses = _normalize_statuses(
            statuses, allowed=SAMPLE_STATUSES, field_name="status"
        )
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            where.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)

        project_value = _optional_nonempty_string(project_id)
        if project_value is not None:
            where.append("project_id = ?")
            params.append(project_value)

        sample_type_value = _optional_nonempty_string(sample_type)
        if sample_type_value is not None:
            where.append("sample_type = ?")
            params.append(sample_type_value)

        query = (
            "SELECT sample_id, project_id, name, sample_type, status, concentration_ng_ul, "
            "volume_ul, storage_location, metadata_json, created_at, updated_at FROM samples"
        )
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(safe_limit)

        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [self._sample_row_to_payload(row) for row in rows]

    def get_sample(self, sample_id: str) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(sample_id, "sample_id")
        with self._lock, closing(self._connect()) as conn:
            sample = self._get_sample_locked(conn, normalized_id)
        if sample is None:
            raise LmsNotFoundError(f"Unknown sample_id: {normalized_id}")
        return sample

    def update_sample_status(
        self,
        sample_id: str,
        *,
        status: str,
        actor: str | None = None,
        notes: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(sample_id, "sample_id")
        normalized_status = _normalize_status(
            status,
            field_name="status",
            allowed=SAMPLE_STATUSES,
        )
        notes_value = _optional_nonempty_string(notes)
        metadata_payload = _normalize_metadata(metadata)
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            current = self._get_sample_locked(conn, normalized_id)
            if current is None:
                raise LmsNotFoundError(f"Unknown sample_id: {normalized_id}")
            conn.execute(
                "UPDATE samples SET status = ?, updated_at = ? WHERE sample_id = ?",
                (normalized_status, now, normalized_id),
            )
            self._insert_sample_event_locked(
                conn,
                sample_id=normalized_id,
                event_type="status_updated",
                actor=actor,
                notes=notes_value,
                metadata={
                    "from": current["status"],
                    "to": normalized_status,
                    **metadata_payload,
                },
                created_at=now,
            )
            self._write_audit_locked(
                conn,
                entity_type="sample",
                entity_id=normalized_id,
                action="status_updated",
                actor=actor,
                payload={
                    "from": current["status"],
                    "to": normalized_status,
                    "notes": notes_value,
                },
                created_at=now,
            )
            conn.commit()
            sample = self._get_sample_locked(conn, normalized_id)

        if sample is None:
            raise RuntimeError("failed to update sample")
        return sample

    def add_sample_event(
        self,
        sample_id: str,
        *,
        event_type: str,
        actor: str | None = None,
        notes: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(sample_id, "sample_id")
        normalized_event = _require_nonempty_string(event_type, "event_type")
        notes_value = _optional_nonempty_string(notes)
        metadata_payload = _normalize_metadata(metadata)
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            if self._get_sample_locked(conn, normalized_id) is None:
                raise LmsNotFoundError(f"Unknown sample_id: {normalized_id}")
            event_id = self._insert_sample_event_locked(
                conn,
                sample_id=normalized_id,
                event_type=normalized_event,
                actor=actor,
                notes=notes_value,
                metadata=metadata_payload,
                created_at=now,
            )
            self._write_audit_locked(
                conn,
                entity_type="sample",
                entity_id=normalized_id,
                action="event_added",
                actor=actor,
                payload={
                    "event_type": normalized_event,
                    "notes": notes_value,
                    "event_id": event_id,
                },
                created_at=now,
            )
            conn.commit()
            event = self._get_sample_event_locked(conn, event_id)

        if event is None:
            raise RuntimeError("failed to create sample event")
        return event

    def list_sample_events(
        self, sample_id: str, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        normalized_id = _require_nonempty_string(sample_id, "sample_id")
        safe_limit = _safe_limit(limit)

        with self._lock, closing(self._connect()) as conn:
            if self._get_sample_locked(conn, normalized_id) is None:
                raise LmsNotFoundError(f"Unknown sample_id: {normalized_id}")
            rows = conn.execute(
                """
                SELECT event_id, sample_id, event_type, actor, notes, metadata_json, created_at
                FROM sample_events
                WHERE sample_id = ?
                ORDER BY created_at DESC, event_id DESC
                LIMIT ?
                """,
                (normalized_id, safe_limit),
            ).fetchall()
        return [self._sample_event_row_to_payload(row) for row in rows]

    def create_plate(
        self, payload: Mapping[str, Any], *, actor: str | None = None
    ) -> dict[str, Any]:
        plate_id = _optional_nonempty_string(payload.get("plate_id")) or str(
            uuid.uuid4()
        )
        project_id = _optional_nonempty_string(payload.get("project_id"))
        plate_type = _require_nonempty_string(
            payload.get("plate_type", "96"), "plate_type"
        )
        label = _require_nonempty_string(payload.get("label"), "label")
        status = _normalize_status(
            payload.get("status", "ready"),
            field_name="status",
            allowed=PLATE_STATUSES,
        )
        metadata = _normalize_metadata(payload.get("metadata"))
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            self._ensure_unique_id(conn, "plates", "plate_id", plate_id, entity="plate")
            if project_id is not None:
                self._ensure_project_exists_locked(conn, project_id)
            conn.execute(
                """
                INSERT INTO plates(
                    plate_id, project_id, plate_type, label, status,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plate_id,
                    project_id,
                    plate_type,
                    label,
                    status,
                    json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                    now,
                    now,
                ),
            )
            self._write_audit_locked(
                conn,
                entity_type="plate",
                entity_id=plate_id,
                action="created",
                actor=actor,
                payload={
                    "project_id": project_id,
                    "plate_type": plate_type,
                    "status": status,
                },
                created_at=now,
            )
            conn.commit()
            plate = self._get_plate_locked(conn, plate_id, include_assignments=True)

        if plate is None:
            raise RuntimeError("failed to create plate")
        return plate

    def list_plates(
        self,
        *,
        limit: int = 100,
        statuses: tuple[str, ...] | None = None,
        project_id: str | None = None,
    ) -> list[dict[str, Any]]:
        safe_limit = _safe_limit(limit)
        where: list[str] = []
        params: list[Any] = []

        normalized_statuses = _normalize_statuses(
            statuses, allowed=PLATE_STATUSES, field_name="status"
        )
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            where.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)

        project_value = _optional_nonempty_string(project_id)
        if project_value is not None:
            where.append("project_id = ?")
            params.append(project_value)

        query = (
            "SELECT plate_id, project_id, plate_type, label, status, metadata_json, "
            "created_at, updated_at FROM plates"
        )
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(safe_limit)

        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [self._plate_row_to_payload(row, assignments=[]) for row in rows]

    def get_plate(self, plate_id: str) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(plate_id, "plate_id")
        with self._lock, closing(self._connect()) as conn:
            plate = self._get_plate_locked(
                conn, normalized_id, include_assignments=True
            )
        if plate is None:
            raise LmsNotFoundError(f"Unknown plate_id: {normalized_id}")
        return plate

    def assign_sample_to_plate(
        self,
        plate_id: str,
        payload: Mapping[str, Any],
        *,
        actor: str | None = None,
    ) -> dict[str, Any]:
        normalized_plate_id = _require_nonempty_string(plate_id, "plate_id")
        sample_id = _require_nonempty_string(payload.get("sample_id"), "sample_id")
        well = _normalize_well(payload.get("well"))
        volume_ul = _require_positive_number(payload.get("volume_ul"), "volume_ul")
        notes = _optional_nonempty_string(payload.get("notes"))
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            plate = self._get_plate_locked(
                conn, normalized_plate_id, include_assignments=False
            )
            if plate is None:
                raise LmsNotFoundError(f"Unknown plate_id: {normalized_plate_id}")
            sample = self._get_sample_locked(conn, sample_id)
            if sample is None:
                raise LmsNotFoundError(f"Unknown sample_id: {sample_id}")

            plate_project = _optional_nonempty_string(plate.get("project_id"))
            sample_project = _optional_nonempty_string(sample.get("project_id"))
            if plate_project and sample_project and plate_project != sample_project:
                raise LmsConflictError(
                    "sample project_id does not match plate project_id"
                )

            existing = conn.execute(
                "SELECT 1 FROM plate_wells WHERE plate_id = ? AND well = ?",
                (normalized_plate_id, well),
            ).fetchone()
            if existing is not None:
                raise LmsConflictError(
                    f"Plate well already assigned: {normalized_plate_id}:{well}"
                )

            sample_volume = sample.get("volume_ul")
            if sample_volume is not None:
                sample_volume_float = float(sample_volume)
                if sample_volume_float + 1e-9 < volume_ul:
                    raise LmsValidationError(
                        f"sample {sample_id} has insufficient volume ({round(sample_volume_float, 3)} uL)"
                    )
                remaining = max(sample_volume_float - volume_ul, 0.0)
                next_status = "consumed" if remaining <= 1e-9 else "in_use"
                conn.execute(
                    """
                    UPDATE samples
                    SET volume_ul = ?, status = ?, updated_at = ?
                    WHERE sample_id = ?
                    """,
                    (remaining, next_status, now, sample_id),
                )
            else:
                remaining = None

            conn.execute(
                """
                INSERT INTO plate_wells(plate_id, well, sample_id, volume_ul, assigned_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (normalized_plate_id, well, sample_id, volume_ul, now),
            )

            if plate["status"] == "ready":
                conn.execute(
                    "UPDATE plates SET status = ?, updated_at = ? WHERE plate_id = ?",
                    ("in_use", now, normalized_plate_id),
                )
            else:
                conn.execute(
                    "UPDATE plates SET updated_at = ? WHERE plate_id = ?",
                    (now, normalized_plate_id),
                )

            self._insert_sample_event_locked(
                conn,
                sample_id=sample_id,
                event_type="aliquoted_to_plate",
                actor=actor,
                notes=notes,
                metadata={
                    "plate_id": normalized_plate_id,
                    "well": well,
                    "volume_ul": volume_ul,
                    "remaining_volume_ul": remaining,
                },
                created_at=now,
            )

            self._write_audit_locked(
                conn,
                entity_type="plate",
                entity_id=normalized_plate_id,
                action="assignment_created",
                actor=actor,
                payload={
                    "sample_id": sample_id,
                    "well": well,
                    "volume_ul": volume_ul,
                },
                created_at=now,
            )
            conn.commit()
            plate_with_assignments = self._get_plate_locked(
                conn,
                normalized_plate_id,
                include_assignments=True,
            )

        if plate_with_assignments is None:
            raise RuntimeError("failed to assign sample to plate")
        return plate_with_assignments

    def create_inventory_item(
        self,
        payload: Mapping[str, Any],
        *,
        actor: str | None = None,
    ) -> dict[str, Any]:
        item_id = _optional_nonempty_string(payload.get("item_id")) or str(uuid.uuid4())
        sku = _optional_nonempty_string(payload.get("sku"))
        name = _require_nonempty_string(payload.get("name"), "name")
        category = _require_nonempty_string(
            payload.get("category", "reagent"), "category"
        )
        unit = _require_nonempty_string(payload.get("unit", "units"), "unit")
        quantity = _require_non_negative_number(payload.get("quantity", 0), "quantity")
        reorder_threshold = _require_non_negative_number(
            payload.get("reorder_threshold", 0),
            "reorder_threshold",
        )
        lot_number = _optional_nonempty_string(payload.get("lot_number"))
        expiration_date = _normalize_date(
            payload.get("expiration_date"), "expiration_date"
        )
        storage_location = _optional_nonempty_string(payload.get("storage_location"))
        status = _normalize_status(
            payload.get("status", "active"),
            field_name="status",
            allowed=INVENTORY_STATUSES,
        )
        metadata = _normalize_metadata(payload.get("metadata"))
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            self._ensure_unique_id(
                conn,
                "inventory_items",
                "item_id",
                item_id,
                entity="inventory item",
            )
            conn.execute(
                """
                INSERT INTO inventory_items(
                    item_id, sku, name, category, unit,
                    quantity, reorder_threshold, lot_number, expiration_date,
                    storage_location, status, metadata_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    sku,
                    name,
                    category,
                    unit,
                    quantity,
                    reorder_threshold,
                    lot_number,
                    expiration_date,
                    storage_location,
                    status,
                    json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                    now,
                    now,
                ),
            )

            if abs(quantity) > 1e-12:
                self._insert_inventory_tx_locked(
                    conn,
                    item_id=item_id,
                    delta=quantity,
                    reason="initial_stock",
                    actor=actor,
                    metadata={"created": True},
                    created_at=now,
                )

            self._write_audit_locked(
                conn,
                entity_type="inventory_item",
                entity_id=item_id,
                action="created",
                actor=actor,
                payload={
                    "name": name,
                    "quantity": quantity,
                    "unit": unit,
                    "status": status,
                    "expiration_date": expiration_date,
                },
                created_at=now,
            )
            conn.commit()
            item = self._get_inventory_item_locked(conn, item_id)

        if item is None:
            raise RuntimeError("failed to create inventory item")
        return item

    def list_inventory_items(
        self,
        *,
        limit: int = 100,
        statuses: tuple[str, ...] | None = None,
        category: str | None = None,
        below_reorder: bool = False,
    ) -> list[dict[str, Any]]:
        safe_limit = _safe_limit(limit)
        where: list[str] = []
        params: list[Any] = []

        normalized_statuses = _normalize_statuses(
            statuses,
            allowed=INVENTORY_STATUSES,
            field_name="status",
        )
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            where.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)

        category_value = _optional_nonempty_string(category)
        if category_value is not None:
            where.append("category = ?")
            params.append(category_value)

        if below_reorder:
            where.append("quantity <= reorder_threshold")

        query = (
            "SELECT item_id, sku, name, category, unit, quantity, reorder_threshold, "
            "lot_number, expiration_date, storage_location, status, metadata_json, "
            "created_at, updated_at FROM inventory_items"
        )
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(safe_limit)

        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [self._inventory_row_to_payload(row) for row in rows]

    def get_inventory_item(self, item_id: str) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(item_id, "item_id")
        with self._lock, closing(self._connect()) as conn:
            item = self._get_inventory_item_locked(conn, normalized_id)
        if item is None:
            raise LmsNotFoundError(f"Unknown item_id: {normalized_id}")
        return item

    def record_inventory_transaction(
        self,
        item_id: str,
        payload: Mapping[str, Any],
        *,
        actor: str | None = None,
    ) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(item_id, "item_id")
        delta = _require_number(payload.get("delta"), "delta")
        if abs(delta) <= 1e-12:
            raise LmsValidationError("delta must be non-zero")
        reason = _require_nonempty_string(payload.get("reason"), "reason")
        metadata = _normalize_metadata(payload.get("metadata"))
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            current = self._get_inventory_item_locked(conn, normalized_id)
            if current is None:
                raise LmsNotFoundError(f"Unknown item_id: {normalized_id}")

            current_quantity = float(current["quantity"])
            next_quantity = round(current_quantity + delta, 6)
            if next_quantity < -1e-9:
                raise LmsValidationError(
                    f"inventory underflow: {normalized_id} would drop below zero"
                )
            next_quantity = max(next_quantity, 0.0)

            next_status = str(current["status"])
            expiration_date = _optional_nonempty_string(current.get("expiration_date"))
            if expiration_date:
                try:
                    if date.fromisoformat(expiration_date) < date.today():
                        next_status = "expired"
                except ValueError:
                    pass
            if next_quantity <= 1e-9 and next_status != "expired":
                next_status = "depleted"
            if next_quantity > 1e-9 and next_status == "depleted":
                next_status = "active"

            conn.execute(
                """
                UPDATE inventory_items
                SET quantity = ?, status = ?, updated_at = ?
                WHERE item_id = ?
                """,
                (next_quantity, next_status, now, normalized_id),
            )
            tx_id = self._insert_inventory_tx_locked(
                conn,
                item_id=normalized_id,
                delta=delta,
                reason=reason,
                actor=actor,
                metadata=metadata,
                created_at=now,
            )
            self._write_audit_locked(
                conn,
                entity_type="inventory_item",
                entity_id=normalized_id,
                action="transaction_recorded",
                actor=actor,
                payload={
                    "tx_id": tx_id,
                    "delta": delta,
                    "reason": reason,
                    "quantity_before": current_quantity,
                    "quantity_after": next_quantity,
                },
                created_at=now,
            )
            conn.commit()
            item = self._get_inventory_item_locked(conn, normalized_id)

        if item is None:
            raise RuntimeError("failed to update inventory item")
        return item

    def list_inventory_transactions(
        self,
        item_id: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        normalized_id = _require_nonempty_string(item_id, "item_id")
        safe_limit = _safe_limit(limit)

        with self._lock, closing(self._connect()) as conn:
            if self._get_inventory_item_locked(conn, normalized_id) is None:
                raise LmsNotFoundError(f"Unknown item_id: {normalized_id}")
            rows = conn.execute(
                """
                SELECT tx_id, item_id, delta, reason, actor, metadata_json, created_at
                FROM inventory_transactions
                WHERE item_id = ?
                ORDER BY created_at DESC, tx_id DESC
                LIMIT ?
                """,
                (normalized_id, safe_limit),
            ).fetchall()
        return [self._inventory_tx_row_to_payload(row) for row in rows]

    def create_experiment(
        self, payload: Mapping[str, Any], *, actor: str | None = None
    ) -> dict[str, Any]:
        experiment_id = _optional_nonempty_string(payload.get("experiment_id")) or str(
            uuid.uuid4()
        )
        project_id = _optional_nonempty_string(payload.get("project_id"))
        name = _require_nonempty_string(payload.get("name"), "name")
        status = _normalize_status(
            payload.get("status", "draft"),
            field_name="status",
            allowed=EXPERIMENT_STATUSES,
        )
        provider = _optional_nonempty_string(payload.get("provider"))
        protocol = payload.get("protocol", {})
        if not isinstance(protocol, dict):
            raise LmsValidationError("protocol must be a JSON object")

        sample_ids = _normalize_string_list(payload.get("sample_ids"), "sample_ids")
        run_id = _optional_nonempty_string(payload.get("run_id"))
        metadata = _normalize_metadata(payload.get("metadata"))
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            self._ensure_unique_id(
                conn,
                "experiments",
                "experiment_id",
                experiment_id,
                entity="experiment",
            )
            if project_id is not None:
                self._ensure_project_exists_locked(conn, project_id)

            for sample_id in sample_ids:
                sample = self._get_sample_locked(conn, sample_id)
                if sample is None:
                    raise LmsNotFoundError(f"Unknown sample_id: {sample_id}")
                sample_project = _optional_nonempty_string(sample.get("project_id"))
                if project_id and sample_project and sample_project != project_id:
                    raise LmsConflictError(
                        f"sample {sample_id} belongs to project {sample_project}, expected {project_id}"
                    )

            conn.execute(
                """
                INSERT INTO experiments(
                    experiment_id, project_id, name, status, provider,
                    protocol_json, sample_ids_json, run_id,
                    metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    experiment_id,
                    project_id,
                    name,
                    status,
                    provider,
                    json.dumps(protocol, ensure_ascii=True, sort_keys=True),
                    json.dumps(sample_ids, ensure_ascii=True),
                    run_id,
                    json.dumps(metadata, ensure_ascii=True, sort_keys=True),
                    now,
                    now,
                ),
            )
            self._write_audit_locked(
                conn,
                entity_type="experiment",
                entity_id=experiment_id,
                action="created",
                actor=actor,
                payload={
                    "project_id": project_id,
                    "provider": provider,
                    "status": status,
                    "sample_count": len(sample_ids),
                },
                created_at=now,
            )
            conn.commit()
            experiment = self._get_experiment_locked(conn, experiment_id)

        if experiment is None:
            raise RuntimeError("failed to create experiment")
        return experiment

    def list_experiments(
        self,
        *,
        limit: int = 100,
        statuses: tuple[str, ...] | None = None,
        project_id: str | None = None,
    ) -> list[dict[str, Any]]:
        safe_limit = _safe_limit(limit)
        where: list[str] = []
        params: list[Any] = []

        normalized_statuses = _normalize_statuses(
            statuses,
            allowed=EXPERIMENT_STATUSES,
            field_name="status",
        )
        if normalized_statuses:
            placeholders = ",".join("?" for _ in normalized_statuses)
            where.append(f"status IN ({placeholders})")
            params.extend(normalized_statuses)

        project_value = _optional_nonempty_string(project_id)
        if project_value is not None:
            where.append("project_id = ?")
            params.append(project_value)

        query = (
            "SELECT experiment_id, project_id, name, status, provider, protocol_json, "
            "sample_ids_json, run_id, metadata_json, created_at, updated_at FROM experiments"
        )
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(safe_limit)

        with self._lock, closing(self._connect()) as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
            return [self._experiment_row_to_payload(conn, row) for row in rows]

    def get_experiment(self, experiment_id: str) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(experiment_id, "experiment_id")
        with self._lock, closing(self._connect()) as conn:
            experiment = self._get_experiment_locked(conn, normalized_id)
        if experiment is None:
            raise LmsNotFoundError(f"Unknown experiment_id: {normalized_id}")
        return experiment

    def update_experiment_status(
        self,
        experiment_id: str,
        *,
        status: str,
        actor: str | None = None,
        notes: str | None = None,
    ) -> dict[str, Any]:
        normalized_id = _require_nonempty_string(experiment_id, "experiment_id")
        normalized_status = _normalize_status(
            status,
            field_name="status",
            allowed=EXPERIMENT_STATUSES,
        )
        notes_value = _optional_nonempty_string(notes)
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            current = self._get_experiment_locked(conn, normalized_id)
            if current is None:
                raise LmsNotFoundError(f"Unknown experiment_id: {normalized_id}")
            conn.execute(
                "UPDATE experiments SET status = ?, updated_at = ? WHERE experiment_id = ?",
                (normalized_status, now, normalized_id),
            )
            self._write_audit_locked(
                conn,
                entity_type="experiment",
                entity_id=normalized_id,
                action="status_updated",
                actor=actor,
                payload={
                    "from": current["status"],
                    "to": normalized_status,
                    "notes": notes_value,
                },
                created_at=now,
            )
            conn.commit()
            experiment = self._get_experiment_locked(conn, normalized_id)

        if experiment is None:
            raise RuntimeError("failed to update experiment")
        return experiment

    def link_experiment_run(
        self,
        experiment_id: str,
        *,
        run_id: str,
        provider: str | None,
        status: str,
        actor: str | None = None,
    ) -> dict[str, Any]:
        normalized_experiment_id = _require_nonempty_string(
            experiment_id, "experiment_id"
        )
        normalized_run_id = _require_nonempty_string(run_id, "run_id")
        normalized_status = _normalize_status(
            status,
            field_name="status",
            allowed=EXPERIMENT_STATUSES,
        )
        normalized_provider = _optional_nonempty_string(provider)
        now = _utc_now_iso()

        with self._lock, closing(self._connect()) as conn:
            current = self._get_experiment_locked(conn, normalized_experiment_id)
            if current is None:
                raise LmsNotFoundError(
                    f"Unknown experiment_id: {normalized_experiment_id}"
                )
            conn.execute(
                """
                UPDATE experiments
                SET run_id = ?, provider = COALESCE(?, provider), status = ?, updated_at = ?
                WHERE experiment_id = ?
                """,
                (
                    normalized_run_id,
                    normalized_provider,
                    normalized_status,
                    now,
                    normalized_experiment_id,
                ),
            )
            self._write_audit_locked(
                conn,
                entity_type="experiment",
                entity_id=normalized_experiment_id,
                action="run_linked",
                actor=actor,
                payload={
                    "run_id": normalized_run_id,
                    "provider": normalized_provider,
                    "status": normalized_status,
                },
                created_at=now,
            )
            conn.commit()
            experiment = self._get_experiment_locked(conn, normalized_experiment_id)

        if experiment is None:
            raise RuntimeError("failed to link experiment run")
        return experiment

    def list_audit_events(
        self,
        *,
        limit: int = 100,
        entity_type: str | None = None,
        entity_id: str | None = None,
        action: str | None = None,
    ) -> list[dict[str, Any]]:
        safe_limit = _safe_limit(limit)
        with self._lock, closing(self._connect()) as conn:
            return self._list_audit_events_locked(
                conn,
                limit=safe_limit,
                entity_type=entity_type,
                entity_id=entity_id,
                action=action,
            )

    def _get_project_locked(
        self, conn: sqlite3.Connection, project_id: str
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT project_id, name, owner, description, status, priority,
                   metadata_json, created_at, updated_at
            FROM projects
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()
        if row is None:
            return None
        return self._project_row_to_payload(row)

    def _get_sample_locked(
        self, conn: sqlite3.Connection, sample_id: str
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT sample_id, project_id, name, sample_type, status, concentration_ng_ul,
                   volume_ul, storage_location, metadata_json, created_at, updated_at
            FROM samples
            WHERE sample_id = ?
            """,
            (sample_id,),
        ).fetchone()
        if row is None:
            return None
        return self._sample_row_to_payload(row)

    def _get_plate_locked(
        self,
        conn: sqlite3.Connection,
        plate_id: str,
        *,
        include_assignments: bool,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT plate_id, project_id, plate_type, label, status,
                   metadata_json, created_at, updated_at
            FROM plates
            WHERE plate_id = ?
            """,
            (plate_id,),
        ).fetchone()
        if row is None:
            return None

        assignments: list[dict[str, Any]] = []
        if include_assignments:
            assignment_rows = conn.execute(
                """
                SELECT plate_id, well, sample_id, volume_ul, assigned_at
                FROM plate_wells
                WHERE plate_id = ?
                ORDER BY well ASC
                """,
                (plate_id,),
            ).fetchall()
            assignments = [
                self._plate_assignment_row_to_payload(item) for item in assignment_rows
            ]

        return self._plate_row_to_payload(row, assignments=assignments)

    def _get_inventory_item_locked(
        self,
        conn: sqlite3.Connection,
        item_id: str,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT item_id, sku, name, category, unit, quantity, reorder_threshold,
                   lot_number, expiration_date, storage_location, status,
                   metadata_json, created_at, updated_at
            FROM inventory_items
            WHERE item_id = ?
            """,
            (item_id,),
        ).fetchone()
        if row is None:
            return None
        return self._inventory_row_to_payload(row)

    def _get_experiment_locked(
        self,
        conn: sqlite3.Connection,
        experiment_id: str,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT experiment_id, project_id, name, status, provider,
                   protocol_json, sample_ids_json, run_id,
                   metadata_json, created_at, updated_at
            FROM experiments
            WHERE experiment_id = ?
            """,
            (experiment_id,),
        ).fetchone()
        if row is None:
            return None
        return self._experiment_row_to_payload(conn, row)

    def _get_sample_event_locked(
        self,
        conn: sqlite3.Connection,
        event_id: int,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT event_id, sample_id, event_type, actor, notes, metadata_json, created_at
            FROM sample_events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        if row is None:
            return None
        return self._sample_event_row_to_payload(row)

    def _list_audit_events_locked(
        self,
        conn: sqlite3.Connection,
        *,
        limit: int,
        entity_type: str | None = None,
        entity_id: str | None = None,
        action: str | None = None,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []

        normalized_entity_type = _optional_nonempty_string(entity_type)
        if normalized_entity_type is not None:
            where.append("entity_type = ?")
            params.append(normalized_entity_type)

        normalized_entity_id = _optional_nonempty_string(entity_id)
        if normalized_entity_id is not None:
            where.append("entity_id = ?")
            params.append(normalized_entity_id)

        normalized_action = _optional_nonempty_string(action)
        if normalized_action is not None:
            where.append("action = ?")
            params.append(normalized_action)

        query = (
            "SELECT event_id, entity_type, entity_id, action, actor, payload_json, created_at "
            "FROM audit_events"
        )
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY created_at DESC, event_id DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, tuple(params)).fetchall()
        return [self._audit_row_to_payload(row) for row in rows]

    @staticmethod
    def _project_row_to_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "project_id": str(row["project_id"]),
            "name": str(row["name"]),
            "owner": _optional_nonempty_string(row["owner"]),
            "description": _optional_nonempty_string(row["description"]),
            "status": str(row["status"]),
            "priority": int(row["priority"]),
            "metadata": _decode_json_object(row["metadata_json"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _sample_row_to_payload(row: sqlite3.Row) -> dict[str, Any]:
        concentration = row["concentration_ng_ul"]
        volume = row["volume_ul"]
        return {
            "sample_id": str(row["sample_id"]),
            "project_id": _optional_nonempty_string(row["project_id"]),
            "name": str(row["name"]),
            "sample_type": str(row["sample_type"]),
            "status": str(row["status"]),
            "concentration_ng_ul": (
                float(concentration) if concentration is not None else None
            ),
            "volume_ul": float(volume) if volume is not None else None,
            "storage_location": _optional_nonempty_string(row["storage_location"]),
            "metadata": _decode_json_object(row["metadata_json"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _sample_event_row_to_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": int(row["event_id"]),
            "sample_id": str(row["sample_id"]),
            "event_type": str(row["event_type"]),
            "actor": _optional_nonempty_string(row["actor"]),
            "notes": _optional_nonempty_string(row["notes"]),
            "metadata": _decode_json_object(row["metadata_json"]),
            "created_at": str(row["created_at"]),
        }

    @staticmethod
    def _plate_assignment_row_to_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "plate_id": str(row["plate_id"]),
            "well": str(row["well"]),
            "sample_id": str(row["sample_id"]),
            "volume_ul": float(row["volume_ul"]),
            "assigned_at": str(row["assigned_at"]),
        }

    @staticmethod
    def _plate_row_to_payload(
        row: sqlite3.Row,
        *,
        assignments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return {
            "plate_id": str(row["plate_id"]),
            "project_id": _optional_nonempty_string(row["project_id"]),
            "plate_type": str(row["plate_type"]),
            "label": str(row["label"]),
            "status": str(row["status"]),
            "assignments": assignments,
            "assignment_count": len(assignments),
            "metadata": _decode_json_object(row["metadata_json"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _inventory_row_to_payload(row: sqlite3.Row) -> dict[str, Any]:
        expiration_date = _optional_nonempty_string(row["expiration_date"])
        expired = False
        if expiration_date is not None:
            try:
                expired = date.fromisoformat(expiration_date) < date.today()
            except ValueError:
                expired = False

        quantity = float(row["quantity"])
        reorder_threshold = float(row["reorder_threshold"])
        return {
            "item_id": str(row["item_id"]),
            "sku": _optional_nonempty_string(row["sku"]),
            "name": str(row["name"]),
            "category": str(row["category"]),
            "unit": str(row["unit"]),
            "quantity": quantity,
            "reorder_threshold": reorder_threshold,
            "below_reorder": quantity <= reorder_threshold,
            "lot_number": _optional_nonempty_string(row["lot_number"]),
            "expiration_date": expiration_date,
            "expired": expired,
            "storage_location": _optional_nonempty_string(row["storage_location"]),
            "status": str(row["status"]),
            "metadata": _decode_json_object(row["metadata_json"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _inventory_tx_row_to_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "tx_id": int(row["tx_id"]),
            "item_id": str(row["item_id"]),
            "delta": float(row["delta"]),
            "reason": str(row["reason"]),
            "actor": _optional_nonempty_string(row["actor"]),
            "metadata": _decode_json_object(row["metadata_json"]),
            "created_at": str(row["created_at"]),
        }

    @staticmethod
    def _experiment_row_to_payload(
        conn: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> dict[str, Any]:
        protocol = _decode_json_object(row["protocol_json"])
        sample_ids = _decode_json_list(row["sample_ids_json"])
        metadata = _decode_json_object(row["metadata_json"])
        run_id = _optional_nonempty_string(row["run_id"])
        run_status: str | None = None
        if run_id is not None:
            try:
                run_row = conn.execute(
                    "SELECT status FROM runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if run_row is not None:
                    run_status = _optional_nonempty_string(run_row["status"])
            except sqlite3.OperationalError:
                run_status = None

        return {
            "experiment_id": str(row["experiment_id"]),
            "project_id": _optional_nonempty_string(row["project_id"]),
            "name": str(row["name"]),
            "status": str(row["status"]),
            "provider": _optional_nonempty_string(row["provider"]),
            "protocol": protocol,
            "sample_ids": sample_ids,
            "run_id": run_id,
            "live_run_status": run_status,
            "metadata": metadata,
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }

    @staticmethod
    def _audit_row_to_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": int(row["event_id"]),
            "entity_type": str(row["entity_type"]),
            "entity_id": str(row["entity_id"]),
            "action": str(row["action"]),
            "actor": _optional_nonempty_string(row["actor"]),
            "payload": _decode_json_object(row["payload_json"]),
            "created_at": str(row["created_at"]),
        }

    @staticmethod
    def _ensure_unique_id(
        conn: sqlite3.Connection,
        table: str,
        column: str,
        entity_id: str,
        *,
        entity: str,
    ) -> None:
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE {column} = ?",
            (entity_id,),
        ).fetchone()
        if row is not None:
            raise LmsConflictError(f"{entity} id already exists: {entity_id}")

    def _ensure_project_exists_locked(
        self, conn: sqlite3.Connection, project_id: str
    ) -> None:
        if self._get_project_locked(conn, project_id) is None:
            raise LmsNotFoundError(f"Unknown project_id: {project_id}")

    @staticmethod
    def _insert_sample_event_locked(
        conn: sqlite3.Connection,
        *,
        sample_id: str,
        event_type: str,
        actor: str | None,
        notes: str | None,
        metadata: Mapping[str, Any],
        created_at: str,
    ) -> int:
        cursor = conn.execute(
            """
            INSERT INTO sample_events(
                sample_id, event_type, actor, notes, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                sample_id,
                event_type,
                _optional_nonempty_string(actor),
                _optional_nonempty_string(notes),
                json.dumps(dict(metadata), ensure_ascii=True, sort_keys=True),
                created_at,
            ),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def _insert_inventory_tx_locked(
        conn: sqlite3.Connection,
        *,
        item_id: str,
        delta: float,
        reason: str,
        actor: str | None,
        metadata: Mapping[str, Any],
        created_at: str,
    ) -> int:
        cursor = conn.execute(
            """
            INSERT INTO inventory_transactions(
                item_id, delta, reason, actor, metadata_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                item_id,
                delta,
                reason,
                _optional_nonempty_string(actor),
                json.dumps(dict(metadata), ensure_ascii=True, sort_keys=True),
                created_at,
            ),
        )
        return int(cursor.lastrowid)

    @staticmethod
    def _write_audit_locked(
        conn: sqlite3.Connection,
        *,
        entity_type: str,
        entity_id: str,
        action: str,
        actor: str | None,
        payload: Mapping[str, Any],
        created_at: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO audit_events(
                entity_type, entity_id, action, actor, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                entity_type,
                entity_id,
                action,
                _optional_nonempty_string(actor),
                json.dumps(dict(payload), ensure_ascii=True, sort_keys=True),
                created_at,
            ),
        )


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _table_count(conn: sqlite3.Connection, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    if row is None:
        return 0
    return int(row["count"])


def _status_counts(conn: sqlite3.Connection, table_name: str) -> dict[str, int]:
    rows = conn.execute(
        f"SELECT status, COUNT(*) AS count FROM {table_name} GROUP BY status"
    ).fetchall()
    counts: dict[str, int] = {}
    for row in rows:
        counts[str(row["status"])] = int(row["count"])
    return counts


def _safe_limit(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LmsValidationError("limit must be an integer")
    return min(max(value, 1), 1000)


def _normalize_statuses(
    values: tuple[str, ...] | None,
    *,
    allowed: frozenset[str],
    field_name: str,
) -> tuple[str, ...] | None:
    if not values:
        return None
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        parsed = _normalize_status(value, field_name=field_name, allowed=allowed)
        if parsed in seen:
            continue
        normalized.append(parsed)
        seen.add(parsed)
    return tuple(normalized)


def _normalize_status(value: Any, *, field_name: str, allowed: frozenset[str]) -> str:
    normalized = _require_nonempty_string(value, field_name)
    if normalized not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise LmsValidationError(
            f"{field_name} '{normalized}' is unsupported. allowed: {allowed_values}"
        )
    return normalized


def _normalize_well(value: Any) -> str:
    normalized = _require_nonempty_string(value, "well").upper()
    if not _WELL_RE.fullmatch(normalized):
        raise LmsValidationError(
            f"well must look like A1, B12, AA1, etc. got '{normalized}'"
        )
    return normalized


def _normalize_metadata(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise LmsValidationError("metadata must be a JSON object")
    return {str(key): val for key, val in value.items()}


def _normalize_string_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise LmsValidationError(f"{field_name} must be an array of strings")
    normalized: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(value, start=1):
        if not isinstance(item, str) or not item.strip():
            raise LmsValidationError(
                f"{field_name}[{index}] must be a non-empty string"
            )
        cleaned = item.strip()
        if cleaned in seen:
            continue
        normalized.append(cleaned)
        seen.add(cleaned)
    return normalized


def _normalize_date(value: Any, field_name: str) -> str | None:
    normalized = _optional_nonempty_string(value)
    if normalized is None:
        return None
    try:
        date.fromisoformat(normalized)
    except ValueError as exc:
        raise LmsValidationError(
            f"{field_name} must be an ISO date like YYYY-MM-DD"
        ) from exc
    return normalized


def _require_nonempty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LmsValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_nonempty_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _require_int(value: Any, field_name: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LmsValidationError(f"{field_name} must be an integer")
    if value < minimum or value > maximum:
        raise LmsValidationError(f"{field_name} must be in [{minimum}, {maximum}]")
    return value


def _require_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LmsValidationError(f"{field_name} must be a number")
    return float(value)


def _require_non_negative_number(value: Any, field_name: str) -> float:
    number = _require_number(value, field_name)
    if number < 0:
        raise LmsValidationError(f"{field_name} must be >= 0")
    return number


def _optional_non_negative_number(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    return _require_non_negative_number(value, field_name)


def _require_positive_number(value: Any, field_name: str) -> float:
    number = _require_number(value, field_name)
    if number <= 0:
        raise LmsValidationError(f"{field_name} must be > 0")
    return number


def _decode_json_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, str):
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(key): item for key, item in parsed.items()}


def _decode_json_list(value: Any) -> list[str]:
    if not isinstance(value, str):
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []

    normalized: list[str] = []
    for item in parsed:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if cleaned:
            normalized.append(cleaned)
    return normalized
