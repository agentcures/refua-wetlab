from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from refua_wetlab.engine import UnifiedWetLabEngine
from refua_wetlab.lms import (
    EXPERIMENT_STATUSES,
    INVENTORY_STATUSES,
    PLATE_STATUSES,
    PROJECT_STATUSES,
    SAMPLE_STATUSES,
    LmsNotFoundError,
    LmsStore,
    LmsValidationError,
)
from refua_wetlab.runner import RunBackgroundRunner
from refua_wetlab.storage import RunStore


class LmsApi:
    """Programmatic LMS API for Refua WetLab.

    This mirrors the LMS HTTP resource model while staying usable directly from Python.
    """

    def __init__(
        self,
        *,
        lms_store: LmsStore,
        run_store: RunStore,
        runner: RunBackgroundRunner,
        engine: UnifiedWetLabEngine,
    ) -> None:
        self.lms = lms_store
        self.store = run_store
        self.runner = runner
        self.engine = engine

    def shutdown(self) -> None:
        self.runner.shutdown()

    def route_get(
        self,
        *,
        path: str,
        query: Mapping[str, Sequence[str] | str] | None = None,
    ) -> dict[str, Any]:
        normalized_query = _normalize_query(query)
        parts = _path_parts(path)
        if len(parts) < 2 or parts[0] != "api" or parts[1] != "lms":
            raise LmsNotFoundError(f"Unknown endpoint: {path}")

        if len(parts) == 2:
            return {
                "service": "refua-wetlab-lms",
                "api_base": "/api/lms",
                "resources": [
                    "summary",
                    "projects",
                    "samples",
                    "plates",
                    "inventory/items",
                    "experiments",
                    "audit",
                ],
            }

        resource = parts[2]

        if resource == "summary" and len(parts) == 3:
            window_days = _query_int(
                normalized_query,
                name="window_days",
                default=14,
                minimum=1,
                maximum=365,
            )
            return self.lms.summary(expiring_within_days=window_days)

        if resource == "audit" and len(parts) == 3:
            limit = _query_int(
                normalized_query,
                name="limit",
                default=100,
                minimum=1,
                maximum=1000,
            )
            return {
                "events": self.lms.list_audit_events(
                    limit=limit,
                    entity_type=_query_string(normalized_query, "entity_type"),
                    entity_id=_query_string(normalized_query, "entity_id"),
                    action=_query_string(normalized_query, "action"),
                )
            }

        if resource == "projects":
            if len(parts) == 3:
                limit = _query_int(
                    normalized_query,
                    name="limit",
                    default=100,
                    minimum=1,
                    maximum=1000,
                )
                statuses = _parse_enum_query(
                    normalized_query,
                    name="status",
                    allowed=PROJECT_STATUSES,
                    field_label="project status",
                )
                return {
                    "projects": self.lms.list_projects(
                        limit=limit,
                        statuses=statuses,
                        owner=_query_string(normalized_query, "owner"),
                    )
                }
            if len(parts) == 4:
                return {"project": self.lms.get_project(parts[3])}

        if resource == "samples":
            if len(parts) == 3:
                limit = _query_int(
                    normalized_query,
                    name="limit",
                    default=100,
                    minimum=1,
                    maximum=1000,
                )
                statuses = _parse_enum_query(
                    normalized_query,
                    name="status",
                    allowed=SAMPLE_STATUSES,
                    field_label="sample status",
                )
                return {
                    "samples": self.lms.list_samples(
                        limit=limit,
                        statuses=statuses,
                        project_id=_query_string(normalized_query, "project_id"),
                        sample_type=_query_string(normalized_query, "sample_type"),
                    )
                }
            if len(parts) == 4:
                return {"sample": self.lms.get_sample(parts[3])}
            if len(parts) == 5 and parts[4] == "events":
                limit = _query_int(
                    normalized_query,
                    name="limit",
                    default=100,
                    minimum=1,
                    maximum=1000,
                )
                return {"events": self.lms.list_sample_events(parts[3], limit=limit)}

        if resource == "plates":
            if len(parts) == 3:
                limit = _query_int(
                    normalized_query,
                    name="limit",
                    default=100,
                    minimum=1,
                    maximum=1000,
                )
                statuses = _parse_enum_query(
                    normalized_query,
                    name="status",
                    allowed=PLATE_STATUSES,
                    field_label="plate status",
                )
                return {
                    "plates": self.lms.list_plates(
                        limit=limit,
                        statuses=statuses,
                        project_id=_query_string(normalized_query, "project_id"),
                    )
                }
            if len(parts) == 4:
                return {"plate": self.lms.get_plate(parts[3])}

        if resource == "inventory" and len(parts) >= 4 and parts[3] == "items":
            if len(parts) == 4:
                limit = _query_int(
                    normalized_query,
                    name="limit",
                    default=100,
                    minimum=1,
                    maximum=1000,
                )
                statuses = _parse_enum_query(
                    normalized_query,
                    name="status",
                    allowed=INVENTORY_STATUSES,
                    field_label="inventory status",
                )
                below_reorder = _query_bool(
                    normalized_query,
                    name="below_reorder",
                    default=False,
                )
                return {
                    "items": self.lms.list_inventory_items(
                        limit=limit,
                        statuses=statuses,
                        category=_query_string(normalized_query, "category"),
                        below_reorder=below_reorder,
                    )
                }
            if len(parts) == 5:
                return {"item": self.lms.get_inventory_item(parts[4])}
            if len(parts) == 6 and parts[5] == "transactions":
                limit = _query_int(
                    normalized_query,
                    name="limit",
                    default=100,
                    minimum=1,
                    maximum=1000,
                )
                return {
                    "transactions": self.lms.list_inventory_transactions(
                        parts[4],
                        limit=limit,
                    )
                }

        if resource == "experiments":
            if len(parts) == 3:
                limit = _query_int(
                    normalized_query,
                    name="limit",
                    default=100,
                    minimum=1,
                    maximum=1000,
                )
                statuses = _parse_enum_query(
                    normalized_query,
                    name="status",
                    allowed=EXPERIMENT_STATUSES,
                    field_label="experiment status",
                )
                return {
                    "experiments": self.lms.list_experiments(
                        limit=limit,
                        statuses=statuses,
                        project_id=_query_string(normalized_query, "project_id"),
                    )
                }
            if len(parts) == 4:
                return {"experiment": self.lms.get_experiment(parts[3])}

        raise LmsNotFoundError(f"Unknown endpoint: {path}")

    def route_post(
        self,
        *,
        path: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        parts = _path_parts(path)
        if len(parts) < 2 or parts[0] != "api" or parts[1] != "lms":
            raise LmsNotFoundError(f"Unknown endpoint: {path}")
        if len(parts) == 2:
            raise LmsNotFoundError(f"Unknown endpoint: {path}")

        actor = _payload_actor(payload)
        resource = parts[2]

        if resource == "projects":
            if len(parts) == 3:
                return {"project": self.lms.create_project(payload, actor=actor)}
            if len(parts) == 5 and parts[4] == "status":
                status = _require_nonempty_string(payload.get("status"), "status")
                return {
                    "project": self.lms.update_project_status(
                        parts[3],
                        status=status,
                        actor=actor,
                        notes=_optional_nonempty_string(payload.get("notes")),
                    )
                }

        if resource == "samples":
            if len(parts) == 3:
                return {"sample": self.lms.create_sample(payload, actor=actor)}
            if len(parts) == 5 and parts[4] == "status":
                status = _require_nonempty_string(payload.get("status"), "status")
                return {
                    "sample": self.lms.update_sample_status(
                        parts[3],
                        status=status,
                        actor=actor,
                        notes=_optional_nonempty_string(payload.get("notes")),
                        metadata=_payload_object(payload.get("metadata"), "metadata"),
                    )
                }
            if len(parts) == 5 and parts[4] == "events":
                event_type = _require_nonempty_string(
                    payload.get("event_type"),
                    "event_type",
                )
                return {
                    "event": self.lms.add_sample_event(
                        parts[3],
                        event_type=event_type,
                        actor=actor,
                        notes=_optional_nonempty_string(payload.get("notes")),
                        metadata=_payload_object(payload.get("metadata"), "metadata"),
                    )
                }

        if resource == "plates":
            if len(parts) == 3:
                return {"plate": self.lms.create_plate(payload, actor=actor)}
            if len(parts) == 5 and parts[4] == "assignments":
                return {
                    "plate": self.lms.assign_sample_to_plate(
                        parts[3],
                        payload,
                        actor=actor,
                    )
                }

        if resource == "inventory" and len(parts) >= 4 and parts[3] == "items":
            if len(parts) == 4:
                return {"item": self.lms.create_inventory_item(payload, actor=actor)}
            if len(parts) == 6 and parts[5] == "transactions":
                return {
                    "item": self.lms.record_inventory_transaction(
                        parts[4],
                        payload,
                        actor=actor,
                    )
                }

        if resource == "experiments":
            if len(parts) == 3:
                return {
                    "experiment": self.create_experiment_record(payload, actor=actor)
                }
            if len(parts) == 5 and parts[4] == "status":
                status = _require_nonempty_string(payload.get("status"), "status")
                return {
                    "experiment": self.lms.update_experiment_status(
                        parts[3],
                        status=status,
                        actor=actor,
                        notes=_optional_nonempty_string(payload.get("notes")),
                    )
                }
            if len(parts) == 5 and parts[4] == "schedule-run":
                return self.schedule_experiment_run(parts[3], payload, actor=actor)

        raise LmsNotFoundError(f"Unknown endpoint: {path}")

    def create_experiment_record(
        self,
        payload: Mapping[str, Any],
        *,
        actor: str | None,
    ) -> dict[str, Any]:
        provider = _optional_nonempty_string(payload.get("provider"))
        if provider is not None:
            self._require_provider(provider)

        protocol_payload = payload.get("protocol")
        normalized_protocol: dict[str, Any]
        if protocol_payload is None:
            normalized_protocol = {}
        elif not isinstance(protocol_payload, dict):
            raise LmsValidationError("protocol must be a JSON object")
        elif protocol_payload and "steps" in protocol_payload:
            normalized_protocol = self.engine.validate_protocol(protocol_payload)
        else:
            normalized_protocol = dict(protocol_payload)

        create_payload = dict(payload)
        create_payload["provider"] = provider
        create_payload["protocol"] = normalized_protocol
        return self.lms.create_experiment(create_payload, actor=actor)

    def schedule_experiment_run(
        self,
        experiment_id: str,
        payload: Mapping[str, Any],
        *,
        actor: str | None,
    ) -> dict[str, Any]:
        experiment = self.lms.get_experiment(experiment_id)

        provider = _optional_nonempty_string(payload.get("provider"))
        if provider is None:
            provider = _optional_nonempty_string(experiment.get("provider"))
        if provider is None:
            raise LmsValidationError(
                "provider is required in payload or existing experiment record"
            )
        self._require_provider(provider)

        protocol_payload = payload.get("protocol")
        if protocol_payload is None:
            protocol_payload = experiment.get("protocol")
        if not isinstance(protocol_payload, dict) or not protocol_payload:
            raise LmsValidationError("experiment protocol is missing")
        protocol = self.engine.validate_protocol(protocol_payload)

        metadata_payload = _payload_object(payload.get("metadata"), "metadata")
        lms_meta = metadata_payload.get("lms")
        if not isinstance(lms_meta, dict):
            lms_meta = {}
        lms_meta = dict(lms_meta)
        lms_meta.update(
            {
                "experiment_id": experiment_id,
                "project_id": experiment.get("project_id"),
                "sample_ids": list(experiment.get("sample_ids", [])),
            }
        )
        metadata_payload["lms"] = lms_meta

        run_payload = {
            "provider": provider,
            "protocol": protocol,
            "dry_run": _payload_bool(payload.get("dry_run", True), name="dry_run"),
            "async_mode": _payload_bool(
                payload.get("async_mode", True),
                name="async_mode",
            ),
            "priority": _payload_int(
                payload.get("priority", 50),
                name="priority",
                minimum=0,
                maximum=100,
            ),
            "metadata": metadata_payload,
        }

        run_response = self.create_run(run_payload)
        run = run_response["run"]
        run_id = _require_nonempty_string(run.get("run_id"), "run.run_id")
        experiment_status = _experiment_status_from_run_status(
            _require_nonempty_string(run.get("status"), "run.status")
        )
        updated_experiment = self.lms.link_experiment_run(
            experiment_id,
            run_id=run_id,
            provider=provider,
            status=experiment_status,
            actor=actor,
        )

        response: dict[str, Any] = {
            "experiment": updated_experiment,
            "run": run,
        }
        if "result" in run_response:
            response["result"] = run_response["result"]
        return response

    def create_run(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        provider = _require_nonempty_string(payload.get("provider"), "provider")
        protocol_payload = _extract_protocol(payload)

        dry_run = _payload_bool(payload.get("dry_run", True), name="dry_run")
        async_mode = _payload_bool(payload.get("async_mode", True), name="async_mode")
        priority = _payload_int(
            payload.get("priority", 50),
            name="priority",
            minimum=0,
            maximum=100,
        )

        metadata_payload = payload.get("metadata", {})
        if metadata_payload is None:
            metadata_payload = {}
        if not isinstance(metadata_payload, dict):
            raise LmsValidationError("metadata must be a JSON object")

        request_payload = {
            "provider": provider,
            "protocol": protocol_payload,
            "dry_run": dry_run,
            "priority": priority,
            "metadata": metadata_payload,
        }

        if async_mode:
            run = self.runner.submit(
                provider=provider,
                request=request_payload,
                priority=priority,
                fn=lambda cancel_event: self.engine.run_protocol(
                    provider_id=provider,
                    protocol_payload=protocol_payload,
                    dry_run=dry_run,
                    metadata=metadata_payload,
                    cancel_event=cancel_event,
                ),
            )
            return {"run": run}

        run = self.store.create_run(provider=provider, request=request_payload)
        run_id = run["run_id"]
        self.store.set_running(run_id)
        try:
            result = self.engine.run_protocol(
                provider_id=provider,
                protocol_payload=protocol_payload,
                dry_run=dry_run,
                metadata=metadata_payload,
            )
        except Exception as exc:
            self.store.set_failed(run_id, str(exc))
            raise

        execution = result.get("execution")
        if isinstance(execution, dict) and str(execution.get("status")) == "cancelled":
            self.store.set_cancelled(run_id, "Cancelled by user during execution.")
        else:
            self.store.set_completed(run_id, result)

        latest = self.store.get_run(run_id) or run
        return {"run": latest, "result": result}

    def _require_provider(self, provider: str) -> None:
        provider_ids = {item["provider_id"] for item in self.engine.list_providers()}
        if provider not in provider_ids:
            allowed = ", ".join(sorted(provider_ids))
            raise LmsValidationError(
                f"Unknown provider '{provider}'. available providers: {allowed}"
            )


def _extract_protocol(payload: Mapping[str, Any]) -> Any:
    if "protocol" in payload:
        return payload.get("protocol")
    return payload


def _path_parts(path: str) -> list[str]:
    return [item for item in path.split("/") if item]


def _normalize_query(
    query: Mapping[str, Sequence[str] | str] | None,
) -> dict[str, list[str]]:
    if query is None:
        return {}

    normalized: dict[str, list[str]] = {}
    for key, raw_value in query.items():
        normalized_key = str(key)
        if isinstance(raw_value, str):
            normalized[normalized_key] = [raw_value]
            continue

        if isinstance(raw_value, Sequence):
            values = [str(item) for item in raw_value]
        else:
            values = [str(raw_value)]
        normalized[normalized_key] = values
    return normalized


def _require_nonempty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LmsValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _optional_nonempty_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _query_int(
    query: dict[str, list[str]],
    *,
    name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw_values = query.get(name)
    if not raw_values:
        return default
    raw_value = raw_values[0]
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise LmsValidationError(
            f"Query parameter '{name}' must be an integer"
        ) from exc
    if value < minimum or value > maximum:
        raise LmsValidationError(
            f"Query parameter '{name}' must be in [{minimum}, {maximum}]"
        )
    return value


def _query_string(query: dict[str, list[str]], name: str) -> str | None:
    raw_values = query.get(name)
    if not raw_values:
        return None
    value = raw_values[0].strip()
    return value or None


def _query_bool(query: dict[str, list[str]], *, name: str, default: bool) -> bool:
    raw_values = query.get(name)
    if not raw_values:
        return default

    normalized = raw_values[0].strip().lower()
    truthy = {"1", "true", "yes", "y", "on"}
    falsy = {"0", "false", "no", "n", "off"}
    if normalized in truthy:
        return True
    if normalized in falsy:
        return False
    raise LmsValidationError(
        f"Query parameter '{name}' must be a boolean (true/false, 1/0)"
    )


def _parse_enum_query(
    query: dict[str, list[str]],
    *,
    name: str,
    allowed: frozenset[str],
    field_label: str,
) -> tuple[str, ...] | None:
    raw_values = query.get(name)
    if not raw_values:
        return None

    parsed: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        for token in raw_value.split(","):
            value = token.strip()
            if not value:
                continue
            if value not in allowed:
                allowed_values = ", ".join(sorted(allowed))
                raise LmsValidationError(
                    f"Unsupported {field_label} '{value}'. allowed: {allowed_values}"
                )
            if value in seen:
                continue
            parsed.append(value)
            seen.add(value)

    if not parsed:
        return None
    return tuple(parsed)


def _payload_int(value: Any, *, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LmsValidationError(f"{name} must be an integer")
    if value < minimum or value > maximum:
        raise LmsValidationError(f"{name} must be in [{minimum}, {maximum}]")
    return value


def _payload_bool(value: Any, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise LmsValidationError(f"{name} must be a boolean")


def _payload_object(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise LmsValidationError(f"{field_name} must be a JSON object")
    return value


def _payload_actor(payload: Mapping[str, Any]) -> str | None:
    return _optional_nonempty_string(payload.get("actor"))


def _experiment_status_from_run_status(run_status: str) -> str:
    if run_status == "queued":
        return "scheduled"
    if run_status == "running":
        return "running"
    if run_status == "completed":
        return "completed"
    if run_status == "failed":
        return "failed"
    if run_status == "cancelled":
        return "cancelled"
    return "scheduled"
