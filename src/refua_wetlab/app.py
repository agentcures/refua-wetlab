from __future__ import annotations

import json
import traceback
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from refua_wetlab.config import WetLabConfig
from refua_wetlab.engine import (
    ProtocolExecutionError,
    UnifiedWetLabEngine,
    UnknownProviderError,
)
from refua_wetlab.lineage import build_wetlab_lineage_event
from refua_wetlab.lms_api import LmsApi
from refua_wetlab.lms import (
    LmsConflictError,
    LmsNotFoundError,
    LmsStore,
    LmsValidationError,
)
from refua_wetlab.models import ProtocolValidationError
from refua_wetlab.runner import RunBackgroundRunner
from refua_wetlab.storage import RunStore

_ALLOWED_RUN_STATUSES = frozenset(
    {"queued", "running", "completed", "failed", "cancelled"}
)
_ROLE_VIEWER = "viewer"
_ROLE_OPERATOR = "operator"
_ROLE_ADMIN = "admin"


class ApiError(Exception):
    status_code: int = HTTPStatus.INTERNAL_SERVER_ERROR

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class BadRequestError(ApiError):
    status_code = HTTPStatus.BAD_REQUEST


class NotFoundError(ApiError):
    status_code = HTTPStatus.NOT_FOUND


class ConflictError(ApiError):
    status_code = HTTPStatus.CONFLICT


class WetLabApp:
    """Application service container and API implementation."""

    def __init__(self, config: WetLabConfig) -> None:
        self.config = config
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.store = RunStore(config.database_path)
        self.lms = LmsStore(config.database_path)
        self.runner = RunBackgroundRunner(self.store, max_workers=config.max_workers)
        self.engine = UnifiedWetLabEngine()
        self.lms_api = LmsApi(
            lms_store=self.lms,
            run_store=self.store,
            runner=self.runner,
            engine=self.engine,
        )

    def shutdown(self) -> None:
        self.runner.shutdown()

    def health(self) -> dict[str, Any]:
        providers = self.engine.list_providers()
        return {
            "ok": True,
            "providers_count": len(providers),
            "run_counts": self.store.status_counts(),
            "lms_counts": self.lms.core_counts(),
            "auth": {
                "enabled": self.config.auth_enabled,
                "viewer_tokens": len(self.config.auth_tokens),
                "operator_tokens": len(self.config.operator_tokens),
                "admin_tokens": len(self.config.admin_tokens),
            },
        }

    def providers_payload(self) -> dict[str, Any]:
        providers = self.engine.list_providers()
        return {
            "providers": providers,
            "provider_ids": [item["provider_id"] for item in providers],
        }

    def validate_protocol(self, payload: dict[str, Any]) -> dict[str, Any]:
        protocol_payload = _extract_protocol(payload)
        protocol = self.engine.validate_protocol(protocol_payload)
        return {
            "valid": True,
            "protocol": protocol,
        }

    def compile_protocol(self, payload: dict[str, Any]) -> dict[str, Any]:
        provider = _require_nonempty_string(payload.get("provider"), "provider")
        protocol_payload = _extract_protocol(payload)
        return self.engine.compile_protocol(
            provider_id=provider,
            protocol_payload=protocol_payload,
        )

    def create_run(self, payload: dict[str, Any]) -> dict[str, Any]:
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
            raise BadRequestError("metadata must be a JSON object")

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
        except Exception as exc:  # noqa: BLE001
            self.store.set_failed(run_id, str(exc))
            raise

        execution = result.get("execution")
        if isinstance(execution, dict) and str(execution.get("status")) == "cancelled":
            self.store.set_cancelled(run_id, "Cancelled by user during execution.")
        else:
            self.store.set_completed(run_id, result)

        latest = self.store.get_run(run_id) or run
        return {"run": latest, "result": result}

    def list_runs(self, *, query: dict[str, list[str]]) -> dict[str, Any]:
        limit = _query_int(query, name="limit", default=100, minimum=1, maximum=1000)
        statuses = _parse_statuses_query(query)
        return {
            "runs": self.store.list_runs(limit=limit, statuses=statuses),
            "counts": self.store.status_counts(),
        }

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self.store.get_run(run_id)
        if run is None:
            raise NotFoundError(f"Unknown run_id: {run_id}")
        return run

    def get_run_lineage(self, run_id: str) -> dict[str, Any]:
        run = self.get_run(run_id)
        result_payload = run.get("result")
        if isinstance(result_payload, dict):
            event = build_wetlab_lineage_event(result_payload, run_id=run_id)
        else:
            request_payload = run.get("request")
            if not isinstance(request_payload, dict):
                request_payload = {}
            metadata = request_payload.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            event = build_wetlab_lineage_event(
                {
                    "provider": run.get("provider"),
                    "metadata": metadata,
                    "execution": {"status": run.get("status")},
                },
                run_id=run_id,
            )
        event["run_status"] = run.get("status")
        return event

    def cancel_run(self, run_id: str) -> dict[str, Any]:
        try:
            return self.runner.cancel(run_id)
        except KeyError as exc:
            raise NotFoundError(f"Unknown run_id: {run_id}") from exc

    def route_lms_get(
        self,
        *,
        path: str,
        query: dict[str, list[str]],
    ) -> dict[str, Any] | None:
        parts = _path_parts(path)
        if len(parts) < 2 or parts[0] != "api" or parts[1] != "lms":
            return None
        return self.lms_api.route_get(path=path, query=query)

    def route_lms_post(
        self, *, path: str, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        parts = _path_parts(path)
        if len(parts) < 2 or parts[0] != "api" or parts[1] != "lms":
            return None
        return self.lms_api.route_post(path=path, payload=payload)

    def create_experiment_record(
        self,
        payload: dict[str, Any],
        *,
        actor: str | None,
    ) -> dict[str, Any]:
        return self.lms_api.create_experiment_record(payload, actor=actor)

    def schedule_experiment_run(
        self,
        experiment_id: str,
        payload: dict[str, Any],
        *,
        actor: str | None,
    ) -> dict[str, Any]:
        return self.lms_api.schedule_experiment_run(
            experiment_id,
            payload,
            actor=actor,
        )


def _required_api_role(*, method: str, path: str) -> str | None:
    if not path.startswith("/api/"):
        return None
    normalized_method = method.upper()
    if normalized_method == "GET":
        return _ROLE_VIEWER
    if normalized_method != "POST":
        return _ROLE_VIEWER
    if path.endswith("/cancel"):
        return _ROLE_OPERATOR
    return _ROLE_OPERATOR


def _extract_bearer_token(handler: BaseHTTPRequestHandler) -> str | None:
    raw = str(handler.headers.get("Authorization", "")).strip()
    if not raw:
        return None
    parts = raw.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def _is_role_allowed(*, token_roles: frozenset[str], required_role: str) -> bool:
    if _ROLE_ADMIN in token_roles:
        return True
    if required_role == _ROLE_VIEWER:
        return bool(token_roles)
    if required_role == _ROLE_OPERATOR:
        return _ROLE_OPERATOR in token_roles
    if required_role == _ROLE_ADMIN:
        return _ROLE_ADMIN in token_roles
    return False


def _authorize_request(
    handler: BaseHTTPRequestHandler,
    app: WetLabApp,
    *,
    method: str,
    path: str,
) -> tuple[int, dict[str, Any]] | None:
    required_role = _required_api_role(method=method, path=path)
    if required_role is None or not app.config.auth_enabled:
        return None

    token = _extract_bearer_token(handler)
    if token is None:
        return (
            HTTPStatus.UNAUTHORIZED,
            {"error": "Missing bearer token.", "required_role": required_role},
        )

    token_roles = app.config.roles_for_token(token)
    if not token_roles:
        return (
            HTTPStatus.UNAUTHORIZED,
            {"error": "Invalid bearer token.", "required_role": required_role},
        )

    if not _is_role_allowed(token_roles=token_roles, required_role=required_role):
        return (
            HTTPStatus.FORBIDDEN,
            {
                "error": "Insufficient role for endpoint.",
                "required_role": required_role,
                "token_roles": sorted(token_roles),
            },
        )
    return None


def create_server(config: WetLabConfig) -> tuple[ThreadingHTTPServer, WetLabApp]:
    app = WetLabApp(config)

    class WetLabHandler(BaseHTTPRequestHandler):
        server_version = "RefuaWetLab/0.7"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)

            try:
                auth_failure = _authorize_request(self, app, method="GET", path=path)
                if auth_failure is not None:
                    status, payload = auth_failure
                    self._send_json(status, payload)
                    return

                if path == "/api/health":
                    self._send_json(HTTPStatus.OK, app.health())
                    return
                if path == "/api/providers":
                    self._send_json(HTTPStatus.OK, app.providers_payload())
                    return
                if path == "/api/runs":
                    self._send_json(HTTPStatus.OK, app.list_runs(query=query))
                    return

                run_id = _extract_run_lineage_id(path)
                if run_id is not None:
                    self._send_json(HTTPStatus.OK, app.get_run_lineage(run_id))
                    return

                run_id = _extract_run_id(path)
                if run_id is not None:
                    self._send_json(HTTPStatus.OK, app.get_run(run_id))
                    return

                lms_response = app.route_lms_get(path=path, query=query)
                if lms_response is not None:
                    self._send_json(HTTPStatus.OK, lms_response)
                    return

                if path == "/":
                    self._send_json(
                        HTTPStatus.OK,
                        {
                            "service": "refua-wetlab",
                            "version": "0.7.1",
                            "api_base": "/api",
                        },
                    )
                    return

                raise NotFoundError(f"Unknown endpoint: {path}")
            except Exception as exc:  # noqa: BLE001
                self._handle_exception(exc)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path

            try:
                auth_failure = _authorize_request(self, app, method="POST", path=path)
                if auth_failure is not None:
                    status, response_payload = auth_failure
                    self._send_json(status, response_payload)
                    return

                payload = self._read_json_payload()

                if path == "/api/protocols/validate":
                    self._send_json(HTTPStatus.OK, app.validate_protocol(payload))
                    return
                if path == "/api/protocols/compile":
                    self._send_json(HTTPStatus.OK, app.compile_protocol(payload))
                    return
                if path == "/api/runs":
                    self._send_json(HTTPStatus.OK, app.create_run(payload))
                    return

                run_id = _extract_run_cancel_id(path)
                if run_id is not None:
                    self._send_json(HTTPStatus.OK, app.cancel_run(run_id))
                    return

                lms_response = app.route_lms_post(path=path, payload=payload)
                if lms_response is not None:
                    self._send_json(HTTPStatus.OK, lms_response)
                    return

                raise NotFoundError(f"Unknown endpoint: {path}")
            except Exception as exc:  # noqa: BLE001
                self._handle_exception(exc)

        def log_message(self, format: str, *args: object) -> None:
            return

        def _read_json_payload(self) -> dict[str, Any]:
            length_header = self.headers.get("Content-Length")
            if length_header is None:
                return {}
            try:
                length = int(length_header)
            except ValueError as exc:
                raise BadRequestError("Invalid Content-Length header") from exc

            if length <= 0:
                return {}
            data = self.rfile.read(length)
            try:
                parsed = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise BadRequestError(f"Invalid JSON payload: {exc.msg}") from exc

            if not isinstance(parsed, dict):
                raise BadRequestError("Payload must be a JSON object")
            return parsed

        def _handle_exception(self, exc: Exception) -> None:
            if isinstance(exc, ApiError):
                self._send_json(exc.status_code, {"error": exc.message})
                return
            if isinstance(exc, (ProtocolValidationError, UnknownProviderError)):
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            if isinstance(exc, ProtocolExecutionError):
                self._send_json(HTTPStatus.UNPROCESSABLE_ENTITY, {"error": str(exc)})
                return
            if isinstance(exc, LmsValidationError):
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            if isinstance(exc, LmsNotFoundError):
                self._send_json(HTTPStatus.NOT_FOUND, {"error": str(exc)})
                return
            if isinstance(exc, LmsConflictError):
                self._send_json(HTTPStatus.CONFLICT, {"error": str(exc)})
                return

            traceback.print_exc()
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {
                    "error": "internal_server_error",
                    "message": "Unexpected server error",
                },
            )

        def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
            response = json.dumps(payload, ensure_ascii=True).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    server = ThreadingHTTPServer((config.host, config.port), WetLabHandler)
    return server, app


def _extract_protocol(payload: dict[str, Any]) -> Any:
    if "protocol" in payload:
        return payload.get("protocol")
    return payload


def _extract_run_id(path: str) -> str | None:
    parts = _path_parts(path)
    if len(parts) == 3 and parts[0] == "api" and parts[1] == "runs":
        return parts[2]
    return None


def _extract_run_cancel_id(path: str) -> str | None:
    parts = _path_parts(path)
    if (
        len(parts) == 4
        and parts[0] == "api"
        and parts[1] == "runs"
        and parts[3] == "cancel"
    ):
        return parts[2]
    return None


def _extract_run_lineage_id(path: str) -> str | None:
    parts = _path_parts(path)
    if (
        len(parts) == 4
        and parts[0] == "api"
        and parts[1] == "runs"
        and parts[3] == "lineage"
    ):
        return parts[2]
    return None


def _path_parts(path: str) -> list[str]:
    return [item for item in path.split("/") if item]


def _require_nonempty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BadRequestError(f"{field_name} must be a non-empty string")
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
        raise BadRequestError(f"Query parameter '{name}' must be an integer") from exc
    if value < minimum or value > maximum:
        raise BadRequestError(
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
    raise BadRequestError(
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
                raise BadRequestError(
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
        raise BadRequestError(f"{name} must be an integer")
    if value < minimum or value > maximum:
        raise BadRequestError(f"{name} must be in [{minimum}, {maximum}]")
    return value


def _payload_bool(value: Any, *, name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise BadRequestError(f"{name} must be a boolean")


def _payload_object(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise BadRequestError(f"{field_name} must be a JSON object")
    return value


def _payload_actor(payload: dict[str, Any]) -> str | None:
    return _optional_nonempty_string(payload.get("actor"))


def _parse_statuses_query(query: dict[str, list[str]]) -> tuple[str, ...] | None:
    return _parse_enum_query(
        query,
        name="status",
        allowed=_ALLOWED_RUN_STATUSES,
        field_label="status",
    )


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
