from __future__ import annotations

from typing import Any, Mapping


def build_wetlab_lineage_event(
    run_payload: Mapping[str, Any],
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Build a normalized lineage event from a wet-lab run payload."""
    protocol = run_payload.get("protocol")
    if not isinstance(protocol, Mapping):
        protocol = {}

    steps = protocol.get("steps")
    if not isinstance(steps, list):
        steps = []

    execution = run_payload.get("execution")
    if not isinstance(execution, Mapping):
        execution = {}

    metadata = run_payload.get("metadata")
    if not isinstance(metadata, Mapping):
        metadata = {}

    return {
        "run_id": run_id,
        "provider": _as_text(run_payload.get("provider")),
        "protocol_name": _as_text(protocol.get("name")),
        "protocol_hash": _as_text(run_payload.get("protocol_hash")),
        "steps_count": len(steps),
        "dry_run": bool(execution.get("dry_run", False)),
        "executed_commands": _as_int(execution.get("executed_commands")),
        "execution_status": _as_text(execution.get("status")),
        "metadata": dict(metadata),
    }


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
