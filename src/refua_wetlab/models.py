from __future__ import annotations

import json
import re
from typing import Any

_SUPPORTED_STEP_TYPES = frozenset(
    {
        "transfer",
        "mix",
        "incubate",
        "read_absorbance",
    }
)
_ALLOWED_TIP_STRATEGIES = frozenset({"always", "on_change", "never"})
_LOCATION_RE = re.compile(r"^[A-Za-z0-9_.-]+:[A-Za-z0-9_.-]+$")


class ProtocolValidationError(ValueError):
    """Raised when protocol payload cannot be normalized."""


def supported_step_types() -> tuple[str, ...]:
    return tuple(sorted(_SUPPORTED_STEP_TYPES))


def canonical_protocol_json(protocol: dict[str, Any]) -> str:
    return json.dumps(
        protocol, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )


def validate_protocol_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ProtocolValidationError("protocol must be a JSON object")

    name = _require_nonempty_string(payload.get("name"), "name")

    version = payload.get("version", "1.0")
    if not isinstance(version, str) or not version.strip():
        raise ProtocolValidationError("version must be a non-empty string")

    steps_payload = payload.get("steps")
    if not isinstance(steps_payload, list) or len(steps_payload) == 0:
        raise ProtocolValidationError("steps must be a non-empty array")

    normalized_steps: list[dict[str, Any]] = []
    for index, step in enumerate(steps_payload, start=1):
        normalized_steps.append(_validate_step(step, index=index))

    labware_payload = payload.get("labware", {})
    if labware_payload is None:
        labware_payload = {}
    if not isinstance(labware_payload, dict):
        raise ProtocolValidationError("labware must be a JSON object when provided")
    labware: dict[str, str] = {}
    for key, value in labware_payload.items():
        if not isinstance(key, str) or not key.strip():
            raise ProtocolValidationError("labware keys must be non-empty strings")
        if not isinstance(value, str) or not value.strip():
            raise ProtocolValidationError(f"labware.{key} must be a non-empty string")
        labware[key.strip()] = value.strip()

    metadata_payload = payload.get("metadata", {})
    if metadata_payload is None:
        metadata_payload = {}
    if not isinstance(metadata_payload, dict):
        raise ProtocolValidationError("metadata must be a JSON object when provided")

    inventory_present = "inventory" in payload
    inventory_payload = payload.get("inventory")
    inventory: dict[str, float] | None = None
    if inventory_present and inventory_payload is not None:
        if not isinstance(inventory_payload, dict):
            raise ProtocolValidationError(
                "inventory must be a JSON object when provided"
            )
        inventory = {}
        for location, volume in inventory_payload.items():
            normalized_location = _require_location_string(location, "inventory key")
            inventory[normalized_location] = _require_non_negative_number(
                volume,
                f"inventory['{normalized_location}']",
            )

    if labware:
        referenced_labware = _referenced_labware_names(normalized_steps)
        if inventory:
            referenced_labware.update(
                _location_labware_name(location) for location in inventory
            )
        missing = sorted(name for name in referenced_labware if name not in labware)
        if missing:
            raise ProtocolValidationError(
                "labware definitions missing for: " + ", ".join(missing)
            )

    return {
        "name": name,
        "version": version.strip(),
        "steps": normalized_steps,
        "labware": labware,
        "metadata": metadata_payload,
        "inventory": inventory,
    }


def _validate_step(step: Any, *, index: int) -> dict[str, Any]:
    if not isinstance(step, dict):
        raise ProtocolValidationError(f"steps[{index}] must be a JSON object")

    step_type = step.get("type")
    if not isinstance(step_type, str) or not step_type.strip():
        raise ProtocolValidationError(f"steps[{index}].type must be a non-empty string")

    normalized_type = step_type.strip()
    if normalized_type not in _SUPPORTED_STEP_TYPES:
        allowed = ", ".join(sorted(_SUPPORTED_STEP_TYPES))
        raise ProtocolValidationError(
            f"steps[{index}].type '{normalized_type}' is unsupported. allowed: {allowed}"
        )

    if normalized_type == "transfer":
        return _validate_transfer_step(step, index=index)
    if normalized_type == "mix":
        return _validate_mix_step(step, index=index)
    if normalized_type == "incubate":
        return _validate_incubate_step(step, index=index)
    return _validate_read_absorbance_step(step, index=index)


def _validate_transfer_step(step: dict[str, Any], *, index: int) -> dict[str, Any]:
    source = _require_location_string(step.get("source"), f"steps[{index}].source")
    destination = _require_location_string(
        step.get("destination"), f"steps[{index}].destination"
    )
    if source == destination:
        raise ProtocolValidationError(
            f"steps[{index}] transfer source and destination must be different"
        )
    volume_ul = _require_positive_number(
        step.get("volume_ul"), f"steps[{index}].volume_ul"
    )
    tip_strategy = step.get("tip_strategy", "always")
    if not isinstance(tip_strategy, str) or not tip_strategy.strip():
        raise ProtocolValidationError(
            f"steps[{index}].tip_strategy must be a non-empty string"
        )
    normalized_tip_strategy = tip_strategy.strip()
    if normalized_tip_strategy not in _ALLOWED_TIP_STRATEGIES:
        allowed = ", ".join(sorted(_ALLOWED_TIP_STRATEGIES))
        raise ProtocolValidationError(
            f"steps[{index}].tip_strategy '{normalized_tip_strategy}' is unsupported. "
            f"allowed: {allowed}"
        )

    return {
        "type": "transfer",
        "source": source,
        "destination": destination,
        "volume_ul": volume_ul,
        "tip_strategy": normalized_tip_strategy,
    }


def _validate_mix_step(step: dict[str, Any], *, index: int) -> dict[str, Any]:
    well = _require_location_string(step.get("well"), f"steps[{index}].well")
    volume_ul = _require_positive_number(
        step.get("volume_ul"), f"steps[{index}].volume_ul"
    )
    cycles = _require_int(step.get("cycles"), f"steps[{index}].cycles", minimum=1)
    return {
        "type": "mix",
        "well": well,
        "volume_ul": volume_ul,
        "cycles": cycles,
    }


def _validate_incubate_step(step: dict[str, Any], *, index: int) -> dict[str, Any]:
    duration_s = _require_positive_number(
        step.get("duration_s"), f"steps[{index}].duration_s"
    )
    temperature_c = step.get("temperature_c")
    shaking_rpm = step.get("shaking_rpm")

    normalized: dict[str, Any] = {
        "type": "incubate",
        "duration_s": duration_s,
    }

    if temperature_c is not None:
        normalized["temperature_c"] = _require_number(
            temperature_c,
            f"steps[{index}].temperature_c",
        )

    if shaking_rpm is not None:
        normalized["shaking_rpm"] = _require_positive_number(
            shaking_rpm,
            f"steps[{index}].shaking_rpm",
        )

    return normalized


def _validate_read_absorbance_step(
    step: dict[str, Any], *, index: int
) -> dict[str, Any]:
    plate = _require_nonempty_string(step.get("plate"), f"steps[{index}].plate")
    wavelength_nm = _require_int(
        step.get("wavelength_nm"),
        f"steps[{index}].wavelength_nm",
        minimum=200,
        maximum=900,
    )

    wells_payload = step.get("wells")
    wells: list[str] | None = None
    if wells_payload is not None:
        if not isinstance(wells_payload, list):
            raise ProtocolValidationError(
                f"steps[{index}].wells must be an array of strings"
            )
        normalized_wells: list[str] = []
        seen_wells: set[str] = set()
        for well_index, well in enumerate(wells_payload, start=1):
            if not isinstance(well, str) or not well.strip():
                raise ProtocolValidationError(
                    f"steps[{index}].wells[{well_index}] must be a non-empty string"
                )
            normalized_well = well.strip()
            if normalized_well in seen_wells:
                raise ProtocolValidationError(
                    f"steps[{index}].wells contains duplicate value: {normalized_well}"
                )
            seen_wells.add(normalized_well)
            normalized_wells.append(normalized_well)
        wells = normalized_wells

    normalized = {
        "type": "read_absorbance",
        "plate": plate,
        "wavelength_nm": wavelength_nm,
    }
    if wells is not None:
        normalized["wells"] = wells
    return normalized


def _require_nonempty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProtocolValidationError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProtocolValidationError(f"{field_name} must be a number")
    return float(value)


def _require_non_negative_number(value: Any, field_name: str) -> float:
    result = _require_number(value, field_name)
    if result < 0:
        raise ProtocolValidationError(f"{field_name} must be >= 0")
    return result


def _require_positive_number(value: Any, field_name: str) -> float:
    result = _require_number(value, field_name)
    if result <= 0:
        raise ProtocolValidationError(f"{field_name} must be > 0")
    return result


def _require_int(
    value: Any,
    field_name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolValidationError(f"{field_name} must be an integer")
    if minimum is not None and value < minimum:
        raise ProtocolValidationError(f"{field_name} must be >= {minimum}")
    if maximum is not None and value > maximum:
        raise ProtocolValidationError(f"{field_name} must be <= {maximum}")
    return value


def _require_location_string(value: Any, field_name: str) -> str:
    normalized = _require_nonempty_string(value, field_name)
    if not _LOCATION_RE.fullmatch(normalized):
        raise ProtocolValidationError(
            f"{field_name} must match '<labware>:<position>' (got '{normalized}')"
        )
    return normalized


def _location_labware_name(location: str) -> str:
    return location.split(":", maxsplit=1)[0]


def _referenced_labware_names(steps: list[dict[str, Any]]) -> set[str]:
    names: set[str] = set()
    for step in steps:
        step_type = step.get("type")
        if step_type == "transfer":
            names.add(_location_labware_name(str(step["source"])))
            names.add(_location_labware_name(str(step["destination"])))
        elif step_type == "mix":
            names.add(_location_labware_name(str(step["well"])))
        elif step_type == "read_absorbance":
            plate = step.get("plate")
            if isinstance(plate, str) and plate.strip():
                names.add(plate.strip())
    return names
