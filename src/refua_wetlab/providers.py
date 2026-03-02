from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class ProviderDescriptor:
    provider_id: str
    display_name: str
    transport: str
    supported_steps: tuple[str, ...]


class WetLabProvider(ABC):
    provider_id: str
    display_name: str
    transport: str
    supported_steps: tuple[str, ...]
    simulation_backend: str

    def descriptor_payload(self) -> dict[str, Any]:
        return asdict(
            ProviderDescriptor(
                provider_id=self.provider_id,
                display_name=self.display_name,
                transport=self.transport,
                supported_steps=self.supported_steps,
            )
        )

    def compile(self, protocol: dict[str, Any]) -> dict[str, Any]:
        commands: list[dict[str, Any]] = []
        estimated_duration_s = 0.0
        for index, step in enumerate(protocol["steps"], start=1):
            compiled_step = self._compile_step(step, index=index)
            # Persist canonical step payload so simulator can reason across providers.
            compiled_step["step"] = dict(step)
            commands.append(compiled_step)
            estimated_duration_s += _estimate_step_seconds(step)
        inventory = _normalize_inventory(protocol.get("inventory"))
        analysis = _build_protocol_analysis(protocol, inventory=inventory)
        return {
            "provider": self.provider_id,
            "protocol_name": protocol["name"],
            "estimated_duration_s": round(estimated_duration_s, 2),
            "commands": commands,
            "inventory": inventory,
            "analysis": analysis,
        }

    def execute(
        self,
        compiled_protocol: dict[str, Any],
        *,
        dry_run: bool,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        events: list[dict[str, Any]] = []
        warnings: list[str] = []
        errors: list[str] = []
        cancelled = False
        strict_inventory = isinstance(compiled_protocol.get("inventory"), dict)
        inventory = (
            {
                name: float(volume)
                for name, volume in compiled_protocol["inventory"].items()
            }
            if strict_inventory
            else {}
        )

        for command in compiled_protocol["commands"]:
            if cancel_event is not None and cancel_event.is_set():
                cancelled = True
                break
            step = command.get("step", {})
            step_type = str(command.get("step_type"))
            events.append(
                {
                    "index": command["index"],
                    "step_type": step_type,
                    "command": command["command"],
                    "status": "planned" if dry_run else "executed",
                }
            )
            event = events[-1]

            if not isinstance(step, dict):
                continue

            if step_type == "transfer":
                error = _simulate_transfer_step(
                    step=step,
                    inventory=inventory,
                    strict_inventory=strict_inventory,
                    event=event,
                )
                if error is not None:
                    event["status"] = "error"
                    event["error"] = error
                    errors.append(f"step {command['index']}: {error}")
                    break

            if step_type == "mix":
                error, warning = _simulate_mix_step(
                    step=step,
                    inventory=inventory,
                    strict_inventory=strict_inventory,
                )
                if warning:
                    warnings.append(f"step {command['index']}: {warning}")
                if error:
                    event["status"] = "error"
                    event["error"] = error
                    errors.append(f"step {command['index']}: {error}")
                    break

            if step_type == "read_absorbance":
                warning = _simulate_read_absorbance_step(
                    step=step,
                    inventory=inventory,
                    strict_inventory=strict_inventory,
                )
                if warning:
                    warnings.append(f"step {command['index']}: {warning}")

            if cancel_event is not None and cancel_event.is_set():
                cancelled = True
                break

        if not dry_run:
            warnings.append(
                "Execution is simulation-only in this project scaffold; "
                "connectors must be wired to real controllers before production use."
            )
        if not strict_inventory:
            warnings.append(
                "No protocol inventory supplied; liquid availability checks were not enforced."
            )

        if cancelled:
            warnings.append("Execution cancelled by user request.")
            status = "cancelled"
        else:
            status = "failed" if errors else "completed"
        executed_commands = len(
            [event for event in events if event.get("status") != "error"]
        )
        return {
            "backend": self.simulation_backend,
            "status": status,
            "dry_run": dry_run,
            "executed_commands": executed_commands,
            "estimated_duration_s": compiled_protocol["estimated_duration_s"],
            "events": events,
            "warnings": warnings,
            "errors": errors,
            "final_inventory": (
                dict(sorted(inventory.items())) if strict_inventory else None
            ),
        }

    @abstractmethod
    def _compile_step(self, step: dict[str, Any], *, index: int) -> dict[str, Any]:
        raise NotImplementedError


class OpenTronsProvider(WetLabProvider):
    provider_id = "opentrons"
    display_name = "Opentrons OT-2 / Flex"
    transport = "robot-http"
    supported_steps = ("incubate", "mix", "read_absorbance", "transfer")
    simulation_backend = "opentrons-sim"

    def _compile_step(self, step: dict[str, Any], *, index: int) -> dict[str, Any]:
        step_type = step["type"]
        if step_type == "transfer":
            command = "pipette.transfer"
            args = {
                "source": step["source"],
                "destination": step["destination"],
                "volume_ul": step["volume_ul"],
                "tip_strategy": step["tip_strategy"],
            }
        elif step_type == "mix":
            command = "pipette.mix"
            args = {
                "well": step["well"],
                "volume_ul": step["volume_ul"],
                "cycles": step["cycles"],
            }
        elif step_type == "incubate":
            command = "module.incubate"
            args = {
                "duration_s": step["duration_s"],
                "temperature_c": step.get("temperature_c"),
                "shaking_rpm": step.get("shaking_rpm"),
            }
        else:
            command = "integration.read_absorbance"
            args = {
                "plate": step["plate"],
                "wavelength_nm": step["wavelength_nm"],
                "wells": step.get("wells"),
            }
        return {
            "index": index,
            "step_type": step_type,
            "command": command,
            "args": args,
        }


class HamiltonProvider(WetLabProvider):
    provider_id = "hamilton"
    display_name = "Hamilton STAR / VANTAGE"
    transport = "venus-api"
    supported_steps = ("incubate", "mix", "read_absorbance", "transfer")
    simulation_backend = "hamilton-sim"

    def _compile_step(self, step: dict[str, Any], *, index: int) -> dict[str, Any]:
        step_type = step["type"]
        if step_type == "transfer":
            command = "hamilton.aspirate_dispense"
            args = {
                "src": step["source"],
                "dst": step["destination"],
                "ul": step["volume_ul"],
                "tip_mode": step["tip_strategy"],
            }
        elif step_type == "mix":
            command = "hamilton.mix_well"
            args = {
                "well": step["well"],
                "ul": step["volume_ul"],
                "cycles": step["cycles"],
            }
        elif step_type == "incubate":
            command = "hamilton.incubator.hold"
            args = {
                "seconds": step["duration_s"],
                "temperature_c": step.get("temperature_c"),
                "shaker_rpm": step.get("shaking_rpm"),
            }
        else:
            command = "hamilton.reader.absorbance"
            args = {
                "plate": step["plate"],
                "wavelength_nm": step["wavelength_nm"],
                "wells": step.get("wells"),
            }
        return {
            "index": index,
            "step_type": step_type,
            "command": command,
            "args": args,
        }


class BenchlingProvider(WetLabProvider):
    provider_id = "benchling"
    display_name = "Benchling Orchestration"
    transport = "rest-api"
    supported_steps = ("incubate", "mix", "read_absorbance", "transfer")
    simulation_backend = "benchling-sim"

    def _compile_step(self, step: dict[str, Any], *, index: int) -> dict[str, Any]:
        step_type = step["type"]
        command = "benchling.workflow_task.create"
        args: dict[str, Any] = {
            "task_index": index,
            "task_type": step_type,
            "payload": step,
        }
        return {
            "index": index,
            "step_type": step_type,
            "command": command,
            "args": args,
        }


def default_provider_registry() -> dict[str, WetLabProvider]:
    providers: list[WetLabProvider] = [
        OpenTronsProvider(),
        HamiltonProvider(),
        BenchlingProvider(),
    ]
    return {provider.provider_id: provider for provider in providers}


def _estimate_step_seconds(step: dict[str, Any]) -> float:
    step_type = step["type"]
    if step_type == "transfer":
        return 6.0
    if step_type == "mix":
        return 4.0
    if step_type == "incubate":
        duration_s = float(step["duration_s"])
        return max(duration_s, 10.0)
    return 8.0


def _build_protocol_analysis(
    protocol: dict[str, Any],
    *,
    inventory: dict[str, float] | None,
) -> dict[str, Any]:
    steps = protocol.get("steps", [])
    transfer_steps = [step for step in steps if step.get("type") == "transfer"]
    total_transferred_ul = round(
        sum(float(step.get("volume_ul", 0.0)) for step in transfer_steps),
        2,
    )
    tips_used_estimate = _estimate_tip_usage(transfer_steps)
    warnings: list[str] = []

    distinct_transfer_sources = {
        str(step["source"])
        for step in transfer_steps
        if isinstance(step.get("source"), str)
    }
    never_tip_sources = {
        str(step["source"])
        for step in transfer_steps
        if str(step.get("tip_strategy")) == "never"
        and isinstance(step.get("source"), str)
    }
    if never_tip_sources and len(distinct_transfer_sources) > 1:
        warnings.append(
            "Tip strategy 'never' is used across multiple transfer sources; "
            "cross-contamination risk is elevated."
        )

    if inventory is None:
        if transfer_steps:
            warnings.append(
                "No inventory supplied; dry-run cannot prove source wells have enough volume."
            )
        inventory_mode = "best_effort"
    else:
        missing_sources = sorted(
            {
                str(step["source"])
                for step in transfer_steps
                if isinstance(step.get("source"), str)
                and str(step["source"]) not in inventory
            }
        )
        if missing_sources:
            preview = ", ".join(missing_sources[:5])
            suffix = " ..." if len(missing_sources) > 5 else ""
            warnings.append(
                "Inventory does not define source volumes for: " + preview + suffix
            )
        inventory_mode = "strict"

    return {
        "steps_count": len(steps),
        "transfer_steps": len(transfer_steps),
        "total_transferred_ul": total_transferred_ul,
        "tips_used_estimate": tips_used_estimate,
        "inventory_mode": inventory_mode,
        "warnings": warnings,
    }


def _estimate_tip_usage(transfer_steps: list[dict[str, Any]]) -> int:
    tips_used = 0
    previous_source: str | None = None
    for step in transfer_steps:
        source = step.get("source")
        tip_strategy = str(step.get("tip_strategy"))
        if tip_strategy == "always":
            tips_used += 1
        elif tip_strategy == "on_change":
            source_text = str(source)
            if source_text != previous_source:
                tips_used += 1
            previous_source = source_text
        else:
            previous_source = str(source)
    return tips_used


def _normalize_inventory(payload: Any) -> dict[str, float] | None:
    if not isinstance(payload, dict):
        return None
    normalized: dict[str, float] = {}
    for location, volume in payload.items():
        if isinstance(volume, bool) or not isinstance(volume, (int, float)):
            continue
        normalized[str(location)] = float(volume)
    return normalized


def _simulate_transfer_step(
    *,
    step: dict[str, Any],
    inventory: dict[str, float],
    strict_inventory: bool,
    event: dict[str, Any],
) -> str | None:
    if not strict_inventory:
        return None
    source = str(step.get("source"))
    destination = str(step.get("destination"))
    volume_ul = float(step.get("volume_ul", 0.0))

    source_before = float(inventory.get(source, 0.0))
    destination_before = float(inventory.get(destination, 0.0))
    if source_before + 1e-9 < volume_ul:
        return (
            f"insufficient volume at {source}: have {round(source_before, 3)} uL, "
            f"need {round(volume_ul, 3)} uL"
        )

    source_after = max(source_before - volume_ul, 0.0)
    destination_after = destination_before + volume_ul
    inventory[source] = source_after
    inventory[destination] = destination_after
    event["inventory_after"] = {
        source: round(source_after, 3),
        destination: round(destination_after, 3),
    }
    return None


def _simulate_mix_step(
    *,
    step: dict[str, Any],
    inventory: dict[str, float],
    strict_inventory: bool,
) -> tuple[str | None, str | None]:
    if not strict_inventory:
        return None, None
    well = str(step.get("well"))
    requested_volume = float(step.get("volume_ul", 0.0))
    available_volume = float(inventory.get(well, 0.0))

    if available_volume <= 0:
        return f"cannot mix empty well {well}", None
    if available_volume + 1e-9 < requested_volume:
        return None, (
            f"mix volume {round(requested_volume, 3)} uL exceeds available "
            f"{round(available_volume, 3)} uL at {well}"
        )
    return None, None


def _simulate_read_absorbance_step(
    *,
    step: dict[str, Any],
    inventory: dict[str, float],
    strict_inventory: bool,
) -> str | None:
    if not strict_inventory:
        return None
    plate = str(step.get("plate"))
    wells = step.get("wells")
    if not isinstance(wells, list) or not wells:
        return None

    empty_wells: list[str] = []
    for well in wells:
        well_name = str(well)
        location = well_name if ":" in well_name else f"{plate}:{well_name}"
        if float(inventory.get(location, 0.0)) <= 0:
            empty_wells.append(well_name)
    if not empty_wells:
        return None
    preview = ", ".join(empty_wells[:5])
    suffix = " ..." if len(empty_wells) > 5 else ""
    return "read includes wells with no tracked volume: " + preview + suffix
