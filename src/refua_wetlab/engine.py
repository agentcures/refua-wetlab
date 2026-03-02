from __future__ import annotations

import hashlib
import threading
from typing import Any

from refua_wetlab.models import canonical_protocol_json, validate_protocol_payload
from refua_wetlab.providers import WetLabProvider, default_provider_registry


class UnknownProviderError(KeyError):
    """Raised when caller references a provider that is not registered."""


class ProtocolExecutionError(RuntimeError):
    """Raised when protocol execution cannot complete safely."""


class UnifiedWetLabEngine:
    def __init__(self, providers: dict[str, WetLabProvider] | None = None) -> None:
        self._providers = providers or default_provider_registry()

    def list_providers(self) -> list[dict[str, Any]]:
        payloads = [
            provider.descriptor_payload() for provider in self._providers.values()
        ]
        return sorted(payloads, key=lambda item: str(item["provider_id"]))

    def validate_protocol(self, protocol_payload: Any) -> dict[str, Any]:
        return validate_protocol_payload(protocol_payload)

    def compile_protocol(
        self, *, provider_id: str, protocol_payload: Any
    ) -> dict[str, Any]:
        provider = self._require_provider(provider_id)
        protocol = validate_protocol_payload(protocol_payload)
        compiled = provider.compile(protocol)
        return {
            "provider": provider.provider_id,
            "protocol": protocol,
            "protocol_hash": _protocol_hash(protocol),
            "compiled": compiled,
        }

    def run_protocol(
        self,
        *,
        provider_id: str,
        protocol_payload: Any,
        dry_run: bool,
        metadata: dict[str, Any] | None = None,
        cancel_event: threading.Event | None = None,
    ) -> dict[str, Any]:
        provider = self._require_provider(provider_id)
        compiled_payload = self.compile_protocol(
            provider_id=provider_id,
            protocol_payload=protocol_payload,
        )
        execution = provider.execute(
            compiled_payload["compiled"],
            dry_run=dry_run,
            cancel_event=cancel_event,
        )
        if execution.get("status") == "cancelled":
            return {
                "provider": provider.provider_id,
                "protocol": compiled_payload["protocol"],
                "protocol_hash": compiled_payload["protocol_hash"],
                "compiled": compiled_payload["compiled"],
                "execution": execution,
                "metadata": metadata or {},
            }
        if execution.get("status") == "failed":
            errors = execution.get("errors")
            if isinstance(errors, list) and errors:
                details = str(errors[0])
            else:
                details = "execution failed"
            raise ProtocolExecutionError(details)
        return {
            "provider": provider.provider_id,
            "protocol": compiled_payload["protocol"],
            "protocol_hash": compiled_payload["protocol_hash"],
            "compiled": compiled_payload["compiled"],
            "execution": execution,
            "metadata": metadata or {},
        }

    def _require_provider(self, provider_id: str) -> WetLabProvider:
        provider = self._providers.get(provider_id)
        if provider is None:
            allowed = ", ".join(sorted(self._providers.keys()))
            raise UnknownProviderError(
                f"Unknown provider '{provider_id}'. available providers: {allowed}"
            )
        return provider


def _protocol_hash(protocol: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        canonical_protocol_json(protocol).encode("ascii")
    ).hexdigest()
    return digest
