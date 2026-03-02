from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from refua_wetlab.app import create_server
from refua_wetlab.config import WetLabConfig


def _sample_protocol() -> dict:
    return {
        "name": "serial-dilution-screen",
        "steps": [
            {
                "type": "transfer",
                "source": "plate:A1",
                "destination": "plate:B1",
                "volume_ul": 50,
            },
            {
                "type": "mix",
                "well": "plate:B1",
                "volume_ul": 40,
                "cycles": 5,
            },
            {
                "type": "incubate",
                "duration_s": 300,
                "temperature_c": 37,
            },
            {
                "type": "read_absorbance",
                "plate": "plate",
                "wavelength_nm": 450,
            },
        ],
    }


def _inventory_protocol() -> dict:
    return {
        "name": "inventory-driven-transfer",
        "inventory": {
            "plate:A1": 10.0,
            "plate:B1": 0.0,
        },
        "steps": [
            {
                "type": "transfer",
                "source": "plate:A1",
                "destination": "plate:B1",
                "volume_ul": 20.0,
            }
        ],
    }


class WetLabApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        config = WetLabConfig(
            host="127.0.0.1",
            port=0,
            data_dir=Path(self._tmp.name) / "data",
            max_workers=1,
        )
        self.server, self.app = create_server(config)
        self.host, self.port = self.server.server_address
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.app.shutdown()
        self._thread.join(timeout=2)
        self._tmp.cleanup()

    def _request(
        self,
        method: str,
        path: str,
        payload: dict | None = None,
        *,
        allow_error: bool = False,
        token: str | None = None,
    ) -> dict:
        url = f"http://{self.host}:{self.port}{path}"
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = f"Bearer {token}"

        request = Request(url, method=method, data=data, headers=headers)
        try:
            with urlopen(request, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
            exc.close()
            if allow_error:
                return {"status_code": exc.code, "body": parsed}
            raise AssertionError(f"HTTP {exc.code} for {path}: {body}") from exc

    def test_health_and_providers(self) -> None:
        health = self._request("GET", "/api/health")
        self.assertTrue(health["ok"])
        self.assertGreaterEqual(health["providers_count"], 3)

        providers = self._request("GET", "/api/providers")
        self.assertIn("providers", providers)
        self.assertIn("opentrons", providers["provider_ids"])
        self.assertIn("hamilton", providers["provider_ids"])
        self.assertIn("benchling", providers["provider_ids"])

    def test_protocol_validate_and_compile(self) -> None:
        validate_payload = self._request(
            "POST",
            "/api/protocols/validate",
            {"protocol": _sample_protocol()},
        )
        self.assertTrue(validate_payload["valid"])
        self.assertEqual(validate_payload["protocol"]["name"], "serial-dilution-screen")

        compile_payload = self._request(
            "POST",
            "/api/protocols/compile",
            {"provider": "opentrons", "protocol": _sample_protocol()},
        )
        self.assertEqual(compile_payload["provider"], "opentrons")
        self.assertEqual(len(compile_payload["compiled"]["commands"]), 4)
        self.assertIn("analysis", compile_payload["compiled"])
        self.assertEqual(
            compile_payload["compiled"]["analysis"]["inventory_mode"], "best_effort"
        )

    def test_sync_run(self) -> None:
        payload = self._request(
            "POST",
            "/api/runs",
            {
                "provider": "hamilton",
                "async_mode": False,
                "dry_run": True,
                "protocol": _sample_protocol(),
                "metadata": {"campaign_id": "abc-123"},
            },
        )
        self.assertIn("run", payload)
        self.assertEqual(payload["run"]["status"], "completed")
        self.assertIn("result", payload)
        self.assertEqual(payload["result"]["provider"], "hamilton")
        self.assertTrue(payload["result"]["execution"]["dry_run"])
        self.assertEqual(payload["result"]["execution"]["executed_commands"], 4)
        self.assertEqual(payload["result"]["execution"]["status"], "completed")

    def test_async_run_lifecycle(self) -> None:
        create_payload = self._request(
            "POST",
            "/api/runs",
            {
                "provider": "opentrons",
                "async_mode": True,
                "dry_run": True,
                "protocol": _sample_protocol(),
            },
        )
        self.assertIn("run", create_payload)
        run_id = create_payload["run"]["run_id"]

        deadline = time.time() + 5
        last_status = "queued"
        while time.time() < deadline:
            run_payload = self._request("GET", f"/api/runs/{run_id}")
            last_status = run_payload["status"]
            if last_status in {"completed", "failed"}:
                break
            time.sleep(0.1)
        self.assertEqual(last_status, "completed")

    def test_cancel_queued_run(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def _blocking() -> dict:
            started.set()
            release.wait(timeout=5)
            return {"ok": True}

        first = self.app.runner.submit(
            provider="opentrons",
            request={"provider": "opentrons", "protocol": _sample_protocol()},
            fn=_blocking,
        )
        self.assertTrue(started.wait(timeout=2))
        self.assertIn(first["status"], {"queued", "running"})

        queued = self._request(
            "POST",
            "/api/runs",
            {
                "provider": "opentrons",
                "async_mode": True,
                "dry_run": True,
                "protocol": _sample_protocol(),
            },
        )
        run_id = queued["run"]["run_id"]

        cancelled = self._request("POST", f"/api/runs/{run_id}/cancel", {})
        self.assertTrue(cancelled["cancelled"])
        self.assertEqual(cancelled["status"], "cancelled")

        after = self._request("GET", f"/api/runs/{run_id}")
        self.assertEqual(after["status"], "cancelled")

        release.set()

    def test_async_run_fails_when_inventory_is_insufficient(self) -> None:
        create_payload = self._request(
            "POST",
            "/api/runs",
            {
                "provider": "opentrons",
                "async_mode": True,
                "dry_run": True,
                "protocol": _inventory_protocol(),
            },
        )
        run_id = create_payload["run"]["run_id"]

        deadline = time.time() + 5
        last_payload: dict = {}
        while time.time() < deadline:
            last_payload = self._request("GET", f"/api/runs/{run_id}")
            if last_payload["status"] in {"completed", "failed"}:
                break
            time.sleep(0.1)

        self.assertEqual(last_payload["status"], "failed")
        self.assertIn("insufficient volume", str(last_payload["error"]))

    def test_run_lineage_endpoint(self) -> None:
        create_payload = self._request(
            "POST",
            "/api/runs",
            {
                "provider": "hamilton",
                "async_mode": False,
                "dry_run": True,
                "protocol": _sample_protocol(),
                "metadata": {"campaign_id": "kras-123"},
            },
        )
        run_id = create_payload["run"]["run_id"]

        lineage_payload = self._request("GET", f"/api/runs/{run_id}/lineage")
        self.assertEqual(lineage_payload["run_id"], run_id)
        self.assertEqual(lineage_payload["provider"], "hamilton")
        self.assertEqual(lineage_payload["steps_count"], 4)
        self.assertEqual(lineage_payload["execution_status"], "completed")
        self.assertEqual(lineage_payload["metadata"]["campaign_id"], "kras-123")

    def test_invalid_provider_returns_400(self) -> None:
        payload = self._request(
            "POST",
            "/api/protocols/compile",
            {"provider": "unknown-provider", "protocol": _sample_protocol()},
            allow_error=True,
        )
        self.assertEqual(payload["status_code"], 400)
        self.assertIn("Unknown provider", payload["body"]["error"])


class WetLabApiAuthTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        config = WetLabConfig(
            host="127.0.0.1",
            port=0,
            data_dir=Path(self._tmp.name) / "data",
            max_workers=1,
            auth_tokens=("viewer-token",),
            operator_tokens=("operator-token",),
            admin_tokens=("admin-token",),
        )
        self.server, self.app = create_server(config)
        self.host, self.port = self.server.server_address
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.app.shutdown()
        self._thread.join(timeout=2)
        self._tmp.cleanup()

    def _request(
        self,
        method: str,
        path: str,
        payload: dict | None = None,
        *,
        allow_error: bool = False,
        token: str | None = None,
    ) -> dict:
        url = f"http://{self.host}:{self.port}{path}"
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = f"Bearer {token}"

        request = Request(url, method=method, data=data, headers=headers)
        try:
            with urlopen(request, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8")
            parsed = json.loads(body) if body else {}
            exc.close()
            if allow_error:
                return {"status_code": exc.code, "body": parsed}
            raise AssertionError(f"HTTP {exc.code} for {path}: {body}") from exc

    def test_auth_required_for_api_get(self) -> None:
        missing = self._request("GET", "/api/health", allow_error=True)
        self.assertEqual(missing["status_code"], 401)

        ok = self._request("GET", "/api/health", token="viewer-token")
        self.assertTrue(ok["ok"])

    def test_post_requires_operator_role(self) -> None:
        payload = self._request(
            "POST",
            "/api/protocols/validate",
            {"protocol": _sample_protocol()},
            token="viewer-token",
            allow_error=True,
        )
        self.assertEqual(payload["status_code"], 403)

        ok = self._request(
            "POST",
            "/api/protocols/validate",
            {"protocol": _sample_protocol()},
            token="operator-token",
        )
        self.assertTrue(ok["valid"])


if __name__ == "__main__":
    unittest.main()
