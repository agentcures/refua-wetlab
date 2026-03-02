from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from refua_wetlab.app import create_server
from refua_wetlab.config import WetLabConfig


def _sample_protocol(name: str = "lms-screen-v1") -> dict:
    return {
        "name": name,
        "steps": [
            {
                "type": "transfer",
                "source": "plate:A1",
                "destination": "plate:B1",
                "volume_ul": 20,
            },
            {
                "type": "mix",
                "well": "plate:B1",
                "volume_ul": 15,
                "cycles": 4,
            },
            {
                "type": "read_absorbance",
                "plate": "plate",
                "wavelength_nm": 450,
            },
        ],
    }


class WetLabLmsApiTest(unittest.TestCase):
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
    ) -> dict:
        url = f"http://{self.host}:{self.port}{path}"
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

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

    def test_lms_end_to_end_lifecycle(self) -> None:
        project = self._request(
            "POST",
            "/api/lms/projects",
            {
                "name": "KRAS panel",
                "owner": "wetlab-team",
                "priority": 90,
                "metadata": {"disease": "NSCLC"},
            },
        )["project"]
        project_id = project["project_id"]

        sample = self._request(
            "POST",
            "/api/lms/samples",
            {
                "project_id": project_id,
                "name": "KRAS_G12D_clone_1",
                "sample_type": "cell_lysate",
                "volume_ul": 100,
                "concentration_ng_ul": 12.5,
                "storage_location": "freezer-1:shelf-A",
            },
        )["sample"]
        sample_id = sample["sample_id"]

        plate = self._request(
            "POST",
            "/api/lms/plates",
            {
                "project_id": project_id,
                "plate_type": "96",
                "label": "KRAS-plate-01",
            },
        )["plate"]
        plate_id = plate["plate_id"]

        assigned = self._request(
            "POST",
            f"/api/lms/plates/{plate_id}/assignments",
            {
                "sample_id": sample_id,
                "well": "A1",
                "volume_ul": 20,
                "notes": "Primary screening aliquot",
            },
        )["plate"]
        self.assertEqual(assigned["assignment_count"], 1)
        self.assertEqual(assigned["status"], "in_use")
        self.assertEqual(assigned["assignments"][0]["well"], "A1")

        sample_after_assignment = self._request("GET", f"/api/lms/samples/{sample_id}")[
            "sample"
        ]
        self.assertEqual(sample_after_assignment["status"], "in_use")
        self.assertAlmostEqual(sample_after_assignment["volume_ul"], 80.0, places=3)

        expires_at = (date.today() + timedelta(days=10)).isoformat()
        item = self._request(
            "POST",
            "/api/lms/inventory/items",
            {
                "name": "DMSO",
                "category": "solvent",
                "unit": "mL",
                "quantity": 10,
                "reorder_threshold": 15,
                "expiration_date": expires_at,
                "storage_location": "room-temp:cabinet-2",
            },
        )["item"]
        item_id = item["item_id"]

        item_after_tx = self._request(
            "POST",
            f"/api/lms/inventory/items/{item_id}/transactions",
            {
                "delta": -3,
                "reason": "screening_batch_1",
                "metadata": {"batch_id": "batch-001"},
            },
        )["item"]
        self.assertAlmostEqual(item_after_tx["quantity"], 7.0, places=3)
        self.assertTrue(item_after_tx["below_reorder"])

        low_stock = self._request(
            "GET",
            "/api/lms/inventory/items?below_reorder=true",
        )["items"]
        self.assertTrue(any(entry["item_id"] == item_id for entry in low_stock))

        experiment = self._request(
            "POST",
            "/api/lms/experiments",
            {
                "project_id": project_id,
                "name": "KRAS absorbance screen",
                "provider": "opentrons",
                "protocol": _sample_protocol(),
                "sample_ids": [sample_id],
                "metadata": {"campaign_id": "kras-campaign-01"},
            },
        )["experiment"]
        experiment_id = experiment["experiment_id"]

        scheduled = self._request(
            "POST",
            f"/api/lms/experiments/{experiment_id}/schedule-run",
            {
                "async_mode": False,
                "dry_run": True,
                "priority": 85,
            },
        )
        self.assertEqual(scheduled["run"]["status"], "completed")
        self.assertEqual(scheduled["experiment"]["status"], "completed")
        self.assertEqual(scheduled["experiment"]["live_run_status"], "completed")

        experiment_after = self._request(
            "GET", f"/api/lms/experiments/{experiment_id}"
        )["experiment"]
        self.assertEqual(experiment_after["status"], "completed")
        self.assertIsNotNone(experiment_after["run_id"])
        self.assertEqual(experiment_after["live_run_status"], "completed")

        summary = self._request("GET", "/api/lms/summary").copy()
        self.assertGreaterEqual(summary["counts"]["projects"]["total"], 1)
        self.assertGreaterEqual(summary["counts"]["samples"]["total"], 1)
        self.assertGreaterEqual(summary["counts"]["inventory_items"]["total"], 1)
        self.assertGreaterEqual(summary["counts"]["experiments"]["total"], 1)
        self.assertTrue(summary["inventory_alerts"]["low_stock"])
        self.assertTrue(summary["inventory_alerts"]["expiring_soon"])

        audit = self._request(
            "GET",
            f"/api/lms/audit?entity_type=experiment&entity_id={experiment_id}",
        )["events"]
        self.assertTrue(audit)
        self.assertTrue(any(entry["action"] == "run_linked" for entry in audit))

    def test_lms_validation_and_conflicts(self) -> None:
        project = self._request(
            "POST",
            "/api/lms/projects",
            {"name": "Validation project"},
        )["project"]
        project_id = project["project_id"]

        missing_project = self._request(
            "POST",
            "/api/lms/samples",
            {
                "project_id": "unknown-project",
                "name": "sample-bad",
                "sample_type": "rna",
            },
            allow_error=True,
        )
        self.assertEqual(missing_project["status_code"], 404)

        sample = self._request(
            "POST",
            "/api/lms/samples",
            {
                "project_id": project_id,
                "name": "sample-good",
                "sample_type": "rna",
                "volume_ul": 12,
            },
        )["sample"]

        plate = self._request(
            "POST",
            "/api/lms/plates",
            {
                "project_id": project_id,
                "label": "validation-plate",
            },
        )["plate"]

        self._request(
            "POST",
            f"/api/lms/plates/{plate['plate_id']}/assignments",
            {
                "sample_id": sample["sample_id"],
                "well": "B2",
                "volume_ul": 5,
            },
        )

        duplicate_assignment = self._request(
            "POST",
            f"/api/lms/plates/{plate['plate_id']}/assignments",
            {
                "sample_id": sample["sample_id"],
                "well": "B2",
                "volume_ul": 2,
            },
            allow_error=True,
        )
        self.assertEqual(duplicate_assignment["status_code"], 409)

        item = self._request(
            "POST",
            "/api/lms/inventory/items",
            {
                "name": "PBS",
                "unit": "mL",
                "quantity": 2,
            },
        )["item"]

        underflow = self._request(
            "POST",
            f"/api/lms/inventory/items/{item['item_id']}/transactions",
            {"delta": -5, "reason": "over-consumption"},
            allow_error=True,
        )
        self.assertEqual(underflow["status_code"], 400)

        experiment = self._request(
            "POST",
            "/api/lms/experiments",
            {
                "project_id": project_id,
                "name": "No-provider experiment",
                "protocol": _sample_protocol("draft-protocol"),
            },
        )["experiment"]

        missing_provider = self._request(
            "POST",
            f"/api/lms/experiments/{experiment['experiment_id']}/schedule-run",
            {"async_mode": False},
            allow_error=True,
        )
        self.assertEqual(missing_provider["status_code"], 400)


if __name__ == "__main__":
    unittest.main()
