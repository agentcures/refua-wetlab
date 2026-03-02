from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from refua_wetlab.engine import UnifiedWetLabEngine
from refua_wetlab.lms import LmsStore
from refua_wetlab.lms_api import LmsApi
from refua_wetlab.runner import RunBackgroundRunner
from refua_wetlab.storage import RunStore


def _sample_protocol(name: str = "python-api-screen") -> dict:
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
                "type": "read_absorbance",
                "plate": "plate",
                "wavelength_nm": 450,
            },
        ],
    }


class WetLabLmsPythonApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp.name) / "wetlab.sqlite3"

        self.store = RunStore(db_path)
        self.lms = LmsStore(db_path)
        self.runner = RunBackgroundRunner(self.store, max_workers=1)
        self.api = LmsApi(
            lms_store=self.lms,
            run_store=self.store,
            runner=self.runner,
            engine=UnifiedWetLabEngine(),
        )

    def tearDown(self) -> None:
        self.api.shutdown()
        self._tmp.cleanup()

    def test_route_api_lifecycle(self) -> None:
        project = self.api.route_post(
            path="/api/lms/projects",
            payload={"name": "Python API project", "owner": "automation"},
        )["project"]
        project_id = project["project_id"]

        sample = self.api.route_post(
            path="/api/lms/samples",
            payload={
                "project_id": project_id,
                "name": "sample-001",
                "sample_type": "rna",
                "volume_ul": 42,
            },
        )["sample"]
        sample_id = sample["sample_id"]

        experiment = self.api.route_post(
            path="/api/lms/experiments",
            payload={
                "project_id": project_id,
                "name": "Python API run",
                "provider": "opentrons",
                "protocol": _sample_protocol(),
                "sample_ids": [sample_id],
            },
        )["experiment"]
        experiment_id = experiment["experiment_id"]

        scheduled = self.api.route_post(
            path=f"/api/lms/experiments/{experiment_id}/schedule-run",
            payload={"async_mode": False, "dry_run": True, "priority": 60},
        )
        self.assertEqual(scheduled["run"]["status"], "completed")
        self.assertEqual(scheduled["experiment"]["status"], "completed")

        summary = self.api.route_get(path="/api/lms/summary")
        self.assertGreaterEqual(summary["counts"]["projects"]["total"], 1)
        self.assertGreaterEqual(summary["counts"]["samples"]["total"], 1)
        self.assertGreaterEqual(summary["counts"]["experiments"]["total"], 1)

        listed = self.api.route_get(
            path="/api/lms/experiments",
            query={"project_id": [project_id]},
        )
        self.assertTrue(
            any(
                item["experiment_id"] == experiment_id for item in listed["experiments"]
            )
        )


if __name__ == "__main__":
    unittest.main()
