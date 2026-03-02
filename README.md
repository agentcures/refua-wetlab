# Refua WetLab

Refua WetLab is a standalone project that exposes a unified HTTP API for wet-lab automation workflows.

It provides:
- A canonical protocol schema for common liquid-handling and assay operations.
- Provider adapters that compile canonical steps into provider-specific commands.
- Async run orchestration with status tracking and cancellation.
- A simulation-safe execution mode (`dry_run`) for workflow development.
- Inventory-aware simulation checks to catch impossible liquid moves before hardware time is spent.
- Priority-based background scheduling for urgent protocols.
- A built-in LMS layer for project/sample/plate/inventory/experiment management.
- End-to-end audit trails and LMS summaries (low-stock + expiring-material alerts).

## Quick Start

```bash
cd refua-wetlab
pip install -e .
refua-wetlab --host 127.0.0.1 --port 8790
```

## API Endpoints

Core WetLab API:
- `GET /api/health`
- `GET /api/providers`
- `POST /api/protocols/validate`
- `POST /api/protocols/compile`
- `POST /api/runs`
- `GET /api/runs`
- `GET /api/runs/{run_id}`
- `GET /api/runs/{run_id}/lineage`
- `POST /api/runs/{run_id}/cancel`

LMS API:
- `GET /api/lms`
- `GET /api/lms/summary`
- `GET /api/lms/audit`
- `GET /api/lms/projects`
- `POST /api/lms/projects`
- `GET /api/lms/projects/{project_id}`
- `POST /api/lms/projects/{project_id}/status`
- `GET /api/lms/samples`
- `POST /api/lms/samples`
- `GET /api/lms/samples/{sample_id}`
- `POST /api/lms/samples/{sample_id}/status`
- `GET /api/lms/samples/{sample_id}/events`
- `POST /api/lms/samples/{sample_id}/events`
- `GET /api/lms/plates`
- `POST /api/lms/plates`
- `GET /api/lms/plates/{plate_id}`
- `POST /api/lms/plates/{plate_id}/assignments`
- `GET /api/lms/inventory/items`
- `POST /api/lms/inventory/items`
- `GET /api/lms/inventory/items/{item_id}`
- `POST /api/lms/inventory/items/{item_id}/transactions`
- `GET /api/lms/inventory/items/{item_id}/transactions`
- `GET /api/lms/experiments`
- `POST /api/lms/experiments`
- `GET /api/lms/experiments/{experiment_id}`
- `POST /api/lms/experiments/{experiment_id}/status`
- `POST /api/lms/experiments/{experiment_id}/schedule-run`

## Python API

`refua-wetlab` now ships a direct Python LMS API so callers can use LMS workflows
without going through HTTP handlers.

```python
from pathlib import Path

from refua_wetlab import LmsApi, LmsStore, UnifiedWetLabEngine
from refua_wetlab.runner import RunBackgroundRunner
from refua_wetlab.storage import RunStore

db_path = Path(".refua-wetlab") / "runs.sqlite3"
run_store = RunStore(db_path)
lms_store = LmsStore(db_path)
runner = RunBackgroundRunner(run_store, max_workers=2)

api = LmsApi(
    lms_store=lms_store,
    run_store=run_store,
    runner=runner,
    engine=UnifiedWetLabEngine(),
)

project = api.route_post(path="/api/lms/projects", payload={"name": "KRAS campaign"})
summary = api.route_get(path="/api/lms/summary")
api.shutdown()
```

## Example Protocol

```json
{
  "name": "serial-dilution-screen",
  "steps": [
    {
      "type": "transfer",
      "source": "plate:A1",
      "destination": "plate:B1",
      "volume_ul": 50
    },
    {
      "type": "mix",
      "well": "plate:B1",
      "volume_ul": 40,
      "cycles": 5
    },
    {
      "type": "incubate",
      "duration_s": 900,
      "temperature_c": 37
    },
    {
      "type": "read_absorbance",
      "plate": "plate",
      "wavelength_nm": 450
    }
  ]
}
```

## Compile For A Provider

```bash
curl -s http://127.0.0.1:8790/api/protocols/compile \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "opentrons",
    "protocol": {
      "name": "serial-dilution-screen",
      "steps": [
        {"type":"transfer","source":"plate:A1","destination":"plate:B1","volume_ul":50}
      ]
    }
  }' | jq
```

## Create Async Run

```bash
curl -s http://127.0.0.1:8790/api/runs \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "hamilton",
    "async_mode": true,
    "dry_run": true,
    "priority": 80,
    "protocol": {
      "name": "screen-v1",
      "inventory": {
        "plate:A1": 120.0,
        "plate:B1": 0.0
      },
      "steps": [
        {"type":"transfer","source":"plate:A1","destination":"plate:B1","volume_ul":20},
        {"type":"incubate","duration_s":300}
      ]
    }
  }' | jq
```

## LMS Workflow Example

```bash
# 1) Create project
PROJECT_ID=$(curl -s http://127.0.0.1:8790/api/lms/projects \
  -X POST -H "Content-Type: application/json" \
  -d '{"name":"KRAS campaign","owner":"wetlab-team"}' | jq -r '.project.project_id')

# 2) Register sample
SAMPLE_ID=$(curl -s http://127.0.0.1:8790/api/lms/samples \
  -X POST -H "Content-Type: application/json" \
  -d '{
    "project_id":"'"$PROJECT_ID"'",
    "name":"KRAS_clone_1",
    "sample_type":"cell_lysate",
    "volume_ul":120
  }' | jq -r '.sample.sample_id')

# 3) Create experiment with protocol
EXP_ID=$(curl -s http://127.0.0.1:8790/api/lms/experiments \
  -X POST -H "Content-Type: application/json" \
  -d '{
    "project_id":"'"$PROJECT_ID"'",
    "name":"KRAS absorbance screen",
    "provider":"opentrons",
    "sample_ids":["'"$SAMPLE_ID"'"],
    "protocol": {
      "name":"kras-screen",
      "steps":[
        {"type":"transfer","source":"plate:A1","destination":"plate:B1","volume_ul":20},
        {"type":"mix","well":"plate:B1","volume_ul":15,"cycles":4},
        {"type":"read_absorbance","plate":"plate","wavelength_nm":450}
      ]
    }
  }' | jq -r '.experiment.experiment_id')

# 4) Schedule run from experiment
curl -s http://127.0.0.1:8790/api/lms/experiments/$EXP_ID/schedule-run \
  -X POST -H "Content-Type: application/json" \
  -d '{"async_mode":false,"dry_run":true,"priority":85}' | jq

# 5) Get LMS summary
curl -s http://127.0.0.1:8790/api/lms/summary | jq
```

## Scope And Limitations

`refua-wetlab` is a simulation-first orchestration layer intended for protocol
design, dry-run validation, run metadata tracking, and LMS operations.

Current limitations:
- Provider execution in this package is mock/simulated; it does not drive real
  robotic hardware without an external adapter/runtime bridge.
- The canonical protocol model covers common liquid-handling and plate-read
  operations, not every vendor-native instruction.
- Scheduling is process-local and SQLite-backed; it is not a distributed queue.
- LMS data model is lightweight and embedded; it is not a replacement for
  enterprise LIMS/ELN systems.
- Auth is bearer-token based; there is no built-in SSO, RBAC policy engine, or
  audit-signature workflow.
- This package does not provide GxP/21 CFR Part 11 compliance guarantees.

## Production Notes

- Treat this as an orchestration service, not as the final source of truth for
  regulated records.
- Run behind a trusted gateway (TLS termination, centralized authn/authz,
  request logging, and secrets management).
- Use routine backups for the SQLite database or replace storage with a managed
  backend in your deployment architecture.

## Development

```bash
cd refua-wetlab
python -m unittest discover -s tests -v
```

Build artifacts:

```bash
cd refua-wetlab
python -m build
python -m twine check dist/*
```

## CI And Hooks

- GitHub Actions CI: `.github/workflows/ci.yml`
- Pre-commit config: `.pre-commit-config.yaml`

Run hooks locally:

```bash
cd refua-wetlab
pre-commit run --all-files
```
