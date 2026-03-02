from __future__ import annotations

from refua_wetlab.lineage import build_wetlab_lineage_event


def test_build_wetlab_lineage_event() -> None:
    payload = {
        "provider": "opentrons",
        "protocol_hash": "abc123",
        "protocol": {
            "name": "screen-v1",
            "steps": [{"type": "transfer"}, {"type": "mix"}],
        },
        "execution": {
            "status": "completed",
            "dry_run": True,
            "executed_commands": 2,
        },
        "metadata": {"campaign_id": "kras-001"},
    }

    event = build_wetlab_lineage_event(payload, run_id="run-1")
    assert event["run_id"] == "run-1"
    assert event["provider"] == "opentrons"
    assert event["steps_count"] == 2
    assert event["dry_run"] is True
    assert event["executed_commands"] == 2
