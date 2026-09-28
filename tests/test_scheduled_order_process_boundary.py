"""Prove exit/restart with real processes and actual broker/cloud serialization.

Only broker storage and HTTP transport are fixtures. This is not a hosted
listener/Node delivery or real-money execution claim.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("later_status", ["fill", "partial_fill", "canceled", "error"])
def test_submission_exit_later_broker_transition_and_repeated_restart(tmp_path, later_status):
    worker = Path(__file__).parent / "fixtures/scheduled_order_process.py"
    broker_state = tmp_path / "broker.json"
    env = {
        "PATH": os.environ.get("PATH", ""),
        "LUMIBOT_DISABLE_DOTENV": "true",
        "AWS_EC2_METADATA_DISABLED": "true",
        "IS_BACKTESTING": "true",
    }

    def run(phase, number):
        output = tmp_path / f"{phase}-{number}.json"
        completed = subprocess.run(
            [sys.executable, str(worker), phase, str(broker_state), str(output)],
            env=env,
            capture_output=True,
            text=True,
            timeout=45,
        )
        assert completed.returncode == 0, completed.stderr[-6000:]
        return json.loads(output.read_text())

    submitted = run("submit", 0)  # Process has exited before the broker changes below.
    first_orders = submitted["payload"]["orders"]
    assert len(first_orders) == 1
    identifier = first_orders[0]["identifier"]
    assert first_orders[0]["status"] == "new"
    stored = json.loads(broker_state.read_text())
    stored["status"] = later_status
    stored["broker_update_date"] = "2026-09-08T15:00:00+00:00"
    if later_status in {"fill", "partial_fill"}:
        stored["avg_fill_price"] = 99.5
    broker_state.write_text(json.dumps(stored))

    second, third = run("reconcile", 1), run("reconcile", 2)
    assert len({submitted["pid"], second["pid"], third["pid"]}) == 3
    for recovered in (second, third):
        orders = recovered["payload"]["orders"]
        assert len(orders) == 1
        assert orders[0]["identifier"] == identifier
        assert orders[0]["status"] == later_status
        assert orders[0]["quantity"] == 3
        assert orders[0]["strategy"] == first_orders[0]["strategy"]
