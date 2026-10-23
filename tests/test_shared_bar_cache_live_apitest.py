"""Real brokers + the configured remote store: a second fresh process reuses the bars.

Each broker runs scripts/measure_shared_bars.py three times in separate processes:
uncached (cache off), cold (fills the cache) and warm (fresh local folder, so only the
remote store can help). Settled bars must come back with zero broker reads and the
same prices; the live read must re-fetch only its open tail. Uploaded objects are
removed afterwards (they live under a per-run connection scope).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "measure_shared_bars.py"

BROKERS = {
    "alpaca": ("ALPACA_TEST_API_KEY", "ALPACA_TEST_API_SECRET"),
    "tradier": ("TRADIER_TEST_ACCESS_TOKEN", "TRADIER_TEST_ACCOUNT_NUMBER"),
    "kraken": (),
}


def _measure(broker, run_id, phase):
    env = dict(os.environ)
    env.pop("LUMIBOT_SHARED_BARS", None)
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--broker", broker, "--run-id", run_id, "--phase", phase],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    for line in proc.stdout.splitlines():
        if line.startswith("SHARED_BARS_MEASUREMENT "):
            return json.loads(line.split(" ", 1)[1])
    raise AssertionError(f"{broker} {phase} produced no measurement: {proc.stdout[-2000:]} {proc.stderr[-2000:]}")


def _remote_cleanup(keys):
    from lumibot.tools.backtest_cache import get_backtest_cache

    manager = get_backtest_cache()
    if not keys or manager is None or not manager.enabled:
        return
    client = manager._get_client()
    for key in keys:
        client.delete_object(Bucket=manager._settings.bucket, Key=key)


@pytest.mark.apitest
@pytest.mark.public_http
@pytest.mark.parametrize("broker", sorted(BROKERS))
def test_second_fresh_process_reuses_real_broker_bars(broker):
    missing = [name for name in BROKERS[broker] if not os.environ.get(name)]
    if missing:
        pytest.skip(f"{broker} live credentials not set: {missing}")
    if (os.environ.get("LUMIBOT_CACHE_BACKEND") or "").lower() != "s3":
        pytest.skip("Needs the remote S3 cache configured (LUMIBOT_CACHE_BACKEND=s3)")
    run_id = f"apitest-{uuid.uuid4().hex[:10]}"
    results = {}
    try:
        results["uncached"] = _measure(broker, run_id, "uncached")
        if results["uncached"]["settledDaily"]["rows"] == 0:
            pytest.skip(f"{broker} returned no bars without the cache (credentials rejected?)")
        for phase in ("cold", "warm"):
            results[phase] = _measure(broker, run_id, phase)
        cold, warm, plain = results["cold"], results["warm"], results["uncached"]
        assert cold["settledDaily"]["rows"] > 0 and cold["liveMinute"]["rows"] > 0
        assert cold["flush"]["flushed"] and cold["uploadedKeys"], "cold process must publish to the remote store"
        # Settled history: zero broker reads in the fresh process, identical prices.
        assert warm["settledDaily"]["historyCache"]["providerReads"] == 0, warm["settledDaily"]
        assert warm["settledDaily"]["historyCache"]["hit"] is True
        assert warm["settledDaily"]["pricesDigest"] == cold["settledDaily"]["pricesDigest"]
        assert warm["settledDaily"]["pricesDigest"] == plain["settledDaily"]["pricesDigest"]
        # Live read: only the open tail goes back to the broker.
        assert warm["liveMinute"]["historyCache"]["providerReads"] <= 1, warm["liveMinute"]
        assert warm["liveMinute"]["rows"] >= cold["liveMinute"]["rows"] - 5
    finally:
        print(f"SHARED_BARS_LIVE_RESULT {broker} " + json.dumps(results, default=str))
        keys = sorted({k for r in results.values() for k in r.get("uploadedKeys", [])})
        _remote_cleanup(keys)
