"""Release gates must work without credentials for paid historical services."""

import os
import subprocess
import sys
from pathlib import Path


def test_remote_historical_benchmarks_are_not_in_the_offline_release_gate():
    # These long-window benchmarks need paid services and mutable remote caches.
    # Running them as ordinary acceptance tests made an inactive subscription
    # block publication, even though the portable backtest engine tests passed.
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ, LUMIBOT_DISABLE_DOTENV="1", LUMIBOT_DISABLE_DOTENV_LOCAL="1")
    for key in ("THETADATA_USERNAME", "THETADATA_PASSWORD", "DATADOWNLOADER_BASE_URL", "DATADOWNLOADER_API_KEY"):
        env.pop(key, None)
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/backtest/test_acceptance_backtests_ci.py",
         "-m", "not apitest and not downloader", "--collect-only", "-q"],
        cwd=root, env=env, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 5, result.stdout + result.stderr  # no selected tests
    assert "9 deselected" in result.stdout
