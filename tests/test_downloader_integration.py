import os
import subprocess
import sys

import pytest


@pytest.mark.downloader
def test_remote_downloader_stock_smoke(tmp_path):
    """Run a tiny stock history fetch through the shared downloader to ensure it stays healthy."""
    base_url = os.environ.get("DATADOWNLOADER_BASE_URL")
    api_key = os.environ.get("DATADOWNLOADER_API_KEY")
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    if not base_url or not api_key:
        pytest.skip("Downloader base URL/API key not configured")
    if not username or not password:
        pytest.skip("ThetaData dev credentials not available")

    env = os.environ.copy()
    env.update(
        {
            "DATADOWNLOADER_BASE_URL": base_url,
            "DATADOWNLOADER_API_KEY": api_key,
            "DATADOWNLOADER_SKIP_LOCAL_START": "true",
            "THETADATA_USERNAME": username,
            "THETADATA_PASSWORD": password,
        }
    )

    script = r"""
import datetime
import pytz
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper

assert thetadata_helper.REMOTE_DOWNLOADER_ENABLED, "Remote downloader flag must be set"

asset = Asset(asset_type="stock", symbol="PLTR")
start = pytz.UTC.localize(datetime.datetime(2024, 9, 16, 9, 30))
end = pytz.UTC.localize(datetime.datetime(2024, 9, 16, 9, 35))

df = thetadata_helper.get_historical_data(
    asset=asset,
    start_dt=start,
    end_dt=end,
    ivl=60000,
    username="%s",
    password="%s",
    datastyle="ohlc",
    include_after_hours=False,
)

assert df is not None and not df.empty, "Downloader did not return any rows"
print(f"remote rows={len(df)} first_ts={df.index[0]}")
"""

    # Write the script to disk so subprocess traces are easier to debug when needed.
    smoke_path = tmp_path / "downloader_smoke.py"
    smoke_path.write_text(script % (username, password), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(smoke_path)],
        text=True,
        capture_output=True,
        env=env,
        check=True,
        timeout=120,
    )

    assert "remote rows=" in result.stdout
