import datetime
import os

import pytest
import pytz

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def _require_downloader_env():
    base_url = os.environ.get("DATADOWNLOADER_BASE_URL")
    api_key = os.environ.get("DATADOWNLOADER_API_KEY")
    if not base_url or not api_key:
        pytest.skip("Remote downloader environment variables not configured")
    return base_url, api_key


@pytest.mark.downloader
def test_remote_downloader_smoke(monkeypatch):
    base_url, api_key = _require_downloader_env()
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", base_url)
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", api_key)
    monkeypatch.setenv("DATADOWNLOADER_SKIP_LOCAL_START", "true")
    monkeypatch.setenv("DATADOWNLOADER_KEY_HEADER", os.environ.get("DATADOWNLOADER_KEY_HEADER", "X-Downloader-Key"))

    asset = Asset(symbol="PLTR", asset_type="stock")
    tz = pytz.timezone("America/New_York")
    start_dt = tz.localize(datetime.datetime(2024, 9, 16, 9, 30))
    end_dt = tz.localize(datetime.datetime(2024, 9, 18, 16, 0))

    df = thetadata_helper.get_price_data(
        username=os.environ.get("THETADATA_USERNAME", "rob-dev@lumiwealth.com"),
        password=os.environ.get("THETADATA_PASSWORD", "TestTestTest"),
        asset=asset,
        start=start_dt,
        end=end_dt,
        timespan="day",
    )

    assert df is not None
    assert not df.empty
