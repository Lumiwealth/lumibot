import os
import socket
import subprocess
import sys
import textwrap
from pathlib import Path
from urllib.parse import urlparse

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOWNLOADER_BASE_URL = "http://localhost:8080"


def _normalize_downloader_base_url(value: str | None) -> str:
    if not value:
        return DEFAULT_DOWNLOADER_BASE_URL

    return value


def _tcp_probe(url: str, *, timeout_s: float = 2.0) -> bool:
    """Fast connectivity probe so smoke tests skip when the remote downloader isn't reachable."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        if not host:
            return False
        port = parsed.port
        if port is None:
            port = 443 if parsed.scheme == "https" else 80
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


@pytest.mark.downloader
def test_remote_downloader_stock_smoke(tmp_path):
    """Run a tiny stock history fetch through the shared downloader to ensure it stays healthy."""
    base_url_raw = os.environ.get("DATADOWNLOADER_BASE_URL")
    if not base_url_raw:
        pytest.skip("Remote downloader base URL not configured")

    base_url = _normalize_downloader_base_url(base_url_raw)
    if "localhost" in base_url or "127.0.0.1" in base_url:
        pytest.skip("Remote downloader base URL points at localhost")
    if not _tcp_probe(base_url):
        pytest.skip("Remote downloader base URL is not reachable")

    api_key = os.environ.get("DATADOWNLOADER_API_KEY")
    api_key_header = os.environ.get("DATADOWNLOADER_API_KEY_HEADER")
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    if not api_key:
        pytest.skip("Downloader API key not configured")
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
            # Keep this smoke test snappy even if the downloader has trouble.
            "THETADATA_QUEUE_LIST_TIMEOUT": "30",
            "THETADATA_QUEUE_HISTORY_TIMEOUT": "30",
            "THETADATA_QUEUE_DEFAULT_TIMEOUT": "30",
            # Avoid accidental `.env` discovery during the subprocess run.
            "LUMIBOT_DISABLE_DOTENV": "1",
        }
    )
    if api_key_header:
        env["DATADOWNLOADER_API_KEY_HEADER"] = api_key_header
    if env.get("PYTHONPATH"):
        env["PYTHONPATH"] = f"{REPO_ROOT}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = str(REPO_ROOT)

    script = textwrap.dedent(
        r"""
        import datetime
        import pytz
        from lumibot.entities import Asset
        from lumibot.tools import thetadata_helper

        assert thetadata_helper.REMOTE_DOWNLOADER_ENABLED, "Remote downloader flag must be set"

        asset = Asset(asset_type="stock", symbol="PLTR")
        # 09:30 ET (DST) == 13:30 UTC
        start = pytz.UTC.localize(datetime.datetime(2024, 9, 16, 13, 30))
        end = pytz.UTC.localize(datetime.datetime(2024, 9, 16, 13, 35))

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
    )

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


@pytest.mark.downloader
def test_remote_downloader_handles_long_eod_spans(tmp_path):
    """Ensure the downloader handles >365-day EOD ranges via chunking."""
    base_url_raw = os.environ.get("DATADOWNLOADER_BASE_URL")
    if not base_url_raw:
        pytest.skip("Remote downloader base URL not configured")

    base_url = _normalize_downloader_base_url(base_url_raw)
    if "localhost" in base_url or "127.0.0.1" in base_url:
        pytest.skip("Remote downloader base URL points at localhost")
    if not _tcp_probe(base_url):
        pytest.skip("Remote downloader base URL is not reachable")

    api_key = os.environ.get("DATADOWNLOADER_API_KEY")
    api_key_header = os.environ.get("DATADOWNLOADER_API_KEY_HEADER")
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    if not api_key:
        pytest.skip("Downloader API key not configured")
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
            "THETADATA_QUEUE_LIST_TIMEOUT": "60",
            "THETADATA_QUEUE_HISTORY_TIMEOUT": "60",
            "THETADATA_QUEUE_DEFAULT_TIMEOUT": "60",
            "LUMIBOT_DISABLE_DOTENV": "1",
        }
    )
    if api_key_header:
        env["DATADOWNLOADER_API_KEY_HEADER"] = api_key_header
    if env.get("PYTHONPATH"):
        env["PYTHONPATH"] = f"{REPO_ROOT}{os.pathsep}{env['PYTHONPATH']}"
    else:
        env["PYTHONPATH"] = str(REPO_ROOT)

    script = textwrap.dedent(
        r"""
        import datetime
        import pytz
        from lumibot.entities import Asset
        from lumibot.tools import thetadata_helper

        assert thetadata_helper.REMOTE_DOWNLOADER_ENABLED, "Remote downloader flag must be set"

        asset = Asset(asset_type="index", symbol="SPX")
        start = pytz.UTC.localize(datetime.datetime(2023, 1, 3))
        # Slightly > 365 days to exercise chunking without turning this into a multi-minute download.
        end = pytz.UTC.localize(datetime.datetime(2024, 2, 15, 23, 59))

        df = thetadata_helper.get_historical_eod_data(
            asset=asset,
            start_dt=start,
            end_dt=end,
            username="%s",
            password="%s",
        )

        assert df is not None and len(df) > 250, f"Expected >365-day EOD rows, got {0 if df is None else len(df)}"
        print(f"long_eod_rows={len(df)} first={df.index.min()} last={df.index.max()}")
        """
    )

    smoke_path = tmp_path / "downloader_eod_smoke.py"
    smoke_path.write_text(script % (username, password), encoding="utf-8")

    result = subprocess.run(
        [sys.executable, str(smoke_path)],
        text=True,
        capture_output=True,
        env=env,
        check=True,
        timeout=180,
    )

    assert "long_eod_rows=" in result.stdout
