from datetime import date, datetime
from pathlib import Path

import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper as helper


class _DummyDisabledCache:
    @property
    def enabled(self):  # pragma: no cover - accessor for clarity
        return False

    def remote_key_for(self, _path):  # pragma: no cover
        raise AssertionError("remote_key_for should not be called when disabled")


class _DummyActiveCache:
    def __init__(self, remote_key: str = "remote/cache.parquet"):
        self._enabled = True
        self.remote_key = remote_key
        self.ensure_calls = []
        self.paths = []

    @property
    def enabled(self):
        return self._enabled

    def remote_key_for(self, path: Path):
        self.paths.append(path)
        return self.remote_key

    def ensure_local_file(self, cache_file, lambda_payload, force_download, invoke_lambda_on_hit=False):
        self.ensure_calls.append(
            {
                "cache_file": cache_file,
                "payload": lambda_payload,
                "force_download": force_download,
                "invoke_lambda_on_hit": invoke_lambda_on_hit,
            }
        )
        return True


def test_remote_cache_disabled_returns_false(monkeypatch, tmp_path):
    monkeypatch.setattr(helper, "get_backtest_cache", lambda: _DummyDisabledCache())
    cache_file = tmp_path / "dummy.parquet"
    asset = Asset(symbol="AAPL")

    result = helper._ensure_remote_cache_file(
        cache_file,
        asset,
        datetime(2024, 1, 1),
        datetime(2024, 1, 2),
        "minute",
        "ohlc",
        True,
    )

    assert result is False


def test_remote_cache_payload_includes_asset_details(monkeypatch, tmp_path):
    cache = _DummyActiveCache(remote_key="prefix/thetadata/file.parquet")
    monkeypatch.setattr(helper, "get_backtest_cache", lambda: cache)

    cache_file = tmp_path / "thetadata" / "file.parquet"
    asset = Asset(symbol="AAPL", asset_type="stock")
    quote_asset = Asset(symbol="USD", asset_type="forex")

    result = helper._ensure_remote_cache_file(
        cache_file,
        asset,
        datetime(2024, 1, 1),
        datetime(2024, 1, 2),
        "minute",
        "ohlc",
        include_after_hours=True,
        quote_asset=quote_asset,
    )

    assert result is True
    assert cache.ensure_calls, "ensure_local_file should be invoked"

    call = cache.ensure_calls[0]
    payload = call["payload"]

    assert payload["provider"] == "thetadata"
    assert payload["cache_key"] == "prefix/thetadata/file.parquet"
    assert payload["asset"]["symbol"] == "AAPL"
    assert payload["asset"]["asset_type"] == "stock"
    assert payload["quote_asset"]["symbol"] == "USD"
    assert payload["quote_asset"]["asset_type"] == "forex"
    assert payload["start"].endswith("+00:00")
    assert payload["end"].endswith("+00:00")
    assert payload["include_after_hours"] is True


def test_get_price_data_invokes_remote_for_missing_cache(monkeypatch, tmp_path):
    cache_path = tmp_path / "thetadata" / "cache.parquet"
    asset = Asset(symbol="AAPL", asset_type="stock")
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 2)

    monkeypatch.setattr(helper, "build_cache_filename", lambda *args, **kwargs: cache_path)
    remote_calls = []

    def fake_remote(cache_file, *args, force_download=False, **kwargs):
        remote_calls.append({
            "cache_file": cache_file,
            "force_download": force_download,
            "start": args[1],
            "end": args[2],
        })
        return False

    monkeypatch.setattr(helper, "_ensure_remote_cache_file", fake_remote)
    monkeypatch.setattr(helper, "load_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(helper, "get_missing_dates", lambda *_args, **_kwargs: [])

    helper.get_price_data("user", "pwd", asset, start, end, "minute")

    assert len(remote_calls) == 1
    assert remote_calls[0]["cache_file"] == cache_path
    assert remote_calls[0]["force_download"] is False


def test_get_price_data_remote_refresh_on_missing_dates(monkeypatch, tmp_path):
    cache_path = tmp_path / "thetadata" / "cache.parquet"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.touch()

    asset = Asset(symbol="AAPL", asset_type="stock")
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 3)

    monkeypatch.setattr(helper, "build_cache_filename", lambda *args, **kwargs: cache_path)

    remote_calls = []

    def fake_remote(cache_file, *args, force_download=False, **kwargs):
        remote_calls.append(force_download)
        return force_download

    monkeypatch.setattr(helper, "_ensure_remote_cache_file", fake_remote)

    df = pd.DataFrame(
        {
            "datetime": pd.date_range(start="2024-01-01", periods=2, freq="min", tz="UTC"),
            "close": [1.0, 2.0],
        }
    ).set_index("datetime")

    monkeypatch.setattr(helper, "load_cache", lambda *_args, **_kwargs: df)

    call_count = {"count": 0}

    def fake_missing_dates(df_all, *_args):
        call_count["count"] += 1
        if call_count["count"] == 1:
            return [date(2024, 1, 2)]
        return []

    monkeypatch.setattr(helper, "get_missing_dates", fake_missing_dates)

    result = helper.get_price_data("user", "pwd", asset, start, end, "minute")

    assert result is not None
    assert remote_calls == [True]
