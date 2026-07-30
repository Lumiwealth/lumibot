from __future__ import annotations

import io
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import pytest

from lumibot.tools import backtest_cache
from lumibot.tools.backtest_cache import (
    BacktestCacheManager,
    BacktestCacheSettings,
    CacheMode,
    reset_backtest_cache_manager,
)


@pytest.fixture(autouse=True)
def reset_manager():
    reset_backtest_cache_manager(for_testing=True)
    yield
    reset_backtest_cache_manager(for_testing=True)


def test_settings_from_env_disabled_when_backend_not_s3():
    config = {
        "backend": "local",
        "mode": "disabled",
    }
    assert BacktestCacheSettings.from_env(config) is None


def test_settings_from_env_requires_bucket():
    config = {
        "backend": "s3",
        "mode": "readwrite",
    }
    with pytest.raises(ValueError):
        BacktestCacheSettings.from_env(config)


class StubS3Client:
    def __init__(self, objects: Dict[Tuple[str, str], bytes] | None = None):
        self.objects = objects or {}
        self.uploads: Dict[Tuple[str, str], bytes] = {}

    def download_file(self, bucket: str, key: str, destination: str) -> None:
        lookup = (bucket, key)
        if lookup not in self.objects:
            raise FileNotFoundError(f"{bucket}/{key} missing")
        Path(destination).write_bytes(self.objects[lookup])

    def upload_file(self, source: str, bucket: str, key: str) -> None:
        self.uploads[(bucket, key)] = Path(source).read_bytes()


class PreconditionFailed(RuntimeError):
    response = {"Error": {"Code": "PreconditionFailed"}}


class ConditionalS3Client:
    def __init__(self, body: bytes, etag: str = '"v1"'):
        self.body = body
        self.etag = etag
        self.put_calls = 0

    def get_object(self, *, Bucket: str, Key: str):
        return {"Body": io.BytesIO(self.body), "ETag": self.etag}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **conditions):
        self.put_calls += 1
        if conditions.get("IfMatch") != self.etag:
            raise PreconditionFailed("stale etag")
        self.body = Body
        self.etag = f'"v{self.put_calls + 1}"'
        return {"ETag": self.etag}


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    frame.to_parquet(output)
    return output.getvalue()


def _daily_frame(day: str, close: float, *, missing: bool = False) -> pd.DataFrame:
    return pd.DataFrame(
        {"close": [close], "missing": [missing]},
        index=pd.DatetimeIndex([pd.Timestamp(f"{day} 16:00", tz="America/New_York")]),
    )


def _build_settings(prefix: str = "prod/cache") -> BacktestCacheSettings:
    return BacktestCacheSettings(
        backend="s3",
        mode=CacheMode.S3_READWRITE,
        bucket="test-bucket",
        prefix=prefix,
        region="us-east-1",
        version="v3",
    )


def test_remote_key_uses_relative_cache_path(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    local_file = cache_root / "thetadata" / "stock" / "minute" / "ohlc" / "stock_SPY_minute_ohlc.parquet"
    local_file.parent.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", cache_root)

    settings = _build_settings(prefix="stage/cache")
    manager = BacktestCacheManager(settings, client_factory=lambda settings: StubS3Client())

    remote_key = manager.remote_key_for(local_file)
    assert remote_key == "stage/cache/v3/thetadata/stock/minute/ohlc/stock_SPY_minute_ohlc.parquet"


def test_ensure_local_file_downloads_from_s3(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    local_file = cache_root / "thetadata" / "stock" / "minute" / "ohlc" / "stock_SPY_minute_ohlc.parquet"

    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", cache_root)

    remote_key = "stage/cache/v3/thetadata/stock/minute/ohlc/stock_SPY_minute_ohlc.parquet"
    objects = {("test-bucket", remote_key): b"cached-data"}

    stub = StubS3Client(objects)
    manager = BacktestCacheManager(_build_settings(prefix="stage/cache"), client_factory=lambda s: stub)

    fetched = manager.ensure_local_file(local_file)
    assert fetched is True
    assert local_file.exists()
    assert local_file.read_bytes() == b"cached-data"


def test_ensure_local_file_handles_missing_remote(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    local_file = cache_root / "thetadata" / "stock" / "minute" / "ohlc" / "stock_SPY_minute_ohlc.parquet"

    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", cache_root)

    stub = StubS3Client()
    manager = BacktestCacheManager(_build_settings(prefix="stage/cache"), client_factory=lambda s: stub)

    fetched = manager.ensure_local_file(local_file)
    assert fetched is False
    assert not local_file.exists()


def test_on_local_update_uploads_file(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    local_file = cache_root / "thetadata" / "stock" / "minute" / "ohlc" / "stock_SPY_minute_ohlc.parquet"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(b"new-data")

    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", cache_root)

    remote_key = "stage/cache/v3/thetadata/stock/minute/ohlc/stock_SPY_minute_ohlc.parquet"
    stub = StubS3Client({("test-bucket", remote_key): b"old"})
    manager = BacktestCacheManager(_build_settings(prefix="stage/cache"), client_factory=lambda s: stub)

    uploaded = manager.on_local_update(local_file)
    assert uploaded is True
    assert stub.uploads[( "test-bucket", remote_key)] == b"new-data"


def test_manager_disabled_skip_upload(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache"
    cache_root.mkdir()
    local_file = cache_root / "foo.parquet"
    local_file.write_bytes(b"noop")

    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", cache_root)

    disabled_settings = BacktestCacheSettings(
        backend="local",
        mode=CacheMode.DISABLED,
    )
    manager = BacktestCacheManager(disabled_settings)

    assert manager.ensure_local_file(local_file) is False
    assert manager.on_local_update(local_file) is False


def test_ibkr_conditional_upload_merges_remote_change_instead_of_losing_it(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache"
    local_file = cache_root / "ibkr" / "stock" / "day" / "QQQ.parquet"
    local_file.parent.mkdir(parents=True)
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", cache_root)

    client = ConditionalS3Client(_parquet_bytes(_daily_frame("2026-07-27", 100.0)))
    manager = BacktestCacheManager(
        _build_settings(prefix="stage/cache"),
        client_factory=lambda _settings: client,
    )
    assert manager.ensure_local_file(local_file, payload={"provider": "ibkr"}) is True

    remote_concurrent = pd.concat(
        [
            _daily_frame("2026-07-27", 100.0),
            _daily_frame("2026-07-28", 101.0),
        ]
    )
    client.body = _parquet_bytes(remote_concurrent)
    client.etag = '"concurrent"'

    local_update = pd.concat(
        [
            _daily_frame("2026-07-27", 100.0),
            _daily_frame("2026-07-29", 102.0),
        ]
    )
    local_update.to_parquet(local_file)

    assert manager.on_local_update(local_file, payload={"provider": "ibkr"}) is True

    merged = pd.read_parquet(io.BytesIO(client.body))
    assert [ts.date().isoformat() for ts in merged.index] == [
        "2026-07-27",
        "2026-07-28",
        "2026-07-29",
    ]


def test_ibkr_conflict_merge_keeps_remote_real_bar_over_local_placeholder(tmp_path, monkeypatch):
    cache_root = tmp_path / "cache"
    local_file = cache_root / "ibkr" / "stock" / "day" / "QQQ.parquet"
    local_file.parent.mkdir(parents=True)
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", cache_root)

    client = ConditionalS3Client(_parquet_bytes(_daily_frame("2026-07-27", 100.0)))
    manager = BacktestCacheManager(
        _build_settings(prefix="stage/cache"),
        client_factory=lambda _settings: client,
    )
    assert manager.ensure_local_file(local_file, payload={"provider": "ibkr"}) is True

    client.etag = '"concurrent"'
    _daily_frame("2026-07-27", float("nan"), missing=True).to_parquet(local_file)

    assert manager.on_local_update(local_file, payload={"provider": "ibkr"}) is True
    merged = pd.read_parquet(io.BytesIO(client.body))
    assert bool(merged.iloc[0]["missing"]) is False
    assert merged.iloc[0]["close"] == 100.0
