"""Optional read-only fallback cache: read misses fall back, writes never do."""
from __future__ import annotations

import io
from pathlib import Path
from typing import Dict, Tuple

import pytest

from lumibot.tools import backtest_cache
from lumibot.tools.backtest_cache import (
    BacktestCacheManager,
    BacktestCacheSettings,
    CacheMode,
    reset_backtest_cache_manager,
)


class NotFound(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class Denied(Exception):
    response = {"Error": {"Code": "AccessDenied"}}


class FakeS3:
    """Minimal S3 double: objects keyed by (bucket, key); records every write."""

    def __init__(self, objects: Dict[Tuple[str, str], bytes] | None = None, deny_buckets=()):
        self.objects = dict(objects or {})
        self.deny_buckets = set(deny_buckets)
        self.reads = []
        self.writes = []

    def get_object(self, *, Bucket, Key, **_):
        self.reads.append((Bucket, Key))
        if Bucket in self.deny_buckets:
            raise Denied("denied")
        if (Bucket, Key) not in self.objects:
            raise NotFound("missing")
        body = self.objects[(Bucket, Key)]
        return {"Body": io.BytesIO(body), "ETag": '"etag-' + Bucket + '"', "ContentLength": len(body)}

    def download_file(self, bucket, key, destination, **_):
        self.reads.append((bucket, key))
        if bucket in self.deny_buckets:
            raise Denied("denied")
        if (bucket, key) not in self.objects:
            raise NotFound("missing")
        Path(destination).write_bytes(self.objects[(bucket, key)])

    def put_object(self, *, Bucket, Key, Body, **_):
        self.writes.append((Bucket, Key))
        self.objects[(Bucket, Key)] = Body
        return {"ETag": '"new"'}

    def upload_file(self, source, bucket, key, **_):
        self.writes.append((bucket, key))
        self.objects[(bucket, key)] = Path(source).read_bytes()


@pytest.fixture(autouse=True)
def reset_manager():
    reset_backtest_cache_manager(for_testing=True)
    yield
    reset_backtest_cache_manager(for_testing=True)


def _settings(**overrides) -> BacktestCacheSettings:
    base = dict(
        backend="s3", mode=CacheMode.S3_READWRITE, bucket="dev-bucket", prefix="dev/cache",
        region="us-east-1", version="v9",
        fallback_bucket="prod-bucket", fallback_prefix="prod/cache",
    )
    base.update(overrides)
    return BacktestCacheSettings(**base)


REL = "bars/v1/ibkr/aa/abc.parquet"
DEV_KEY = f"dev/cache/v9/{REL}"
PROD_KEY = f"prod/cache/v9/{REL}"


def _manager(client, **overrides):
    return BacktestCacheManager(_settings(**overrides), client_factory=lambda s: client)


def test_from_env_reads_fallback_and_off_switch():
    env = {"backend": "s3", "mode": "readwrite", "s3_bucket": "b", "s3_prefix": "dev/cache", "s3_version": "v9",
           "fallback_s3_bucket": "p", "fallback_s3_prefix": "prod/cache"}
    settings = BacktestCacheSettings.from_env(env)
    assert (settings.fallback_bucket, settings.fallback_prefix, settings.fallback_version) == ("p", "prod/cache", None)
    assert BacktestCacheSettings.from_env({**env, "fallback_mode": "off"}).fallback_bucket is None
    assert BacktestCacheSettings.from_env({k: v for k, v in env.items() if "fallback" not in k}).fallback_bucket is None


def test_miss_in_primary_is_served_from_fallback_read_only(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = FakeS3({("prod-bucket", PROD_KEY): b"prod-bytes"})
    manager = _manager(fake)
    local = tmp_path / REL
    assert manager.ensure_local_file(local) is True
    assert local.read_bytes() == b"prod-bytes"
    assert fake.writes == []
    assert manager.stats_snapshot()["fallback_hits"] == 1.0


def test_primary_hit_never_touches_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = FakeS3({("dev-bucket", DEV_KEY): b"dev", ("prod-bucket", PROD_KEY): b"prod"})
    manager = _manager(fake)
    manager.ensure_local_file(tmp_path / REL)
    assert (tmp_path / REL).read_bytes() == b"dev"
    assert all(bucket == "dev-bucket" for bucket, _ in fake.reads)


def test_miss_everywhere_is_still_a_plain_miss(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    manager = _manager(FakeS3())
    assert manager.ensure_local_file(tmp_path / REL) is False
    assert not (tmp_path / REL).exists()


def test_uploads_only_go_to_primary(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = FakeS3({("prod-bucket", PROD_KEY): b"prod"})
    manager = _manager(fake)
    local = tmp_path / REL
    manager.ensure_local_file(local)
    local.write_bytes(b"newer")
    manager.on_local_update(local)
    assert fake.writes == [("dev-bucket", DEV_KEY)]
    assert fake.objects[("prod-bucket", PROD_KEY)] == b"prod"


def test_fallback_etag_is_not_reused_as_a_primary_condition(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = FakeS3({("prod-bucket", PROD_KEY): b"prod"})
    manager = _manager(fake)
    response = manager._get_client().get_object(Bucket="dev-bucket", Key=DEV_KEY)
    assert response["Body"].read() == b"prod"
    assert not response.get("ETag")


def test_unreadable_fallback_degrades_to_a_miss_and_stops_trying(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = FakeS3(deny_buckets={"prod-bucket"})
    manager = _manager(fake)
    assert manager.ensure_local_file(tmp_path / "a" / REL) is False
    assert manager.ensure_local_file(tmp_path / "b" / REL) is False
    assert [r for r in fake.reads if r[0] == "prod-bucket"] == [("prod-bucket", f"prod/cache/v9/a/{REL}")]


def test_fallback_version_can_differ(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = FakeS3({("prod-bucket", f"prod/cache/v44/{REL}"): b"prod44"})
    manager = _manager(fake, fallback_version="v44")
    manager.ensure_local_file(tmp_path / REL)
    assert (tmp_path / REL).read_bytes() == b"prod44"


def test_no_fallback_configured_keeps_old_behaviour(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = FakeS3({("prod-bucket", PROD_KEY): b"prod"})
    manager = _manager(fake, fallback_bucket=None, fallback_prefix="")
    assert manager.ensure_local_file(tmp_path / REL) is False
    assert fake.reads == [("dev-bucket", DEV_KEY)]


# --- fallback_mode="prefer_larger": a fuller fallback object beats a thinner primary one ---------


class HeadS3(FakeS3):
    def head_object(self, *, Bucket, Key, **_):
        if Bucket in self.deny_buckets:
            raise Denied("denied")
        if (Bucket, Key) not in self.objects:
            raise NotFound("missing")
        return {"ContentLength": len(self.objects[(Bucket, Key)])}


def test_from_env_reads_prefer_larger_mode():
    env = {"backend": "s3", "mode": "readwrite", "s3_bucket": "b", "s3_prefix": "dev/cache", "s3_version": "v9",
           "fallback_s3_bucket": "p", "fallback_s3_prefix": "prod/cache"}
    assert BacktestCacheSettings.from_env(env).fallback_prefer_larger is False
    preferred = BacktestCacheSettings.from_env({**env, "fallback_mode": "prefer_larger"})
    assert preferred.fallback_bucket == "p" and preferred.fallback_prefer_larger is True


def test_prefer_larger_serves_the_fuller_fallback_object_and_never_writes_it(tmp_path, monkeypatch):
    # A cache that is filled only by occasional runs can hold a short copy of a series (few
    # months, holes) under the same name as a complete copy in the fallback. Reading the short
    # one sends the run to the provider for the rest.
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = HeadS3({("dev-bucket", DEV_KEY): b"short", ("prod-bucket", PROD_KEY): b"complete-two-year-series"})
    manager = _manager(fake, fallback_prefer_larger=True)
    local = tmp_path / REL
    assert manager.ensure_local_file(local) is True
    assert local.read_bytes() == b"complete-two-year-series"
    assert fake.writes == []
    assert manager.stats_snapshot()["fallback_hits"] == 1.0


def test_prefer_larger_keeps_the_primary_when_it_is_as_large_or_larger(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = HeadS3({("dev-bucket", DEV_KEY): b"dev-is-the-longer-copy", ("prod-bucket", PROD_KEY): b"prod"})
    manager = _manager(fake, fallback_prefer_larger=True)
    manager.ensure_local_file(tmp_path / REL)
    assert (tmp_path / REL).read_bytes() == b"dev-is-the-longer-copy"
    assert manager.stats_snapshot()["fallback_hits"] == 0.0


def test_default_mode_still_reads_a_present_primary_even_when_the_fallback_is_larger(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = HeadS3({("dev-bucket", DEV_KEY): b"short", ("prod-bucket", PROD_KEY): b"complete-two-year-series"})
    manager = _manager(fake)
    manager.ensure_local_file(tmp_path / REL)
    assert (tmp_path / REL).read_bytes() == b"short"


def test_prefer_larger_with_an_unreadable_fallback_reads_the_primary(tmp_path, monkeypatch):
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", tmp_path)
    fake = HeadS3({("dev-bucket", DEV_KEY): b"short"}, deny_buckets={"prod-bucket"})
    manager = _manager(fake, fallback_prefer_larger=True)
    manager.ensure_local_file(tmp_path / REL)
    assert (tmp_path / REL).read_bytes() == b"short"
    assert fake.writes == []
