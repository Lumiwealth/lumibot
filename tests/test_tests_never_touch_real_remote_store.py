"""Tests must never read or write a real remote cache.

A developer's ``.env`` can point LumiBot's remote backtest cache at a real bucket with
read/write credentials. Tests load that file, so without a guard a test that saves a
fixture through the default cache uploads it to the real bucket. ``tests/conftest.py``
switches the remote cache off for the whole test run and blocks creation of a real S3
client. A test that needs a remote store injects a fake one (``client_factory=``).

Real-service integration runs can opt out with ``LUMIBOT_TEST_ALLOW_REAL_REMOTE_CACHE=1``.
"""

import os

import pytest

from lumibot.tools import backtest_cache
from lumibot.tools.backtest_cache import BacktestCacheManager, BacktestCacheSettings, CacheMode


def _real_looking_settings() -> BacktestCacheSettings:
    return BacktestCacheSettings(
        backend="s3",
        mode=CacheMode.S3_READWRITE,
        bucket="a-real-dev-bucket",
        prefix="lumibot",
        region="us-east-1",
        access_key_id="AKIA-not-a-real-key",
        secret_access_key="not-a-real-secret",
    )


@pytest.fixture
def boto3_session_calls(monkeypatch):
    """Record any attempt to build a real boto3 session; never contact AWS."""
    boto3 = pytest.importorskip("boto3")
    calls = []

    class _RecordingSession:
        def __init__(self, *args, **kwargs):
            calls.append(kwargs)

        def client(self, *args, **kwargs):
            raise AssertionError("a real S3 client was created during a test")

    monkeypatch.setattr(boto3.session, "Session", _RecordingSession)
    return calls


def test_remote_cache_environment_is_off_for_tests():
    if os.environ.get("LUMIBOT_TEST_ALLOW_REAL_REMOTE_CACHE") == "1":
        pytest.skip("real remote cache explicitly allowed for this run")
    assert (os.environ.get("LUMIBOT_CACHE_BACKEND") or "local").lower() != "s3"
    assert (os.environ.get("LUMIBOT_CACHE_MODE") or "disabled").lower() in {"disabled", "off", "local"}
    assert not os.environ.get("LUMIBOT_CACHE_S3_BUCKET")
    assert not os.environ.get("LUMIBOT_CACHE_S3_ACCESS_KEY_ID")
    assert not os.environ.get("LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY")


def test_default_backtest_cache_is_disabled_in_tests():
    if os.environ.get("LUMIBOT_TEST_ALLOW_REAL_REMOTE_CACHE") == "1":
        pytest.skip("real remote cache explicitly allowed for this run")
    from lumibot import credentials

    assert (credentials.CACHE_REMOTE_CONFIG.get("backend") or "local").lower() != "s3"
    backtest_cache.reset_backtest_cache_manager(for_testing=True)
    try:
        assert backtest_cache.get_backtest_cache().enabled is False
    finally:
        backtest_cache.reset_backtest_cache_manager(for_testing=True)


def test_a_test_cannot_create_a_real_s3_client(boto3_session_calls, tmp_path):
    if os.environ.get("LUMIBOT_TEST_ALLOW_REAL_REMOTE_CACHE") == "1":
        pytest.skip("real remote cache explicitly allowed for this run")
    manager = BacktestCacheManager(_real_looking_settings())

    with pytest.raises(RuntimeError, match="real remote cache"):
        manager._get_client()

    # The write path must not reach a real client either.
    local = tmp_path / "file.parquet"
    local.write_bytes(b"fixture")
    try:
        manager.on_local_update(local, payload={"provider": "test"})
    except RuntimeError:
        pass
    assert boto3_session_calls == []


def test_an_injected_fake_remote_still_works(tmp_path):
    uploads = []

    class _FakeS3:
        def upload_file(self, Filename, Bucket, Key, **kwargs):  # noqa: N803 - boto3 signature
            uploads.append((Bucket, Key))

    manager = BacktestCacheManager(_real_looking_settings(), client_factory=lambda _settings: _FakeS3())
    assert isinstance(manager._get_client(), _FakeS3)
