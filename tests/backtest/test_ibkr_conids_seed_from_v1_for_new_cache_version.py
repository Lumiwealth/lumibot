from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper
from lumibot.tools.backtest_cache import CacheMode


@dataclass(frozen=True)
class _StubSettings:
    backend: str = "s3"
    bucket: str = "stub-bucket"
    prefix: str = "stub/prefix"
    version: str = "v1_cold_test"


class _StubCacheManager:
    enabled = True
    mode = CacheMode.S3_READWRITE
    _settings = _StubSettings()

    def ensure_local_file(self, local_path: Path, payload=None, force_download: bool = False) -> bool:
        # Simulate: current cache namespace does not have conids.json yet.
        return False


def test_ibkr_conids_seed_falls_back_to_v1_when_cache_version_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cache_root = tmp_path / "cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    # Patch cache folder constants used by ibkr_helper.
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", str(cache_root))

    # Use a stubbed cache manager so we don't touch real S3.
    monkeypatch.setattr(ibkr_helper, "get_backtest_cache", lambda: _StubCacheManager())

    # Seed mapping returned from the *v1* namespace.
    fut = Asset("NQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 3, 21))
    key = ibkr_helper.IbkrConidKey("future", "NQ", "", "CME", "20250321").to_key()
    seed_mapping = {key: 123}

    called = {"uploaded": 0, "lookups": 0}

    monkeypatch.setattr(ibkr_helper, "_download_remote_conids_json", lambda *args, **kwargs: dict(seed_mapping))
    monkeypatch.setattr(
        ibkr_helper,
        "_merge_upload_conids_json",
        lambda *args, **kwargs: called.__setitem__("uploaded", called["uploaded"] + 1),
    )

    def _no_remote_lookup(*args, **kwargs):
        called["lookups"] += 1
        raise AssertionError("_lookup_conid_remote should not be called when seed mapping contains the key")

    monkeypatch.setattr(ibkr_helper, "_lookup_conid_remote", _no_remote_lookup)

    conid = ibkr_helper._resolve_conid(asset=fut, quote=None, exchange="CME")
    assert conid == 123
    assert called["uploaded"] == 1
    assert called["lookups"] == 0

    conids_path = cache_root / "ibkr" / "conids.json"
    assert conids_path.exists()
    text = conids_path.read_text(encoding="utf-8")
    assert key in text
