from datetime import date

import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_get_theta_splits_memoizes_in_memory(monkeypatch):
    calls = {"count": 0}

    def fake_ensure_event_cache(asset, event_type, start_date, end_date, username=None, password=None):
        calls["count"] += 1
        assert event_type == "splits"
        assert asset.symbol == "NVDA"
        assert start_date <= end_date
        return pd.DataFrame(
            {
                "event_date": [pd.Timestamp("2024-06-07")],
                "ratio": [10.0],
            }
        )

    monkeypatch.setattr(thetadata_helper, "_ensure_event_cache", fake_ensure_event_cache)
    thetadata_helper._event_cache_memory.clear()

    asset = Asset("NVDA", asset_type="stock")

    # First call populates the in-memory cache.
    df1 = thetadata_helper._get_theta_splits(asset, date(2013, 1, 1), date(2026, 1, 1))
    assert not df1.empty

    # Second call is fully covered by the cached window and must not call _ensure_event_cache again.
    df2 = thetadata_helper._get_theta_splits(asset, date(2014, 1, 1), date(2025, 1, 1))
    assert not df2.empty

    assert calls["count"] == 1


def test_get_theta_splits_failure_ttl_suppresses_retry_storm(monkeypatch):
    calls = {"count": 0}

    def fake_ensure_event_cache(asset, event_type, start_date, end_date, username=None, password=None):
        calls["count"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(thetadata_helper, "_ensure_event_cache", fake_ensure_event_cache)
    thetadata_helper._event_cache_memory.clear()
    monkeypatch.setenv("THETADATA_EVENT_CACHE_FAILURE_TTL_S", "60")

    asset = Asset("NVDA", asset_type="stock")

    df1 = thetadata_helper._get_theta_splits(asset, date(2020, 1, 1), date(2020, 1, 2))
    df2 = thetadata_helper._get_theta_splits(asset, date(2020, 1, 1), date(2020, 1, 2))

    assert df1.empty
    assert df2.empty
    assert calls["count"] == 1

