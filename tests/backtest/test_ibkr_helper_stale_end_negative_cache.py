from __future__ import annotations

import shutil
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset


def test_ibkr_stale_end_bounds_retries_without_persisting_inferred_absence(monkeypatch):
    import lumibot.tools.backtest_cache as backtest_cache
    import lumibot.tools.ibkr_helper as ibkr_helper

    # Keep all artifacts under the repo tree (no /tmp writes).
    cache_root = Path(__file__).resolve().parent / "_tmp_ibkr_cache" / uuid.uuid4().hex
    cache_root.mkdir(parents=True, exist_ok=True)

    try:
        # Disable remote cache; this is a unit test for local parquet behavior.
        monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
        monkeypatch.setenv("LUMIBOT_CACHE_MODE", "disabled")
        monkeypatch.setattr(
            backtest_cache,
            "CACHE_REMOTE_CONFIG",
            {"backend": "local", "mode": "disabled"},
        )
        backtest_cache.reset_backtest_cache_manager(for_testing=True)

        # Patch module-level cache root constants (ibkr_helper imports by value).
        monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", str(cache_root))
        monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(cache_root))
        monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
        monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {})

        # Avoid any contract resolution/network calls.
        monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda *args, **kwargs: 123)

        asset = Asset("GC", asset_type=Asset.AssetType.FUTURE, expiration=date(2026, 2, 25), multiplier=100)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

        start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 19, 18, 0))
        last_bar = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 19, 19, 0))
        end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 19, 23, 0))

        timestep = "60minute"
        exchange = "COMEX"
        source = "Trades"

        cache_file = ibkr_helper._cache_file_for(  # type: ignore[attr-defined]
            asset=asset,
            quote=quote,
            timestep=timestep,
            exchange=exchange,
            source=source,
            include_after_hours=True,
        )

        df_seed = pd.DataFrame(
            {
                "open": [100.0, 100.0],
                "high": [101.0, 101.0],
                "low": [99.0, 99.0],
                "close": [100.0, 100.0],
                "volume": [1000, 1000],
                "missing": [False, False],
            },
            index=pd.DatetimeIndex([start, last_bar]),
        )
        ibkr_helper._write_cache_frame(cache_file, df_seed)  # type: ignore[attr-defined]

        calls: list[dict] = []
        provider_has_tail = False

        def fake_queue_request(*, url, querystring, headers=None, timeout=None):
            calls.append({"url": url, "querystring": dict(querystring or {})})
            available = [start, last_bar, end] if provider_has_tail else [last_bar]
            return {
                "data": [
                    {"t": int(pd.Timestamp(ts).value // 1_000_000),
                     "o": 100.0, "h": 101.0, "l": 99.0, "c": 100.0, "v": 1000}
                    for ts in available
                ]
            }

        monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

        df1 = ibkr_helper.get_price_data(
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start,
            end_dt=end,
            exchange=exchange,
            include_after_hours=True,
            source=source,
        )
        history_calls = [c for c in calls if "/ibkr/iserver/marketdata/history" in c["url"]]
        assert len(history_calls) == 1
        # Underfilled windows should not synthesize an empty dataset. Return the real cached bars;
        # the downloader is responsible for rejecting non-cacheable/partial IBKR payloads.
        assert not df1.empty
        assert list(df1.index) == [start, last_bar]
        cached_mid = pd.read_parquet(cache_file)
        assert last_bar in cached_mid.index
        assert end not in cached_mid.index
        assert not cached_mid["missing"].any()

        # Same-process attempted-segment tracking bounds identical requests without
        # converting an underfilled response into a persisted no-data verdict.
        df2 = ibkr_helper.get_price_data(
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start,
            end_dt=end,
            exchange=exchange,
            include_after_hours=True,
            source=source,
        )
        history_calls = [c for c in calls if "/ibkr/iserver/marketdata/history" in c["url"]]
        assert len(history_calls) == 1
        assert not df2.empty
        assert list(df2.index) == [start, last_bar]

        cached = pd.read_parquet(cache_file)
        assert last_bar in cached.index
        assert bool(cached.loc[last_bar, "missing"]) is False
        assert end not in cached.index
        assert not cached["missing"].any()

        # A subsequent worker must be able to obtain a bar that the earlier
        # response omitted. This models the actual warmer restart boundary.
        monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {})
        monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
        provider_has_tail = True
        repaired = ibkr_helper.get_price_data(
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=start,
            end_dt=end,
            exchange=exchange,
            include_after_hours=True,
            source=source,
        )
        history_calls = [c for c in calls if "/ibkr/iserver/marketdata/history" in c["url"]]
        assert len(history_calls) == 2
        assert list(repaired.index) == [start, last_bar, end]
        cached_repaired = pd.read_parquet(cache_file)
        assert bool(cached_repaired.loc[end, "missing"]) is False
        assert not cached_repaired["missing"].any()
    finally:
        shutil.rmtree(cache_root, ignore_errors=True)


def test_legacy_expired_or_ambiguous_placeholders_do_not_suppress_retry() -> None:
    import lumibot.tools.ibkr_helper as ibkr_helper

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 3, 9, 30))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 3, 16, 0))
    now = datetime(2026, 2, 4, tzinfo=timezone.utc)

    legacy = pd.DataFrame(
        {"missing": [True, True]},
        index=pd.DatetimeIndex([start, end]),
    )
    assert not ibkr_helper._window_is_placeholder_covered(
        legacy, start_local=start, end_local=end, now=now
    )

    expired = legacy.assign(
        missing_retry_after=[
            datetime(2026, 2, 3, tzinfo=timezone.utc).isoformat(),
            datetime(2026, 2, 3, tzinfo=timezone.utc).isoformat(),
        ],
        missing_outcome=["confirmed_no_data", "confirmed_no_data"],
    )
    assert not ibkr_helper._window_is_placeholder_covered(
        expired, start_local=start, end_local=end, now=now
    )

    ambiguous = expired.assign(
        missing_retry_after=[
            datetime(2026, 2, 5, tzinfo=timezone.utc).isoformat(),
            datetime(2026, 2, 5, tzinfo=timezone.utc).isoformat(),
        ],
        missing_outcome=["partial", "partial"],
    )
    assert not ibkr_helper._window_is_placeholder_covered(
        ambiguous, start_local=start, end_local=end, now=now
    )


def test_ibkr_placeholder_window_suppresses_subwindow_refetch_after_restart(monkeypatch):
    import lumibot.tools.backtest_cache as backtest_cache
    import lumibot.tools.ibkr_helper as ibkr_helper

    cache_root = Path(__file__).resolve().parent / "_tmp_ibkr_cache" / uuid.uuid4().hex
    cache_root.mkdir(parents=True, exist_ok=True)

    try:
        monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
        monkeypatch.setenv("LUMIBOT_CACHE_MODE", "disabled")
        monkeypatch.setattr(
            backtest_cache,
            "CACHE_REMOTE_CONFIG",
            {"backend": "local", "mode": "disabled"},
        )
        backtest_cache.reset_backtest_cache_manager(for_testing=True)

        monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", str(cache_root))
        monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(cache_root))
        # Simulate a fresh process where only persisted parquet markers exist.
        monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})

        monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda *args, **kwargs: 123)

        asset = Asset("RAPT", asset_type=Asset.AssetType.STOCK)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

        missing_start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 3, 9, 30))
        missing_end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 3, 16, 0))
        req_start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 3, 11, 0))
        req_end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 3, 12, 0))

        timestep = "1minute"
        source = "Trades"

        cache_file = ibkr_helper._cache_file_for(  # type: ignore[attr-defined]
            asset=asset,
            quote=quote,
            timestep=timestep,
            exchange=None,
            source=source,
            include_after_hours=True,
        )

        df_seed = pd.DataFrame(
            {
                "open": [pd.NA, pd.NA],
                "high": [pd.NA, pd.NA],
                "low": [pd.NA, pd.NA],
                "close": [pd.NA, pd.NA],
                "volume": [pd.NA, pd.NA],
                "missing": [True, True],
                "missing_retry_after": [
                    (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat(),
                    (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat(),
                ],
                "missing_reason": ["confirmed_no_data", "confirmed_no_data"],
                "missing_outcome": ["confirmed_no_data", "confirmed_no_data"],
            },
            index=pd.DatetimeIndex([missing_start, missing_end]),
        )
        ibkr_helper._write_cache_frame(cache_file, df_seed)  # type: ignore[attr-defined]

        calls: list[dict] = []

        def fake_queue_request(*, url, querystring, headers=None, timeout=None):
            calls.append({"url": url, "querystring": dict(querystring or {})})
            return {"error": "unexpected network request"}

        monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

        frame = ibkr_helper.get_price_data(
            asset=asset,
            quote=quote,
            timestep=timestep,
            start_dt=req_start,
            end_dt=req_end,
            exchange=None,
            include_after_hours=True,
            source=source,
        )

        history_calls = [c for c in calls if "/ibkr/iserver/marketdata/history" in c["url"]]
        assert len(history_calls) == 0
        assert frame.empty

        cached = pd.read_parquet(cache_file)
        assert missing_start in cached.index
        assert missing_end in cached.index
        assert bool(cached.loc[missing_start, "missing"]) is True
        assert bool(cached.loc[missing_end, "missing"]) is True
    finally:
        shutil.rmtree(cache_root, ignore_errors=True)
