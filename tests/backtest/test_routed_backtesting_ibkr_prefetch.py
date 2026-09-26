from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset


def _minute_ohlc(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    start_ts = pd.Timestamp(start_dt)
    end_ts = pd.Timestamp(end_dt)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    start_ts = start_ts.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    end_ts = end_ts.tz_convert(LUMIBOT_DEFAULT_PYTZ)

    idx = pd.date_range(start_ts, end_ts, freq="1min")
    px = (pd.Series(range(len(idx)), index=idx, dtype="float64") * 0.01) + 100.0
    return pd.DataFrame(
        {
            "open": px,
            "high": px + 0.01,
            "low": px - 0.01,
            "close": px,
            "volume": 1000,
        },
        index=idx,
    )


def _multi_minute_ohlc(start_dt: datetime, end_dt: datetime, *, minutes: int) -> pd.DataFrame:
    start_ts = pd.Timestamp(start_dt)
    end_ts = pd.Timestamp(end_dt)
    if start_ts.tzinfo is None:
        start_ts = start_ts.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    if end_ts.tzinfo is None:
        end_ts = end_ts.tz_localize(LUMIBOT_DEFAULT_PYTZ)
    start_ts = start_ts.tz_convert(LUMIBOT_DEFAULT_PYTZ)
    end_ts = end_ts.tz_convert(LUMIBOT_DEFAULT_PYTZ)

    idx = pd.date_range(start_ts, end_ts, freq=f"{int(minutes)}min")
    px = (pd.Series(range(len(idx)), index=idx, dtype="float64") * 0.1) + 1000.0
    return pd.DataFrame(
        {
            "open": px,
            "high": px + 0.1,
            "low": px - 0.1,
            "close": px,
            "volume": 1000,
        },
        index=idx,
    )


def test_router_ibkr_prefetches_full_window_once_for_cont_future_minute(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    # Avoid any local ThetaTerminal side effects during datasource init.
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 2, 0))

    router = RoutedBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
        config={
            "backtesting_data_routing": {
                "default": "thetadata",
                "future": "ibkr",
                "cont_future": "ibkr",
                "crypto": "ibkr",
            }
        },
    )
    router.load_data()

    asset = Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    calls: list[dict] = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append(
            {
                "asset": asset,
                "quote": quote,
                "timestep": timestep,
                "start_dt": start_dt,
                "end_dt": end_dt,
                "exchange": exchange,
                "include_after_hours": include_after_hours,
                "source": source,
            }
        )
        return _minute_ohlc(start_dt, end_dt)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    # First access should prefetch the full backtest window once.
    router._datetime = start
    _ = router.get_historical_prices(asset, length=80, timestep="minute", quote=quote)

    # Subsequent accesses must slice in-memory without calling the underlying fetch again.
    router._datetime = start + timedelta(minutes=30)
    _ = router.get_historical_prices(asset, length=80, timestep="minute", quote=quote)

    router._datetime = start + timedelta(minutes=90)
    _ = router.get_historical_prices(asset, length=80, timestep="minute", quote=quote)

    assert len(calls) == 1, f"Expected a single IBKR prefetch call, got {len(calls)}"

    first = calls[0]
    assert first["timestep"] == "minute"
    assert first["end_dt"] == router.datetime_end
    assert first["start_dt"] <= router.datetime_start


def test_router_ibkr_prefetches_full_window_once_for_cont_future_multi_minute(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 6, 0, 0))

    router = RoutedBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
        config={"backtesting_data_routing": {"default": "thetadata", "cont_future": "ibkr"}},
    )
    router.load_data()

    asset = Asset("GC", asset_type=Asset.AssetType.CONT_FUTURE)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    calls: list[dict] = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append({"timestep": timestep, "start_dt": start_dt, "end_dt": end_dt})
        assert timestep == "60minute"
        return _multi_minute_ohlc(start_dt, end_dt, minutes=60)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    router._datetime = start + timedelta(hours=10)
    bars = router.get_historical_prices(asset, length=5, timestep="60m", quote=quote)
    assert bars is not None
    assert getattr(bars, "df", None) is not None
    assert len(bars.df) == 5

    router._datetime = start + timedelta(hours=14)
    _ = router.get_historical_prices(asset, length=5, timestep="60m", quote=quote)

    assert len(calls) == 1, f"Expected a single IBKR prefetch call, got {len(calls)}"

    canonical_key = (asset, quote, "60minute")
    data_obj = router._data_store.get(canonical_key)
    assert data_obj is not None
    assert getattr(data_obj, "_native_timestep_quantity", None) == 60
    assert getattr(data_obj, "_native_timestep_unit", None) == "minute"


def test_router_ibkr_prefetch_slices_expected_window(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 2, 0))

    router = RoutedBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
        config={"backtesting_data_routing": {"default": "thetadata", "cont_future": "ibkr"}},
    )
    router.load_data()

    asset = Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        return _minute_ohlc(start_dt, end_dt)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    router._datetime = start + timedelta(minutes=75)
    bars = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)
    assert bars is not None
    df = getattr(bars, "df", None)
    assert df is not None and not df.empty
    assert len(df) == 5

    last_ts = df.index.max()
    assert last_ts <= pd.Timestamp(router.get_datetime())


def test_router_ibkr_prefetches_full_window_once_for_crypto_minute(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 2, 0))

    router = RoutedBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
        config={"backtesting_data_routing": {"default": "thetadata", "crypto": "ibkr"}},
    )
    router.load_data()

    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    calls: list[dict] = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        calls.append({"timestep": timestep, "start_dt": start_dt, "end_dt": end_dt})
        return _minute_ohlc(start_dt, end_dt)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    router._datetime = start + timedelta(minutes=30)
    _ = router.get_historical_prices(asset, length=10, timestep="minute", quote=quote)
    router._datetime = start + timedelta(minutes=90)
    _ = router.get_historical_prices(asset, length=10, timestep="minute", quote=quote)

    assert len(calls) == 1
    assert calls[0]["end_dt"] == router.datetime_end
    assert calls[0]["start_dt"] <= router.datetime_start


# ---------------------------------------------------------------------------
# Empty-prefetch short-circuit tests (routed IBKR path).
#
# Bug: when the full-window prefetch returned an empty frame (e.g. TQQQ minute
# when IBKR has only daily cached), the canonical key was NOT marked, so every
# subsequent iteration re-ran the full prefetch and burned tens of thousands of
# redundant parquet reads. The fix adds `_empty_prefetch_series` alongside
# `_fully_loaded_series`, marked on the empty path and checked at the top of
# `update_pandas_data` to short-circuit.
# ---------------------------------------------------------------------------


def _make_router(start, end, routing):
    router = RoutedBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
        config={"backtesting_data_routing": routing},
    )
    router.load_data()
    return router


def test_empty_prefetch_short_circuits_stock_minute(monkeypatch):
    """Stock/minute: empty first prefetch must be cached; no further fetches."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 2, 0))
    router = _make_router(start, end, {"default": "thetadata", "stock": "ibkr"})

    asset = Asset("TQQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    calls: list[dict] = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **_):
        calls.append({"timestep": timestep})
        return pd.DataFrame()  # empty: no IBKR minute data

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    router._datetime = start
    _ = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)
    router._datetime = start + timedelta(minutes=15)
    _ = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)
    router._datetime = start + timedelta(minutes=45)
    _ = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)

    assert len(calls) == 1, f"Expected single empty prefetch, got {len(calls)}"
    adapter = router._registry._adapters["ibkr"]
    canonical_key, _ = router._build_dataset_keys(asset, quote, "minute")
    assert canonical_key in adapter._empty_prefetch_series
    assert canonical_key not in adapter._fully_loaded_series


def test_empty_prefetch_short_circuits_stock_day(monkeypatch):
    """Stock/day: empty first prefetch must be cached; no further fetches."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 5, 0, 0))
    router = _make_router(start, end, {"default": "thetadata", "stock": "ibkr"})

    asset = Asset("OBSCURE", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    calls: list[dict] = []

    def fake_get_price_data(**_):
        calls.append({})
        return pd.DataFrame()

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    for days in (0, 1, 2, 5):
        router._datetime = start + timedelta(days=days)
        _ = router.get_historical_prices(asset, length=10, timestep="day", quote=quote)

    assert len(calls) == 1, f"Expected single prefetch, got {len(calls)}"


def test_empty_prefetch_short_circuits_future_minute(monkeypatch):
    """Future/minute: empty first prefetch must short-circuit."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 2, 0))
    router = _make_router(start, end, {"default": "thetadata", "cont_future": "ibkr"})

    asset = Asset("NQ", asset_type=Asset.AssetType.CONT_FUTURE)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    calls: list[dict] = []

    def fake_get_price_data(**_):
        calls.append({})
        return None  # also covers the None branch

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    for minutes in (0, 15, 30, 60):
        router._datetime = start + timedelta(minutes=minutes)
        _ = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)

    assert len(calls) == 1, f"Expected single empty prefetch, got {len(calls)}"


def test_empty_prefetch_short_circuits_crypto_minute(monkeypatch):
    """Crypto/minute: empty first prefetch must short-circuit."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 2, 0))
    router = _make_router(start, end, {"default": "thetadata", "crypto": "ibkr"})

    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    calls: list[dict] = []

    def fake_get_price_data(**_):
        calls.append({})
        return pd.DataFrame()

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    for minutes in (0, 10, 30, 90):
        router._datetime = start + timedelta(minutes=minutes)
        _ = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)

    assert len(calls) == 1


def test_empty_prefetch_short_circuits_crypto_day(monkeypatch):
    """Crypto/day: empty first prefetch must short-circuit."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 5, 0, 0))
    router = _make_router(start, end, {"default": "thetadata", "crypto": "ibkr"})

    asset = Asset("DOGE", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    calls: list[dict] = []

    def fake_get_price_data(**_):
        calls.append({})
        return pd.DataFrame()

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    for days in (0, 1, 2, 5):
        router._datetime = start + timedelta(days=days)
        _ = router.get_historical_prices(asset, length=5, timestep="day", quote=quote)

    assert len(calls) == 1


def test_empty_prefetch_does_not_affect_other_canonical_keys(monkeypatch):
    """An empty minute prefetch must not block a non-empty day prefetch for the same asset."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 2, 5, 0, 0))
    router = _make_router(start, end, {"default": "thetadata", "stock": "ibkr"})

    asset = Asset("TQQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    calls: list[dict] = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **_):
        calls.append({"timestep": str(timestep)})
        if "minute" in str(timestep):
            return pd.DataFrame()  # empty for minute
        # Non-empty daily bars.
        idx = pd.date_range(start_dt, end_dt, freq="1D", tz=LUMIBOT_DEFAULT_PYTZ)
        px = pd.Series(range(len(idx)), index=idx, dtype="float64") + 100.0
        return pd.DataFrame({"open": px, "high": px + 1, "low": px - 1, "close": px, "volume": 1000}, index=idx)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    router._datetime = start + timedelta(days=1)
    _ = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)  # empty
    _ = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)  # short-circuit
    bars = router.get_historical_prices(asset, length=5, timestep="day", quote=quote)  # must fetch

    assert bars is not None and bars.df is not None and not bars.df.empty
    minute_calls = [c for c in calls if "minute" in c["timestep"]]
    day_calls = [c for c in calls if c["timestep"] == "day"]
    assert len(minute_calls) == 1, f"Expected single minute prefetch, got {len(minute_calls)}"
    assert len(day_calls) == 1, f"Expected single day prefetch, got {len(day_calls)}"

    adapter = router._registry._adapters["ibkr"]
    minute_key, _ = router._build_dataset_keys(asset, quote, "minute")
    day_key, _ = router._build_dataset_keys(asset, quote, "day")
    assert minute_key in adapter._empty_prefetch_series
    assert day_key not in adapter._empty_prefetch_series
    assert day_key in adapter._fully_loaded_series


def test_non_empty_prefetch_not_marked_as_empty(monkeypatch):
    """A successful prefetch must go into `_fully_loaded_series`, NOT `_empty_prefetch_series`."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 5, 2, 0))
    router = _make_router(start, end, {"default": "thetadata", "stock": "ibkr"})

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    def fake_get_price_data(*, start_dt, end_dt, **_):
        return _minute_ohlc(start_dt, end_dt)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    router._datetime = start
    _ = router.get_historical_prices(asset, length=5, timestep="minute", quote=quote)

    adapter = router._registry._adapters["ibkr"]
    canonical_key, _ = router._build_dataset_keys(asset, quote, "minute")
    assert canonical_key in adapter._fully_loaded_series
    assert canonical_key not in adapter._empty_prefetch_series


# ---------------------------------------------------------------------------
# 2026-09-23: a production backtest (LumiBot 4.5.92, routed IBKR, SPY 5minute).
#
# The run started at 00:42 ET on Sep 15, so `backtesting_end` was clamped to a time
# before that day's session. The shared cache already held every real bar. The routed
# prefetch still never counted the window as loaded, and every 5-minute iteration of the
# first simulated day re-submitted the identical downloader request
# `startTime=20260908-08:00:00` (77 times). This reproduces it through the real
# `ibkr_helper.get_price_data()` with only the downloader boundary faked.
# ---------------------------------------------------------------------------


def _ibkr_5min_extended_hours(days: list[str]) -> pd.DataFrame:
    frames = []
    base = 650.0
    for day in days:
        idx = pd.date_range(
            pd.Timestamp(f"{day} 04:00", tz="America/New_York"),
            pd.Timestamp(f"{day} 19:55", tz="America/New_York"),
            freq="5min",
        )
        px = base + pd.Series(range(len(idx)), index=idx, dtype="float64") * 0.01
        frames.append(
            pd.DataFrame(
                {"open": px, "high": px + 0.05, "low": px - 0.05, "close": px + 0.01, "volume": 1000.0},
                index=idx,
            )
        )
        base += 1.0
    return pd.concat(frames).sort_index()


def test_router_ibkr_stock_minute_clamped_pre_open_end_does_not_refetch_every_bar(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 756733)

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Warm shared cache, exactly the coverage production had: Sep 8 (after Labor Day)
    # 04:00 ET through Sep 14 19:55 ET.
    cached = _ibkr_5min_extended_hours(["2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14"])
    cached["missing"] = False
    cache_file = ibkr_helper._cache_file_for(
        asset=asset, quote=quote, timestep="5minute", exchange=None, source="Trades", include_after_hours=True
    )
    ibkr_helper._write_cache_frame(cache_file, cached)

    # What IBKR holds. `startTime` is the END of the returned window (see _fetch_history_between_dates).
    vendor = _ibkr_5min_extended_hours(
        ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08", "2026-09-09",
         "2026-09-10", "2026-09-11", "2026-09-14"]
    )
    requests: list[dict] = []

    def fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        assert str(url).endswith("/ibkr/iserver/marketdata/history")
        requests.append(dict(querystring))
        window_end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        window_start = window_end - pd.Timedelta(minutes=int(str(querystring["period"]).removesuffix("min")))
        rows = vendor.loc[(vendor.index >= window_start) & (vendor.index <= window_end)]
        return {
            "data": [
                {
                    "t": int(ts.timestamp() * 1000),
                    "o": float(row["open"]),
                    "h": float(row["high"]),
                    "l": float(row["low"]),
                    "c": float(row["close"]),
                    "v": float(row["volume"]),
                }
                for ts, row in rows.iterrows()
            ]
        }

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 9, 8, 0, 0))
    clamped_end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 9, 15, 0, 42, 41))
    router = _make_router(start, clamped_end, {"default": "ibkr", "stock": "ibkr"})

    seen_last_bars = []
    for step in range(12):
        router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 9, 8, 9, 30)) + timedelta(minutes=5 * step)
        bars = router.get_historical_prices(asset, length=250, timestep="5minute", quote=quote)
        assert bars is not None and not bars.df.empty
        seen_last_bars.append(bars.df.index[-1])

    request_keys = [tuple(sorted(r.items())) for r in requests]
    assert len(request_keys) == len(set(request_keys)), f"identical IBKR requests repeated: {requests}"
    assert len(requests) <= 2, f"expected at most the first-call edge probes, got {len(requests)}: {requests}"
    # The strategy sees the real session bars, including the 09:30 open.
    assert pd.Timestamp("2026-09-08 09:30", tz="America/New_York") in set(seen_last_bars)


# ---------------------------------------------------------------------------
# 2026-09-24: a production 58-ETF backtest (LumiBot 4.5.91, routed IBKR, Jan 2 to
# Sep 18 2026) asked for 390 one-minute SPY bars at every hourly review. The routed
# prefetch pages 1-minute history backwards in 1000-minute pages (16.7 hours). Every
# weekend is about 56 closed hours, so the page ending Monday 04:00 ET is empty
# (verified on the production downloader: startTime=20260914-08:00:00 returned
# points=0). The pager treated that empty page as the start of history and stopped,
# so the shared cache only ever held the last five sessions, SPY minute history was
# "underfilled" on 1,046 reviews and the strategy never traded.
# ---------------------------------------------------------------------------


def _ibkr_1min_extended_hours(days: list[str]) -> pd.DataFrame:
    frames = []
    base = 650.0
    for day in days:
        idx = pd.date_range(
            pd.Timestamp(f"{day} 04:00", tz="America/New_York"),
            pd.Timestamp(f"{day} 19:59", tz="America/New_York"),
            freq="1min",
        )
        px = base + pd.Series(range(len(idx)), index=idx, dtype="float64") * 0.001
        frames.append(
            pd.DataFrame(
                {"open": px, "high": px + 0.05, "low": px - 0.05, "close": px + 0.01, "volume": 1000.0},
                index=idx,
            )
        )
        base += 1.0
    return pd.concat(frames).sort_index()


def test_router_ibkr_stock_minute_prefetch_pages_across_weekends(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 756733)

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Three weeks of sessions, including the Labor Day holiday (Sep 7, 2026).
    sessions = ["2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03",
                "2026-09-04", "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14",
                "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"]
    vendor = _ibkr_1min_extended_hours(sessions)
    requests: list[dict] = []

    def fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        # IBKR semantics: `startTime` is the END of the window, `period` reaches back from it,
        # at most 1000 points, and a window with no bars answers an empty list.
        assert str(url).endswith("/ibkr/iserver/marketdata/history")
        requests.append(dict(querystring))
        window_end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        window_start = window_end - pd.Timedelta(minutes=int(str(querystring["period"]).removesuffix("min")))
        rows = vendor.loc[(vendor.index > window_start) & (vendor.index <= window_end)].tail(1000)
        return {
            "data": [
                {"t": int(ts.timestamp() * 1000), "o": float(r["open"]), "h": float(r["high"]),
                 "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])}
                for ts, r in rows.iterrows()
            ]
        }

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 31, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 9, 19, 0, 0))
    router = _make_router(start, end, {"default": "ibkr", "stock": "ibkr"})

    # First hourly review of the backtest: Tuesday Sep 1, 10:30 ET, 390 one-minute bars.
    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 9, 1, 10, 30))
    bars = router.get_historical_prices(asset, length=390, timestep="minute", quote=quote)
    assert bars is not None and not bars.df.empty, f"no SPY minute bars at Sep 1 10:30; requests={requests}"
    got = bars.df.index.tz_convert("America/New_York")
    assert got[-1] <= pd.Timestamp("2026-09-01 10:30", tz="America/New_York")
    assert pd.Timestamp("2026-09-01 09:30", tz="America/New_York") in set(got), "Sep 1 session bars missing"
    assert len(bars.df) == 390

    # The prefetch reached every session of the backtest window, across two weekends and
    # the Labor Day holiday, with no identical request repeated.
    cache_file = ibkr_helper._cache_file_for(
        asset=asset, quote=quote, timestep="minute", exchange=None, source="Trades", include_after_hours=True
    )
    cached = pd.read_parquet(cache_file)
    real = cached[~cached.get("missing", pd.Series(False, index=cached.index)).fillna(False).astype(bool)]
    cached_days = set(real.index.tz_convert("America/New_York").strftime("%Y-%m-%d"))
    for day in sessions[2:]:
        assert day in cached_days, f"session {day} never fetched; oldest cached {real.index.min()}"
    request_keys = [tuple(sorted(r.items())) for r in requests]
    assert len(request_keys) == len(set(request_keys)), f"identical IBKR requests repeated: {requests}"
    # One page per session plus the empty closed pages it had to step over; far from one per bar.
    assert len(requests) <= 3 * len(sessions), f"too many downloader requests: {len(requests)}"


# ---------------------------------------------------------------------------
# 2026-09-23 production: SPY 1-minute and 5-minute backtests whose end date was "today",
# started during market hours (14:46 ET). The account's IBKR history runs about 13 to 17
# minutes behind, so the newest page ended at "now" and the downloader rejected it as
# `stale_tail:gap_seconds=807` ("IBKR history remained invalid after rebuild"). That page
# is the first page, so the strategy got no SPY bars at all.
# ---------------------------------------------------------------------------


def test_ibkr_stock_minute_request_ending_now_stays_behind_the_delayed_feed(monkeypatch, tmp_path):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 756733)

    now = pd.Timestamp("2026-09-23 18:46:00", tz="UTC")
    monkeypatch.setattr(ibkr_helper, "_ibkr_history_now_utc", lambda: now.to_pydatetime())
    feed_lag = pd.Timedelta(minutes=16)
    vendor = _ibkr_1min_extended_hours(["2026-09-21", "2026-09-22", "2026-09-23"])
    vendor = vendor.loc[vendor.index <= now - feed_lag]
    requests: list[dict] = []

    def fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        requests.append(dict(querystring))
        window_end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        rows = vendor.loc[vendor.index <= window_end]
        # The downloader's tail check: the last bar must be within 3 bars of the requested end.
        if not rows.empty and (window_end - rows.index.max()) > pd.Timedelta(minutes=3) and window_end > now - feed_lag:
            raise RuntimeError(
                "Request r1 permanently failed: IBKR history remained invalid after rebuild "
                f"(conid=756733 period=1000min bar=1min reason=stale_tail:gap_seconds={int((window_end - rows.index.max()).total_seconds())}:tolerance_seconds=180)"
            )
        window_start = window_end - pd.Timedelta(minutes=int(str(querystring["period"]).removesuffix("min")))
        rows = rows.loc[rows.index > window_start].tail(1000)
        return {"data": [{"t": int(ts.timestamp() * 1000), "o": float(r["open"]), "h": float(r["high"]),
                          "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])}
                         for ts, r in rows.iterrows()]}

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    frame = ibkr_helper.get_price_data(
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="minute",
        start_dt=datetime(2026, 9, 21, 4, 0, tzinfo=timezone.utc),
        end_dt=now.to_pydatetime(),
        include_after_hours=True,
    )

    assert not frame.empty, f"no SPY bars for a backtest ending now; requests={requests}"
    assert frame.index.max() >= now - pd.Timedelta(minutes=25)
    assert frame.index.max() <= now - feed_lag
    assert pd.Timestamp("2026-09-22 09:30", tz="America/New_York") in set(frame.index)


def test_ibkr_index_minute_history_pages_across_overnight_and_weekend_gaps(monkeypatch, tmp_path):
    """SPX 1-minute bars exist 09:30 to 16:00 ET only (verified live 2026-09-24: the page ending
    Fri 21:00 UTC held 09:30 to 15:59). The overnight gap is 17.5 hours, longer than a
    1000-minute page, so the page ending at 09:30 was empty and paging stopped after ONE
    session. A production SPX options backtest logged "remained underfilled" 3,240 times."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 416904)

    sessions = ["2026-07-23", "2026-07-24", "2026-07-27", "2026-07-28", "2026-07-29", "2026-07-30", "2026-07-31"]
    frames = []
    for i, day in enumerate(sessions):
        idx = pd.date_range(pd.Timestamp(f"{day} 09:30", tz="America/New_York"),
                            pd.Timestamp(f"{day} 15:59", tz="America/New_York"), freq="1min")
        px = 6400.0 + i + pd.Series(range(len(idx)), index=idx, dtype="float64") * 0.01
        frames.append(pd.DataFrame({"open": px, "high": px + 1, "low": px - 1, "close": px, "volume": 0.0}, index=idx))
    vendor = pd.concat(frames)
    requests: list[dict] = []

    def fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        requests.append(dict(querystring))
        window_end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        window_start = window_end - pd.Timedelta(minutes=int(str(querystring["period"]).removesuffix("min")))
        rows = vendor.loc[(vendor.index > window_start) & (vendor.index <= window_end)].tail(1000)
        return {"data": [{"t": int(ts.timestamp() * 1000), "o": float(r["open"]), "h": float(r["high"]),
                          "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])}
                         for ts, r in rows.iterrows()]}

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    frame = ibkr_helper.get_price_data(
        asset=Asset("SPX", asset_type=Asset.AssetType.INDEX),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="minute",
        start_dt=datetime(2026, 7, 24, 13, 0, tzinfo=timezone.utc),
        end_dt=datetime(2026, 7, 31, 21, 0, tzinfo=timezone.utc),
        include_after_hours=True,
    )

    days = sorted(set(frame.index.tz_convert("America/New_York").strftime("%Y-%m-%d")))
    assert days == sessions[1:], f"sessions returned {days}; requests={[r['startTime'] for r in requests]}"
    assert len(frame) == 390 * len(sessions[1:])
    request_keys = [tuple(sorted(r.items())) for r in requests]
    assert len(request_keys) == len(set(request_keys))


def test_ibkr_stock_minute_paging_uses_one_request_per_session(monkeypatch, tmp_path):
    """Speed: a 1000-minute page anchored at the oldest bar just received straddles the closed
    overnight gap, so production paging made ~1.8 downloader requests per session (90 SPY
    requests for 50 sessions, ~20 minutes of wall time per calendar month on the shared
    downloader). Anchor the next page at the previous session's close: one page per session."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 756733)

    sessions = ["2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08",
                "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16",
                "2026-09-17", "2026-09-18"]
    vendor = _ibkr_1min_extended_hours(sessions)
    requests: list[dict] = []

    def fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        requests.append(dict(querystring))
        window_end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        window_start = window_end - pd.Timedelta(minutes=int(str(querystring["period"]).removesuffix("min")))
        rows = vendor.loc[(vendor.index > window_start) & (vendor.index <= window_end)].tail(1000)
        return {"data": [{"t": int(ts.timestamp() * 1000), "o": float(r["open"]), "h": float(r["high"]),
                          "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])}
                         for ts, r in rows.iterrows()]}

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    frame = ibkr_helper.get_price_data(
        asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="minute",
        start_dt=datetime(2026, 8, 31, 8, 0, tzinfo=timezone.utc),
        end_dt=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
        include_after_hours=True,
    )

    assert len(frame) == len(vendor), "every real bar must still arrive"
    assert len(requests) <= len(sessions) + 1, f"{len(requests)} requests for {len(sessions)} sessions: {[r['startTime'] for r in requests]}"


class _SimulatedStop(BaseException):
    """Stands in for a force-stopped or timed-out backtest process."""


def test_ibkr_minute_paging_checkpoints_pages_so_a_stopped_run_keeps_its_progress(monkeypatch, tmp_path):
    """2026-09-24: a cold 8-month 1-minute series takes hours on the shared downloader. The cache
    was written only after the whole backward walk finished, so a customer who force-stopped
    after an hour (or a run that hit its time limit) kept nothing and every retry started over.
    Pages must reach the cache while the walk is in progress."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 756733)

    days = [d.strftime("%Y-%m-%d") for d in pd.bdate_range("2026-07-01", "2026-09-18")
            if d.strftime("%Y-%m-%d") not in {"2026-07-03", "2026-09-07"}]
    vendor = _ibkr_1min_extended_hours(days)
    served = {"pages": 0}

    def fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        served["pages"] += 1
        if served["pages"] > 25:
            raise _SimulatedStop()
        window_end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        window_start = window_end - pd.Timedelta(minutes=int(str(querystring["period"]).removesuffix("min")))
        rows = vendor.loc[(vendor.index > window_start) & (vendor.index <= window_end)].tail(1000)
        return {"data": [{"t": int(ts.timestamp() * 1000), "o": float(r["open"]), "h": float(r["high"]),
                          "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])}
                         for ts, r in rows.iterrows()]}

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    with pytest.raises(_SimulatedStop):
        ibkr_helper.get_price_data(asset=asset, quote=quote, timestep="minute",
            start_dt=datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc),
            end_dt=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc), include_after_hours=True)

    cache_file = ibkr_helper._cache_file_for(asset=asset, quote=quote, timestep="minute", exchange=None,
                                             source="Trades", include_after_hours=True)
    assert cache_file.exists(), "nothing reached the cache before the run stopped"
    cached = pd.read_parquet(cache_file)
    cached_days = set(cached.index.tz_convert("America/New_York").strftime("%Y-%m-%d"))
    # 25 pages served, one session each; at least the first 20 must be kept.
    assert len(cached_days) >= 20, f"only {len(cached_days)} sessions kept"
    # Everything kept is a real vendor bar.
    assert set(cached.index).issubset(set(vendor.index))


# ---------------------------------------------------------------------------
# A minute request must never be served daily bars (routed IBKR, daily sleeptime).
#
# Bug (2026-09-24, SPCX replay on 4.6.1): a strategy with sleeptime="1D" called
# get_historical_prices(Asset("SPCX", STOCK), 1440, "minute") in a routed IBKR
# backtest and got 52 DAILY bars back. The executor primes daily cadence for
# "1D" sleeptimes (`_timestep="day"`), and the Theta base `_pull_source_symbol_bars`
# then rewrote the explicit "minute" request into "day". Rule: a minute request
# returns minute bars or nothing, never daily bars.
# ---------------------------------------------------------------------------


def _session_minute_ohlc(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """Regular-session (09:30-16:00 ET) weekday minute bars between two datetimes."""
    frame = _minute_ohlc(start_dt, end_dt)
    idx = frame.index
    mask = (idx.dayofweek < 5) & (idx.time >= datetime(2000, 1, 1, 9, 30).time()) & (
        idx.time < datetime(2000, 1, 1, 16, 0).time()
    )
    return frame[mask]


def _daily_ohlc(start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    start_ts = pd.Timestamp(start_dt).tz_convert(LUMIBOT_DEFAULT_PYTZ).normalize()
    end_ts = pd.Timestamp(end_dt).tz_convert(LUMIBOT_DEFAULT_PYTZ).normalize()
    idx = pd.date_range(start_ts, end_ts, freq="B")
    px = pd.Series(range(len(idx)), index=idx, dtype="float64") + 50.0
    return pd.DataFrame(
        {"open": px, "high": px + 1, "low": px - 1, "close": px, "volume": 10_000},
        index=idx,
    )


def _prime_daily_sleeptime_cadence(router: RoutedBacktestingPandas) -> None:
    """Mirror StrategyExecutor's priming for a sleeptime ending in "d" (e.g. "1D")."""
    router._timestep = "day"
    router._effective_day_mode = True
    router._observed_intraday_cadence = False


def _assert_intraday_spacing(df: pd.DataFrame) -> None:
    gaps = df.index[1:] - df.index[:-1]
    positive = gaps[gaps > pd.Timedelta(0)]
    assert len(positive) > 0
    assert positive.min() < pd.Timedelta(hours=1), f"expected minute spacing, got min gap {positive.min()}"


def test_daily_sleeptime_minute_request_returns_minute_bars_not_daily(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 28, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 29, 0, 0))
    router = _make_router(start, end, {"default": "thetadata", "stock": "ibkr"})
    _prime_daily_sleeptime_cadence(router)

    asset = Asset("SPCX", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    requested_timesteps: list[str] = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **_):
        requested_timesteps.append(timestep)
        if timestep == "day":
            return _daily_ohlc(start_dt, end_dt)
        return _session_minute_ohlc(start_dt, end_dt)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    # First daily iteration of the SPCX replay: market open on 2026-08-28.
    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 28, 9, 30))
    bars = router.get_historical_prices(asset, 1440, "minute", quote=quote)

    assert "day" not in requested_timesteps, f"minute request fetched daily bars: {requested_timesteps}"
    assert bars is not None
    df = bars.df
    assert df is not None and not df.empty
    _assert_intraday_spacing(df)
    assert len(df) <= 1440
    assert df.index.max() <= pd.Timestamp(router.get_datetime())


def test_daily_sleeptime_minute_request_without_minute_data_returns_nothing(monkeypatch):
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 28, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 29, 0, 0))
    router = _make_router(start, end, {"default": "thetadata", "stock": "ibkr"})
    _prime_daily_sleeptime_cadence(router)

    asset = Asset("SPCX", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **_):
        # Only daily history exists for this symbol.
        if timestep == "day":
            return _daily_ohlc(start_dt, end_dt)
        return pd.DataFrame()

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    # Daily bars already warmed by an earlier daily lookup must not answer a minute request.
    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 28, 9, 30))
    daily = router.get_historical_prices(asset, 20, "day", quote=quote)
    assert daily is not None and not daily.df.empty

    bars = router.get_historical_prices(asset, 1440, "minute", quote=quote)
    df = getattr(bars, "df", None) if bars is not None else None
    assert df is None or df.empty, f"minute request returned {len(df)} non-minute bars"


def _flat_minute_ohlc(start_dt: datetime, end_dt: datetime, price: float) -> pd.DataFrame:
    frame = _session_minute_ohlc(start_dt, end_dt)
    for column in ("open", "high", "low", "close"):
        frame[column] = price
    # ibkr_helper fills bid/ask from the close when no history source is chosen.
    frame["bid"] = price
    frame["ask"] = price
    return frame


def test_intraday_stock_quote_uses_loaded_minute_bars_not_yesterdays_daily_close(monkeypatch):
    """2026-09-24, a real 1-minute ETF strategy on routed IBKR: SELL SLV on Jan 7 with a completed
    minute close of 69.67 filled at 73.71, SLV's Jan 6 DAILY close, although SLV minute bars were
    loaded. Market orders fill from get_quote() first, and RoutedBacktestingPandas forced every
    stock quote to daily bars (PREFER_NATIVE_DAY_BARS_FOR_STOCK_INDEX). Daily bars are only a
    shortcut for strategies that never loaded intraday bars for the asset."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 2, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 9, 0, 0))
    router = _make_router(start, end, {"default": "ibkr", "stock": "ibkr", "index": "ibkr"})
    asset = Asset("SLV", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **_):
        if timestep == "day":
            daily = _daily_ohlc(start_dt, end_dt)
            daily[["open", "high", "low", "close", "bid", "ask"]] = 73.71
            return daily
        return _flat_minute_ohlc(start_dt, end_dt, 69.67)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)

    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 7, 10, 30))
    router.get_historical_prices(asset, 75, "day", quote=quote)
    minute = router.get_historical_prices(asset, 8, "minute", quote=quote)
    assert minute is not None and not minute.df.empty

    q = router.get_quote(asset, quote=quote)
    last = router.get_last_price(asset, quote=quote)
    assert q is not None
    assert round(float(q.bid), 2) == 69.67 and round(float(q.ask), 2) == 69.67, (q.bid, q.ask)
    assert round(float(last), 2) == 69.67, last


def test_daily_strategy_quote_still_uses_daily_bars_without_intraday_series(monkeypatch):
    """The daily shortcut stays for strategies that never loaded intraday bars (no minute download)."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 2, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 9, 0, 0))
    router = _make_router(start, end, {"default": "ibkr", "stock": "ibkr", "index": "ibkr"})
    asset = Asset("SLV", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    requested: list[str] = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **_):
        requested.append(timestep)
        return _daily_ohlc(start_dt, end_dt)

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 7, 10, 30))
    router.get_historical_prices(asset, 75, "day", quote=quote)
    router.get_quote(asset, quote=quote)
    router.get_last_price(asset, quote=quote)
    assert "minute" not in requested, requested


@pytest.fixture
def no_local_theta_terminal(monkeypatch):
    """Fail a test that would touch a developer's local ThetaTerminal.

    CodeRabbit on PR #1180: without DATADOWNLOADER_BASE_URL (a clean CI box or a shell
    without the lumibot .env), constructing the router reaches ThetaDataBacktestingPandas.__init__,
    which kills every local ThetaTerminal.jar process. The variable is removed here so the
    guard sees the worst case.
    """
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas

    def _refuse(self, keyword):
        raise AssertionError(f"test tried to kill local processes matching {keyword!r}")

    monkeypatch.delenv("DATADOWNLOADER_BASE_URL", raising=False)
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", _refuse)
    return monkeypatch


def _router_with_minute_store(frame: pd.DataFrame, now: datetime, monkeypatch):
    from types import SimpleNamespace

    # A configured downloader keeps router construction away from local ThetaTerminal processes.
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 2, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 20, 0, 0))
    router = _make_router(start, end, {"default": "ibkr", "stock": "ibkr", "index": "ibkr"})
    asset = Asset("SLV", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    router._data_store = {(asset, quote, "minute"): SimpleNamespace(df=frame)}
    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(now)
    return router, asset, quote


@pytest.mark.parametrize(
    "now, expected",
    [
        (datetime(2026, 1, 7, 10, 30), True),   # last bar one minute ago
        (datetime(2026, 1, 7, 10, 29), True),   # a bar exactly at the simulated time
        (datetime(2026, 1, 12, 10, 30), False),  # last bar more than 4 days old
        (datetime(2026, 1, 5, 9, 0), False),    # only bars after the simulated time
    ],
)
def test_loaded_intraday_series_check_edges(now, expected, no_local_theta_terminal):
    frame = _flat_minute_ohlc(
        LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 6, 9, 30)),
        LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 7, 10, 29)),
        69.67,
    )
    router, asset, quote = _router_with_minute_store(frame, now, no_local_theta_terminal)
    assert router._has_loaded_intraday_series(asset, quote) is expected


def test_loaded_intraday_series_check_uses_binary_search_not_a_full_index_scan(monkeypatch, no_local_theta_terminal):
    """The check runs on every quote and last-price lookup. A full-index boolean mask over an
    8-month minute series cost about 0.4 ms per call (7x the binary search) in the SEH Simple
    replay; with 58 symbols that is seconds per backtest spent re-scanning the same index."""
    frame = _flat_minute_ohlc(
        LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 6, 9, 30)),
        LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 1, 7, 10, 29)),
        69.67,
    )
    router, asset, quote = _router_with_minute_store(frame, datetime(2026, 1, 7, 10, 30), monkeypatch)

    def _no_full_scan(self, other):
        raise AssertionError("full-index comparison")

    monkeypatch.setattr(pd.DatetimeIndex, "__le__", _no_full_scan)
    monkeypatch.setattr(pd.DatetimeIndex, "__gt__", _no_full_scan)
    assert router._has_loaded_intraday_series(asset, quote) is True


def test_ibkr_thin_stock_minute_paging_still_uses_one_request_per_session(monkeypatch, tmp_path):
    """Live 2026-09-24: XLK's first pre-market print is often 04:01 or later, so "only closed time
    before the oldest bar" never held and its pages kept stepping at 08:01 and 15:21 UTC (about
    1.8 requests per session). A page whose window already covered the session start has every
    bar that exists before its oldest bar, so the next page can start at the previous close."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 4215230)

    sessions = ["2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-08",
                "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16",
                "2026-09-17", "2026-09-18"]
    vendor = _ibkr_1min_extended_hours(sessions)
    # Thin pre-market: no prints before 04:07 ET and a sparse after-hours tail.
    local = vendor.index.tz_convert("America/New_York")
    keep = ~((local.hour == 4) & (local.minute < 7)) & ~((local.hour >= 19) & (local.minute % 3 != 0))
    vendor = vendor[keep]
    requests: list[dict] = []

    def fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        requests.append(dict(querystring))
        window_end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        window_start = window_end - pd.Timedelta(minutes=int(str(querystring["period"]).removesuffix("min")))
        rows = vendor.loc[(vendor.index > window_start) & (vendor.index <= window_end)].tail(1000)
        return {"data": [{"t": int(ts.timestamp() * 1000), "o": float(r["open"]), "h": float(r["high"]),
                          "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])}
                         for ts, r in rows.iterrows()]}

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    frame = ibkr_helper.get_price_data(
        asset=Asset("XLK", asset_type=Asset.AssetType.STOCK),
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
        timestep="minute",
        start_dt=datetime(2026, 8, 31, 8, 0, tzinfo=timezone.utc),
        end_dt=datetime(2026, 9, 19, 0, 0, tzinfo=timezone.utc),
        include_after_hours=True,
    )

    assert len(frame) == len(vendor), "every real bar must still arrive"
    assert len(requests) <= len(sessions) + 1, f"{len(requests)} requests for {len(sessions)} sessions: {[r['startTime'] for r in requests]}"


def test_ibkr_thin_stock_resume_from_a_quiet_monday_open_backfills_older_sessions(monkeypatch, tmp_path):
    """Live 2026-09-24: a thin ETF's cache started at Monday 04:19 ET (no prints before 04:19). The
    resume request for older bars ended there; its 1000-minute window held Sunday plus Monday
    04:00 to 04:19, open minutes without trades, so IBKR answered empty, the window was not
    "fully closed" and paging stopped on the first page. The older sessions never loaded."""
    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_HISTORY_NO_DATA_WINDOWS", {})
    monkeypatch.setattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", {}, raising=False)
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 4215205)

    asset = Asset("XLV", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    sessions = ["2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23", "2026-07-24", "2026-07-27",
                "2026-07-28", "2026-07-29", "2026-07-30", "2026-07-31", "2026-08-03", "2026-08-04",
                "2026-08-05"]
    vendor = _ibkr_1min_extended_hours(sessions)
    local = vendor.index.tz_convert("America/New_York")
    vendor = vendor[~((local.hour == 4) & (local.minute < 19))]  # thin: first print 04:19

    cached = vendor.loc[vendor.index >= pd.Timestamp("2026-08-03 04:19", tz="America/New_York")].copy()
    cached["missing"] = False
    cache_file = ibkr_helper._cache_file_for(asset=asset, quote=quote, timestep="minute", exchange=None,
                                             source="Trades", include_after_hours=True)
    ibkr_helper._write_cache_frame(cache_file, cached)
    requests: list[dict] = []

    def fake_queue_request(url, querystring=None, headers=None, timeout=None, **_):
        requests.append(dict(querystring))
        window_end = pd.Timestamp(datetime.strptime(querystring["startTime"], "%Y%m%d-%H:%M:%S"), tz="UTC")
        window_start = window_end - pd.Timedelta(minutes=int(str(querystring["period"]).removesuffix("min")))
        # Live IBKR leaves out the bar stamped at the page end (a page ending 00:00 UTC returned 23:58
        # as its last bar), so the resume page ending at the cached 04:19 bar comes back empty.
        rows = vendor.loc[(vendor.index > window_start) & (vendor.index < window_end)].tail(1000)
        return {"data": [{"t": int(ts.timestamp() * 1000), "o": float(r["open"]), "h": float(r["high"]),
                          "l": float(r["low"]), "c": float(r["close"]), "v": float(r["volume"])}
                         for ts, r in rows.iterrows()]}

    monkeypatch.setattr(ibkr_helper, "queue_request", fake_queue_request)

    frame = ibkr_helper.get_price_data(asset=asset, quote=quote, timestep="minute",
        start_dt=datetime(2026, 7, 20, 8, 0, tzinfo=timezone.utc),
        end_dt=datetime(2026, 8, 6, 0, 0, tzinfo=timezone.utc), include_after_hours=True)

    days = sorted(set(frame.index.tz_convert("America/New_York").strftime("%Y-%m-%d")))
    assert days == sessions, f"sessions {days}; requests {[r['startTime'] for r in requests]}"
    assert len(requests) <= 12, f"{len(requests)} requests: {[r['startTime'] for r in requests]}"


def _daily_with_dividends(start_dt: datetime, end_dt: datetime, dividends: dict[str, float]) -> pd.DataFrame:
    """Daily bars shaped like ibkr_helper's output: 16:00 ET rows plus the corporate-action columns."""
    frame = _daily_ohlc(start_dt, end_dt)
    frame.index = frame.index + pd.Timedelta(hours=16)
    frame["dividend"] = 0.0
    frame["stock_splits"] = 0.0
    for day, amount in dividends.items():
        ts = pd.Timestamp(f"{day} 16:00", tz=LUMIBOT_DEFAULT_PYTZ)
        if ts in frame.index:  # never add rows outside the window
            frame.loc[ts, "dividend"] = amount
    return frame


def test_ibkr_routed_stock_dividends_come_from_ibkr_daily_bars_not_thetadata(monkeypatch, no_local_theta_terminal):
    """2026-09-25: a BotSpot Auto backtest holding 400 TLT and 50 SPY from June to August 2026 kept
    its cash flat. RoutedBacktestingPandas inherited ThetaData's dividend lookup, which asks the
    ThetaData corporate-actions API even for stocks whose bars come from IBKR. ThetaData is switched
    off (2026-09-23), every lookup failed quietly, and every dividend was zero. The IBKR daily bars
    already carry the dividend on its ex-date."""
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 1, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 31, 0, 0))
    router = _make_router(start, end, {"default": "ibkr", "stock": "ibkr", "index": "ibkr"})
    tlt = Asset("TLT", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    theta_calls: list[str] = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **_):
        return _daily_with_dividends(start_dt, end_dt, {"2026-07-01": 0.318, "2026-08-03": 0.330})

    def theta_disabled(asset, *_args, **_kwargs):
        theta_calls.append(asset.symbol)
        raise RuntimeError("thetadata_disabled: ThetaData is disabled on this data downloader")

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(thetadata_helper, "_get_theta_dividends", theta_disabled)
    monkeypatch.setattr(thetadata_helper, "_get_theta_splits", theta_disabled)

    paid = {}
    for day in ("2026-06-30", "2026-07-01", "2026-07-02", "2026-08-03"):
        router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime.fromisoformat(f"{day}T09:30"))
        paid[day] = float(router.get_yesterday_dividends([tlt], quote=quote).get(tlt) or 0.0)

    assert paid == {"2026-06-30": 0.0, "2026-07-01": 0.318, "2026-07-02": 0.0, "2026-08-03": 0.330}
    assert theta_calls == []


def test_thetadata_routed_stock_dividends_still_use_thetadata(monkeypatch, no_local_theta_terminal):
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 1, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 31, 0, 0))
    router = _make_router(start, end, {"default": "thetadata", "stock": "thetadata"})
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    def theta_dividends(asset, *_args, **_kwargs):
        return pd.DataFrame({"event_date": [pd.Timestamp("2026-06-18")], "cash_amount": [1.904]})

    monkeypatch.setattr(thetadata_helper, "_get_theta_dividends", theta_dividends)
    monkeypatch.setattr(thetadata_helper, "_get_theta_splits", lambda *_a, **_k: pd.DataFrame())

    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 18, 9, 30))
    assert float(router.get_yesterday_dividends([spy], quote=quote).get(spy)) == 1.904


def _ibkr_dividend_router(monkeypatch, *, frame_end: str):
    import lumibot.tools.ibkr_helper as ibkr_helper
    import lumibot.tools.thetadata_helper as thetadata_helper

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 1, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 31, 0, 0))
    router = _make_router(start, end, {"default": "ibkr", "stock": "ibkr", "index": "ibkr"})
    fetches = []

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, **_):
        fetches.append((start_dt, end_dt))
        stop = min(pd.Timestamp(end_dt), pd.Timestamp(frame_end, tz=LUMIBOT_DEFAULT_PYTZ))
        return _daily_with_dividends(start_dt, stop, {"2026-07-01": 0.318, "2026-08-03": 0.330})

    monkeypatch.setattr(ibkr_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(thetadata_helper, "_get_theta_dividends", lambda *_a, **_k: pytest.fail("ThetaData called"))
    monkeypatch.setattr(thetadata_helper, "_get_theta_splits", lambda *_a, **_k: pytest.fail("ThetaData called"))
    return router, fetches


def test_routed_dividend_lookup_does_not_rescan_a_daily_frame_that_ends_before_today(monkeypatch, no_local_theta_terminal):
    """CodeRabbit on PR #1180 (routed_backtesting.py:1227): when the daily frame ends before the
    current date (IBKR has no newer daily bar yet), every lookup rebuilt the whole dividend map.
    An intraday strategy asks on every iteration, so this rescanned each held asset's full daily
    frame hundreds of times per simulated day."""
    router, _ = _ibkr_dividend_router(monkeypatch, frame_end="2026-07-31")
    tlt = Asset("TLT", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 7, 1, 9, 30))
    assert float(router.get_yesterday_dividends([tlt], quote=quote).get(tlt)) == 0.318

    lookups = []
    original = router._get_backtest_daily_corporate_action_frame
    monkeypatch.setattr(router, "_get_backtest_daily_corporate_action_frame",
                        lambda *a, **k: lookups.append(1) or original(*a, **k))
    for day in (3, 4):
        for minute in range(60):
            router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, day, 9, 30 + minute // 2))
            assert float(router.get_yesterday_dividends([tlt], quote=quote).get(tlt) or 0.0) == 0.0
    assert len(lookups) <= 4, f"{len(lookups)} daily-frame lookups for 2 simulated days"


def test_routed_dividend_lookup_refreshes_a_stale_daily_frame_once(monkeypatch, no_local_theta_terminal):
    """A daily frame loaded before the newer bars existed must be refreshed, not reused: the Aug 3
    dividend is only in the newer bars."""
    from lumibot.entities import Data

    router, fetches = _ibkr_dividend_router(monkeypatch, frame_end="2026-08-31")
    tlt = Asset("TLT", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    stale = _daily_with_dividends(
        LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 1)),
        LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 7, 31)),
        {"2026-07-01": 0.318},
    )
    canonical_key, _ = router._build_dataset_keys(tlt, quote, "day")
    router._data_store[canonical_key] = Data(tlt, stale, timestep="day", quote=quote)

    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 7, 1, 9, 30))
    assert float(router.get_yesterday_dividends([tlt], quote=quote).get(tlt)) == 0.318
    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 3, 9, 30))
    assert float(router.get_yesterday_dividends([tlt], quote=quote).get(tlt) or 0.0) == 0.330
    fetched = len(fetches)
    for minute in range(28):
        router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 3, 9, 31 + minute))
        router.get_yesterday_dividends([tlt], quote=quote)
    assert len(fetches) == fetched


def test_alpaca_routed_stocks_get_no_cash_dividend_because_prices_are_dividend_adjusted(monkeypatch, no_local_theta_terminal):
    """CodeRabbit on PR #1180 (routed_backtesting.py:1210) suggested sending Alpaca-routed stocks to
    the ThetaData dividend lookup. That would count dividends twice: the routed Alpaca source keeps
    AlpacaBacktesting's default auto_adjust=True, which requests adjustment="all" bars (splits AND
    dividends already in the price). This test pins both halves so a change to either is deliberate."""
    import lumibot.backtesting.routed_backtesting as routed
    import lumibot.tools.thetadata_helper as thetadata_helper
    from lumibot.backtesting.alpaca_backtesting import AlpacaBacktesting

    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "<redacted>")
    monkeypatch.setattr(routed, "ALPACA_CONFIG", {"API_KEY": "test", "API_SECRET": "test", "PAPER": True})
    monkeypatch.setattr(thetadata_helper, "_get_theta_dividends", lambda *_a, **_k: pytest.fail("ThetaData called"))
    monkeypatch.setattr(thetadata_helper, "_get_theta_splits", lambda *_a, **_k: pytest.fail("ThetaData called"))
    sources = []

    def fake_between_dates(self, *, base_asset, quote_asset, timestep, data_datetime_start, data_datetime_end, **_):
        sources.append(self)
        frame = _daily_ohlc(data_datetime_start, data_datetime_end)
        frame.index = frame.index + pd.Timedelta(hours=16)
        return frame

    monkeypatch.setattr(AlpacaBacktesting, "get_historical_prices_between_dates", fake_between_dates)

    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 1, 0, 0))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 8, 31, 0, 0))
    router = _make_router(start, end, {"default": "alpaca", "stock": "alpaca"})
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    router._datetime = LUMIBOT_DEFAULT_PYTZ.localize(datetime(2026, 6, 18, 9, 30))

    assert float(router.get_yesterday_dividends([spy], quote=quote).get(spy) or 0.0) == 0.0
    assert sources, "the Alpaca source was never asked for daily bars"
    assert sources[0]._auto_adjust is True  # adjustment="all": dividends are in the prices
