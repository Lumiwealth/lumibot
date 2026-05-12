from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

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

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
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

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
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

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
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

    def fake_get_price_data(
        *, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None
    ):
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
