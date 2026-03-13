from __future__ import annotations

import pytz
import pandas as pd
from datetime import datetime

from lumibot.data_sources.pandas_data import PandasData
from lumibot.entities import Asset, Data

# ---------------------------------------------------------------------------
# Module-level datetime constants used by the real-constructor helpers below.
# PandasData.__init__ → DataSourceBacktesting.__init__ requires a timezone-aware
# datetime_start so that get_timezone_from_datetime() does not crash.  Using the
# real constructor (rather than __new__) means any future attribute added to
# find_asset_in_data_store will be present, making test failures meaningful
# rather than cryptic AttributeErrors.
# ---------------------------------------------------------------------------
_TZ = pytz.timezone("America/New_York")
_START = datetime(2025, 1, 2, 9, 30, tzinfo=_TZ)
_END = datetime(2025, 1, 3, 16, 0, tzinfo=_TZ)


def _minute_df(tz: str = "America/New_York") -> pd.DataFrame:
    idx = pd.date_range("2025-01-02 09:30", periods=60, freq="1min", tz=tz)
    price = [100.0 + i * 0.01 for i in range(60)]
    return pd.DataFrame(
        {"open": price, "high": price, "low": price, "close": price, "volume": [1000] * 60},
        index=idx,
    )


def _day_df(tz: str = "America/New_York") -> pd.DataFrame:
    idx = pd.date_range("2025-01-02", periods=5, freq="D", tz=tz)
    return pd.DataFrame(
        {"open": [1, 2, 3, 4, 5], "high": [1, 2, 3, 4, 5], "low": [1, 2, 3, 4, 5],
         "close": [1, 2, 3, 4, 5], "volume": [0, 0, 0, 0, 0]},
        index=idx,
    )


def _make_ds(data_store: dict, allow_day_resampling: bool = True) -> PandasData:
    """Build a PandasData instance using the real constructor.

    Passes the Data objects in *data_store* as a list; PandasData._set_pandas_data_keys()
    derives the same (asset, quote) 2-tuple key from each Data object's .asset/.quote
    attributes, so the resulting _data_store is equivalent to passing the dict directly.

    Using the real constructor (rather than PandasData.__new__()) ensures all instance
    attributes are properly initialised, so a test failure is a meaningful assertion
    error rather than a cryptic AttributeError if find_asset_in_data_store is ever
    changed to read an additional attribute.
    """
    return PandasData(
        datetime_start=_START,
        datetime_end=_END,
        pandas_data=list(data_store.values()),
        allow_day_resampling=allow_day_resampling,
    )


def test_find_asset_in_data_store_does_not_return_daily_for_minute_requests():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    daily = Data(base, _day_df(), timestep="day", quote=quote)

    ds = _make_ds({(base, quote): daily})

    # Simulate how crypto/forex often passes assets as a tuple while quote=None.
    asset_tuple = (base, quote)
    assert ds.find_asset_in_data_store(asset_tuple, quote=None, timestep="minute") is None
    assert ds.find_asset_in_data_store(asset_tuple, quote=None, timestep="day") == (base, quote)


def test_find_asset_in_data_store_allows_minute_data_to_satisfy_day_requests():
    """Crypto assets with only minute data can still satisfy day-bar requests (unchanged behavior)."""
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    minute = Data(base, _minute_df(), timestep="minute", quote=quote)

    ds = _make_ds({(base, quote): minute})

    asset_tuple = (base, quote)
    assert ds.find_asset_in_data_store(asset_tuple, quote=None, timestep="day") == (base, quote)
    assert ds.find_asset_in_data_store(asset_tuple, quote=None, timestep="minute") == (base, quote)


# ---------------------------------------------------------------------------
# Tests for the allow_day_resampling flag — the fix for the Polygon vs ThetaData
# conflict described in the original bug report.
#
# allow_day_resampling=True  (default, Polygon / base PandasData):
#   minute data in the store CAN satisfy a day-bar lookup for stocks/indices so
#   that Data.get_bars() minute→day resampling fires.
#
# allow_day_resampling=False (ThetaData):
#   Exact timestep match is required. A cached minute dataset must NEVER proxy
#   for a day request so that ThetaData's split-adjusted day bars are used.
# ---------------------------------------------------------------------------


def test_find_asset_in_data_store_stock_minute_data_day_request_allow_resampling():
    """
    FIX VERIFICATION: With allow_day_resampling=True (Polygon / base PandasData default),
    a stock asset that only has minute data in the store can satisfy a day-bar request.

    This is the root cause of the silent get_historical_prices failure when a live backtest
    calls get_historical_prices(asset, timestep="15 minute") first and then
    get_historical_prices(asset, timestep="1 day").  With allow_day_resampling=True the
    second call finds the minute data and lets Data.get_bars() resample it to daily bars.
    """
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    minute_data = Data(spy, _minute_df(), timestep="minute", quote=quote)

    # Default (allow_day_resampling=True) — the fix
    ds = _make_ds({(spy, quote): minute_data}, allow_day_resampling=True)

    # 15-minute request: minute data IS accepted (works correctly both before and after fix)
    min_key = ds.find_asset_in_data_store(spy, quote=quote, timestep="15 minute")
    assert min_key == (spy, quote), (
        "15-minute request must find the minute data store so the strategy can read intraday bars."
    )

    # 1-day request: with allow_day_resampling=True, minute data IS now accepted for stocks
    day_key = ds.find_asset_in_data_store(spy, quote=quote, timestep="day")
    assert day_key == (spy, quote), (
        "With allow_day_resampling=True, minute data must satisfy day requests so that "
        "Data.get_bars() can resample minute → day bars."
    )


def test_find_asset_in_data_store_stock_minute_data_day_request_no_resampling():
    """
    With allow_day_resampling=False (ThetaData), a stock with only minute data in the
    store must NOT satisfy a day-bar request.  This forces an explicit day-bar fetch and
    preserves ThetaData's split-adjusted day-bar normalisation.
    """
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    minute_data = Data(spy, _minute_df(), timestep="minute", quote=quote)

    # ThetaData-style (allow_day_resampling=False)
    ds = _make_ds({(spy, quote): minute_data}, allow_day_resampling=False)

    # 15-minute request still works (minute data satisfies minute requests)
    min_key = ds.find_asset_in_data_store(spy, quote=quote, timestep="15 minute")
    assert min_key == (spy, quote)

    # 1-day request must NOT find the minute data (forces fresh day-bar fetch)
    day_key = ds.find_asset_in_data_store(spy, quote=quote, timestep="day")
    assert day_key is None, (
        "With allow_day_resampling=False (ThetaData), minute data must not satisfy day requests."
    )


def test_find_asset_in_data_store_stock_vs_crypto_symmetry_allow_resampling():
    """
    With allow_day_resampling=True, stocks and crypto behave symmetrically:
    both can use minute data to satisfy day requests.
    """
    usd = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Crypto: minute data satisfies day requests
    btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    btc_minute = Data(btc, _minute_df(), timestep="minute", quote=usd)
    ds_crypto = _make_ds({(btc, usd): btc_minute}, allow_day_resampling=True)
    assert ds_crypto.find_asset_in_data_store((btc, usd), quote=None, timestep="day") == (btc, usd), (
        "Crypto with minute data should satisfy day requests."
    )

    # Stock: minute data also satisfies day requests with allow_day_resampling=True
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    spy_minute = Data(spy, _minute_df(), timestep="minute", quote=usd)
    ds_stock = _make_ds({(spy, usd): spy_minute}, allow_day_resampling=True)
    assert ds_stock.find_asset_in_data_store(spy, quote=usd, timestep="day") == (spy, usd), (
        "Stock with minute data should also satisfy day requests when allow_day_resampling=True."
    )


def test_find_asset_in_data_store_stock_vs_crypto_no_resampling():
    """
    With allow_day_resampling=False (ThetaData), BOTH stocks AND crypto/other types
    must use exact timestep matching — minute data never satisfies a day request.
    This is stricter than the old stock/index-only guard but correct for ThetaData
    because it stores data under separate (asset, quote, timestep) canonical keys.
    """
    usd = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Crypto: minute data must NOT satisfy day requests with allow_day_resampling=False
    btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    btc_minute = Data(btc, _minute_df(), timestep="minute", quote=usd)
    ds_crypto = _make_ds({(btc, usd): btc_minute}, allow_day_resampling=False)
    assert ds_crypto.find_asset_in_data_store((btc, usd), quote=None, timestep="day") is None, (
        "With allow_day_resampling=False, even crypto minute data must not satisfy day requests."
    )

    # Stock: same
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    spy_minute = Data(spy, _minute_df(), timestep="minute", quote=usd)
    ds_stock = _make_ds({(spy, usd): spy_minute}, allow_day_resampling=False)
    assert ds_stock.find_asset_in_data_store(spy, quote=usd, timestep="day") is None


def test_find_asset_in_data_store_index_minute_data_day_request_allow_resampling():
    """
    INDEX assets behave the same as stocks: with allow_day_resampling=True they can
    use minute data for day requests; with allow_day_resampling=False they cannot.
    """
    spx = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    minute_data = Data(spx, _minute_df(), timestep="minute", quote=quote)

    # Minute requests always work regardless of flag
    ds = _make_ds({(spx, quote): minute_data}, allow_day_resampling=True)
    assert ds.find_asset_in_data_store(spx, quote=quote, timestep="minute") == (spx, quote)

    # Day request: allowed when allow_day_resampling=True
    assert ds.find_asset_in_data_store(spx, quote=quote, timestep="day") == (spx, quote)


def test_find_asset_in_data_store_index_minute_data_day_request_no_resampling():
    """
    The same INDEX guard: with allow_day_resampling=False, day requests are blocked.
    """
    spx = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    minute_data = Data(spx, _minute_df(), timestep="minute", quote=quote)

    ds = _make_ds({(spx, quote): minute_data}, allow_day_resampling=False)

    # Minute requests still work for index assets
    assert ds.find_asset_in_data_store(spx, quote=quote, timestep="minute") == (spx, quote)

    # Day requests are blocked
    assert ds.find_asset_in_data_store(spx, quote=quote, timestep="day") is None


def test_find_asset_in_data_store_native_day_data_always_found():
    """
    Regardless of allow_day_resampling, native day data must always satisfy day requests.
    """
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_data = Data(spy, _day_df(), timestep="day", quote=quote)

    for flag in (True, False):
        ds = _make_ds({(spy, quote): day_data}, allow_day_resampling=flag)
        assert ds.find_asset_in_data_store(spy, quote=quote, timestep="day") == (spy, quote), (
            f"Native day data must always satisfy day requests (allow_day_resampling={flag})."
        )


def test_find_asset_in_data_store_day_data_never_satisfies_minute_request():
    """
    Day data must never satisfy minute requests, regardless of allow_day_resampling.
    """
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    day_data = Data(spy, _day_df(), timestep="day", quote=quote)

    for flag in (True, False):
        ds = _make_ds({(spy, quote): day_data}, allow_day_resampling=flag)
        assert ds.find_asset_in_data_store(spy, quote=quote, timestep="minute") is None, (
            f"Day data must never satisfy minute requests (allow_day_resampling={flag})."
        )


# ---------------------------------------------------------------------------
# End-to-end regression test: 15m request → 1d request on a stock.
#
# Simulates the exact live-backtest scenario described in the bug report using
# PandasData._pull_source_symbol_bars directly (no live network calls).
# ---------------------------------------------------------------------------

def _make_full_pandas_ds(day_df_data: pd.DataFrame, minute_df_data: pd.DataFrame,
                         spy: Asset, quote: Asset, allow_day_resampling: bool) -> PandasData:
    """
    Build a PandasData instance that has BOTH a minute dataset and a day dataset in its
    store.  This mirrors the situation after a first 15m get_historical_prices call has
    populated minute data, and then a day call is made.
    """
    minute_data = Data(spy, minute_df_data, timestep="minute", quote=quote)
    return PandasData(
        datetime_start=_START,
        datetime_end=_END,
        pandas_data=[minute_data],
        allow_day_resampling=allow_day_resampling,
    )


def test_get_historical_prices_day_after_minute_cache_allow_resampling():
    """
    Regression: after a 15m request has populated minute data for a stock, a subsequent
    1d get_historical_prices call must return non-None Bars when allow_day_resampling=True.

    This is the exact failure scenario from the bug report (Polygon path).
    """
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Build a minute DataFrame covering a full trading day (390 1-min bars)
    idx = pd.date_range("2025-01-02 09:31", periods=390, freq="1min", tz="America/New_York")
    price = [450.0 + i * 0.01 for i in range(390)]
    minute_df = pd.DataFrame(
        {"open": price, "high": price, "low": price, "close": price, "volume": [1000] * 390},
        index=idx,
    )

    minute_data = Data(spy, minute_df, timestep="minute", quote=quote)

    ds = PandasData(
        datetime_start=_START,
        datetime_end=_END,
        pandas_data=[minute_data],
        allow_day_resampling=True,  # Polygon / base PandasData default
    )

    # Simulate the backtest clock sitting at end of the trading day.
    # get_datetime() is monkey-patched so _pull_source_symbol_bars gets the right "now"
    # without having to advance the backtesting iterator.
    now = pd.Timestamp("2025-01-02 16:00:00", tz="America/New_York").to_pydatetime()
    ds.get_datetime = lambda: now

    response = ds._pull_source_symbol_bars(spy, length=1, timestep="day", quote=quote)
    assert response is not None, (
        "With allow_day_resampling=True and a full trading day of minute data in the store, "
        "a day-bar request must return non-None (resampled) bars."
    )


def test_get_historical_prices_day_after_minute_cache_no_resampling():
    """
    With allow_day_resampling=False (ThetaData), the same scenario must return None from
    _pull_source_symbol_bars so that the caller is forced to fetch native day bars.
    """
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    idx = pd.date_range("2025-01-02 09:31", periods=390, freq="1min", tz="America/New_York")
    price = [450.0 + i * 0.01 for i in range(390)]
    minute_df = pd.DataFrame(
        {"open": price, "high": price, "low": price, "close": price, "volume": [1000] * 390},
        index=idx,
    )

    minute_data = Data(spy, minute_df, timestep="minute", quote=quote)

    ds = PandasData(
        datetime_start=_START,
        datetime_end=_END,
        pandas_data=[minute_data],
        allow_day_resampling=False,  # ThetaData-style: exact match required
    )

    now = pd.Timestamp("2025-01-02 16:00:00", tz="America/New_York").to_pydatetime()
    ds.get_datetime = lambda: now

    response = ds._pull_source_symbol_bars(spy, length=1, timestep="day", quote=quote)
    assert response is None, (
        "With allow_day_resampling=False (ThetaData), minute data must not satisfy a day request. "
        "_pull_source_symbol_bars must return None so the caller fetches native day bars."
    )


# ---------------------------------------------------------------------------
# Verify that PandasData.__init__ correctly initialises the flag.
# ---------------------------------------------------------------------------

def test_pandas_data_default_allow_day_resampling_is_true():
    """PandasData defaults allow_day_resampling=True (Polygon / base PandasData behaviour)."""
    from datetime import timezone

    ds = PandasData(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 5, tzinfo=timezone.utc),
        pandas_data={},
    )
    assert ds.allow_day_resampling is True


def test_pandas_data_explicit_false_allow_day_resampling():
    """allow_day_resampling=False can be passed explicitly to PandasData."""
    from datetime import timezone

    ds = PandasData(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 5, tzinfo=timezone.utc),
        pandas_data={},
        allow_day_resampling=False,
    )
    assert ds.allow_day_resampling is False


def test_thetadata_backtesting_sets_allow_day_resampling_false(monkeypatch):
    """ThetaDataBacktestingPandas must hard-set allow_day_resampling=False."""
    import lumibot.tools.thetadata_helper as thetadata_helper
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
    from datetime import timezone

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_kw: None)
    monkeypatch.setattr(thetadata_helper, "reset_theta_terminal_tracking", lambda *_a, **_kw: None)

    ds = ThetaDataBacktestingPandas(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 5, tzinfo=timezone.utc),
        username="test",
        password="test",
    )
    assert ds.allow_day_resampling is False, (
        "ThetaDataBacktestingPandas must set allow_day_resampling=False to preserve "
        "split-adjusted day-bar integrity."
    )


def test_polygon_backtesting_sets_allow_day_resampling_true(monkeypatch):
    """PolygonDataBacktesting must hard-set allow_day_resampling=True."""
    from lumibot.backtesting.polygon_backtesting import PolygonDataBacktesting
    from lumibot.tools.polygon_helper import PolygonClient
    from datetime import timezone

    monkeypatch.setattr(PolygonClient, "create", lambda api_key: None)

    ds = PolygonDataBacktesting(
        datetime_start=datetime(2025, 1, 1, tzinfo=timezone.utc),
        datetime_end=datetime(2025, 1, 5, tzinfo=timezone.utc),
        api_key="test",
    )
    assert ds.allow_day_resampling is True, (
        "PolygonDataBacktesting must set allow_day_resampling=True so that cached minute "
        "data can satisfy day requests via Data.get_bars() resampling."
    )
