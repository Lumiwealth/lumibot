from __future__ import annotations

import pandas as pd

from lumibot.data_sources.pandas_data import PandasData
from lumibot.entities import Asset, Data


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


def test_find_asset_in_data_store_does_not_return_daily_for_minute_requests():
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    daily = Data(base, _day_df(), timestep="day", quote=quote)

    ds = PandasData.__new__(PandasData)
    ds._data_store = {(base, quote): daily}  # type: ignore[attr-defined]
    ds._find_asset_in_data_store_cache = {}

    # Simulate how crypto/forex often passes assets as a tuple while quote=None.
    asset_tuple = (base, quote)
    assert ds.find_asset_in_data_store(asset_tuple, quote=None, timestep="minute") is None
    assert ds.find_asset_in_data_store(asset_tuple, quote=None, timestep="day") == (base, quote)


def test_find_asset_in_data_store_allows_minute_data_to_satisfy_day_requests():
    """Crypto assets with only minute data can still satisfy day-bar requests (unchanged behavior)."""
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    minute = Data(base, _minute_df(), timestep="minute", quote=quote)

    ds = PandasData.__new__(PandasData)
    ds._data_store = {(base, quote): minute}  # type: ignore[attr-defined]
    ds._find_asset_in_data_store_cache = {}

    asset_tuple = (base, quote)
    assert ds.find_asset_in_data_store(asset_tuple, quote=None, timestep="day") == (base, quote)
    assert ds.find_asset_in_data_store(asset_tuple, quote=None, timestep="minute") == (base, quote)


# ---------------------------------------------------------------------------
# Regression tests for the stock/index guard introduced in _accepts_timestep
# (pandas_data.py lines ~416-426):
#
#   if requested_unit == "day":
#       if requested_asset_type in {"stock", "index"}:
#           return data_ts == "day"   # <-- minute data REJECTED for stocks
#       return data_ts in {"day", "minute"}
#
# Effect: a strategy that loads only minute data for a stock (e.g., via a 15m bar
# fetch) and then calls get_historical_prices(..., timestep="1 day") receives None,
# even though data.py's get_bars() (lines 1307-1312) fully supports minute-to-day
# resampling.
# ---------------------------------------------------------------------------


def test_find_asset_in_data_store_stock_minute_data_day_request_returns_none():
    """
    BUG DEMONSTRATION: find_asset_in_data_store returns None when a STOCK asset
    has only minute data in the store and a day-bar request is made.

    This is the root cause of the silent get_historical_prices failure observed in
    live backtests that call get_historical_prices(asset, timestep="15 minute") first
    and then get_historical_prices(asset, timestep="1 day").

    The 15-minute call caches minute data in _data_store for the stock. The
    subsequent day call is then gated out by the stock guard in _accepts_timestep
    before data.get_bars() is ever reached.

    NOTE: When the bug is fixed (the stock guard is removed or relaxed), the assertion
    ``assert day_key is None`` below will fail.  At that point change it to
    ``assert day_key == (spy, quote)`` and add a test that the returned Bars are
    non-empty daily bars.
    """
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    minute_data = Data(spy, _minute_df(), timestep="minute", quote=quote)

    ds = PandasData.__new__(PandasData)
    ds._data_store = {(spy, quote): minute_data}
    ds._find_asset_in_data_store_cache = {}

    # 15-minute request: minute data IS accepted → key is found (works correctly)
    min_key = ds.find_asset_in_data_store(spy, quote=quote, timestep="15 minute")
    assert min_key == (spy, quote), (
        "15-minute request must find the minute data store so the strategy can read intraday bars."
    )

    # 1-day request: minute data is NOT accepted for stocks → None returned (BUG)
    # data.py get_bars() would happily resample minute → day if it were reached, but it
    # never is because _accepts_timestep blocks the lookup at pandas_data.py:424-425.
    day_key = ds.find_asset_in_data_store(spy, quote=quote, timestep="day")
    assert day_key is None, (
        "BUG: find_asset_in_data_store returns None for stock + minute data + day request. "
        "Fix: relax the stock guard in _accepts_timestep so minute data can satisfy day "
        "requests and reach data.get_bars() minute-to-day resampling."
    )


def test_find_asset_in_data_store_stock_vs_crypto_asymmetry():
    """
    Demonstrates the stock/crypto asymmetry introduced by the stock guard.

    Crypto (BTC) with minute data → day request returns the key (unchanged).
    Stock (SPY) with minute data → day request returns None (broken by the guard).
    """
    usd = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # --- Crypto: minute data still satisfies day requests ---
    btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    btc_minute = Data(btc, _minute_df(), timestep="minute", quote=usd)
    ds_crypto = PandasData.__new__(PandasData)
    ds_crypto._data_store = {(btc, usd): btc_minute}
    ds_crypto._find_asset_in_data_store_cache = {}

    assert ds_crypto.find_asset_in_data_store((btc, usd), quote=None, timestep="day") == (btc, usd), (
        "Crypto with minute data should still satisfy day requests."
    )

    # --- Stock: minute data NO LONGER satisfies day requests (the bug) ---
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    spy_minute = Data(spy, _minute_df(), timestep="minute", quote=usd)
    ds_stock = PandasData.__new__(PandasData)
    ds_stock._data_store = {(spy, usd): spy_minute}
    ds_stock._find_asset_in_data_store_cache = {}

    assert ds_stock.find_asset_in_data_store(spy, quote=usd, timestep="day") is None, (
        "BUG: Stock with minute data should also satisfy day requests (same as crypto), "
        "but the stock guard in _accepts_timestep blocks this."
    )


def test_find_asset_in_data_store_index_minute_data_day_request_returns_none():
    """
    The same stock guard also blocks INDEX assets from using minute data for day requests.
    """
    spx = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    minute_data = Data(spx, _minute_df(), timestep="minute", quote=quote)

    ds = PandasData.__new__(PandasData)
    ds._data_store = {(spx, quote): minute_data}
    ds._find_asset_in_data_store_cache = {}

    # Minute requests still work for index assets
    assert ds.find_asset_in_data_store(spx, quote=quote, timestep="minute") == (spx, quote)

    # Day requests are blocked for index assets too (same guard as stocks)
    assert ds.find_asset_in_data_store(spx, quote=quote, timestep="day") is None


