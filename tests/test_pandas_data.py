from datetime import datetime, timedelta

import pandas as pd
import pytz

from lumibot.data_sources import PandasData
from lumibot.entities import Asset
from lumibot.entities.data import Data

from tests.fixtures import pandas_data_fixture


class TestPandasData:

    def test_pandas_data_fixture(self, pandas_data_fixture):
        assert pandas_data_fixture is not None

    def test_spy_has_dividends(self, pandas_data_fixture):
        spy = pandas_data_fixture[0]
        expected_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividend",
        ]
        assert spy.df.columns.tolist() == expected_columns

    def test_get_start_datetime_and_ts_unit(self):
        start = datetime(2023, 3, 25)
        end = datetime(2023, 4, 5)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        length = 30
        timestep = '1day'
        start_datetime, ts_unit = data_source.get_start_datetime_and_ts_unit(
            length,
            timestep,
            start,
            start_buffer=timedelta(days=0)  # just test our math
        )
        extra_padding_days = (length // 5) * 3
        expected_datetime = datetime(2023, 3, 25) - timedelta(days=length + extra_padding_days)
        assert start_datetime == expected_datetime

    def test_data_get_quote_handles_missing_bid_ask(self):
        idx = pd.date_range("2024-01-01", periods=1, freq="D")
        df = pd.DataFrame(
            {
                "open": [1.0],
                "high": [1.2],
                "low": [0.9],
                "close": [1.1],
                "volume": [1000],
            },
            index=idx,
        )
        asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
        data = Data(asset=asset, df=df, quote=quote, timestep="day")

        quote_dict = data.get_quote(data.datetime_start)

        assert quote_dict["bid"] is None
        assert quote_dict["ask"] is None
        assert quote_dict["open"] == 1.0

    def test_get_last_price_returns_none_for_non_positive_prices(self):
        idx = pd.date_range("2024-01-01", periods=2, freq="min", tz="UTC")
        df = pd.DataFrame(
            {
                "open": [100.0, 0.0],
                "high": [101.0, 0.0],
                "low": [99.0, 0.0],
                "close": [100.5, 0.0],
                "volume": [1000, 500],
            },
            index=idx,
        )
        asset = Asset("CVNA", asset_type=Asset.AssetType.STOCK)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
        data = Data(asset=asset, df=df, quote=quote, timestep="minute")

        source = PandasData(
            datetime_start=idx[0],
            datetime_end=idx[-1],
            pandas_data=[data],
        )
        source.load_data()
        source._datetime = idx[-1]

        assert source.get_last_price(asset) is None

    def test_get_price_snapshot_returns_metadata(self):
        idx = pd.date_range("2024-05-01 09:30", periods=2, freq="min", tz="America/New_York")
        df = pd.DataFrame(
            {
                "open": [10.0, 10.5],
                "high": [10.2, 10.7],
                "low": [9.9, 10.4],
                "close": [10.1, 10.6],
                "volume": [1_000, 900],
                "bid": [9.95, 10.3],
                "ask": [10.15, 10.55],
                "last_trade_time": idx,
                "last_bid_time": idx,
                "last_ask_time": idx,
            },
            index=idx,
        )
        asset = Asset("CVNA", asset_type=Asset.AssetType.STOCK)
        data = Data(asset=asset, df=df, timestep="minute")

        snapshot = data.get_price_snapshot(idx[-1])
        assert snapshot["open"] == 10.5
        assert snapshot["bid"] == 10.3
        assert snapshot["ask"] == 10.55
        assert snapshot["last_trade_time"] == idx[-1].to_pydatetime()
        assert snapshot["last_bid_time"] == idx[-1].to_pydatetime()
        assert snapshot["last_ask_time"] == idx[-1].to_pydatetime()


class TestGetHistoricalPricesMinuteToDayRegression:
    """
    Integration-level regression tests for the stock/index guard in _accepts_timestep
    (pandas_data.py lines ~416-426) that silently blocks minute-to-day resampling.

    The guard reads:
        if requested_unit == "day":
            if requested_asset_type in {"stock", "index"}:
                return data_ts == "day"   # minute data REJECTED for stocks

    This causes get_historical_prices(asset, timestep="1 day") to return None whenever
    only minute-level data is present for a stock/index asset, even though data.py's
    get_bars() (lines 1307-1312) supports minute→day resampling.

    Observed failure scenario (live backtest):
      1. get_historical_prices(asset, 5, timestep="15 minute") – works fine
      2. get_historical_prices(asset, 3, timestep="1 day")     – returns None (BUG)
    """

    @staticmethod
    def _make_minute_data(asset: Asset, quote: Asset, n_bars: int = 120) -> Data:
        """
        Build n_bars of 1-minute bars starting at NYSE open on 2025-01-02.
        Uses UTC so no tz-localize issues arise inside Data.__init__.
        """
        idx = pd.date_range("2025-01-02 14:30:00", periods=n_bars, freq="1min", tz="UTC")
        price = [100.0 + i * 0.01 for i in range(n_bars)]
        df = pd.DataFrame(
            {
                "open": price,
                "high": [p + 0.5 for p in price],
                "low": [p - 0.5 for p in price],
                "close": price,
                "volume": [1000] * n_bars,
            },
            index=idx,
        )
        data = Data(asset=asset, df=df, timestep="minute", quote=quote)
        # Prime datalines so data.get_bars() is callable if the lookup ever succeeds.
        data.repair_times_and_fill(data.df.index)
        return data

    @staticmethod
    def _make_ds(minute_data: Data, asset: Asset, quote: Asset, current_dt) -> PandasData:
        """
        Build a PandasData instance using the real constructor with the supplied
        minute-level Data object in its store and _datetime set to current_dt.

        Uses the real constructor (not PandasData.__new__) so that all instance
        attributes — including allow_day_resampling — are properly initialised.
        Overrides _datetime after construction to position the backtest clock.
        """
        tz = pytz.timezone("America/New_York")
        start = datetime(2025, 1, 2, 9, 30, tzinfo=tz)
        end = datetime(2025, 1, 3, 16, 0, tzinfo=tz)
        ds = PandasData(
            datetime_start=start,
            datetime_end=end,
            pandas_data=[minute_data],
        )
        ds._datetime = current_dt
        return ds

    def test_15m_request_finds_minute_data_store(self):
        """
        Baseline: find_asset_in_data_store must find the minute store for a
        15-minute intraday request.  This is the call that works before the day
        request fails.
        """
        spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
        minute_data = self._make_minute_data(spy, quote)
        current_dt = minute_data.df.index[60].to_pydatetime()
        ds = self._make_ds(minute_data, spy, quote, current_dt)

        found = ds.find_asset_in_data_store(spy, quote=quote, timestep="15 minute")
        assert found == (spy, quote), (
            "15-minute request must locate the minute data store for SPY."
        )

    def test_1day_request_returns_none_for_stock_with_only_minute_data(self):
        """
        FIX VERIFIED: get_historical_prices with timestep="1 day" no longer returns None
        when a STOCK asset has only minute data in the store.

        With allow_day_resampling=True (the PandasData default), find_asset_in_data_store
        accepts the minute dataset for a day request and Data.get_bars() resamples
        minute → day bars, so the result is a non-None Bars object.
        """
        spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
        minute_data = self._make_minute_data(spy, quote, n_bars=120)
        current_dt = minute_data.df.index[100].to_pydatetime()
        ds = self._make_ds(minute_data, spy, quote, current_dt)

        result_day = ds.get_historical_prices(spy, length=1, timestep="1 day", quote=quote)

        assert result_day is not None, (
            "FIX VERIFIED: get_historical_prices must return resampled day bars for a "
            "stock asset when only minute data is in the store and allow_day_resampling=True."
        )

    def test_1day_request_after_15m_request_same_asset(self):
        """
        FIX VERIFIED: mirrors the exact live-backtest failure sequence.

        Step 1: call get_historical_prices with "15 minute" → succeeds (not None).
        Step 2: call get_historical_prices with "1 day"     → now also succeeds (not None).

        With allow_day_resampling=True, the second call finds the cached minute data and
        Data.get_bars() resamples it to daily bars instead of returning None.
        """
        spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
        minute_data = self._make_minute_data(spy, quote, n_bars=120)
        current_dt = minute_data.df.index[100].to_pydatetime()
        ds = self._make_ds(minute_data, spy, quote, current_dt)

        # Step 1 – 15-minute bars: find_asset_in_data_store succeeds
        ds.get_historical_prices(spy, length=5, timestep="15 minute", quote=quote)
        assert (spy, quote) in ds._find_asset_in_data_store_cache.values(), (
            "After the 15-minute call the lookup cache must contain the (spy, quote) key."
        )

        # Step 2 – 1-day bars: now returns resampled bars instead of None (FIX)
        result_day = ds.get_historical_prices(spy, length=1, timestep="1 day", quote=quote)
        assert result_day is not None, (
            "FIX VERIFIED: after a successful 15-minute call, a subsequent 1-day call on "
            "the same stock must return resampled day bars (allow_day_resampling=True)."
        )

    def test_crypto_1day_request_with_only_minute_data_not_blocked(self):
        """
        Symmetry test: crypto assets behave the same as stocks with allow_day_resampling=True —
        day requests on a crypto asset with only minute data are satisfied via resampling.

        Both stock and crypto assets use the same allow_day_resampling flag path, so this
        test verifies the uniform behavior post-fix.
        """
        btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
        usd = Asset("USD", asset_type=Asset.AssetType.FOREX)
        minute_data = self._make_minute_data(btc, usd, n_bars=120)
        current_dt = minute_data.df.index[100].to_pydatetime()
        ds = self._make_ds(minute_data, btc, usd, current_dt)

        # For crypto, find_asset_in_data_store must find the minute data for day requests.
        found = ds.find_asset_in_data_store((btc, usd), quote=None, timestep="day")
        assert found == (btc, usd), (
            "Crypto with minute data must satisfy day requests (allow_day_resampling=True)."
        )
