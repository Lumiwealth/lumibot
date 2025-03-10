from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import AlpacaBacktestingNew, PandasDataBacktesting, BacktestingBroker
from lumibot.brokers import Broker
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.entities import Asset

from tests.fixtures import (
    BuyOnceTestStrategy,
    GetHistoricalTestStrategy
)

if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


import pytest
from datetime import datetime
from zoneinfo import ZoneInfo
from lumibot.entities import Asset


class TestAssetKey:

    def _create_data_source(
            self,
            *,
            datetime_start=datetime(2025, 1, 1),
            datetime_end=datetime(2025, 1, 31),
            config=ALPACA_TEST_CONFIG,
            timestep="day",
            tzinfo=ZoneInfo("America/New_York"),
            refresh_cache=False,
            market="NYSE",
            warm_up_trading_days: int = 0,
            auto_adjust: bool = True,
    ):
        """
        Create an instance of AlpacaBacktestingNew with default or provided parameters.
        """
        return AlpacaBacktestingNew(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            config=config,
            timestep=timestep,
            tzinfo=tzinfo,
            refresh_cache=refresh_cache,
            market=market,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

    def test_create_data_source(self):
        data_source = self._create_data_source()
        assert isinstance(data_source, AlpacaBacktestingNew)

    def test_basic_key_generation_crypto(self):
        datetime_start = datetime(2025, 1, 1)
        datetime_end = datetime(2025, 2, 1)
        base_asset = Asset("BTC", asset_type='crypto')
        quote_asset = Asset("USD", asset_type='forex')
        market = "24/7"
        tzinfo = ZoneInfo("America/Chicago")
        timestep = "day"
        
        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            tzinfo=tzinfo,
            timestep=timestep,
        )

        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
            market=market,
            tzinfo=tzinfo,
            timestep=timestep,
            data_datetime_start=datetime_start,
            data_datetime_end=datetime_end
        )

        expected = "BTC-CRYPTO_USD-FOREX_24-7_DAY_AMERICA-CHICAGO_AA_2025-01-01_2025-02-01"
        assert key == expected

    def test_basic_key_generation_stock(self):
        datetime_start = datetime(2025, 1, 1)
        datetime_end = datetime(2025, 2, 1)
        base_asset = Asset("AAPL", asset_type='stock')
        quote_asset = Asset("USD", asset_type='forex')
        market = "NYSE"
        tzinfo = ZoneInfo("America/New_York")
        timestep = "day"

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            tzinfo=tzinfo,
            timestep=timestep,
        )

        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
            market=market,
            tzinfo=tzinfo,
            timestep=timestep,
            data_datetime_start=datetime_start,
            data_datetime_end=datetime_end
        )

        expected = "AAPL-STOCK_USD-FOREX_NYSE_DAY_AMERICA-NEW-YORK_AA_2025-01-01_2025-02-01"
        assert key == expected

    def test_with_auto_adjust(self):
        base_asset = Asset("AAPL", asset_type="stock")
        quote_asset = Asset("USD", asset_type="forex")
        market = "NYSE"
        tz = ZoneInfo("America/New_York")
        timestep = "day"
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        auto_adjust = True

        data_source = self._create_data_source(
            datetime_start=start_date,
            datetime_end=end_date,
            market=market,
            tzinfo=tz,
            timestep=timestep,
            auto_adjust=auto_adjust
        )

        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
            market=market,
            tzinfo=tz,
            timestep=timestep,
            data_datetime_start=start_date,
            data_datetime_end=end_date,
            auto_adjust=auto_adjust
        )

        expected = "AAPL-STOCK_USD-FOREX_NYSE_DAY_AMERICA-NEW-YORK_AA_2023-01-01_2023-12-31"
        assert key == expected

    def test_empty_market_false_auto_adjust(self):
        base_asset = Asset("SPY", asset_type="stock")
        quote_asset = Asset("USD", asset_type="forex")
        tz = ZoneInfo("America/New_York")
        timestep = "minute"
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        auto_adjust = False

        data_source = self._create_data_source(
            datetime_start=start_date,
            datetime_end=end_date,
            tzinfo=tz,
            timestep=timestep,
            auto_adjust=auto_adjust
        )

        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
            tzinfo=tz,
            timestep=timestep,
            data_datetime_start=start_date,
            data_datetime_end=end_date,
            auto_adjust=auto_adjust
        )

        expected = "SPY-STOCK_USD-FOREX_NYSE_MINUTE_AMERICA-NEW-YORK_2023-01-01_2023-12-31"
        assert key == expected

    def test_different_timezones(self):
        base_asset = Asset("BTC", asset_type="crypto")
        quote_asset = Asset("EUR", asset_type="forex")
        market = "24/7"
        timezones = [
            ZoneInfo("Asia/Tokyo"),
            ZoneInfo("Europe/London"),
            ZoneInfo("US/Pacific")
        ]
        timestep = "day"
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 12, 31)
        auto_adjust = True

        data_source = self._create_data_source(
            datetime_start=start_date,
            datetime_end=end_date,
            market=market,
            timestep=timestep
        )

        expected_results = [
            "BTC-CRYPTO_EUR-FOREX_24-7_DAY_ASIA-TOKYO_AA_2023-01-01_2023-12-31",
            "BTC-CRYPTO_EUR-FOREX_24-7_DAY_EUROPE-LONDON_AA_2023-01-01_2023-12-31",
            "BTC-CRYPTO_EUR-FOREX_24-7_DAY_US-PACIFIC_AA_2023-01-01_2023-12-31"
        ]

        for tz, expected in zip(timezones, expected_results):
            key = data_source._get_asset_key(
                base_asset=base_asset,
                quote_asset=quote_asset,
                market=market,
                tzinfo=tz,
                timestep=timestep,
                data_datetime_start=start_date,
                data_datetime_end=end_date,
                auto_adjust=auto_adjust
            )
            assert key == expected

    def test_refresh_cache(self):
        """Test that refresh_cache properly refreshes data and uses the refreshed_keys dict"""

        datetime_start = datetime(2024, 1, 1)
        datetime_end = datetime(2024, 2, 1)
        base_asset = Asset("BTC", asset_type='crypto')
        quote_asset = Asset("USD", asset_type='forex')
        market = "24/7"
        tzinfo = ZoneInfo("America/Chicago")
        timestep = "day"
        refresh_cache = True

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            tzinfo=tzinfo,
            timestep=timestep,
            refresh_cache=refresh_cache
        )

        # Mock the _download_and_cache_ohlcv_data method to track calls
        original_download = data_source._download_and_cache_ohlcv_data
        download_calls = []

        def mock_download(**kwargs):
            download_calls.append(kwargs)
            return original_download(**kwargs)

        data_source._download_and_cache_ohlcv_data = mock_download

        # First call should trigger a download
        data_source.get_historical_prices(base_asset, length=1, quote=quote_asset)
        assert len(download_calls) == 1

        # Second call should not trigger a download (should use refreshed_keys)
        data_source.get_historical_prices(base_asset, length=1, quote=quote_asset)
        assert len(download_calls) == 1

        # Verify the key is in refreshed_keys
        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset
        )
        assert key in data_source._refreshed_keys

    def test_no_refresh_cache(self):
        """Test that when refresh_cache is False, data is loaded from cache when available"""

        datetime_start = datetime(2024, 1, 1)
        datetime_end = datetime(2024, 2, 1)
        base_asset = Asset("BTC", asset_type='crypto')
        quote_asset = Asset("USD", asset_type='forex')
        market = "24/7"
        tzinfo = ZoneInfo("America/Chicago")
        timestep = "day"
        refresh_cache = False

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            tzinfo=tzinfo,
            timestep=timestep,
            refresh_cache=refresh_cache
        )
        # Mock both download and load methods
        download_calls = []
        load_calls = []

        original_download = data_source._download_and_cache_ohlcv_data
        original_load = data_source._load_ohlcv_into_data_store

        def mock_download(**kwargs):
            download_calls.append(kwargs)
            return original_download(**kwargs)

        def mock_load(key):
            load_calls.append(key)
            return original_load(key)

        data_source._download_and_cache_ohlcv_data = mock_download
        data_source._load_ohlcv_into_data_store = mock_load

        # First call will try to load from cache first
        data_source.get_historical_prices(base_asset, length=1, quote=quote_asset)
        assert len(load_calls) == 1

        # Verify key is not in refreshed_keys
        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset
        )
        assert key not in data_source._refreshed_keys


class TestAlpacaBacktestingNew:
    """Tests for the AlpacaBacktestingNew datasource class as well as using it in strategies."""

    def test_bo_single_stock_day_bars_america_new_york(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 17)
        sleeptime = '1D'
        timestep = 'day'
        market = 'NYSE'
        tzinfo = ZoneInfo("America/New_York")
        refresh_cache = False
        auto_adjust = True
        warm_up_trading_days = 0

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": sleeptime,
                "market": market
            },

            # AlpacaBacktestingNew kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T00:00:00-05:00'
        key = list(data_source._data_store.keys())[0]
        assert key == 'AMZN-STOCK_USD-FOREX_NYSE_DAY_AMERICA-NEW-YORK_AA_2025-01-13_2025-01-17'
        df = list(data_source._data_store.values())[0]
        assert not df.empty
        assert len(df.index) == 5

        # daily bars are NORMALLY indexed at midnight (the open of the bar).
        # To enable lumibot to use the open price of the bar for the get_last_price and fills,
        # the alpaca backtester adjusts daily bars to the open bar of the market.
        assert df.index[0].isoformat() == "2025-01-13T09:30:00-05:00"
        assert df.index[-1].isoformat() == "2025-01-17T09:30:00-05:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-15T09:30:00-05:00'
        assert strategy.num_trading_iterations == 3

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        assert tracker['last_price'] == Decimal('218.06')  # Open of '2025-01-13T09:30:00-05:00'
        assert tracker["avg_fill_price"] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

    # @pytest.mark.skip()
    def test_bo_single_crypto_day_bars_america_chicago(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 17)
        sleeptime = '1D'
        timestep = 'day'
        market = '24/7'
        tzinfo = ZoneInfo("America/Chicago")  # Alpaca crypto daily bars are natively indexed at midnight central time
        refresh_cache = False
        auto_adjust = True
        warm_up_trading_days = 0

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('BTC', asset_type='crypto'),
                "sleeptime": sleeptime,
                "market": market
            },

            # AlpacaBacktestingNew kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-13T00:00:00-06:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T00:00:00-06:00'
        key = list(data_source._data_store.keys())[0]
        assert key == 'BTC-CRYPTO_USD-FOREX_24-7_DAY_AMERICA-CHICAGO_AA_2025-01-13_2025-01-17'
        df = list(data_source._data_store.values())[0]
        assert not df.empty
        assert len(df.index) == 5

        assert df.index[0].isoformat() == "2025-01-13T00:00:00-06:00"
        assert df.index[-1].isoformat() == "2025-01-17T00:00:00-06:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00-06:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T00:00:00-06:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-15T00:00:00-06:00'
        assert strategy.num_trading_iterations == 3

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T00:00:00-06:00'

        assert tracker['last_price'] == Decimal('94066.35')  # Open of '2025-01-13T00:00:00-06:00'
        assert tracker["avg_fill_price"] == 94066.35  # Open of '2025-01-13T00:00:00-06:00'

    def test_bo_single_stock_minute_bars_america_new_york_1d_sleeptime(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 17)
        sleeptime = '1D'
        timestep = 'minute'
        market = 'NYSE'
        tzinfo = ZoneInfo("America/New_York")
        refresh_cache = False
        auto_adjust = True
        warm_up_trading_days = 0

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": sleeptime,
                "market": market
            },

            # AlpacaBacktestingNew kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-05:00'
        key = list(data_source._data_store.keys())[0]
        assert key == 'AMZN-STOCK_USD-FOREX_NYSE_MINUTE_AMERICA-NEW-YORK_AA_2025-01-13_2025-01-17'
        df = list(data_source._data_store.values())[0]
        assert not df.empty
        # assert len(df.index) == 6.5 * 60 * 5  # after we align with trading hours and fill missing bars...

        # assert df.index[0].isoformat() == "2025-01-13T09:30:00-05:00"
        # assert df.index[-1].isoformat() == "2025-01-17T09:30:00-05:00"

        # Trading strategy tests
        # assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        # assert strategy.trading_iterations[-1].isoformat() == '2025-01-15T09:30:00-05:00'
        # assert strategy.num_trading_iterations == 3

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        assert tracker['last_price'] == Decimal('218.06')  # Open of '2025-01-13T09:30:00-05:00'
        assert tracker["avg_fill_price"] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

    def test_bo_single_stock_minute_bars_america_new_york_extended_hours(self):
        # TODO We need an extended hours market to make this test work
        pass

    def test_bo_single_stock_minute_bars_america_new_york_with_30m_sleeptime(self):
        tickers = "AMZN"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 14)
        timestep = 'minute'
        refresh_cache = False
        tzinfo = ZoneInfo("America/New_York")

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": "60M",
                "market": "NYSE"
            },

            # AlpacaBacktestingNew kwargs
            tickers=tickers,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == '2025-01-13T00:00:00-05:00'
        assert data_source.datetime_end.isoformat() == "2025-01-13T23:59:00-05:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty

        # this assumes we get a minute bar for every bar in normal trading hours.
        assert len(df.index) == 6.5 * 60

        # Regular trading opens at 930am EDT
        assert df.index[0].isoformat() == '2025-01-13T09:30:00-05:00'

        # Regular trading ends at 4pm EDT which is 16 in military time.
        assert df.index[-1].isoformat() == '2025-01-13T15:59:00-05:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-13T15:30:00-05:00'
        assert strategy.num_trading_iterations == 7

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        # When using minute data, last price and fill price are the open price of the current bar.
        assert tracker['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'

    def test_ghp_day_single_stock_day_bars_america_new_york(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 17)
        sleeptime = '1D'
        timestep = 'day'
        lookback_timestep = timestep
        market = 'NYSE'
        tzinfo = ZoneInfo("America/New_York")
        refresh_cache = False
        auto_adjust = True
        warm_up_trading_days = 5

        strategy: GetHistoricalTestStrategy
        results, strategy = GetHistoricalTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": sleeptime,
                "lookback_timestep": lookback_timestep,
                "market": market
            },

            # AlpacaBacktestingNew kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-03T00:00:00-05:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T00:00:00-05:00'
        key = list(data_source._data_store.keys())[0]
        assert key == 'AMZN-STOCK_USD-FOREX_NYSE_DAY_AMERICA-NEW-YORK_AA_2025-01-03_2025-01-17'
        df = list(data_source._data_store.values())[0]
        assert not df.empty
        assert len(df.index) == 10

        # daily bars are NORMALLY indexed at midnight (the open of the bar).
        # To enable lumibot to use the open price of the bar for the get_last_price and fills,
        # the alpaca backtester adjusts daily bars to the open bar of the market.
        assert df.index[0].isoformat() == "2025-01-03T09:30:00-05:00"
        assert df.index[-1].isoformat() == "2025-01-17T09:30:00-05:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # strategy tests
        assert strategy.last_historical_prices_df is not None
        assert strategy.last_trading_iteration.isoformat() == '2025-01-15T09:30:00-05:00'
        assert strategy.last_historical_prices_df.index[0].isoformat() == '2025-01-07T09:30:00-05:00'
        assert strategy.last_historical_prices_df.index[-1].isoformat() == '2025-01-14T09:30:00-05:00'

    def test_ghp_day_single_crypto_day_bars_america_chicago(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 17)
        sleeptime = '1D'
        timestep = 'day'
        lookback_timestep = timestep
        market = '24/7'
        tzinfo = ZoneInfo("America/Chicago")
        refresh_cache = False
        auto_adjust = True
        warm_up_trading_days = 5

        strategy: GetHistoricalTestStrategy
        results, strategy = GetHistoricalTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('BTC', asset_type='crypto'),
                "sleeptime": sleeptime,
                "lookback_timestep": lookback_timestep,
                "market": market
            },

            # AlpacaBacktestingNew kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-08T00:00:00-06:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T00:00:00-06:00'
        key = list(data_source._data_store.keys())[0]
        assert key == 'BTC-CRYPTO_USD-FOREX_24-7_DAY_AMERICA-CHICAGO_AA_2025-01-08_2025-01-17'
        df = list(data_source._data_store.values())[0]
        assert not df.empty
        assert len(df.index) == 10

        assert df.index[0].isoformat() == "2025-01-08T00:00:00-06:00"
        assert df.index[-1].isoformat() == "2025-01-17T00:00:00-06:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00-06:00'

        # strategy tests
        assert strategy.last_historical_prices_df is not None
        assert strategy.last_trading_iteration.isoformat() == '2025-01-15T00:00:00-06:00'
        assert strategy.last_historical_prices_df.index[0].isoformat() == '2025-01-10T00:00:00-06:00'
        assert strategy.last_historical_prices_df.index[-1].isoformat() == '2025-01-14T00:00:00-06:00'

    # @pytest.mark.skip()
    def test_bo_single_crypto_minute_bars_america_chicago(self):
        tickers = "BTC/USD"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 14)
        timestep = 'minute'
        refresh_cache = False
        tzinfo = ZoneInfo("America/Chicago")
        market = '24/7'

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('BTC', asset_type='crypto'),
                "sleeptime": "1M",
                "market": market,
            },

            # AlpacaBacktestingNew kwargs
            tickers=tickers,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            market=market,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00-06:00"
        assert data_source.datetime_end.isoformat() == "2025-01-13T23:59:00-06:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty

        # the data is missing minutes but it won't be more than one bar per minute
        assert len(df.index) <= 1440

        # first bar on 1/13  was a 00:01 of the day.
        assert df.index[0].isoformat() == '2025-01-13T00:01:00-06:00'
        assert df.index[-1].isoformat() == '2025-01-13T23:59:00-06:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T00:00:00-06:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-13T23:59:00-06:00'
        assert strategy.num_trading_iterations == 1440

        tracker = strategy.tracker
        # no data at 00:00; first bar is 00:01
        assert tracker["iteration_at"].isoformat() == '2025-01-13T00:01:00-06:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T00:01:00-06:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T00:01:00-06:00'

        assert tracker['last_price'] == 94066.35  # open of 2025-01-13 06:00:00+00:00
        assert tracker["avg_fill_price"] == 94066.35  # Open of 2025-01-13 06:00:00+00:00

    # @pytest.mark.skip()
    def test_bo_single_crypto_hour_bars_utc(self):
        tickers = "BTC/USD"
        backtesting_start = datetime(2025, 1, 1)
        backtesting_end = datetime(2025, 1, 2)
        timestep = 'hour'
        refresh_cache = False
        tzinfo = ZoneInfo("UTC")
        market = '24/7'

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('BTC', asset_type='crypto'),
                "sleeptime": "60M",
                "market": market,
            },

            # AlpacaBacktestingNew kwargs
            tickers=tickers,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            market=market,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-01T00:00:00+00:00"
        assert data_source.datetime_end.isoformat() == "2025-01-01T23:59:00+00:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty

        # 24 hour-long bars
        # 60 minutes per hour
        assert len(df.index) == (24 * 60)
        assert df.index[0].isoformat() == '2025-01-01T00:00:00+00:00'
        assert df.index[-1].isoformat() == '2025-01-01T23:59:00+00:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-01T00:00:00+00:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-01T23:00:00+00:00'
        assert strategy.num_trading_iterations == 24

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-01T00:00:00+00:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-01T00:00:00+00:00'
        assert tracker["filled_at"].isoformat() == '2025-01-01T00:00:00+00:00'

        # open of the current bar.
        assert tracker['last_price'] == 93381.5825  # open of 2025-01-01 00:00:00+00:00

        # open price of the current bar.
        assert tracker["avg_fill_price"] == 93381.58  # open of 2025-01-01 00:00:00+00:00

    # @pytest.mark.skip()
    def test_bo_single_crypto_hour_bars_america_chicago(self):
        tickers = "BTC/USD"
        backtesting_start = datetime(2025, 1, 1)
        backtesting_end = datetime(2025, 1, 2)
        timestep = 'hour'
        refresh_cache = False
        tzinfo = ZoneInfo("America/Chicago")
        market = '24/7'

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('BTC', asset_type='crypto'),
                "sleeptime": "60M",
                "market": market
            },

            # AlpacaBacktestingNew kwargs
            tickers=tickers,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            market=market,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-01T00:00:00-06:00"
        assert data_source.datetime_end.isoformat() == "2025-01-01T23:59:00-06:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty

        # 24 hour-long bars
        # 60 minutes per hour
        assert len(df.index) == (24 * 60)
        assert df.index[0].isoformat() == '2025-01-01T00:00:00-06:00'
        assert df.index[-1].isoformat() == '2025-01-01T23:59:00-06:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-01T00:00:00-06:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-01T23:00:00-06:00'
        assert strategy.num_trading_iterations == 24

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-01T00:00:00-06:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-01T00:00:00-06:00'
        assert tracker["filled_at"].isoformat() == '2025-01-01T00:00:00-06:00'

        # open of the current bar.
        assert tracker['last_price'] == 93486.63  # open of 2025-01-01 06:00:00+00:00

        # open price of the current bar.
        assert tracker["avg_fill_price"] == 93486.63  # Open of 2025-01-01 06:00:00+00:00

    def test_ghp_day_single_crypto_hour_bars_america_chicago(self):
        tickers = "BTC/USD"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        timestep = 'hour'
        refresh_cache = True
        tzinfo = ZoneInfo("America/Chicago")
        warm_up_trading_days = 5
        market = "24/7"

        strategy: GetHistoricalTestStrategy
        results, strategy = GetHistoricalTestStrategy.run_backtest(
            datasource_class=AlpacaBacktestingNew,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('BTC', asset_type='crypto'),
                "sleeptime": "60M",
                "lookback_timestep": "day",
                "market": market
            },

            # AlpacaBacktestingNew kwargs
            tickers=tickers,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            warm_up_trading_days=warm_up_trading_days,
            market=market,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktestingNew)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00-06:00"
        assert data_source.datetime_end.isoformat() == '2025-01-17T23:59:00-06:00'
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty

        # 24 hour-long bars in a 24-hour day (0-23)
        # 60 minutes per hour
        assert len(df.index) == (24*60*10)

        assert df.index[0].isoformat() == '2025-01-08T00:00:00-06:00'
        assert df.index[-1].isoformat() == "2025-01-17T23:59:00-06:00"

        assert strategy.last_historical_prices_df is not None
        assert strategy.last_trading_iteration.isoformat() == "2025-01-17T23:00:00-06:00"
        assert strategy.last_historical_prices_df.index[-1].isoformat() == "2025-01-16T00:00:00-06:00"