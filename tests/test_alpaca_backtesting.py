from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import AlpacaBacktesting, PandasDataBacktesting, BacktestingBroker
from lumibot.brokers import Broker
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.entities import Asset
from lumibot.tools import get_trading_times

from tests.fixtures import (
    BacktestingTestStrategy
)

if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


import pytest
from datetime import datetime
from zoneinfo import ZoneInfo
from lumibot.entities import Asset


class TestAlpacaBacktesting:
    """Tests for the AlpacaBacktesting datasource class as well as using it in strategies."""

    def _create_data_source(
            self,
            *,
            datetime_start=datetime(2025, 1, 1, tzinfo=ZoneInfo("America/New_York")),
            datetime_end=datetime(2025, 1, 31, tzinfo=ZoneInfo("America/New_York")),
            config=ALPACA_TEST_CONFIG,
            timestep="day",
            refresh_cache=False,
            market="NYSE",
            warm_up_trading_days: int = 0,
            auto_adjust: bool = True,
    ):
        """
        Create an instance of AlpacaBacktesting with default or provided parameters.
        """
        return AlpacaBacktesting(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            config=config,
            timestep=timestep,
            refresh_cache=refresh_cache,
            market=market,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

    def test_create_data_source(self):
        data_source = self._create_data_source()
        assert isinstance(data_source, AlpacaBacktesting)

    def test_basic_key_generation_crypto(self):
        datetime_start = datetime(2025, 1, 1, tzinfo=ZoneInfo("America/Chicago"))
        datetime_end = datetime(2025, 2, 1, tzinfo=ZoneInfo("America/Chicago"))
        base_asset = Asset("BTC", asset_type='crypto')
        quote_asset = Asset("USD", asset_type='forex')
        market = "24/7"
        timestep = "day"
        
        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
            market=market,
            timestep=timestep,
            data_datetime_start=datetime_start,
            data_datetime_end=datetime_end
        )

        expected = "BTC-CRYPTO_USD-FOREX_24-7_DAY_AMERICA-CHICAGO_AA_2025-01-01_2025-02-01"
        assert key == expected

    def test_basic_key_generation_stock(self):
        datetime_start = datetime(2025, 1, 1, tzinfo=ZoneInfo("America/New_York"))
        datetime_end = datetime(2025, 2, 1, tzinfo=ZoneInfo("America/New_York"))
        base_asset = Asset("AAPL", asset_type='stock')
        quote_asset = Asset("USD", asset_type='forex')
        market = "NYSE"
        timestep = "day"

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
            market=market,
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
        timestep = "day"
        start_date = datetime(2023, 1, 1, tzinfo=ZoneInfo("America/New_York"))
        end_date = datetime(2023, 12, 31, tzinfo=ZoneInfo("America/New_York"))
        auto_adjust = True

        data_source = self._create_data_source(
            datetime_start=start_date,
            datetime_end=end_date,
            market=market,
            timestep=timestep,
            auto_adjust=auto_adjust
        )

        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
            market=market,
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
        timestep = "minute"
        start_date = datetime(2023, 1, 1, tzinfo=ZoneInfo("America/New_York"))
        end_date = datetime(2023, 12, 31, tzinfo=ZoneInfo("America/New_York"))
        auto_adjust = False

        data_source = self._create_data_source(
            datetime_start=start_date,
            datetime_end=end_date,
            timestep=timestep,
            auto_adjust=auto_adjust
        )

        key = data_source._get_asset_key(
            base_asset=base_asset,
            quote_asset=quote_asset,
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

        datetime_start = datetime(2024, 1, 1, tzinfo=ZoneInfo("America/Chicago"))
        datetime_end = datetime(2024, 2, 1, tzinfo=ZoneInfo("America/Chicago"))
        base_asset = Asset("BTC", asset_type='crypto')
        quote_asset = Asset("USD", asset_type='forex')
        market = "24/7"
        timestep = "day"
        refresh_cache = True

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
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

        datetime_start = datetime(2024, 1, 1, tzinfo=ZoneInfo("America/Chicago"))
        datetime_end = datetime(2024, 2, 1, tzinfo=ZoneInfo("America/Chicago"))
        base_asset = Asset("BTC", asset_type='crypto')
        quote_asset = Asset("USD", asset_type='forex')
        market = "24/7"
        timestep = "day"
        refresh_cache = False

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
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

    def test_amzn_day_1d(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-05:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 5 + lookback_length
        assert timestep_data_df.index[0].isoformat() == "2025-01-13T09:30:00-05:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T09:30:00-05:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T09:30:00-05:00'
        assert last_price_keys[-1] == '2025-01-15T09:30:00-05:00'
        assert last_prices['2025-01-13T09:30:00-05:00'] == Decimal('218.06')  # Open of '2025-01-13T09:30:00-05:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["avg_fill_price"] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

    def test_amzn_day_1d_5(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-03T00:00:00-05:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-05:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 5 + lookback_length
        assert timestep_data_df.index[0].isoformat() == "2025-01-03T09:30:00-05:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T09:30:00-05:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T09:30:00-05:00'
        assert last_price_keys[-1] == '2025-01-15T09:30:00-05:00'
        assert last_prices['2025-01-13T09:30:00-05:00'] == Decimal('218.06')  # Open of '2025-01-13T09:30:00-05:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["avg_fill_price"] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

        if lookback_length > 0:
            historical_prices = strategy.historical_prices
            historical_price_keys = list(historical_prices.keys())
            assert len(historical_prices) == 3 # iterations
            last_df = historical_prices[historical_price_keys[-1]]
            assert last_df.index[0].isoformat() == '2025-01-07T09:30:00-05:00'
            assert last_df.index[-1].isoformat() == '2025-01-14T09:30:00-05:00'

    def test_amzn_minute_1d(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'minute',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-05:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 6.5 * 60 * 5
        assert timestep_data_df.index[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert timestep_data_df.index[-1].isoformat() == '2025-01-17T15:59:00-05:00'

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T09:30:00-05:00'
        assert last_price_keys[-1] == '2025-01-15T09:30:00-05:00'
        assert last_prices['2025-01-13T09:30:00-05:00'] == Decimal('218.06')  # Open of '2025-01-13T09:30:00-05:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["avg_fill_price"] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

    def test_amzn_minute_1d_5(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'minute',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-03T00:00:00-05:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-05:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 6.5 * 60 * 10
        assert timestep_data_df.index[0].isoformat() == '2025-01-03T09:30:00-05:00'
        assert timestep_data_df.index[-1].isoformat() == '2025-01-17T15:59:00-05:00'

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T09:30:00-05:00'
        assert last_price_keys[-1] == '2025-01-15T09:30:00-05:00'
        assert last_prices['2025-01-13T09:30:00-05:00'] == Decimal('218.06')  # Open of '2025-01-13T09:30:00-05:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["avg_fill_price"] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

        if lookback_length > 0:
            historical_prices = strategy.historical_prices
            historical_price_keys = list(historical_prices.keys())
            assert len(historical_prices) == 3 # iterations
            last_df = historical_prices[historical_price_keys[-1]]
            assert last_df.index[0].isoformat() == '2025-01-07T09:30:00-05:00'
            assert last_df.index[-1].isoformat() == '2025-01-14T09:30:00-05:00'

        if lookback_length > 0 and timestep == 'minute':
            lookback_data_key = data_source._get_asset_key(
                base_asset=asset,
                quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
                timestep='day',
            )
            lookback_data_df = data_source._data_store[lookback_data_key]

            assert len(lookback_data_df.index) == 10
            assert lookback_data_df.index[0].isoformat() == '2025-01-03T09:30:00-05:00'
            assert lookback_data_df.index[-1].isoformat() == '2025-01-17T09:30:00-05:00'

    def test_amzn_minute_30m_5(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'minute',
            sleeptime: str = '30M',
            tzinfo: ZoneInfo = ZoneInfo('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-03T00:00:00-05:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-05:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 6.5 * 60 * 10
        assert timestep_data_df.index[0].isoformat() == '2025-01-03T09:30:00-05:00'
        assert timestep_data_df.index[-1].isoformat() == '2025-01-17T15:59:00-05:00'

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 13 * 2 + 1 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T09:30:00-05:00'
        assert last_price_keys[-1] == '2025-01-15T09:30:00-05:00'
        assert last_prices['2025-01-13T09:30:00-05:00'] == Decimal('218.06')  # Open of '2025-01-13T09:30:00-05:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["avg_fill_price"] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

        if lookback_length > 0:
            historical_prices = strategy.historical_prices
            historical_price_keys = list(historical_prices.keys())
            assert len(historical_prices) == 13 * 2 + 1 # number of trading iterations
            last_df = historical_prices[historical_price_keys[-1]]
            assert last_df.index[0].isoformat() == '2025-01-07T09:30:00-05:00'
            assert last_df.index[-1].isoformat() == '2025-01-14T09:30:00-05:00'

        if lookback_length > 0 and timestep == 'minute':
            lookback_data_key = data_source._get_asset_key(
                base_asset=asset,
                quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
                timestep='day',
            )
            lookback_data_df = data_source._data_store[lookback_data_key]

            assert len(lookback_data_df.index) == 10
            assert lookback_data_df.index[0].isoformat() == '2025-01-03T09:30:00-05:00'
            assert lookback_data_df.index[-1].isoformat() == '2025-01-17T09:30:00-05:00'

    def test_btc_day_1d(
            self,
            asset: Asset = Asset('BTC', asset_type='crypto'),
            market: str = '24/7',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-13T00:00:00-06:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-06:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 5 + lookback_length
        assert timestep_data_df.index[0].isoformat() == "2025-01-13T00:00:00-06:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T00:00:00-06:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00-06:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T00:00:00-06:00'
        assert last_price_keys[-1] == '2025-01-15T00:00:00-06:00'
        assert last_prices['2025-01-13T00:00:00-06:00'] == Decimal('94066.35')  # Open of '2025-01-13T00:00:00-06:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["avg_fill_price"] == 94066.35  # Open of '2025-01-13T00:00:00-06:00'

    def test_btc_day_1d_5(
            self,
            asset: Asset = Asset('BTC', asset_type='crypto'),
            market: str = '24/7',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-08T00:00:00-06:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-06:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 5 + lookback_length
        assert timestep_data_df.index[0].isoformat() == "2025-01-08T00:00:00-06:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T00:00:00-06:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00-06:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T00:00:00-06:00'
        assert last_price_keys[-1] == '2025-01-15T00:00:00-06:00'
        assert last_prices['2025-01-13T00:00:00-06:00'] == Decimal('94066.35')  # Open of '2025-01-13T00:00:00-06:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["avg_fill_price"] == 94066.35  # Open of '2025-01-13T00:00:00-06:00'

    def test_btc_day_1d_utc(
            self,
            asset: Asset = Asset('BTC', asset_type='crypto'),
            market: str = '24/7',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('UTC'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-13T00:00:00+00:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59+00:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 5 + lookback_length
        assert timestep_data_df.index[0].isoformat() == "2025-01-13T00:00:00+00:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T00:00:00+00:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00+00:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T00:00:00+00:00'
        assert last_price_keys[-1] == '2025-01-15T00:00:00+00:00'
        assert last_prices['2025-01-13T00:00:00+00:00'] == Decimal('94066.35')  # Open of '2025-01-13T00:00:00+00:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00+00:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00+00:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T00:00:00+00:00'
        assert order_tracker["avg_fill_price"] == 94066.35  # Open of '2025-01-13T00:00:00+00:00'

    def test_btc_minute_1d(
            self,
            asset: Asset = Asset('BTC', asset_type='crypto'),
            market: str = '24/7',
            timestep: str = 'minute',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-13T00:00:00-06:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-06:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 24 * 60 * 5
        assert timestep_data_df.index[0].isoformat() == '2025-01-13T00:00:00-06:00'
        assert timestep_data_df.index[-1].isoformat() == '2025-01-17T23:59:00-06:00'

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00-06:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T00:00:00-06:00'
        assert last_price_keys[-1] == '2025-01-15T00:00:00-06:00'
        assert last_prices['2025-01-13T00:00:00-06:00'] == Decimal('94066.35')  # Open of '2025-01-13T09:30:00-05:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["avg_fill_price"] == 94066.35  # Open of '2025-01-13T00:00:00-06:00'

    def test_btc_minute_1d_5(
            self,
            asset: Asset = Asset('BTC', asset_type='crypto'),
            market: str = '24/7',
            timestep: str = 'minute',
            sleeptime: str = '1D',
            tzinfo: ZoneInfo = ZoneInfo('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-08T00:00:00-06:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-06:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 24 * 60 * 10
        assert timestep_data_df.index[0].isoformat() == "2025-01-08T00:00:00-06:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T23:59:00-06:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00-06:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T00:00:00-06:00'
        assert last_price_keys[-1] == '2025-01-15T00:00:00-06:00'
        assert last_prices['2025-01-13T00:00:00-06:00'] == Decimal('94153.0455')  # Open of '2025-01-13T00:00:00-06:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["avg_fill_price"] == 94153.05  # Open of '2025-01-13T00:00:00-06:00'

    def test_btc_minute_30m_5(
            self,
            asset: Asset = Asset('BTC', asset_type='crypto'),
            market: str = '24/7',
            timestep: str = 'minute',
            sleeptime: str = '30M',
            tzinfo: ZoneInfo = ZoneInfo('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = datetime(2025, 1, 13, tzinfo=tzinfo)
        backtesting_end = datetime(2025, 1, 17, tzinfo=tzinfo)
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BacktestingTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": asset,
                "market": market,
                "sleeptime": sleeptime,
                "lookback_timestep": 'day',
                "lookback_length": lookback_length,
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source._data_datetime_start.isoformat() == "2025-01-08T00:00:00-06:00"
        assert data_source._data_datetime_end.isoformat() == '2025-01-17T23:59:59-06:00'

        timestep_data_key = data_source._get_asset_key(
            base_asset=asset,
            quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
            timestep=timestep,
        )
        timestep_data_df = data_source._data_store[timestep_data_key]

        assert len(timestep_data_df.index) == 24 * 60 * 10
        assert timestep_data_df.index[0].isoformat() == "2025-01-08T00:00:00-06:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T23:59:00-06:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00-06:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 24 * 4 + 1
        assert last_price_keys[0] == '2025-01-13T00:00:00-06:00'
        assert last_price_keys[-1] == '2025-01-15T00:00:00-06:00'
        assert last_prices['2025-01-13T00:00:00-06:00'] == Decimal('94153.0455')  # Open of '2025-01-13T00:00:00-06:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["avg_fill_price"] == 94153.05  # Open of '2025-01-13T00:00:00-06:00'
