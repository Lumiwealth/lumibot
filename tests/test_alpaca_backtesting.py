import pytest
import pytz
from datetime import datetime, timedelta, time
import logging

import pandas as pd

from lumibot.backtesting import AlpacaBacktesting, BacktestingBroker
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.entities import Asset, Bars
from lumibot.tools import (
    get_trading_days,
    get_trading_times,
)

from tests.fixtures import (
    BacktestingTestStrategy,
    BaseDataSourceTester
)

if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)

logger = logging.getLogger(__name__)


class TestAlpacaBacktesting:
    """Tests for the AlpacaBacktesting datasource class as well as using it in strategies."""

    def _create_data_source(
            self,
            *,
            datetime_start=datetime(2025, 1, 1, tzinfo=pytz.timezone("America/New_York")),
            datetime_end=datetime(2025, 1, 31, tzinfo=pytz.timezone("America/New_York")),
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
        datetime_start = datetime(2025, 1, 1, tzinfo=pytz.timezone("America/Chicago"))
        datetime_end = datetime(2025, 2, 1, tzinfo=pytz.timezone("America/Chicago"))
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
        datetime_start = datetime(2025, 1, 1, tzinfo=pytz.timezone("America/New_York"))
        datetime_end = datetime(2025, 2, 1, tzinfo=pytz.timezone("America/New_York"))
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
        start_date = datetime(2023, 1, 1, tzinfo=pytz.timezone("America/New_York"))
        end_date = datetime(2023, 12, 31, tzinfo=pytz.timezone("America/New_York"))
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
        start_date = datetime(2023, 1, 1, tzinfo=pytz.timezone("America/New_York"))
        end_date = datetime(2023, 12, 31, tzinfo=pytz.timezone("America/New_York"))
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
            pytz.timezone("Asia/Tokyo"),
            pytz.timezone("Europe/London"),
            pytz.timezone("US/Pacific")
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

        datetime_start = datetime(2024, 1, 1, tzinfo=pytz.timezone("America/Chicago"))
        datetime_end = datetime(2024, 2, 1, tzinfo=pytz.timezone("America/Chicago"))
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

        datetime_start = datetime(2024, 1, 1, tzinfo=pytz.timezone("America/Chicago"))
        datetime_end = datetime(2024, 2, 1, tzinfo=pytz.timezone("America/Chicago"))
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

    def test_reindex_and_fill_day_when_all_data_exists(self):
        datetime_start = datetime(2025, 1, 13, tzinfo=pytz.timezone("America/New_York"))
        datetime_end = datetime(2025, 1, 17, tzinfo=pytz.timezone("America/New_York"))
        market = "NYSE"
        timestep = "day"

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        trading_times = get_trading_times(pcal=data_source._trading_days, timestep=timestep)

        data = [
            ["2025-01-13 00:00:00-05:00", 218.06, 219.4, 216.47, 218.46, 27262655.0],
            ["2025-01-14 00:00:00-05:00", 220.44, 221.82, 216.2, 217.76, 24711650.0],
            ["2025-01-15 00:00:00-05:00", 222.83, 223.57, 220.75, 223.35, 31291257.0],
            ["2025-01-16 00:00:00-05:00", 224.42, 224.65, 220.31, 220.66, 24757276.0],
            ["2025-01-17 00:00:00-05:00", 225.84, 226.51, 223.08, 225.94, 42370123.0],
        ]
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])  # Ensure timestamps are converted

        expected_df = df.copy()
        actual_df = data_source._reindex_and_fill(df=df, trading_times=trading_times, timestep=timestep)

        # Convert both DataFrames to the same format and check all columns
        pd.testing.assert_frame_equal(
            expected_df.sort_values('timestamp').reset_index(drop=True),
            actual_df.sort_values('timestamp').reset_index(drop=True),
            check_dtype=False  # If you want to ignore dtype differences
        )

    def test_reindex_and_fill_day_when_missing_dates(self):
        datetime_start = datetime(2025, 1, 13, tzinfo=pytz.timezone("America/New_York"))
        datetime_end = datetime(2025, 1, 17, tzinfo=pytz.timezone("America/New_York"))
        market = "NYSE"
        timestep = "day"

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        trading_times = get_trading_times(pcal=data_source._trading_days, timestep=timestep)

        data = [
            ["2025-01-13 00:00:00-05:00", 218.06, 219.4, 216.47, 218.46, 27262655.0],
            ["2025-01-14 00:00:00-05:00", 220.44, 221.82, 216.2, 217.76, 24711650.0],
            # ["2025-01-15 00:00:00-05:00", 222.83, 223.57, 220.75, 223.35, 31291257.0],
            ["2025-01-16 00:00:00-05:00", 224.42, 224.65, 220.31, 220.66, 24757276.0],
            ["2025-01-17 00:00:00-05:00", 225.84, 226.51, 223.08, 225.94, 42370123.0],
        ]
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])  # Ensure timestamps are converted

        actual_df = data_source._reindex_and_fill(df=df, trading_times=trading_times, timestep=timestep)

        data = [
            ["2025-01-13 00:00:00-05:00", 218.06, 219.4, 216.47, 218.46, 27262655.0],
            ["2025-01-14 00:00:00-05:00", 220.44, 221.82, 216.2, 217.76, 24711650.0],
            ["2025-01-15 00:00:00-05:00", 217.76, 217.76, 217.76, 217.76, 0.0],
            ["2025-01-16 00:00:00-05:00", 224.42, 224.65, 220.31, 220.66, 24757276.0],
            ["2025-01-17 00:00:00-05:00", 225.84, 226.51, 223.08, 225.94, 42370123.0],
        ]
        expected_df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        expected_df["timestamp"] = pd.to_datetime(expected_df["timestamp"])  # Ensure timestamps are converted

        # Convert both DataFrames to the same format and check all columns
        pd.testing.assert_frame_equal(
            expected_df.sort_values('timestamp').reset_index(drop=True),
            actual_df.sort_values('timestamp').reset_index(drop=True),
            check_dtype=False  # If you want to ignore dtype differences
        )

    def test_reindex_and_fill_day_when_missing_dates_beginning(self):
        datetime_start = datetime(2025, 1, 13, tzinfo=pytz.timezone("America/New_York"))
        datetime_end = datetime(2025, 1, 17, tzinfo=pytz.timezone("America/New_York"))
        market = "NYSE"
        timestep = "day"

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        trading_times = get_trading_times(pcal=data_source._trading_days, timestep=timestep)

        data = [
            # ["2025-01-13 00:00:00-05:00", 218.06, 219.4, 216.47, 218.46, 27262655.0],
            ["2025-01-14 00:00:00-05:00", 220.44, 221.82, 216.2, 217.76, 24711650.0],
            ["2025-01-15 00:00:00-05:00", 222.83, 223.57, 220.75, 223.35, 31291257.0],
            ["2025-01-16 00:00:00-05:00", 224.42, 224.65, 220.31, 220.66, 24757276.0],
            ["2025-01-17 00:00:00-05:00", 225.84, 226.51, 223.08, 225.94, 42370123.0],
        ]
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])  # Ensure timestamps are converted

        actual_df = data_source._reindex_and_fill(df=df, trading_times=trading_times, timestep=timestep)

        data = [
            ["2025-01-13 00:00:00-05:00", 220.44, 220.44, 220.44, 220.44, 0.0],
            ["2025-01-14 00:00:00-05:00", 220.44, 221.82, 216.2, 217.76, 24711650.0],
            ["2025-01-15 00:00:00-05:00", 222.83, 223.57, 220.75, 223.35, 31291257.0],
            ["2025-01-16 00:00:00-05:00", 224.42, 224.65, 220.31, 220.66, 24757276.0],
            ["2025-01-17 00:00:00-05:00", 225.84, 226.51, 223.08, 225.94, 42370123.0],
        ]
        expected_df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        expected_df["timestamp"] = pd.to_datetime(expected_df["timestamp"])  # Ensure timestamps are converted

        # Convert both DataFrames to the same format and check all columns
        pd.testing.assert_frame_equal(
            expected_df.sort_values('timestamp').reset_index(drop=True),
            actual_df.sort_values('timestamp').reset_index(drop=True),
            check_dtype=False  # If you want to ignore dtype differences
        )

    def test_reindex_and_fill_minute_when_missing_dates(self):
        datetime_start = datetime(2025, 1, 13, tzinfo=pytz.timezone("America/New_York"))
        datetime_end = datetime(2025, 1, 17, tzinfo=pytz.timezone("America/New_York"))
        market = "NYSE"
        timestep = "minute"

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        trading_times = get_trading_times(pcal=data_source._trading_days, timestep=timestep)

        data = [
            ["2025-01-13 09:30:00-05:00", 218.06, 219.4, 216.47, 218.46, 27262655.0],
            ["2025-01-13 09:31:00-05:00", 220.44, 221.82, 216.2, 217.76, 24711650.0],
            # ["2025-01-15 00:00:00-05:00", 222.83, 223.57, 220.75, 223.35, 31291257.0],
            ["2025-01-13 09:33:00-05:00", 224.42, 224.65, 220.31, 220.66, 24757276.0],
            ["2025-01-13 09:34:00-05:00", 225.84, 226.51, 223.08, 225.94, 42370123.0],
        ]
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"])  # Ensure timestamps are converted

        actual_df = data_source._reindex_and_fill(df=df, trading_times=trading_times, timestep=timestep)
        assert len(actual_df.index) == 6.5 * 60 * 5

        data = [
            ["2025-01-13 09:30:00-05:00", 218.06, 219.4, 216.47, 218.46, 27262655.0],
            ["2025-01-13 09:31:00-05:00", 220.44, 221.82, 216.2, 217.76, 24711650.0],
            ["2025-01-13 09:32:00-05:00", 217.76, 217.76, 217.76, 217.76, 0.0],
            ["2025-01-13 09:33:00-05:00", 224.42, 224.65, 220.31, 220.66, 24757276.0],
            ["2025-01-13 09:34:00-05:00", 225.84, 226.51, 223.08, 225.94, 42370123.0],
        ]
        expected_df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
        expected_df["timestamp"] = pd.to_datetime(expected_df["timestamp"])  # Ensure timestamps are converted

        # Convert both DataFrames to the same format and check all columns
        pd.testing.assert_frame_equal(
            expected_df.sort_values('timestamp').reset_index(drop=True),
            actual_df.head(5).sort_values('timestamp').reset_index(drop=True),
            check_dtype=False
        )

    def test_amzn_day_1d(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert timestep_data_df.index[0].isoformat() == "2025-01-13T00:00:00-05:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T00:00:00-05:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T09:30:00-05:00'
        assert last_price_keys[-1] == '2025-01-15T09:30:00-05:00'
        # get_last_price should return open of '2025-01-13T00:00:00-05:00'
        assert last_prices['2025-01-13T09:30:00-05:00'] == 218.0600

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        # Market order should be filled with open price of '2025-01-13T09:30:00-05:00
        assert order_tracker["avg_fill_price"] == 218.0600

    def test_amzn_day_1d_5(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert timestep_data_df.index[0].isoformat() == "2025-01-03T00:00:00-05:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T00:00:00-05:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T09:30:00-05:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T09:30:00-05:00'
        assert last_price_keys[-1] == '2025-01-15T09:30:00-05:00'
        # get_last_price should return open of '2025-01-13T00:00:00-05:00'
        assert last_prices['2025-01-13T09:30:00-05:00'] == 218.0600

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        # Market order should be filled with open price of '2025-01-13T09:30:00-05:00
        assert order_tracker["avg_fill_price"] == 218.0600

        if lookback_length > 0:
            historical_prices = strategy.historical_prices
            historical_price_keys = list(historical_prices.keys())
            assert len(historical_prices) == 3 # iterations
            last_df = historical_prices[historical_price_keys[-1]]
            assert last_df.index[0].isoformat() == '2025-01-08T00:00:00-05:00'
            assert last_df.index[-1].isoformat() == '2025-01-15T00:00:00-05:00'

    def test_amzn_minute_1d(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'minute',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert last_prices['2025-01-13T09:30:00-05:00'] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

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
            tzinfo: pytz.tzinfo = pytz.timezone('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert last_prices['2025-01-13T09:30:00-05:00'] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

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
            assert last_df.index[0].isoformat() == '2025-01-08T00:00:00-05:00'
            assert last_df.index[-1].isoformat() == '2025-01-15T00:00:00-05:00'

        if lookback_length > 0 and timestep == 'minute':
            lookback_data_key = data_source._get_asset_key(
                base_asset=asset,
                quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
                timestep='day',
            )
            lookback_data_df = data_source._data_store[lookback_data_key]

            assert len(lookback_data_df.index) == 10
            assert lookback_data_df.index[0].isoformat() == '2025-01-03T00:00:00-05:00'
            assert lookback_data_df.index[-1].isoformat() == '2025-01-17T00:00:00-05:00'

    def test_amzn_minute_30m_5(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'minute',
            sleeptime: str = '30M',
            tzinfo: pytz.tzinfo = pytz.timezone('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert last_prices['2025-01-13T09:30:00-05:00'] == 218.06  # Open of '2025-01-13T09:30:00-05:00'

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
            assert last_df.index[0].isoformat() == '2025-01-08T00:00:00-05:00'
            assert last_df.index[-1].isoformat() == '2025-01-15T00:00:00-05:00'

        if lookback_length > 0 and timestep == 'minute':
            lookback_data_key = data_source._get_asset_key(
                base_asset=asset,
                quote_asset=AlpacaBacktesting.LUMIBOT_DEFAULT_QUOTE_ASSET,
                timestep='day',
            )
            lookback_data_df = data_source._data_store[lookback_data_key]

            assert len(lookback_data_df.index) == 10
            assert lookback_data_df.index[0].isoformat() == '2025-01-03T00:00:00-05:00'
            assert lookback_data_df.index[-1].isoformat() == '2025-01-17T00:00:00-05:00'

    def test_btc_day_1d(
            self,
            asset: Asset = Asset('BTC', asset_type='crypto'),
            market: str = '24/7',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert last_prices['2025-01-13T00:00:00-06:00'] == 94066.35  # Open of '2025-01-13T00:00:00-06:00'

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
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert last_prices['2025-01-13T00:00:00-06:00'] == 94066.35  # Open of '2025-01-13T00:00:00-06:00'

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
            tzinfo: pytz.tzinfo = pytz.timezone('UTC'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        # Alpaca provides crypto data back at midnight central which is 6am UTC
        assert timestep_data_df.index[0].isoformat() == "2025-01-13T06:00:00+00:00"
        assert timestep_data_df.index[-1].isoformat() == "2025-01-17T06:00:00+00:00"

        # Trading strategy tests
        assert data_source.datetime_end.isoformat() == '2025-01-15T00:00:00+00:00'

        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 3 # number of trading iterations
        assert last_price_keys[0] == '2025-01-13T00:00:00+00:00'
        assert last_price_keys[-1] == '2025-01-15T00:00:00+00:00'
        assert last_prices['2025-01-13T00:00:00+00:00'] == 94066.35  # Open of '2025-01-13T00:00:00+00:00'

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
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert last_prices['2025-01-13T00:00:00-06:00'] == 94066.35  # Open of '2025-01-13T09:30:00-05:00'

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
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert last_prices['2025-01-13T00:00:00-06:00'] == 94153.0455  # Open of '2025-01-13T00:00:00-06:00'

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
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 5,
            lookback_length: int = 5,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
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
        assert last_prices['2025-01-13T00:00:00-06:00'] == 94153.0455  # Open of '2025-01-13T00:00:00-06:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["filled_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert order_tracker["avg_fill_price"] == 94153.05  # Open of '2025-01-13T00:00:00-06:00'

    def test_amzn_day_1d_dump_benchmark_stats(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BuyAndHold.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=Asset('AMZN'),
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "buy_symbol": 'AMZN'
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

        assert not strategy._benchmark_returns_df.empty
        assert strategy._benchmark_returns_df.index[0] == backtesting_start
        assert strategy._benchmark_returns_df.iloc[0].open == 218.06

    def test_amzn_day_1d_benchmark_asset_loaded_when_benchmark_asset_not_in_strategy(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BuyAndHold.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=Asset('SPY'),
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "buy_symbol": 'AMZN'
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

        assert not strategy._benchmark_returns_df.empty
        assert strategy._benchmark_returns_df.index[0] == backtesting_start
        # Verify that we have a valid opening price (should be a reasonable positive number for SPY)
        open_price = strategy._benchmark_returns_df.iloc[0].open
        assert isinstance(open_price, (int, float))
        assert 400 < open_price < 800, f"SPY open price {open_price} seems unreasonable for the test date"


    def test_amzn_day_1d_benchmark_asset_loaded_when_benchmark_asset_is_crypto(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = '24/7',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BuyAndHold.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=Asset('BTC', asset_type=Asset.AssetType.CRYPTO),
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "buy_symbol": 'AMZN'
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

        assert not strategy._benchmark_returns_df.empty
        assert strategy._benchmark_returns_df.index[0] == backtesting_start
        assert strategy._benchmark_returns_df.iloc[0].open == 94066.35

    def test_amzn_day_1m_dump_benchmark_stats(
            self,
            asset: Asset = Asset('AMZN'),
            market: str = 'NYSE',
            timestep: str = 'minute',
            sleeptime: str = '30M',
            tzinfo: pytz.tzinfo = pytz.timezone('America/New_York'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
            lookback_length: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
        refresh_cache = False

        strategy: BacktestingTestStrategy
        results, strategy = BuyAndHold.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset='AMZN',
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "buy_symbol": 'AMZN'
            },

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

        assert not strategy._benchmark_returns_df.empty

    # ============= OAuth Tests for AlpacaBacktesting =============
    
    def test_oauth_config_backtesting(self):
        """Test that AlpacaBacktesting works with OAuth configuration."""
        oauth_config = {
            "OAUTH_TOKEN": "test_oauth_backtesting_token",
            "PAPER": True
        }
        
        datetime_start = datetime(2025, 1, 1, tzinfo=pytz.timezone("America/New_York"))
        datetime_end = datetime(2025, 1, 31, tzinfo=pytz.timezone("America/New_York"))
        
        data_source = AlpacaBacktesting(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            config=oauth_config,
            timestep="day"
        )
        
        # Verify the OAuth token is properly set in the underlying AlpacaData instance
        assert hasattr(data_source, '_alpaca_data')
        assert data_source._alpaca_data.oauth_token == "test_oauth_backtesting_token"
        
    def test_oauth_mixed_config_backtesting(self):
        """Test backtesting with mixed OAuth and API key config (OAuth takes precedence)."""
        mixed_config = {
            "OAUTH_TOKEN": "test_oauth_backtesting_mixed",
            "API_KEY": "should_not_be_used",
            "API_SECRET": "should_not_be_used_either",
            "PAPER": True
        }
        
        datetime_start = datetime(2025, 1, 1, tzinfo=pytz.timezone("America/New_York"))
        datetime_end = datetime(2025, 1, 31, tzinfo=pytz.timezone("America/New_York"))
        
        data_source = AlpacaBacktesting(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            config=mixed_config,
            timestep="day"
        )
        
        # Verify OAuth is used over API key/secret
        assert data_source._alpaca_data.oauth_token == "test_oauth_backtesting_mixed"
        assert data_source._alpaca_data.api_key is None
        assert data_source._alpaca_data.api_secret is None
        
    def test_oauth_fallback_backtesting(self):
        """Test backtesting fallback when OAuth token is empty."""
        fallback_config = {
            "OAUTH_TOKEN": "",  # Empty OAuth token
            "API_KEY": "fallback_test_key",
            "API_SECRET": "fallback_test_secret",
            "PAPER": True
        }
        
        datetime_start = datetime(2025, 1, 1, tzinfo=pytz.timezone("America/New_York"))
        datetime_end = datetime(2025, 1, 31, tzinfo=pytz.timezone("America/New_York"))
        
        data_source = AlpacaBacktesting(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            config=fallback_config,
            timestep="day"
        )
        
        # Verify fallback to API key/secret
        assert data_source._alpaca_data.oauth_token is None
        assert data_source._alpaca_data.api_key == "fallback_test_key"
        assert data_source._alpaca_data.api_secret == "fallback_test_secret"


class TestAlpacaBacktestingDataSource(BaseDataSourceTester):

    def _create_data_source(
            self,
            *,
            datetime_start=datetime(2025, 1, 1, tzinfo=pytz.timezone("America/New_York")),
            datetime_end=datetime(2025, 1, 31, tzinfo=pytz.timezone("America/New_York")),
            config=ALPACA_TEST_CONFIG,
            timestep="day",
            refresh_cache=False,
            market="NYSE",
            warm_up_trading_days: int = 0,
            auto_adjust: bool = True,
            remove_incomplete_current_bar: bool = False
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
            remove_incomplete_current_bar=remove_incomplete_current_bar,
        )

    def test_get_last_price_daily_bars_stock(self):
        tzinfo = pytz.timezone("America/New_York")
        datetime_start = tzinfo.localize(datetime(2025, 1, 1))
        datetime_end = tzinfo.localize(datetime(2025, 3, 1))
        market = "NYSE"
        timestep = "day"
        asset = Asset("FNGS") # Use an ETN which doesn't pay dividends or split

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        now = tzinfo.localize(datetime(2025, 2, 21, 0, 0))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 60.1  # open price of the daily bar
        assert isinstance(price, float)

        now = tzinfo.localize(datetime(2025, 2, 21, 9, 30))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 60.1  # open price of the daily bar

        now = tzinfo.localize(datetime(2025, 2, 21, 15, 59))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 60.1  # open price of the daily bar

        now = tzinfo.localize(datetime(2025, 2, 21, 16, 0))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 60.1  # open price of the daily bar

        # test tuple
        quote = Asset("USD", Asset.AssetType.FOREX)
        asset_tuple = (quote, quote)
        self.check_get_last_price(data_source, asset_tuple)

    def test_get_last_price_daily_bars_crypto(self):
        tzinfo = pytz.timezone("America/Chicago")
        datetime_start = tzinfo.localize(datetime(2025, 1, 1))
        datetime_end = tzinfo.localize(datetime(2025, 3, 1))
        market = "24/7"
        timestep = "day"
        asset = Asset("BTC", 'crypto')

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        now = tzinfo.localize(datetime(2025, 2, 21, 0, 0))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 98308.914000000 # open price of the daily bar
        assert isinstance(price, float)

        now = tzinfo.localize(datetime(2025, 2, 21, 9, 30))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 98308.914000000 # open price of the daily bar

        now = tzinfo.localize(datetime(2025, 2, 21, 15, 59))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 98308.914000000 # open price of the daily bar

        now = tzinfo.localize(datetime(2025, 2, 21, 16, 0))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 98308.914000000 # open price of the daily bar

        # test tuple
        quote = Asset("USD", Asset.AssetType.FOREX)
        asset_tuple = (quote, quote)
        self.check_get_last_price(data_source, asset_tuple)

    def test_get_last_price_minute_bars_stock(self):
        tzinfo = pytz.timezone("America/New_York")
        datetime_start = tzinfo.localize(datetime(2025, 2, 19))
        datetime_end = tzinfo.localize(datetime(2025, 2, 22))
        market = "NYSE"
        timestep = "minute"
        asset = Asset("FNGS")  # Use an ETN which doesn't pay dividends or split

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        now = tzinfo.localize(datetime(2025, 2, 21, 0, 0))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 60.1  # open price of the minute bar
        assert isinstance(price, float)

        now = tzinfo.localize(datetime(2025, 2, 21, 9, 30))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 60.1  # open price of the minute bar

        now = tzinfo.localize(datetime(2025, 2, 21, 15, 59))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 57.7306 # open price of the minute bar

    def test_get_last_price_minute_bars_crypto(self):
        tzinfo = pytz.timezone("America/Chicago")
        datetime_start = tzinfo.localize(datetime(2025, 2, 19))
        datetime_end = tzinfo.localize(datetime(2025, 2, 22))
        market = "24/7"
        timestep = "minute"
        asset = Asset("BTC", 'crypto')

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        now = tzinfo.localize(datetime(2025, 2, 21, 0, 0))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 98308.914000000  # open price of the minute bar
        assert isinstance(price, float)

        now = tzinfo.localize(datetime(2025, 2, 21, 9, 30))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 98198.475000000  # open price of the minute bar

        now = tzinfo.localize(datetime(2025, 2, 21, 15, 59))
        data_source._datetime = now
        price = data_source.get_last_price(asset=asset)
        assert price == 95321.295000000  # open price of the minute bar

    def test_get_historical_prices_minute_bars_stock(self):
        tzinfo = pytz.timezone("America/New_York")
        datetime_start = tzinfo.localize(datetime(2025, 2, 19))
        datetime_end = tzinfo.localize(datetime(2025, 2, 22))
        market = "NYSE"
        timestep = "minute"
        asset = Asset("SPY")

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        now = tzinfo.localize(datetime(2025, 2, 21, 9, 30))
        data_source._datetime = now

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market
            )

        now = tzinfo.localize(datetime(2025, 2, 21, 10, 0))
        data_source._datetime = now

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
            )

        with pytest.raises(Exception):
            length = -1
            bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)

        # check tuple support
        quote = Asset("USD", Asset.AssetType.FOREX)
        asset_tuple = (asset, quote)
        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset_tuple,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
            )

    def test_get_historical_prices_minute_bars_stock_remove_incomplete_current_bar(self):
        tzinfo = pytz.timezone("America/New_York")
        datetime_start = tzinfo.localize(datetime(2025, 2, 19))
        datetime_end = tzinfo.localize(datetime(2025, 2, 22))
        market = "NYSE"
        timestep = "minute"
        asset = Asset("SPY")

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
            remove_incomplete_current_bar=True
        )

        now = tzinfo.localize(datetime(2025, 2, 21, 9, 30))
        data_source._datetime = now

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
                remove_incomplete_current_bar=True
            )

        now = tzinfo.localize(datetime(2025, 2, 21, 10, 0))
        data_source._datetime = now

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
                remove_incomplete_current_bar=True
            )

        with pytest.raises(Exception):
            length = -1
            bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)

        # check tuple support
        quote = Asset("USD", Asset.AssetType.FOREX)
        asset_tuple = (asset, quote)
        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset_tuple,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
                remove_incomplete_current_bar=True
            )

    def test_get_historical_prices_minute_bars_crypto(self):
        tzinfo = pytz.timezone("America/Chicago")
        datetime_start = tzinfo.localize(datetime(2025, 2, 19))
        datetime_end = tzinfo.localize(datetime(2025, 2, 22))
        market = "24/7"
        timestep = "minute"
        asset = Asset("BTC", 'crypto')

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        now = tzinfo.localize(datetime(2025, 2, 21, 0, 0))
        data_source._datetime = now
        for length in [1, 10]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market
            )

        now = tzinfo.localize(datetime(2025, 2, 21, 10, 0))
        data_source._datetime = now
        for length in [1, 10]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
            )

        with pytest.raises(Exception):
            length = -1
            bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)

        # check tuple support
        quote = Asset("USD", Asset.AssetType.FOREX)
        asset_tuple = (asset, quote)
        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset_tuple,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
            )

    def test_get_historical_prices_daily_bars_stock(self):
        tzinfo = pytz.timezone("America/New_York")
        datetime_start = tzinfo.localize(datetime(2025, 1, 1))
        datetime_end = tzinfo.localize(datetime(2025, 3, 1))
        market = "NYSE"
        timestep = "day"
        asset = Asset("SPY")

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        now = tzinfo.localize( datetime(2025, 2, 21, 9, 30))
        data_source._datetime = now
        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                time_check=time(0 ,0),
                market=market
            )

        # MLK was 1/20 so long 3 day weekend
        now = tzinfo.localize(datetime(2025, 1, 21, 9, 30))
        data_source._datetime = now
        for length in [1, 10]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                time_check=time(0 ,0),
                market=market,
            )

        with pytest.raises(Exception):
            length = -1
            bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)

    def test_get_historical_prices_daily_bars_crypto_chicago(self):
        tzinfo = pytz.timezone("America/Chicago")
        datetime_start = datetime(2025, 1, 1, tzinfo=pytz.timezone("America/Chicago"))
        datetime_end = datetime(2025, 3, 1, tzinfo=pytz.timezone("America/Chicago"))
        market = "24/7"
        timestep = "day"
        asset = Asset("BTC", 'crypto')

        data_source = self._create_data_source(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            market=market,
            timestep=timestep,
        )

        now = tzinfo.localize(datetime(2025, 2, 21, 0, 0))
        data_source._datetime = now
        for length in [1, 10]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                time_check=time(0 ,0),
                market=market
            )

        with pytest.raises(Exception):
            length = -1
            bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)


