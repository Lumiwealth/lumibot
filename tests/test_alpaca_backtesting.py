from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from lumibot.backtesting import AlpacaBacktesting, PandasDataBacktesting, BacktestingBroker
from lumibot.brokers import Broker
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.entities import Asset

from tests.fixtures import (
    BuyOnceTestStrategy,
    GetHistoricalTestStrategy
)

if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


class TestAlpacaBacktesting:
    """Tests for the AlpacaBacktesting datasource class as well as using it in strategies."""

    def test_single_stock_day_bars_america_new_york(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        timestep = 'day'
        tzinfo = ZoneInfo("America/New_York")
        tickers = "AMZN"
        refresh_cache = False

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": "1D",
                "market": "NYSE"
            },


            # AlpacaBacktesting kwargs
            tickers=tickers,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source.datetime_end.isoformat() == '2025-01-17T23:59:00-05:00'
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"
        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty
        assert len(df.index) == 5

        # daily bars are indexed at midnight (the open of the bar).
        assert df.index[0].isoformat() == "2025-01-13T00:00:00-05:00"
        assert df.index[-1].isoformat() == "2025-01-17T00:00:00-05:00"

        # Trading strategy tests

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-17T09:30:00-05:00'
        assert strategy.num_trading_iterations == 5

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        # With daily data, last price gets the close of the current bar because
        # daily bars are indexed at midnight and at 930am the open has already passed.
        assert tracker['last_price'] == 218.46  # Close of '2025-01-13T09:30:00-05:00'

        # Since marked orders are always fill with the open of a bar, with daily data,
        # they are filled at teh open of the next bar, because the open of the current bar (midnight)
        # has already passed.
        assert tracker["avg_fill_price"] == 220.44  # Open of '2025-01-14T09:30:00-05:00'

        # TODO: fix pandas backtesting bug. These should be called but aren't.
        # Check lifeCycle methods are being called during PANDAS backtesting
        # assert len(strategy.market_opens) == 5
        # assert len(strategy.market_closes) == 5

    def test_get_historical_prices_single_stock_day_bars_america_new_york(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        timestep = 'day'
        tzinfo = ZoneInfo("America/New_York")
        tickers = "AMZN"
        refresh_cache = False
        warm_up_trading_days = 5

        strategy: GetHistoricalTestStrategy
        results, strategy = GetHistoricalTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": "1D",
                "timestep": timestep,
                "market": "NYSE"
            },

            # AlpacaBacktesting kwargs
            tickers=tickers,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            warm_up_trading_days=warm_up_trading_days
        )

        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source.datetime_end.isoformat() == '2025-01-17T23:59:00-05:00'
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty
        assert len(df.index) == 10

        # daily bars are indexed at midnight (the open of the bar).
        assert df.index[0].isoformat() == "2025-01-03T00:00:00-05:00"
        assert df.index[-1].isoformat() == "2025-01-17T00:00:00-05:00"

        # strategy tests
        assert strategy.last_historical_prices_df is not None
        assert strategy.last_trading_iteration.isoformat() == "2025-01-17T09:30:00-05:00"
        assert strategy.last_historical_prices_df.index[-1].isoformat() == "2025-01-16T00:00:00-05:00"

    def test_single_stock_minute_bars_america_new_york_regular_hours(self):
        tickers = "AMZN"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 14)
        timestep = 'minute'
        refresh_cache = False
        tzinfo = ZoneInfo("America/New_York")

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": "1M",
                "timestep": timestep,
                "market": "NYSE"
            },

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

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
        # However, trading_hours_start, trading_hours_end are inclusive.
        assert df.index[-1].isoformat() == '2025-01-13T15:59:00-05:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-13T15:59:00-05:00'
        assert strategy.num_trading_iterations == 6.5 * 60

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        # When using minute data, last price and fill price are the open price of the current bar.
        assert tracker['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'

    @pytest.mark.skip(reason="We need an extended hours market to make this test work")
    def test_single_stock_minute_bars_america_new_york_extended_hours(self):
        tickers = "AMZN"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 14)
        timestep = 'minute'
        refresh_cache = False
        tzinfo = ZoneInfo("America/New_York")

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": "1M",
                "market": "NYSE"
            },

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

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

        # 16 hours in extended trading
        # TODO: um... shouldn't extended hours have caused more hours?
        assert len(df.index) == 16 * 60

        # Pre-market trading opens at 4am EDT
        assert df.index[0].isoformat() == '2025-01-13T04:00:00-05:00'

        # extended trading ended at 8pm EDT which is 20 in military time.
        assert df.index[-1].isoformat() == '2025-01-13T19:59:00-05:00'

        # check when trading iterations happened
        # TODO: um... shouldn't extended hours have caused more trading iterations?
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-13T15:59:00-05:00'
        assert strategy.num_trading_iterations == 6.5 * 60

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        # When using minute data, last price and fill price are the open price of the current bar.
        assert tracker['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'

    def test_single_stock_30_minute_bars_america_new_york_regular_hours(self):
        tickers = "AMZN"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 14)
        timestep = '30m'
        refresh_cache = False
        tzinfo = ZoneInfo("America/New_York")

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": "30M",
                "timestep": timestep,
                "market": "NYSE"
            },

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

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
        # 6.5 trading hours in a day, 60 minutes per hour
        assert len(df.index) == (6.5 * 60)

        # Regular trading opens at 930am EDT
        assert df.index[0].isoformat() == '2025-01-13T09:30:00-05:00'

        # Regular trading ends at 4pm EDT which is 16 in military time.
        assert df.index[-1].isoformat() == '2025-01-13T15:59:00-05:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-13T15:30:00-05:00'
        assert strategy.num_trading_iterations == 6.5 * 2

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        # When using minute data, last price and fill price are the open price of the current bar.
        assert tracker['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'

    def test_single_stock_minute_bars_america_new_york_with_60m_sleeptime(self):
        tickers = "AMZN"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 14)
        timestep = 'minute'
        refresh_cache = False
        tzinfo = ZoneInfo("America/New_York")

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
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

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

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

    # @pytest.mark.skip()
    def test_single_stock_hour_bars_america_new_york(self):
        tickers = "AMZN"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        timestep = 'hour'
        refresh_cache = False
        tzinfo = ZoneInfo("America/New_York")

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
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

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == '2025-01-13T00:00:00-05:00'
        assert data_source.datetime_end.isoformat() == '2025-01-17T23:59:00-05:00'
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]

        # PandasData only knows about day and minute timestep. Hourly bars are handled by minute mode.
        assert data.timestep == 'minute'

        df = data.df
        assert not df.empty

        # 6.5 hours in a trading day 5 days a week, 60 minutes per hour,
        assert len(df.index) == (6.5 * 5 * 60)

        assert df.index[0].isoformat() == '2025-01-13T09:30:00-05:00'

        # Regular trading ends at 4pm EDT which is 16 in military time, but hour bars start at 00 minutes
        assert df.index[-1].isoformat() == '2025-01-17T15:59:00-05:00'

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-17T15:30:00-05:00'
        assert strategy.num_trading_iterations == 35

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        # When using minute data, last price and fill price are the open price of the current bar.
        assert tracker['last_price'] == 217.615  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 217.62  # Open price of '2025-01-13T09:30:00-05:00'

    def test_get_historical_prices_single_stock_hour_bars_america_new_york(self):
        tickers = "AMZN"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        timestep = 'hour'
        refresh_cache = False
        tzinfo = ZoneInfo("America/New_York")
        warm_up_trading_days = 5

        strategy: GetHistoricalTestStrategy
        results, strategy = GetHistoricalTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": "1D",
                "lookback_timestep": "day",
                "market": "NYSE"
            },

            # AlpacaBacktesting kwargs
            tickers=tickers,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tzinfo=tzinfo,
            warm_up_trading_days=warm_up_trading_days,
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source.datetime_end.isoformat() == '2025-01-17T23:59:00-05:00'
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty

        # 5 days of lookback plus 5 days of backtesting
        # 6.5 hours in a trading day
        # 60 minutes an hour
        assert len(df.index) == (10 * 6.5 * 60)

        assert df.index[0].isoformat() == "2025-01-03T09:30:00-05:00"
        assert df.index[-1].isoformat() == "2025-01-17T15:59:00-05:00"

        assert strategy.last_historical_prices_df is not None
        assert strategy.last_trading_iteration.isoformat() == "2025-01-17T09:30:00-05:00"
        assert strategy.last_historical_prices_df.index[-1].isoformat() == "2025-01-16T00:00:00-05:00"

    # @pytest.mark.skip()
    def test_single_crypto_day_bars_america_chicago(self):
        tickers = "BTC/USD"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        timestep = 'day'
        refresh_cache = False
        market = '24/7'

        # Alpaca crypto daily bars are natively indexed at midnight central time
        tzinfo = ZoneInfo("America/Chicago")

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('BTC', asset_type='crypto'),
                "sleeptime": "1D",
                "market": market,
            },

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00-06:00"
        assert data_source.datetime_end.isoformat() == "2025-01-17T23:59:00-06:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 5
        assert df.index[0] == data_source.datetime_start
        assert df.index[-1].isoformat() == "2025-01-17T00:00:00-06:00"


        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T00:00:00-06:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-17T00:00:00-06:00'
        assert strategy.num_trading_iterations == 5

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T00:00:00-06:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T00:00:00-06:00'

        # with crypto, the open of the bar is midnight and thats a tradable bar so
        # get_last_price returns the open of the current bar just like it does with minute and hour data.
        assert tracker['last_price'] == 94066.35  # open of 2025-01-13 06:00:00+00:00

        # with crypto, the open of the bar is midnight and thats a tradable bar so
        # the backtest broker returns the open of the current bar just like it does with minute and hour data.
        assert tracker["avg_fill_price"] == 94066.35  # Open of 2025-01-13 06:00:00+00:00

    # @pytest.mark.skip()
    def test_single_crypto_minute_bars_america_chicago(self):
        tickers = "BTC/USD"
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 14)
        timestep = 'minute'
        refresh_cache = False
        tzinfo = ZoneInfo("America/Chicago")
        market = '24/7'

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
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

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

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
    def test_single_crypto_hour_bars_utc(self):
        tickers = "BTC/USD"
        backtesting_start = datetime(2025, 1, 1)
        backtesting_end = datetime(2025, 1, 2)
        timestep = 'hour'
        refresh_cache = False
        tzinfo = ZoneInfo("UTC")
        market = '24/7'

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
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

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

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
    def test_single_crypto_hour_bars_america_chicago(self):
        tickers = "BTC/USD"
        backtesting_start = datetime(2025, 1, 1)
        backtesting_end = datetime(2025, 1, 2)
        timestep = 'hour'
        refresh_cache = False
        tzinfo = ZoneInfo("America/Chicago")
        market = '24/7'

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=AlpacaBacktesting,
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

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

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

    def test_get_historical_prices_single_crypto_hour_bars_america_chicago(self):
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
            datasource_class=AlpacaBacktesting,
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

            # AlpacaBacktesting kwargs
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
        assert isinstance(strategy.broker.data_source, AlpacaBacktesting)

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