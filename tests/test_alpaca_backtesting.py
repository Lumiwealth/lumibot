import pytest
from datetime import datetime

from lumibot.backtesting import AlpacaBacktesting, PandasDataBacktesting
from lumibot.credentials import ALPACA_TEST_CONFIG

from tests.fixtures import (
    BuyOneShareTestStrategy
)


if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


class TestAlpacaBacktests:
    """Tests for running backtests with AlpacaBacktesting, BacktestingBroker, and Trader."""

    # @pytest.mark.skip()
    def test_day_data_backtest(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        tickers = "AMZN"
        timestep = 'day'
        refresh_cache = False
        tz_name = "America/New_York"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=backtesting_start.date().isoformat(),
            end_date=backtesting_end.date().isoformat(),
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        strategy: BuyOneShareTestStrategy
        results, strategy = BuyOneShareTestStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            pandas_data=data_source.pandas_data,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            parameters={
                "symbol": "AMZN",
                "sleeptime": "1D",
                "market": "NYSE"
            },
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )
        assert results

        # Assert the end datetime is before the next trading day
        assert strategy.broker.datetime.isoformat() == '2025-01-18T09:25:00-05:00'
        assert strategy.num_trading_iterations == 5

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2025-01-13T09:30:00-05:00'

        assert tracker['last_price'] == 218.46  # Close of '2025-01-13T09:30:00-05:00'
        assert tracker["avg_fill_price"] == 220.44  # Open of '2025-01-14T09:30:00-05:00'

        # Checks bug where LifeCycle methods not being called during PANDAS backtesting
        # assert len(strategy.market_opens) == 5
        # assert len(strategy.market_closes) == 5

    # @pytest.mark.skip()
    def test_minute_data_backtest(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        tickers = "AMZN"
        timestep = 'minute'
        refresh_cache = False
        tz_name = "America/New_York"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=backtesting_start.date().isoformat(),
            end_date=backtesting_end.date().isoformat(),
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        strategy: BuyOneShareTestStrategy
        results, strategy = BuyOneShareTestStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            pandas_data=data_source.pandas_data,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            parameters={
                "symbol": "AMZN",
                "sleeptime": "1M",
                "market": "NYSE"
            },
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )
        assert results

        # Assert the end datetime is before the next trading day
        assert strategy.broker.datetime.isoformat() == '2025-01-21T08:30:00-05:00'
        assert strategy.num_trading_iterations == 1930

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2025-01-13T09:30:00-05:00'

        # current prices seem wrong to me
        assert tracker['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 218.06   # Open price of '2025-01-13T09:30:00-05:00'

        # I think it should be:
        # assert tracker['last_price'] == 217.92  # Close price of '2025-01-13T09:30:00-05:00'
        # assert tracker['avg_fill_price'] == 218.0  # Open price of '2025-01-13T09:31:00-05:00'

        # Checks bug where LifeCycle methods not being called during PANDAS backtesting
        # assert len(strategy.market_opens) == 5
        # assert len(strategy.market_closes) == 5

    def test_minute_data_with_60_sleeptime_backtest(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        tickers = "AMZN"
        timestep = 'minute'
        refresh_cache = False
        tz_name = "America/New_York"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=backtesting_start.date().isoformat(),
            end_date=backtesting_end.date().isoformat(),
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        strategy: BuyOneShareTestStrategy
        results, strategy = BuyOneShareTestStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            pandas_data=data_source.pandas_data,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            parameters={
                "symbol": "AMZN",
                "sleeptime": "60M",
                "market": "NYSE"
            },
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )
        assert results

        # Assert the end datetime is before the next trading day
        assert strategy.broker.datetime.isoformat() == '2025-01-21T08:30:00-05:00'
        assert strategy.num_trading_iterations == 5 * 7

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2025-01-13T09:30:00-05:00'

        # current prices seem wrong to me
        assert tracker['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 218.06   # Open price of '2025-01-13T09:30:00-05:00'

        # I think it should be:
        # assert tracker['last_price'] == 217.92  # Close price of '2025-01-13T09:30:00-05:00'
        # assert tracker['avg_fill_price'] == 218.0  # Open price of '2025-01-13T09:31:00-05:00'

        # Checks bug where LifeCycle methods not being called during PANDAS backtesting
        # assert len(strategy.market_opens) == 5
        # assert len(strategy.market_closes) == 5

    def test_hour_data_with_60_sleeptime_backtest(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        tickers = "AMZN"
        timestep = 'hour'
        refresh_cache = False
        tz_name = "America/New_York"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=backtesting_start.date().isoformat(),
            end_date=backtesting_end.date().isoformat(),
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        strategy: BuyOneShareTestStrategy
        results, strategy = BuyOneShareTestStrategy.run_backtest(
            datasource_class=PandasDataBacktesting,
            pandas_data=data_source.pandas_data,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            parameters={
                "symbol": "AMZN",
                "sleeptime": "60M",
                "market": "NYSE"
            },
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )
        assert results

        # Assert the end datetime is before the next trading day
        assert strategy.broker.datetime.isoformat() == '2025-01-21T08:30:00-05:00'
        assert strategy.num_trading_iterations == 5 * 7

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2025-01-13T09:30:00-05:00'

        # current prices seem wrong to me
        assert tracker['last_price'] == 217.615
        assert tracker['avg_fill_price'] == 217.62


class TestAlpacaBacktesting:
    """Tests for the AlpacaBacktesting class itself."""

    # @pytest.mark.skip()
    def test_single_stock_day_bars(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = False

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
        )

        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00+00:00"
        assert data_source.datetime_end.isoformat() == "2025-01-17T23:59:00+00:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 5
        assert df.index[0].isoformat() == "2025-01-13T00:00:00-05:00"
        assert df['open'].iloc[0] == 218.06
        assert df['close'].iloc[0] == 218.46
        assert df.index[-1].isoformat() == "2025-01-17T00:00:00-05:00"
        assert df['open'].iloc[-1] == 225.84
        assert df['close'].iloc[-1] == 225.94

    # @pytest.mark.skip()
    def test_single_stock_minute_bars(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-14"
        timestep = 'minute'
        refresh_cache = False

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
        )

        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00+00:00"
        assert data_source.datetime_end.isoformat() == "2025-01-13T23:59:00+00:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 732
        assert df.index[0].isoformat() == '2025-01-13T04:00:00-05:00'
        assert df.index[-1].isoformat() == '2025-01-13T18:58:00-05:00'
        assert df['open'].iloc[0] == 217.73
        assert df['close'].iloc[0] == 216.68
        assert df['open'].iloc[-1] == 219.27
        assert df['close'].iloc[-1] == 219.27

    # @pytest.mark.skip()
    def test_single_stock_hour_bars(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'hour'
        refresh_cache = False

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
        )

        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00+00:00"
        assert data_source.datetime_end.isoformat() == "2025-01-17T23:59:00+00:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]

        # PandasData only knows about day and minute timestep. Hourly bars are handled by minute mode.
        assert data.timestep == 'minute'

        df = data.df
        assert not df.empty
        assert len(df.index) == 80
        assert df.index[0].isoformat() == '2025-01-13T04:00:00-05:00'
        assert df.index[-1].isoformat() == '2025-01-17T19:00:00-05:00'
        assert df['open'].iloc[0] == 217.73
        assert df['close'].iloc[0] == 215.68
        assert df['open'].iloc[-1] == 226.3
        assert df['close'].iloc[-1] == 226.39

    # @pytest.mark.skip()
    def test_tz_name_day_bars(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = False
        tz_name = "US/Eastern"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source.datetime_end.isoformat() == "2025-01-17T23:59:00-05:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 5
        assert df.index[0].isoformat() == "2025-01-13T00:00:00-05:00"
        assert df['open'].iloc[0] == 218.06
        assert df['close'].iloc[0] == 218.46
        assert df.index[-1].isoformat() == "2025-01-17T00:00:00-05:00"
        assert df['open'].iloc[-1] == 225.84
        assert df['close'].iloc[-1] == 225.94

    # @pytest.mark.skip()
    def test_single_crypto_daily_bars(self):
        tickers = "BTC/USD"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = False

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
        )

        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00+00:00"
        assert data_source.datetime_end.isoformat() == "2025-01-17T23:59:00+00:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 5
        assert df.index[0].isoformat() == "2025-01-13T01:00:00-05:00"
        assert df['open'].iloc[0] == 94066.35
        assert df['close'].iloc[0] == 94861.625
        assert df.index[-1].isoformat() == "2025-01-17T01:00:00-05:00"
        assert df['open'].iloc[-1] == 101416.579115
        assert df['close'].iloc[-1] == 102846.0

    # @pytest.mark.skip()
    def test_single_crypto_minute_bars(self):
        tickers = "BTC/USD"
        start_date = "2025-01-13"
        end_date = "2025-01-14"
        timestep = 'minute'
        refresh_cache = False

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
        )

        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00+00:00"
        assert data_source.datetime_end.isoformat() == "2025-01-13T23:59:00+00:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 1196
        assert df.index[0].isoformat() == '2025-01-12T19:00:00-05:00'
        assert df.index[-1].isoformat() == '2025-01-13T18:58:00-05:00'
        assert df['open'].iloc[0] == 94558.75
        assert df['close'].iloc[0] == 94558.75
        assert df['open'].iloc[-1] == 94511.24
        assert df['close'].iloc[-1] == 94511.24
