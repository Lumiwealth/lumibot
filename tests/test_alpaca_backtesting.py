import pytest
from datetime import datetime, timedelta

from lumibot.backtesting import AlpacaBacktesting, PandasDataBacktesting
from lumibot.credentials import ALPACA_TEST_CONFIG

from tests.fixtures import (
    BuyOneShareTestStrategy
)


if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


class TestAlpacaBacktestingBacktests:
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


class TestAlpacaBacktestingDataSource:
    """Tests for the AlpacaBacktesting datasource class itself."""

    # @pytest.mark.skip()
    def test_single_stock_day_bars_utc(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        tz_name = "UTC"
        refresh_cache = False

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
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
        assert df.index[0].isoformat() == "2025-01-13T05:00:00+00:00"
        assert df.index[-1].isoformat() == "2025-01-17T05:00:00+00:00"

    def test_single_stock_day_bars_america_new_york(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = False
        tz_name = "America/New_York"

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
        assert data_source.datetime_end.isoformat() == '2025-01-17T23:59:00-05:00'
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 5
        assert df.index[0].isoformat() == "2025-01-13T00:00:00-05:00"
        assert df.index[-1].isoformat() == "2025-01-17T00:00:00-05:00"

    # @pytest.mark.skip()
    def test_single_stock_minute_bars_utc(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-14"
        timestep = 'minute'
        refresh_cache = False
        tz_name = "UTC"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
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
        assert df.index[0].isoformat() == '2025-01-13T09:00:00+00:00'
        assert df.index[-1].isoformat() == '2025-01-13T23:58:00+00:00'

    def test_single_stock_minute_bars_america_new_york(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-14"
        timestep = 'minute'
        refresh_cache = False
        tz_name = "America/New_York"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        assert data_source.datetime_start.isoformat() == '2025-01-13T00:00:00-05:00'
        assert data_source.datetime_end.isoformat() == "2025-01-13T23:59:00-05:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "AMZN"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty
        assert len(df.index) <= 1440

        # Pre-market trading opens at 4am EDT which is 9 UTC.
        assert df.index[0].isoformat() == '2025-01-13T04:00:00-05:00'

        # extended trading ended at 8pm EDT which is 20 UTC.
        assert df.index[-1].isoformat() == '2025-01-13T19:59:00-05:00'

    # @pytest.mark.skip()
    def test_single_stock_hour_bars_utc(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'hour'
        refresh_cache = False
        tz_name = "UTC"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
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
        assert len(df.index) == 79  # minute data is missing bars.

        # Pre-market trading opens at 4am EDT which is 9 UTC.
        assert df.index[0].isoformat() == "2025-01-13T09:00:00+00:00"

        # midnight UTC is 7pm EST and extended hours markets are still open.
        assert df.index[-1].isoformat() == '2025-01-17T23:00:00+00:00'

    # @pytest.mark.skip()
    def test_single_stock_hour_bars_america_new_york(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'hour'
        refresh_cache = False
        tz_name = "America/New_York"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

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
        assert len(df.index) == 80

        #
        assert df.index[0].isoformat() == '2025-01-13T04:00:00-05:00'
        assert df.index[-1].isoformat() == '2025-01-17T19:00:00-05:00'

    # @pytest.mark.skip()
    def test_single_crypto_daily_bars_utc(self):
        tickers = "BTC/USD"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = False
        tz_name = "UTC"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
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
        assert df.index[0].isoformat() == "2025-01-13T06:00:00+00:00"
        assert df.index[-1].isoformat() == "2025-01-17T06:00:00+00:00"

    # @pytest.mark.skip()
    def test_single_crypto_daily_bars_america_new_york(self):
        tickers = "BTC/USD"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = False
        tz_name = "America/New_York"

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
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 5
        assert df.index[0].isoformat() == "2025-01-13T01:00:00-05:00"
        assert df.index[-1].isoformat() == "2025-01-17T01:00:00-05:00"

    # @pytest.mark.skip()
    def test_single_crypto_daily_bars_america_chicago(self):
        tickers = "BTC/USD"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = False

        # Alpaca crypto daily bars are natively indexed at midnight central time
        tz_name = "America/Chicago"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

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

    # @pytest.mark.skip()
    def test_single_crypto_minute_bars_utc(self):
        tickers = "BTC/USD"
        start_date = "2025-01-01"
        end_date = "2025-01-02"
        timestep = 'minute'
        refresh_cache = False
        tz_name = "UTC"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        assert data_source.datetime_start.isoformat() == "2025-01-01T00:00:00+00:00"
        assert data_source.datetime_end.isoformat() == "2025-01-01T23:59:00+00:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty

        # the data is missing minutes but it won't be more than one bar per minute
        assert len(df.index) <= 1440

        assert df.index[0] == data_source.datetime_start
        assert df.index[-1] == data_source.datetime_end

    # @pytest.mark.skip()
    def test_single_crypto_minute_bars_america_chicago(self):
        tickers = "BTC/USD"
        start_date = "2025-01-01"
        end_date = "2025-01-02"
        timestep = 'minute'
        refresh_cache = False
        tz_name = "America/Chicago"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        assert data_source.datetime_start.isoformat() == "2025-01-01T00:00:00-06:00"
        assert data_source.datetime_end.isoformat() == "2025-01-01T23:59:00-06:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df
        assert not df.empty

        # the data is missing minutes but it won't be more than one bar per minute
        assert len(df.index) <= 1440

        # Minute data has missing bars. The irst bar of 2025 is 22 minutes in
        assert df.index[0].isoformat() == '2025-01-01T00:22:00-06:00'

        # last bar was 4 minutes before the hour.
        assert df.index[-1].isoformat() == '2025-01-01T23:56:00-06:00'

    # @pytest.mark.skip()
    def test_single_crypto_hour_bars_utc(self):
        tickers = "BTC/USD"
        start_date = "2025-01-01"
        end_date = "2025-01-02"
        timestep = 'hour'
        refresh_cache = False
        tz_name = "UTC"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        assert data_source.datetime_start.isoformat() == "2025-01-01T00:00:00+00:00"
        assert data_source.datetime_end.isoformat() == "2025-01-01T23:59:00+00:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 24
        assert df.index[0] == data_source.datetime_start
        assert df.index[-1] == data_source.datetime_end + timedelta(minutes=1) - timedelta(hours=1)

    # @pytest.mark.skip()
    def test_single_crypto_hour_bars_america_chicago(self):
        tickers = "BTC/USD"
        start_date = "2025-01-01"
        end_date = "2025-01-02"
        timestep = 'hour'
        refresh_cache = False
        tz_name = "America/Chicago"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            tz_name=tz_name
        )

        assert data_source.datetime_start.isoformat() == "2025-01-01T00:00:00-06:00"
        assert data_source.datetime_end.isoformat() == "2025-01-01T23:59:00-06:00"
        assert isinstance(data_source.pandas_data, dict)
        assert next(iter(data_source.pandas_data))[0].symbol == "BTC"
        assert next(iter(data_source.pandas_data))[1].symbol == "USD"

        data = list(data_source.pandas_data.values())[0]
        df = data.df

        assert not df.empty
        assert len(df.index) == 24
        assert df.index[0] == data_source.datetime_start
        assert df.index[-1] == data_source.datetime_end + timedelta(minutes=1) - timedelta(hours=1)

