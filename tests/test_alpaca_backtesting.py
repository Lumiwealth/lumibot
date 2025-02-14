from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from lumibot.backtesting import AlpacaBacktesting, BacktestingBroker
from lumibot.traders import Trader
from lumibot.credentials import ALPACA_CONFIG
from lumibot.strategies import Strategy
from lumibot.entities import Asset


class AlpacaBacktestTestStrategy(Strategy):

    # Set the initial values for the strategy
    # noinspection PyAttributeOutsideInit
    def initialize(self, parameters: Any = None) -> None:
        self.set_market(self.parameters.get("market", "NYSE"))
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        self.symbol = self.parameters.get("symbol", "AMZN")

        self.orders = []
        self.prices = {}
        self.market_opens = []
        self.market_closes = []
        self.trading_iterations = []
        # Track times to test LifeCycle order methods. Format: {order_id: {'fill': timestamp, 'submit': timestamp}}
        self.order_time_tracker = defaultdict(lambda: defaultdict(datetime))

    def before_market_opens(self):
        self.log_message(f"Before market opens called at {self.get_datetime().isoformat()}")
        self.market_opens.append(self.get_datetime())

    def after_market_closes(self):
        self.log_message(f"After market closes called at {self.get_datetime().isoformat()}")
        self.market_closes.append(self.get_datetime())
        orders = self.get_orders()
        self.log_message(f"AlpacaBacktestTestStrategy: {len(orders)} orders executed today")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"AlpacaBacktestTestStrategy: Filled Order: {order}")
        self.order_time_tracker[order.identifier]["fill"] = self.get_datetime()

    def on_new_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: New Order: {order}")
        self.order_time_tracker[order.identifier]["submit"] = self.get_datetime()

    def on_canceled_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: Canceled Order: {order}")
        self.order_time_tracker[order.identifier]["cancel"] = self.get_datetime()

    # Trading Strategy: Backtest will only buy traded assets on first iteration
    def on_trading_iteration(self):
        self.trading_iterations.append(self.get_datetime())
        if self.first_iteration:
            now = self.get_datetime()

            asset = Asset(self.parameters["symbol"])
            current_asset_price = self.get_last_price(asset)

            # Buy 1 shares of the asset for the test
            qty = 1
            self.log_message(f"Buying {qty} shares of {asset} at {current_asset_price} @ {now}")
            order = self.create_order(asset, quantity=qty, side="buy")
            submitted_order = self.submit_order(order)
            self.orders.append(submitted_order)
            self.prices[submitted_order.identifier] = current_asset_price

        # Not the 1st iteration, cancel orders.
        else:
            self.cancel_open_orders()


# @pytest.mark.skip()
@pytest.mark.skipif(
    not ALPACA_CONFIG['API_KEY'],
    reason="This test requires an alpaca API key"
)
@pytest.mark.skipif(
    ALPACA_CONFIG['API_KEY'] == '<your key here>',
    reason="This test requires an alpaca API key"
)
class TestAlpacaBacktests:
    """Tests for running backtests with AlpacaBacktesting, BacktestingBroker, and Trader."""

    def test_day_data_backtest(self):
        """
        Test AlpacaBacktesting with Lumibot Backtesting and real API calls to Alpaca.
        This test will buy 1 shares of something.
        """
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = True
        tz_name = "America/New_York"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_CONFIG,
            # refresh_cache=refresh_cache,
            tz_name=tz_name,
        )
        broker = BacktestingBroker(data_source=data_source)
        strat_obj = AlpacaBacktestTestStrategy(
            broker=broker,
            parameters={
                "symbol": "AMZN",
                "sleeptime": "1D",
                "market": "NYSE"
            },
        )
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, tearsheet_file="")
        # Assert the end datetime is before the next trading day
        assert broker.datetime.isoformat() == "2025-01-18T09:29:00-05:00"
        assert results
        self.verify_backtest_results(strat_obj)

        assert list(strat_obj.prices.values())[0] == 218.46
        assert strat_obj.orders[0].avg_fill_price == 220.44
        assert list(strat_obj.order_time_tracker.values())[0]['fill'].isoformat() == '2025-01-13T09:30:00-05:00'

    # @pytest.mark.skip()
    def test_minute_data_backtest(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'minute'
        refresh_cache = True
        tz_name = "America/New_York"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_CONFIG,
            # refresh_cache=refresh_cache,
            tz_name=tz_name
        )
        broker = BacktestingBroker(data_source=data_source)
        strat_obj = AlpacaBacktestTestStrategy(
            broker=broker,
            parameters={
                "symbol": "AMZN",
                "sleeptime": "60M",
                "market": "NYSE"
            },
        )

        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, tearsheet_file="")
        assert results
        # Assert the end datetime is before the next trading day
        assert broker.datetime.isoformat() == '2025-01-21T08:30:00-05:00'
        self.verify_backtest_results(strat_obj)

        assert list(strat_obj.prices.values())[0] == 218.06
        assert strat_obj.orders[0].avg_fill_price == 218.06
        assert list(strat_obj.order_time_tracker.values())[0]['fill'].isoformat() == '2025-01-13T09:30:00-05:00'

    # noinspection PyMethodMayBeStatic
    def verify_backtest_results(self, strat_obj):
        assert isinstance(strat_obj, AlpacaBacktestTestStrategy)

        # Checks bug where LifeCycle methods not being called during PANDAS backtesting
        # assert len(strat_obj.market_opens) == 5
        # assert len(strat_obj.market_closes) == 5

        # check the right number of bars were called
        if strat_obj.sleeptime == '1D':
            assert len(strat_obj.trading_iterations) == 5
        elif strat_obj.sleeptime == "60M":
            assert len(strat_obj.trading_iterations) == 5 * 7

        # Check order submitted
        assert len(strat_obj.orders) == 1
        stock_order = strat_obj.orders[0]
        asset_order_id = stock_order.identifier
        assert asset_order_id in strat_obj.prices

        # Check that the on_*_order methods were called
        assert len(strat_obj.order_time_tracker) == 1
        assert asset_order_id in strat_obj.order_time_tracker
        assert strat_obj.order_time_tracker[asset_order_id]["submit"]
        assert (
            strat_obj.order_time_tracker[asset_order_id]["fill"]
            >= strat_obj.order_time_tracker[asset_order_id]["submit"]
        )


# @pytest.mark.skip()
@pytest.mark.skipif(
    not ALPACA_CONFIG['API_KEY'],
    reason="This test requires an alpaca API key"
)
@pytest.mark.skipif(
    ALPACA_CONFIG['API_KEY'] == '<your key here>',
    reason="This test requires an alpaca API key"
)
class TestAlpacaBacktesting:
    """Tests for the AlpacaBacktesting class itself."""

    # @pytest.mark.skip()
    def test_single_stock_day_bars(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = True

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_CONFIG,
            # refresh_cache=refresh_cache,
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
        refresh_cache = True

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_CONFIG,
            # refresh_cache=refresh_cache,
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
    def test_tz_name_day_bars(self):
        tickers = "AMZN"
        start_date = "2025-01-13"
        end_date = "2025-01-18"
        timestep = 'day'
        refresh_cache = True
        tz_name = "US/Eastern"

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_CONFIG,
            # refresh_cache=refresh_cache,
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
        refresh_cache = True

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_CONFIG,
            # refresh_cache=refresh_cache,
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
        refresh_cache = True

        data_source = AlpacaBacktesting(
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            timestep=timestep,
            config=ALPACA_CONFIG,
            # refresh_cache=refresh_cache,
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
