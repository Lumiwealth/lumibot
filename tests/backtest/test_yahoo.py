import datetime
import pandas as pd
import pytz
import pytest

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.strategies import Strategy
from lumibot.traders import Trader


class YahooPriceTest(Strategy):
    parameters = {
        "symbol": "SPY",  # The symbol to trade
    }

    def initialize(self):
        # There is only one trading operation per day
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        # Get the parameters
        symbol = self.parameters["symbol"]

        # Get the datetime
        self.dt = self.get_datetime()

        # Get the last price
        self.last_price = self.get_last_price(symbol)


class TestYahooBacktestFull:

    def test_yahoo_no_future_bars_before_open(self, monkeypatch):
        tz = pytz.timezone('America/New_York')
        asset = 'SPY'
        index = pd.DatetimeIndex([
            tz.localize(datetime.datetime(2023, 10, 31, 16, 0)),
            tz.localize(datetime.datetime(2023, 11, 1, 16, 0)),
        ])

        frame = pd.DataFrame(
            {
                'Open': [416.18, 419.20],
                'High': [416.50, 420.10],
                'Low': [415.80, 418.90],
                'Close': [418.53, 419.54],
                'Volume': [1_000_000, 1_100_000],
                'Dividends': [0.0, 0.0],
                'Stock Splits': [0.0, 0.0],
            },
            index=index,
        )

        monkeypatch.setattr(
            'lumibot.tools.YahooHelper.get_symbol_data',
            lambda *args, **kwargs: frame,
        )

        data_source = YahooDataBacktesting(
            datetime_start=datetime.datetime(2023, 10, 30),
            datetime_end=datetime.datetime(2023, 11, 2),
        )
        data_source._datetime = tz.localize(datetime.datetime(2023, 11, 1, 8, 45))

        price = data_source.get_last_price(asset, timestep='day')
        assert round(price, 2) == 416.18

        bars = data_source.get_historical_prices(asset, 1, timestep='day')
        # The bar timestamp must be strictly before the current backtest clock to avoid lookahead.
        assert bars.df.index[-1] < data_source._datetime

    def test_yahoo_last_price(self):
        """
        Test the YahooDataBacktesting class by running a backtest and checking that the strategy object is returned
        along with the correct results
        """
        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime.datetime(2023, 11, 1)
        backtesting_end = datetime.datetime(2023, 11, 2)

        data_source = YahooDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
        )

        broker = BacktestingBroker(data_source=data_source)

        poly_strat_obj = YahooPriceTest(
            broker=broker,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
        )

        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(poly_strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False, tearsheet_file="")

        assert results

        last_price = poly_strat_obj.last_price
        # Round to 2 decimal places
        last_price = round(last_price, 2)

        assert last_price == 416.18  # This is the correct price for 2023-11-01 (the open price)

