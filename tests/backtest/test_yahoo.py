from datetime import datetime

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.entities import Asset

from tests.fixtures import (
    BuyOnceTestStrategy,
    GetHistoricalTestStrategy
)


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

    def test_yahoo_last_price(self):
        """
        Test the YahooDataBacktesting class by running a backtest and checking that the strategy object is returned
        along with the correct results
        """
        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime(2023, 11, 1)
        backtesting_end = datetime(2023, 11, 2)

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
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, tearsheet_file="")

        assert results

        last_price = poly_strat_obj.last_price
        # Round to 2 decimal places
        last_price = round(last_price, 2)

        assert last_price == 419.20  # This is the correct price for 2023-11-01 (the open price)

    def test_single_stock_day_bars_america_new_york(self):
        backtesting_start = datetime(2025, 1, 13)
        backtesting_end = datetime(2025, 1, 18)
        sleeptime = '1D'
        market = 'NYSE'

        strategy: BuyOnceTestStrategy
        results, strategy = BuyOnceTestStrategy.run_backtest(
            datasource_class=YahooDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "asset": Asset('AMZN', asset_type='stock'),
                "sleeptime": sleeptime,
                "market": market
            },
        )
        assert results
        assert strategy
        assert strategy.broker
        assert isinstance(strategy.broker, BacktestingBroker)
        assert strategy.broker.data_source
        assert isinstance(strategy.broker.data_source, YahooDataBacktesting)

        # Data source tests
        data_source = strategy.broker.data_source
        assert data_source.datetime_start.isoformat() == "2025-01-13T00:00:00-05:00"
        assert data_source.datetime_end.isoformat() == '2025-01-17T23:59:00-05:00'
        
        df = list(data_source._data_store.values())[0]
        assert not df.empty

        # daily bars are indexed at close for yahoo. Weird.
        assert df.index[0].isoformat() == "1997-05-15T16:00:00-04:00"

        # Trading strategy tests

        # check when trading iterations happened
        assert strategy.trading_iterations[0].isoformat() == '2025-01-13T09:30:00-05:00'
        assert strategy.trading_iterations[-1].isoformat() == '2025-01-17T09:30:00-05:00'
        assert strategy.num_trading_iterations == 5

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'

        assert tracker['last_price'] == 218.05999755859375  # Open of '2025-01-13T16:00:00-05:00'
        assert tracker["avg_fill_price"] == 218.06  # Open of '2025-01-13T16:00:00-05:00'
