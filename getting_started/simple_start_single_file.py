from datetime import datetime

from credentials import AlpacaConfig
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class MyStrategy(Strategy):
    def initialize(self, symbol=""):
        # Built in parameters
        self.sleeptime = 180

        # Custom parameters
        self.symbol = symbol
        self.quantity = 1
        self.side = "buy"

    def on_trading_iteration(self):
        self.order = self.create_order(self.symbol, self.quantity, self.side)
        self.submit_order(self.order)


logfile = "../logs/test.log"
trader = Trader(logfile=logfile)
broker = Alpaca(AlpacaConfig)

budget = 100000
backtesting_start = datetime(2020, 1, 1)
backtesting_end = datetime(2020, 12, 31)
strategy_name = "MyStrategy"

strategy = MyStrategy(strategy_name, budget, broker, symbol="SPY")

strategy.backtest(
    strategy_name,
    budget,
    YahooDataBacktesting,
    backtesting_start,
    backtesting_end,
    symbol="SPY",
)

trader.add_strategy(strategy)
trader.run_all()
