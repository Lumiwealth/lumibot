from datetime import datetime

from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class AlpacaConfig:
    # Put your own Alpaca key here:
    API_KEY = "YOUR_ALPACA_API_KEY"
    # Put your own Alpaca secret here:
    API_SECRET = "YOUR_ALPACA_SECRET"
    # If you want to go live, you must change this. It is currently set for paper trading
    ENDPOINT = "https://paper-api.alpaca.markets"


class MyStrategy(Strategy):
    def initialize(self, symbol=""):
        # Will make on_trading_iteration() run every 180 minutes
        self.sleeptime = 180

        # Custom parameters
        self.symbol = symbol
        self.quantity = 1
        self.side = "buy"

    def on_trading_iteration(self):
        self.order = self.create_order(self.symbol, self.quantity, self.side)
        self.submit_order(self.order)


budget = 100000
strategy_name = "My Strategy"

trader = Trader()
broker = Alpaca(AlpacaConfig)
strategy = MyStrategy(strategy_name, budget, broker, symbol="SPY")

# Backtest this strategy
backtesting_start = datetime(2020, 1, 1)
backtesting_end = datetime(2020, 12, 31)
strategy.backtest(
    strategy_name,
    budget,
    YahooDataBacktesting,
    backtesting_start,
    backtesting_end,
    symbol="SPY",
)

# Run the strategy live
trader.add_strategy(strategy)
trader.run_all()
