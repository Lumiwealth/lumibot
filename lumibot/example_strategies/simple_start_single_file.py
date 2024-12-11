from datetime import datetime

from credentials import AlpacaConfig

from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.strategies.strategy import Strategy

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


if __name__ == "__main__":
    live = True

    broker = Alpaca(AlpacaConfig)
    strategy = MyStrategy(broker, symbol="SPY")

    if not live:
        # Backtest this strategy
        backtesting_start = datetime(2020, 1, 1)
        backtesting_end = datetime(2020, 12, 31)
        strategy.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            symbol="SPY",
        )
    else:
        # Run the strategy live
        strategy.run_live()