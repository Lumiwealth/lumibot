from datetime import datetime
from lumibot.strategies import Strategy
from lumibot.backtesting import YahooDataBacktesting


class MyStrategy(Strategy):
    def on_trading_iteration(self):
        if self.first_iteration:
            aapl = self.create_order("SNDK", 10, "buy")
            self.submit_order(aapl)


MyStrategy.backtest(
    YahooDataBacktesting,
    datetime(2026, 1, 1),
    datetime(2026, 9, 10),
)
