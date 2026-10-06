"""Opening Range Breakout AI Trading Bot.

Marks each stock's high and low from the first 15 minutes of the day, then buys
the stock that breaks out above that high. A research agent scans a list of big
stocks every hour. A trading agent buys the best breakout with a profit target
and a stop, and is out of the market by the close.
"""

from lumibot.strategies import Strategy


class AIOpeningRangeBreakoutStrategy(Strategy):
    parameters = {"universe": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD"]}

    def initialize(self):
        self.sleeptime = "1H"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "For each stock in the universe, find today's opening range: the high and low from 9:30 to "
                "9:45 ET. List the stocks that have since closed above their opening range high, best first. "
                "Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Hold at most one stock. If we hold none, buy the best breakout. The stop is the opening "
                "range low: size the trade so the stop loses at most 1% of the account, and place a profit "
                "target at 1.5 times that risk. Sell if the price falls back into the range, and always "
                "before the close."
            ),
        )

    def on_trading_iteration(self):
        if self.get_datetime().hour < 10:  # the opening range is not finished yet
            return
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Find the best breakout.", context=facts)
        self.agents["trader"].run(task_prompt="Trade the breakout.", context={**facts, "research": research.summary})


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AIOpeningRangeBreakoutStrategy.backtest(AlpacaBacktesting)
    else:
        AIOpeningRangeBreakoutStrategy().run_live()
