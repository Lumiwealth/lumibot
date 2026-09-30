"""Opening Range Breakout AI Trading Bot.

Waits for the first 15 minutes of the trading day, marks each stock's high and
low, and buys the stock that breaks out above its high. A research agent scans a
list of big, liquid stocks every hour. A trading agent buys the best breakout,
sets a profit target and a stop, and is out of the market by the close.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AIOpeningRangeBreakoutStrategy(Strategy):
    parameters = {
        "universe": ["SPY", "QQQ", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AMD",
                     "AVGO", "NFLX", "JPM", "XOM", "LLY", "COST", "DIS", "UBER", "BA", "CRM"],
    }

    def initialize(self):
        self.sleeptime = "1H"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You scan the universe for opening range breakouts. Load today's 5-minute bars for every "
                "symbol with one history call into a table, then use SQL. A symbol's opening range is the "
                "high and low of 9:30 to 9:45 ET. A breakout is a completed bar that closes above the range "
                "high. Rank breakouts by how far they closed above the high. Report the best one or none, "
                "with its range high and low, plus any positions we hold. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You trade opening range breakouts from the universe. Hold at most one stock. If we hold "
                "none, buy the best breakout from the research. The stop is the range low: size so hitting "
                "it loses at most 1% of the account, at most 200 shares. Right after the buy, place a "
                "take-profit limit at 1.5 times that risk and a stop order at the range low. Sell if a bar "
                "closes back inside the range, and sell everything before the close. One entry per stock a day."
            ),
        )

    def on_trading_iteration(self):
        if self.get_datetime().hour < 10:  # the opening range is not finished yet
            return
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Find the best breakout.", context=facts)
        self.agents["trader"].run(
            task_prompt="Trade the breakout.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AIOpeningRangeBreakoutStrategy.backtest(AlpacaBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 9))
    else:
        AIOpeningRangeBreakoutStrategy().run_live()
