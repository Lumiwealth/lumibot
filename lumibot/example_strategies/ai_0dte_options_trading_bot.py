"""0DTE Options AI Trading Bot.

Sells a same-day (0DTE) bear call spread on SPY, the S&P 500 ETF, and lets it
expire worthless when the market stays below the short strike. A research agent
checks SPY and today's expiring calls every 5 minutes. A trading agent opens one
spread a day as one order and closes it early when the trade goes wrong.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class ZeroDTEOptionsTradingBot(Strategy):
    parameters = {"symbol": "SPY"}

    def initialize(self):
        self.sleeptime = "5M"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You research a bear call spread on the symbol in the context that expires today. Check its "
                "price and today's expiring calls. Find the call with delta closest to +0.20 and the "
                "call exactly 5 points higher. Report both exact contracts, their deltas, bid and ask, and "
                "the net credit. Also report any spread we already hold, with its cost to close. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You trade a bear call spread that expires today on the symbol in the context. Use the "
                "options-trading skill. First manage the spread we hold. Close it as one order when we have "
                "kept 50% of the credit, the cost to close reaches 2x the credit, the price rises above the "
                "short strike, or less than 10 minutes remain before the close. If we hold none and have not "
                "opened one today, sell the researched spread as one multi-leg order for a net credit. Risk "
                "about 1% of the account, at most 2 contracts. Never open a new spread in the last 10 minutes."
            ),
        )

    def on_trading_iteration(self):
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(task_prompt="Check today's 0DTE bear call spread.", context=facts)
        self.agents["trader"].run(
            task_prompt="Manage or open today's spread.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        ZeroDTEOptionsTradingBot.backtest(AlpacaBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 6))
    else:
        ZeroDTEOptionsTradingBot().run_live()
