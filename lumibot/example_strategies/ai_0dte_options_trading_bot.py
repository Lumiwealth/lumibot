"""0DTE Options AI Trading Bot.

Sells a same-day (0DTE) bear call spread on SPY, the S&P 500 ETF, and lets it
expire worthless when SPY stays below the short strike. A research agent checks
SPY and today's expiring calls every 15 minutes. A trading agent opens one spread
a day and closes it early when the trade goes wrong.
"""

from lumibot.strategies import Strategy


class ZeroDTEOptionsTradingBot(Strategy):
    parameters = {"symbol": "SPY"}

    def initialize(self):
        self.sleeptime = "15M"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "Look at the symbol's price and the calls that expire today. Find a bear call spread: sell "
                "the call near 0.20 delta and buy the call 5 points higher. Report the two contracts and the "
                "credit, and any spread we already hold with its cost to close. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Sell one bear call spread that expires today, once a day. Risk about 1% of the account. "
                "Close it early when we have kept half the credit, when closing costs twice the credit, or "
                "when the price rises above the short strike. Do not open a new spread in the last 30 minutes "
                "of the day."
            ),
        )

    def on_trading_iteration(self):
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(task_prompt="Check today's 0DTE call spread.", context=facts)
        self.agents["trader"].run(
            task_prompt="Manage or open today's spread.", context={**facts, "research": research.summary}
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        ZeroDTEOptionsTradingBot.backtest(AlpacaBacktesting)
    else:
        ZeroDTEOptionsTradingBot().run_live()
