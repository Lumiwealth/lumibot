"""Put Credit Spread AI Trading Bot.

Sells a put credit spread on SPY about a month out and keeps the premium if SPY
stays above the short strike. A research agent picks the two contracts from the
live option chain. A trading agent opens the spread and closes it at the profit
target, the loss limit, or the time stop.
"""

from lumibot.strategies import Strategy


class AICreditSpreadStrategy(Strategy):
    parameters = {"symbol": "SPY"}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "Look at the symbol's price and option chain. Find a put credit spread 30 to 45 days out: "
                "sell a put near 0.16 delta and buy a put 5 points lower. Report the two contracts and the "
                "credit, and any spread we already hold with its cost to close. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Hold one put credit spread at a time. Close it when we have kept half the credit, when "
                "closing costs twice the credit, or when 21 days are left. If we hold none, sell the "
                "researched spread. Risk about 2% of the account."
            ),
        )

    def on_trading_iteration(self):
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(task_prompt="Find today's put credit spread.", context=facts)
        self.agents["trader"].run(
            task_prompt="Manage or open the spread.", context={**facts, "research": research.summary}
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AICreditSpreadStrategy.backtest(AlpacaBacktesting)
    else:
        AICreditSpreadStrategy().run_live()
