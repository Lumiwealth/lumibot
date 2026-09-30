"""Put Credit Spread AI Trading Bot.

Sells a put credit spread on SPY about a month out and keeps the premium if SPY
stays above the short strike. A research agent picks the two option contracts
from the live option chain. A trading agent opens the spread as one order and
closes it at the profit target, the loss limit, or the time stop.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AICreditSpreadStrategy(Strategy):
    parameters = {"symbol": "SPY"}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You research a put credit spread on the symbol in the context. Check its price, trend, "
                "and option chain. Pick one expiration 30 to 45 days out. Choose a short put near -0.16 "
                "delta and a long put exactly 5 points lower. Report both exact contracts, their deltas, "
                "bid and ask, and the net credit. Also report any spread we already hold, with its cost to "
                "close and its short delta. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You trade one put credit spread at a time on the symbol in the context. Use the "
                "options-trading skill. First manage the spread we hold. Close it as one order when we have "
                "kept 50% of the credit, the cost to close reaches 2x the credit, 21 days or less are left, "
                "or the short put reaches -0.30 delta. If we hold none, open the researched spread as one "
                "multi-leg order for a net credit. Risk about 2% of the account, at most 10 contracts."
            ),
        )

    def on_trading_iteration(self):
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(task_prompt="Find today's put credit spread.", context=facts)
        self.agents["trader"].run(
            task_prompt="Manage or open the credit spread.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AICreditSpreadStrategy.backtest(AlpacaBacktesting, datetime(2026, 1, 5), datetime(2026, 2, 6))
    else:
        AICreditSpreadStrategy().run_live()
