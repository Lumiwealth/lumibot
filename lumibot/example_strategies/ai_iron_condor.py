"""Iron Condor AI Trading Bot.

Sells an iron condor on SPY about a month out and collects the premium while SPY
stays in a range. A research agent picks the four option contracts from the live
option chain. A trading agent opens the condor as one order and closes it at the
profit target, the loss limit, or the time stop.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AIIronCondorStrategy(Strategy):
    parameters = {"symbol": "SPY"}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You research an iron condor on the symbol in the context. Check its price, recent moves, "
                "and option chain. Pick one expiration 30 to 45 days out. Choose a short put near -0.16 "
                "delta and a short call near +0.16 delta, each with a long option exactly 5 points farther "
                "out. Report the four exact contracts, their deltas, bid and ask, and the net credit. Also "
                "report any iron condor we already hold, with its cost to close and its short deltas. "
                "Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You trade one iron condor at a time on the symbol in the context. Use the options-trading "
                "skill. First manage the condor we hold. Close it as one order when we have kept 50% of the "
                "credit, the cost to close reaches 2x the credit, 21 days or less are left, the price "
                "crosses a short strike, or a short option reaches 0.30 delta. If we hold none, open the "
                "researched condor as one multi-leg order for a net credit. Risk about 2% of the account, "
                "at most 10 contracts."
            ),
        )

    def on_trading_iteration(self):
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(task_prompt="Find today's iron condor.", context=facts)
        self.agents["trader"].run(
            task_prompt="Manage or open the iron condor.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AIIronCondorStrategy.backtest(AlpacaBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 16))
    else:
        AIIronCondorStrategy().run_live()
