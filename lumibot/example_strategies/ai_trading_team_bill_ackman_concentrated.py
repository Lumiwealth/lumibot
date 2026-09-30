"""Bill Ackman Portfolio AI Trading Bot.

Invests the way Bill Ackman describes his style: own just a few simple,
high-quality companies and put real money behind them. A research agent finds
the best ideas. A short seller agent attacks each one the way a short seller
would. A trading agent holds the 3 to 5 that survive, with most of the account
in the top ideas.
Not affiliated with or endorsed by Bill Ackman or Pershing Square.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AITradingTeamBillAckmanConcentratedStrategy(Strategy):
    parameters = {"universe": ["GOOGL", "CMG", "HLT", "QSR", "UBER", "CP", "LOW", "MDLZ", "BKNG", "MSFT"]}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You research companies the way Bill Ackman does. For each stock in the universe, check its "
                "financial statements and recent price. Look for simple, predictable businesses that make "
                "lots of cash and are priced well. Rank your top 5 ideas and say why. Do not trade."
            ),
        )
        self.agents.create(
            name="short_seller",
            allow_trading=False,
            system_prompt=(
                "You are a short seller. Attack each idea in the research: too much debt, weak management, "
                "strong rivals, accounting that looks off, or a price that is too high. Say which ideas "
                "survive your attack and which do not. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You invest like Bill Ackman. Own the 3 to 5 ideas that survived the short seller, with "
                "bigger weights on the best ones, near 100% of the account. Only buy stocks in the universe. "
                "Sell a stock when it no longer survives the attack or drops out of the top 5."
            ),
        )

    def on_trading_iteration(self):
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Rank the universe.", context=facts)
        attack = self.agents["short_seller"].run(
            task_prompt="Attack each idea.", context={**facts, "research": research.summary}
        )
        self.agents["trader"].run(
            task_prompt="Hold the ideas that survived.",
            context={**facts, "research": research.summary, "short_seller": attack.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBillAckmanConcentratedStrategy.backtest(YahooDataBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 16))
    else:
        AITradingTeamBillAckmanConcentratedStrategy().run_live()
