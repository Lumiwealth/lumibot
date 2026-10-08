"""Bill Ackman Portfolio AI Trading Bot.

Invests the way Bill Ackman describes his style: own just a few simple,
high-quality companies and put real money behind them. A research agent finds
the best ideas. A short seller agent attacks each one. A trading agent holds the
3 to 5 that survive, with the most money in the best ideas.
Not affiliated with or endorsed by Bill Ackman or Pershing Square.
"""

from lumibot.strategies import Strategy


class AITradingTeamBillAckmanConcentratedStrategy(Strategy):
    parameters = {"universe": ["GOOGL", "CMG", "HLT", "QSR", "UBER", "CP", "LOW", "MDLZ", "BKNG", "MSFT"]}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "Find the simple, predictable companies in the universe that make lots of cash and trade at a "
                "good price. Rank your top 5 ideas and say why. Do not trade."
            ),
        )
        self.agents.create(
            name="short_seller",
            allow_trading=False,
            system_prompt=(
                "You are a short seller. Attack each idea: too much debt, weak management, strong rivals, or "
                "a price that is too high. Say which ideas survive. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Hold the 3 to 5 ideas that survived, with more money in the best ones. Sell a stock when it "
                "no longer survives the attack."
            ),
        )

    def on_trading_iteration(self):
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Rank your best ideas.", context=facts)
        attack = self.agents["short_seller"].run(
            task_prompt="Attack each idea.", context={**facts, "research": research.summary}
        )
        self.agents["trader"].run(task_prompt="Hold the survivors.", context={**facts, "short_seller": attack.summary})


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBillAckmanConcentratedStrategy.backtest(YahooDataBacktesting)
    else:
        AITradingTeamBillAckmanConcentratedStrategy().run_live()
