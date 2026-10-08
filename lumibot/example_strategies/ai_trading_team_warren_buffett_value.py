"""Warren Buffett AI Stock Picker.

Invests the way Warren Buffett describes in his Berkshire Hathaway letters: buy
great businesses at fair prices and hold them. A research agent reads each
company's reports and picks the best mix of quality and price. A skeptic agent,
like Charlie Munger, attacks each pick. A trading agent owns what survives.
Not affiliated with or endorsed by Warren Buffett or Berkshire Hathaway.
"""

from lumibot.strategies import Strategy


class AITradingTeamWarrenBuffettValueStrategy(Strategy):
    parameters = {"universe": ["AAPL", "MSFT", "GOOGL", "COST", "V", "MA", "KO", "AXP", "JPM", "PG"]}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "Read each company's latest financial reports and check its stock price today. Pick the 3 to "
                "5 companies with the best mix of steady profits, a lasting edge over rivals, and a fair "
                "price. Do not trade."
            ),
        )
        self.agents.create(
            name="skeptic",
            allow_trading=False,
            system_prompt=(
                "You are a skeptic like Charlie Munger. Attack each pick: is the price too high, is the edge "
                "shrinking, is there too much debt? Keep only the picks that survive. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Own the picks the skeptic kept, split about evenly. Hold for the long run and ignore small "
                "price moves. Sell a stock only when the skeptic drops it."
            ),
        )

    def on_trading_iteration(self):
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Pick the best companies.", context=facts)
        review = self.agents["skeptic"].run(
            task_prompt="Attack each pick.", context={**facts, "research": research.summary}
        )
        self.agents["trader"].run(
            task_prompt="Own the picks that survived.", context={**facts, "skeptic": review.summary}
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamWarrenBuffettValueStrategy.backtest(YahooDataBacktesting)
    else:
        AITradingTeamWarrenBuffettValueStrategy().run_live()
