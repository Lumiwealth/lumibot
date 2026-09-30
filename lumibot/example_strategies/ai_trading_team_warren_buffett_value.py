"""Warren Buffett AI Stock Picker.

Invests the way Warren Buffett describes in his Berkshire Hathaway letters: buy
great businesses at fair prices and hold them. A research agent reads each
company's SEC filings and picks the best mix of quality and price. A skeptic
agent, like Charlie Munger, attacks each pick. A trading agent owns the
companies that survive.
Not affiliated with or endorsed by Warren Buffett or Berkshire Hathaway.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AITradingTeamWarrenBuffettValueStrategy(Strategy):
    parameters = {"universe": ["AAPL", "MSFT", "GOOGL", "COST", "V", "MA", "KO", "AXP", "JPM", "PG"]}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You research companies the way Warren Buffett does. For each stock in the universe, read "
                "its latest SEC filings and financial statements. Judge business quality: steady profits, "
                "high return on capital, and a lasting edge over rivals. Then check the price as of today: "
                "earnings yield, free cash flow yield, P/E, and net debt. Compare the companies with each "
                "other instead of building a full valuation model. Name the 3 to 5 best mixes of business "
                "quality and price. Do not trade."
            ),
        )
        self.agents.create(
            name="skeptic",
            allow_trading=False,
            system_prompt=(
                "You are the skeptic, like Charlie Munger. For each pick in the research, look for the "
                "reason it is a mistake: a price that is too high, a moat that is shrinking, too much debt, "
                "or numbers that do not add up. Keep the picks that survive, 3 to 5 of them. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You invest like Warren Buffett. Own the 3 to 5 stocks the skeptic kept, split about "
                "evenly, near 100% of the account. Hold for the long run and do not trade on small moves. "
                "Sell a stock only when the skeptic drops it because the business got worse or the price "
                "got far too high."
            ),
        )

    def on_trading_iteration(self):
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Rank the universe.", context=facts)
        review = self.agents["skeptic"].run(
            task_prompt="Attack each pick.", context={**facts, "research": research.summary}
        )
        self.agents["trader"].run(
            task_prompt="Own the picks that survived.",
            context={**facts, "research": research.summary, "skeptic": review.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamWarrenBuffettValueStrategy.backtest(YahooDataBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 16))
    else:
        AITradingTeamWarrenBuffettValueStrategy().run_live()
