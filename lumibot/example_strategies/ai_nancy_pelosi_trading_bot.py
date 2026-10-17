"""Nancy Pelosi Stock Trading Bot.

Holds the same stocks Nancy Pelosi owns, in the same proportions. Every day a
research agent works out what she owns today from her public financial
disclosure reports, and a trading agent makes the account match.
"""

from lumibot.strategies import Strategy


class NancyPelosiTradingBot(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "Find out which stocks Nancy Pelosi owns today and how much of each. Use her official financial "
                "disclosure reports on the U.S. House Clerk website (https://disclosures-clerk.house.gov): her "
                "newest yearly report of everything she owns, plus every trade report filed after it. Use only "
                "reports filed before today. If your last answer is given, start from it and only read reports filed "
                "since then. Skip options, funds, bonds, real estate and private companies. Give each stock's "
                "ticker and its share of her stock portfolio in percent, using the middle of each reported dollar "
                "range. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Make the account hold the same stocks in the same percentages as the research. Sell stocks that "
                "are not in it and buy the ones you do not own. Leave a stock alone when it is within 2 percentage "
                "points of its target. Sell before you buy, never short, and never spend more cash than you have."
            ),
        )

    def on_trading_iteration(self):
        # The researcher gets its last answer back, so it only needs to read reports filed since then.
        research = self.agents["researcher"].run(
            task_prompt="What does Nancy Pelosi own today?",
            context={"your_last_answer": self.vars.get("last_research", "none yet")},
        )
        self.vars.set("last_research", research.summary)
        self.agents["trader"].run(
            task_prompt="Match the account to the research.", context={"research": research.summary}
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
