"""Nancy Pelosi Copy Trading Bot.

Copies Nancy Pelosi's stocks and her call options (same stock, same expiration),
sized to your account. Every day a research agent works out what she owns today
from her public financial disclosure reports, and a trading agent makes the
account match. "options_share" sets the share of the account in calls.
"""

from lumibot.strategies import Strategy


class NancyPelosiCopyTradingBot(Strategy):
    parameters = {"options_share": 0.2}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "Find out which stocks and call options Nancy Pelosi owns today and how much of each. Use her "
                "official financial disclosure reports on the U.S. House Clerk website "
                "(https://disclosures-clerk.house.gov): her newest yearly report of everything she owns, plus every "
                "trade report filed after it. Use only reports filed before today. If your last answer is given, "
                "start from it and only read reports filed since then. Skip expired options, funds, bonds, real "
                "estate and private companies. Give each stock's ticker and dollar range, and each call "
                "option's stock, strike price, expiration date, number of contracts and dollar range. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Make the account copy the research. Put options_share of the account into her call options and the "
                "rest into her stocks, each in proportion to the middle of its dollar range. Use the same stock and "
                "expiration as each of her calls, her strike if one contract fits that call's money, otherwise the "
                "nearest higher strike that fits. Sell holdings that are not in the research and buy the ones you do "
                "not own. Leave a holding alone when it is within 2 percentage points of its target. Sell before you "
                "buy, never short, and never spend more cash than you have."
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
            task_prompt="Match the account to the research.",
            context={"research": research.summary, "options_share": self.parameters["options_share"]},
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        NancyPelosiCopyTradingBot.backtest(AlpacaBacktesting)
    else:
        NancyPelosiCopyTradingBot().run_live()
