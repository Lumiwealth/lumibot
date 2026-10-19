"""Nancy Pelosi Stock Trading Bot.

Holds the same stocks Nancy Pelosi owns, in the same proportions. Every day a
research agent works out what she owns today from her public financial
disclosure reports, a portfolio agent turns that into target weights, and a
trading agent makes the account match.
"""

from lumibot.strategies import Strategy

HOUSE = "https://disclosures-clerk.house.gov/public_disc"


class NancyPelosiTradingBot(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "Find out which stocks Nancy Pelosi owns today. Use her official financial disclosure reports on "
                "the U.S. House Clerk website: her newest yearly report of everything she owns, plus every trade "
                f"report filed after it. Each year's list of filings is a ZIP file at {HOUSE}/financial-pdfs/2026FD.ZIP "
                f"(change 2026 to the year); yearly reports are at {HOUSE}/financial-pdfs/YEAR/DOCID.pdf and trade "
                f"reports at {HOUSE}/ptr-pdfs/YEAR/DOCID.pdf. A yearly report sits in the list of the year it covers, "
                "so check this year's and last year's lists. The lists are updated over time; that is fine, just use "
                "the rows filed before today. Read every page of each report you use, since the yearly report is long. If "
                "your last answer lists her stocks, start from it and only read reports filed since then; otherwise "
                "start over from the yearly report. Skip options, funds, bonds, real estate and private companies. "
                "List every stock she still owns with the ticker it trades under today and its reported dollar range. "
                "Do not trade."
            ),
        )
        self.agents.create(
            name="portfolio",
            allow_trading=False,
            system_prompt=(
                "Turn the research into target weights for our account. Use the middle of each dollar range as "
                "that stock's value; each weight is its value divided by the total. Do the math with a calculator. "
                "Return one line per ticker with its target percent. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Make the account hold the stocks in the plan at their target percents. Sell stocks that are not in "
                "the plan and buy the ones you do not own. Leave a stock alone when it is within 2 percentage points "
                "of its target. If a stock has no price, skip it and spread its weight over the others. Sell before "
                "you buy, never short, and never spend more cash than you have."
            ),
        )

    def on_trading_iteration(self):
        # The researcher gets its last answer back, so it only needs to read reports filed since then.
        research = self.agents["researcher"].run(
            task_prompt="What does Nancy Pelosi own today?",
            context={"your_last_answer": self.vars.get("last_research", "none yet")},
        )
        self.vars.set("last_research", research.summary)
        plan = self.agents["portfolio"].run(task_prompt="Set the target weights.", context={"holdings": research.summary})
        self.agents["trader"].run(task_prompt="Match the account to the plan.", context={"plan": plan.summary})


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
