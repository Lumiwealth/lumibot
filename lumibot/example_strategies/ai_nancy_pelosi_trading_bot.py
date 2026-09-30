"""Nancy Pelosi Stock Trading Bot.

Holds the same stocks Nancy Pelosi owns, in the same proportions. A research
agent reads her reports on the House Clerk website: the yearly report of
everything she owns, plus every newer trade report. A portfolio agent turns that
into target weights, and a trading agent rebalances only when a new report
appears. Change "last_name" to copy any other member of the House.
"""

from lumibot.strategies import Strategy

HOUSE = "https://disclosures-clerk.house.gov/public_disc"


class NancyPelosiTradingBot(Strategy):
    parameters = {"last_name": "Pelosi"}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "You find out which stocks a member of Congress owns today, from the House Clerk website. "
                f"Each year's list of filings is a ZIP file such as {HOUSE}/financial-pdfs/2026FD.ZIP (change "
                "2026 to the year). Each row is one filing with its filing date. FilingType O is a yearly report of "
                "everything the member owned on December 31 of that Year; P is a trade report. Yearly reports are "
                f"at {HOUSE}/financial-pdfs/YEAR/DOCID.pdf and trade reports at {HOUSE}/ptr-pdfs/YEAR/DOCID.pdf. "
                "Look at the lists for this year and the two years before, and only use filings dated before "
                "today. Start from the newest yearly report, then apply every stock trade dated after the December "
                "31 it covers. List every stock the member still owns with its dollar value range. Skip options, "
                "real estate, private companies, funds, and bonds. End with the date of the newest report you "
                "used. If your notes show you already reported that same newest report, reply only NOTHING NEW. "
                "Do not trade."
            ),
        )
        self.agents.create(
            name="portfolio",
            allow_trading=False,
            system_prompt=(
                "You turn a member's holdings into target weights for our account. Use the middle of each dollar "
                "range as the value of that stock. Each stock's weight is its value divided by the total value of "
                "all the stocks. Return one line per ticker with its target percent of the account. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You move the account to the target percents in the plan. Sell every stock that is not in the "
                "plan. Only trade a stock when it is more than 2 percentage points away from its target, so the "
                "account does not trade every day. Sell before you buy, never short, and never spend more cash "
                "than you have. Check that every order filled."
            ),
        )

    def on_trading_iteration(self):
        facts = {"last_name": self.parameters["last_name"]}
        research = self.agents["researcher"].run(task_prompt="What does the member own today?", context=facts)
        if "NOTHING NEW" in (research.summary or ""):
            return  # No new report, so no rebalance today.
        plan = self.agents["portfolio"].run(task_prompt="Set the target weights.", context={"holdings": research.summary})
        self.agents["trader"].run(task_prompt="Rebalance to the plan.", context={"plan": plan.summary})


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
