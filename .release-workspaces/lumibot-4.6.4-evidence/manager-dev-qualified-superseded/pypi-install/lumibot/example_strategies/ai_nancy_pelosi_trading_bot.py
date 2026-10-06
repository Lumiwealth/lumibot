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
                "You find out which stocks a member of Congress owns today, from the House Clerk website. Each "
                f"year's list of filings is a ZIP file such as {HOUSE}/financial-pdfs/2026FD.ZIP (change 2026 to the "
                "year). Each row is one filing with its filing date. FilingType O is a yearly report of everything "
                "the member owned on December 31 of that Year; P is a trade report. Yearly reports are at "
                f"{HOUSE}/financial-pdfs/YEAR/DOCID.pdf and trade reports at {HOUSE}/ptr-pdfs/YEAR/DOCID.pdf. Every "
                "day, first check the lists for this year and the two years before, using only filings dated before "
                "today. Start your answer with \"Newest filing:\" and the date and DocID of the member's own newest "
                "filing, yearly or trade report (other people's filings do not count). If your notes start with "
                "that same newest filing, reply only NOTHING NEW. Otherwise start from the newest yearly report and "
                "apply every trade dated after the December 31 it covers; ignore earlier trades, because the yearly "
                "report already includes them. Shares bought, including shares from exercised call options, add to "
                "a stock. A sale removes a stock only when the report says all shares or the entire position were "
                "sold; \"Sold 10,000 shares\" alone is a partial sale, so keep the stock. Partnership units with a "
                "ticker, such as AB, count as stocks. Skip options, real estate, private companies, funds, and "
                "bonds. List every stock the member still owns with a dollar range: the yearly report's range, or "
                "the trade amount for a stock bought since. Do not trade."
            ),
        )
        self.agents.create(
            name="portfolio",
            allow_trading=False,
            system_prompt=(
                "You turn a member's holdings into target weights for our account. Use the middle of each dollar "
                "range as the value of that stock. Each stock's weight is its value divided by the total value of "
                "all the stocks. Do the math with a calculator, not in your head. Return one line per ticker with "
                "its target percent of the account. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You move the account to the target percents in the plan. Sell every stock that is not in the plan "
                "and buy every stock in the plan you do not own yet. Only change a stock you already own when it is "
                "more than 2 percentage points away from its target, so the account does not trade every day. Sell "
                "before you buy, never short, and never spend more cash than you have. Check that every order "
                "filled."
            ),
        )

    def on_trading_iteration(self):
        facts = {"last_name": self.parameters["last_name"]}
        research = self.agents["researcher"].run(task_prompt="What does the member own today?", context=facts)
        answer = research.summary or ""
        # Rebalance only when her newest filing changes, never on a re-read of the same filings.
        newest = "".join(ch for ch in answer.split("DocID", 1)[-1][:20] if ch.isdigit()) if "DocID" in answer else ""
        if (newest and newest == self.vars.get("newest_filing")) or (not newest and "NOTHING NEW" in answer):
            return
        plan = self.agents["portfolio"].run(
            task_prompt="Set the target weights.", context={"holdings": research.summary}
        )
        self.agents["trader"].run(task_prompt="Rebalance to the plan.", context={"plan": plan.summary})
        if newest:
            self.vars.set("newest_filing", newest)


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
