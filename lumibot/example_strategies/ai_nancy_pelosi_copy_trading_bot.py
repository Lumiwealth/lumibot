"""Nancy Pelosi Copy Trading Bot.

Copies Nancy Pelosi's whole portfolio, call options included: her stocks, and
calls on the same stocks with the same expiration dates. Her calls cost $9,000 to
$18,000 a contract, so the bot uses her strike when it fits your account and the
nearest cheaper strike when it does not. "options_share" sets how much of the
account goes to calls. A research agent reads her reports on the House Clerk
website, a portfolio agent sizes each holding, and a trading agent rebalances
only when a new report appears. Change "last_name" to copy any House member.
"""

from lumibot.strategies import Strategy

HOUSE = "https://disclosures-clerk.house.gov/public_disc"


class NancyPelosiCopyTradingBot(Strategy):
    parameters = {"last_name": "Pelosi", "options_share": 0.2}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "You find out which stocks and call options a member of Congress owns today, from the House "
                "Clerk website. "
                f"Each year's list of filings is a ZIP file such as {HOUSE}/financial-pdfs/2026FD.ZIP (change "
                "2026 to the year). Each row is one filing with its filing date. FilingType O is a yearly report of "
                "everything the member owned on December 31 of that Year; P is a trade report. Yearly reports are "
                f"at {HOUSE}/financial-pdfs/YEAR/DOCID.pdf and trade reports at {HOUSE}/ptr-pdfs/YEAR/DOCID.pdf. "
                "Every day, first check the lists for this year and the two years before, using only filings dated "
                "before today. Start your answer with \"Newest filing:\" and the date and DocID of the member's own "
                "newest filing, yearly or trade report (other people's filings do not count). If your notes start with "
                "that same newest filing, reply only NOTHING NEW. Otherwise start from the newest yearly report and "
                "apply every trade dated after the December 31 it covers; ignore earlier trades, because the yearly "
                "report already includes them. Shares bought, including shares from exercised call options, add to a "
                "stock. A sale removes a stock only when the report says the whole position was sold. Partnership "
                "units with a ticker, such as AB, count as stocks. List every stock the member still owns with a "
                "dollar range: the yearly report's range, or the trade amount for a stock bought since. List every "
                "call option she still owns with the number of contracts, strike price, expiration date, and dollar "
                "range. Leave out expired options, real estate, private companies, funds, and bonds. Do not trade."
            ),
        )
        self.agents.create(
            name="portfolio",
            allow_trading=False,
            system_prompt=(
                "You scale a member's holdings to our account. Put options_share of the account into her call "
                "options and the rest into her stocks. Use the middle of each dollar range as a holding's value. "
                "Stocks: each stock gets its value divided by the total value of her stocks. Calls: split the "
                "options money across her calls in proportion to their values. For each call use the same stock "
                "and the same expiration date. Use her strike if one contract fits that call's money; if not, use "
                "the nearest higher strike where one contract fits, and buy as many contracts as fit. If no strike "
                "fits, give that money to her other calls. Do the math with a calculator, not in your head. "
                "Return one line per holding: the stock and its target percent, or the exact call (strike and "
                "expiration) and its number of contracts. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You move the account to the targets in the plan: shares for stocks, and the exact call options "
                "in the plan (its strike price, expiration date, and number of contracts). Sell every holding that is "
                "not in the plan and buy every holding in the plan you do not own yet. Only change a holding you "
                "already own when it is more than 2 percentage points away from its target, so the account does not "
                "trade every day. Sell before you buy, never short, never sell options you do not own, and never "
                "spend more cash than you have. Check that every order filled."
            ),
        )

    def on_trading_iteration(self):
        facts = {"last_name": self.parameters["last_name"]}
        research = self.agents["researcher"].run(task_prompt="What does the member own today?", context=facts)
        if "NOTHING NEW" in (research.summary or "")[:40]:
            return  # No new report, so no rebalance today. A full answer starts with "Newest filing:".
        plan = self.agents["portfolio"].run(
            task_prompt="Set the targets.",
            context={"holdings": research.summary, "options_share": self.parameters["options_share"]},
        )
        self.agents["trader"].run(task_prompt="Rebalance to the plan.", context={"plan": plan.summary})


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        NancyPelosiCopyTradingBot.backtest(AlpacaBacktesting)
    else:
        NancyPelosiCopyTradingBot().run_live()
