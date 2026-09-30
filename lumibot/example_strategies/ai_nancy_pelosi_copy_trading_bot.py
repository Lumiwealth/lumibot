"""Nancy Pelosi Copy Trading Bot.

Copies Nancy Pelosi's stocks and her call options (same stock, same expiration),
sized to your account. A research agent reads her House Clerk reports, a
portfolio agent sizes each holding, and a trading agent rebalances when she files
a new report. "options_share" sets the share of the account in calls.
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
                "You find out which stocks and call options a member of Congress owns today, from the House Clerk "
                f"website. Each year's list of filings is a ZIP file such as {HOUSE}/financial-pdfs/2026FD.ZIP "
                "(change 2026 to the year). Each row is one filing with its filing date. FilingType O is a yearly "
                "report of everything the member owned on December 31 of that Year; P is a trade report. Yearly "
                f"reports are at {HOUSE}/financial-pdfs/YEAR/DOCID.pdf and trade reports at "
                f"{HOUSE}/ptr-pdfs/YEAR/DOCID.pdf. Every day, first check the lists for this year and the two years "
                "before, using only filings dated before today. Start your answer with \"Newest filing:\" and the "
                "date and DocID of the member's own newest filing, yearly or trade report (other people's filings "
                "do not count). If your notes start with that same newest filing, reply only NOTHING NEW. Otherwise "
                "start from the newest yearly report and apply every trade dated after the December 31 it covers; "
                "ignore earlier trades, because the yearly report already includes them. Shares bought, including "
                "shares from exercised call options, add to a stock. A sale removes a stock only when the report "
                "says all shares or the entire position were sold; \"Sold 10,000 shares\" alone is a partial sale, so "
                "keep the stock. Partnership units with a ticker, such as AB, count as stocks. List every stock the "
                "member still owns with a dollar range: the yearly report's range, or the trade amount for a stock "
                "bought since. List every call option she still owns with the number of contracts, strike price, "
                "expiration date, and dollar range. Leave out expired options, real estate, private companies, "
                "funds, and bonds. Do not trade."
            ),
        )
        self.agents.create(
            name="portfolio",
            allow_trading=False,
            system_prompt=(
                "You scale a member's holdings to our account. Put options_share of the account into her call "
                "options and the rest into her stocks. Use the middle of each dollar range as a holding's value. "
                "Stocks: each stock gets its value divided by the total value of her stocks. Calls: split the "
                "options money across her calls in proportion to their values. For each call use the same stock and "
                "the same expiration date. Use her strike if one contract fits that call's money; if not, use the "
                "nearest higher strike where one contract fits, and buy as many contracts as fit. If no strike "
                "fits, give that money to her other calls. Do the math with a calculator, not in your head. Return "
                "one line per holding: the stock and its target percent, or the exact call (strike and expiration) "
                "and its number of contracts. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You move the account to the targets in the plan: shares for stocks, and the exact call options in "
                "the plan (its strike price, expiration date, and number of contracts). Sell every holding that is "
                "not in the plan and buy every holding in the plan you do not own yet. Only change a holding you "
                "already own when it is more than 2 percentage points away from its target, so the account does not "
                "trade every day. If you already hold a call on the same stock and expiration as a planned call, "
                "keep it instead of switching strikes. Sell before you buy, never short, never sell options you do "
                "not own, and never spend more cash than you have. Check that every order filled."
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
            task_prompt="Set the targets.",
            context={"holdings": research.summary, "options_share": self.parameters["options_share"]},
        )
        self.agents["trader"].run(task_prompt="Rebalance to the plan.", context={"plan": plan.summary})
        if newest:
            self.vars.set("newest_filing", newest)


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        NancyPelosiCopyTradingBot.backtest(AlpacaBacktesting)
    else:
        NancyPelosiCopyTradingBot().run_live()
