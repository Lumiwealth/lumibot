"""Nancy Pelosi Copy Trading Bot.

Estimates Nancy Pelosi's disclosed household stocks and calls as of each trading
date, sized to your account. "options_share" sets the allocation to calls;
disclosure delays and value ranges make the reconstruction approximate.
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
                "Build an investable estimate of Nancy Pelosi's full publicly disclosed household stocks and calls "
                "as of as_of. This is an approximate model portfolio, not verification of her exact account. "
                "Find annual reports, transactions and amendments in the official yearly indexes at "
                "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/<year>FD.zip. Check the as_of year "
                "and earlier years needed for the baseline. Under the same /public_disc/ root, transaction PDFs "
                "use ptr-pdfs/<filing-year>/<DocID>.pdf; annual PDFs use financial-pdfs/<report-year>/<DocID>.pdf. "
                "Use actual indexed IDs and cite the reports read. Never use a later disclosure in a backtest. "
                "Start with the newest eligible annual snapshot. Assume its reporting year-end is the valuation "
                "date when implicit, and disclose that convention. Apply eligible transactions AFTER that date "
                "once, including those filed before the annual report. Earlier trades are already reflected. "
                "Read full asset-value ranges across wrapped lines; income is separate and a None asset value "
                "does not establish a holding. Partial sales reduce holdings; exercised calls remove contracts and add shares. "
                "Reconcile amendments, gifts, splits and ticker changes. Include listed partnership units; "
                "exclude funds, bonds, private assets and property. "
                "Estimate unknown baseline shares as the asset-value range midpoint divided by its snapshot-date "
                "price. Apply later share changes and mark the remainder as of as_of. If a mark is unavailable, "
                "check the issuer's ticker changes, then retain a clearly labeled stale range-midpoint proxy. "
                "Return usable central values for each stock and call, with estimated quantities, method, "
                "sources, applied report IDs and latest filing checked. For calls retain underlying, strike, expiry and contracts; account for expiration. Estimates are explicitly requested. "
                "Check indexes each day and return the FULL ledger, reusing your prior answer when unchanged. "
                "A lookup failure is not a sale: retain supported prior findings, label stale evidence and explain "
                "gaps. Never substitute a favorite-stock basket. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Mirror the full estimated portfolio as of as_of, including initial purchases from cash. Put "
                "options_share of this account in her calls and the rest in stocks, each proportional to its "
                "estimated value. Use the same underlying and expiration for calls: her strike if one contract "
                "fits its allocation, otherwise the nearest higher strike that fits. Use the contract multiplier, "
                "disclose substitutions and leave unaffordable or unpriced targets in cash. An empty sleeve stays "
                "in cash. Buy missing affordable holdings; drift tolerance must not skip initial small positions. "
                "Leave established positions within 2 percentage points of target alone. Verify sales and cash "
                "before dependent buys; never short or overspend. Disclosed valuation and coverage estimates are "
                "usable without exact household quantities. If no supported basket was produced, compare "
                "previous_research and preserve holdings; an error or 'nothing new' is not a sale. Report targets "
                "versus actual weights, fills, cash, substitutions and unresolved mismatches."
            ),
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime().isoformat()
        previous = self.vars.get("last_research", "none yet")
        research = self.agents["researcher"].run(
            task_prompt=(
                "Return the full estimated stock-and-call model portfolio as of as_of with usable central "
                "values for sizing. The range-midpoint estimation method is explicitly requested; calculate "
                "and label estimates instead of requiring exact disclosed quantities. Read each chosen "
                "report's relevant pages completely, check new disclosures, and explain coverage gaps."
            ),
            context={"as_of": as_of, "your_last_answer": previous},
        )
        self.vars.set("last_research", research.summary)
        self.agents["trader"].run(
            task_prompt="Match the account to the complete portfolio, including the initial purchases.",
            context={"as_of": as_of, "research": research.summary, "previous_research": previous,
                     "options_share": self.parameters["options_share"]},
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        NancyPelosiCopyTradingBot.backtest(AlpacaBacktesting)
    else:
        NancyPelosiCopyTradingBot().run_live()
