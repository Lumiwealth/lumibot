"""Nancy Pelosi Stock Trading Bot.

Estimates Nancy Pelosi's disclosed household stock portfolio as of each trading
date, then mirrors its weights. Disclosure delays and value ranges mean this
is an estimate, not her exact current account.
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
                "Build an investable estimate of Nancy Pelosi's full publicly disclosed household stock portfolio "
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
                "does not establish a holding. Partial sales reduce holdings; exercised calls add shares. "
                "Reconcile amendments, gifts, splits and ticker changes. Include listed partnership units; "
                "exclude unexercised options, funds, bonds, private assets and property. "
                "Estimate unknown baseline shares as the asset-value range midpoint divided by its snapshot-date "
                "price. Apply later share changes and mark the remainder as of as_of. If a mark is unavailable, "
                "check the issuer's ticker changes, then retain a clearly labeled stale range-midpoint proxy. "
                "Return usable central values and weights normalized to 100%, with estimated quantities, method, "
                "sources, applied report IDs and latest filing checked. Estimates are explicitly requested. "
                "Check indexes each day and return the FULL ledger, reusing your prior answer when unchanged. "
                "A lookup failure is not a sale: retain supported prior findings, label stale evidence and explain "
                "gaps. Never substitute a favorite-stock basket. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Mirror the full estimated stock basket as of as_of, sized to this account. Buy the basket on "
                "the first iteration from cash; do not wait for a new Pelosi trade. Buy missing positive-weight "
                "holdings when a whole share fits; leave established positions within 2 percentage points of "
                "target alone. Sell removed or overweight holdings before dependent buys and verify fills and "
                "cash. Never short or overspend. Leave an unpriced or unaffordable holding's allocation in cash "
                "while trading the supported remainder. Disclosed valuation and coverage estimates are usable; "
                "exact household quantities are not required. If no supported basket was produced, compare "
                "previous_research and preserve holdings; a lookup error or 'nothing new' is not a liquidation "
                "signal. Report target versus actual weights, actual fills, cash and unresolved mismatches."
            ),
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime().isoformat()
        previous = self.vars.get("last_research", "none yet")
        research = self.agents["researcher"].run(
            task_prompt=(
                "Return the full estimated model portfolio as of as_of with usable central target percentages. "
                "The range-midpoint estimation method is explicitly requested; calculate and label the "
                "estimates instead of requiring exact disclosed share counts. Read each chosen report's "
                "relevant pages completely, check newly available disclosures, and explain coverage gaps."
            ),
            context={"as_of": as_of, "your_last_answer": previous},
        )
        self.vars.set("last_research", research.summary)
        self.agents["trader"].run(
            task_prompt="Match the account to the complete portfolio, including the initial purchases.",
            context={"as_of": as_of, "research": research.summary, "previous_research": previous},
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
