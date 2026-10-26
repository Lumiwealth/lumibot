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
            name="researcher", model="openai/gpt-6-luna", reasoning_effort="high",
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
                "Reconcile amendments, gifts, splits, mergers and delistings using evidence available by as_of. Verify current listings and ticker changes; return current tickers and historical aliases. "
                "Keep pre-snapshot trades in reconciliation notes, not in applied share changes. "
                "Inspect every transaction row: separate stock and option purchases can share an issuer. "
                "Include listed partnership units; "
                "exclude unexercised options, funds, bonds, private assets and property. "
                "Return the FULL source ledger: each stock or listed partnership, baseline asset-value range, "
                "dated post-snapshot share changes, sources, applied report IDs and latest filing checked. "
                "Include additions absent from the baseline, including shares from exercised calls and spinoffs. "
                "Audit the names and count of positive listed-stock rows against the full annual report, then audit all eligible later stock changes. Explain exclusions instead of silently dropping a source row. "
                "The trader handles valuation and sizing; missing prices do not prevent this source ledger. "
                "Check indexes for newly eligible filings each day. Without new evidence, copy the FULL prior ledger with refreshed source status. "
                "A lookup failure is not a sale: retain supported prior findings, label stale evidence and explain "
                "gaps. Never substitute a favorite-stock basket. Do not trade."
            ),
        )
        self.agents.create(
            name="trader", model="openai/gpt-6-luna", reasoning_effort="high",
            allow_trading=True,
            system_prompt=(
                "Use the source ledger to calculate an approximate model portfolio as of as_of. For unknown "
                "baseline shares, divide the reported asset-value range midpoint by its snapshot-date price. "
                "Apply only post-snapshot share changes and mark remaining shares as of as_of. If shares turn "
                "negative, recheck dates and double counting; do not clip a contradictory estimate to zero. "
                "Retain a labeled baseline-value proxy if quantity reconciliation remains unresolved. Check "
                "ticker changes if a quote is missing; otherwise use a clearly labeled stale midpoint proxy. "
                "Check that snapshot and current prices refer to the same issuer, listing, currency and share basis. Never divide a USD value by a foreign-currency price. "
                "Normalize estimated stock values to 100% and explain assumptions. Size the basket jointly using 99% of capital; scale buys to available cash rather than dropping whole constituents when their full targets exceed cash. "
                "Build valuation rows from the FULL current source ledger. Compare its constituent names and count with the target table before ordering; an old calculation missing a newly restored source row is not current. "
                "Mirror the full estimated stock basket as of as_of, sized to this account. Buy the basket on "
                "the first iteration from cash; do not wait for a new Pelosi trade. Replication includes small target positions and requires no separate conviction thesis. Complete every feasible "
                "basket order in this iteration, not just the first stock. A pending order for one stock does "
                "not cover other missing stocks: reserve its cash and continue independently affordable targets. "
                "During the open session, use market orders when immediate execution is required and verify "
                "their terminal status with orders_wait_for_terminal. Submit one order at a time. After each mutation or ORDER_READINESS_REQUIRED, refresh account_portfolio and complete account_positions plus orders_open_orders pagination, then continue the remaining basket. Buy missing positive-weight "
                "holdings when a whole share fits. On quiet days leave established positions within 2 percentage points of "
                "target alone only after every feasible basket constituent is held. A missing target position is never covered by that deadband: complete initial omissions on later iterations, funding them with small trims when needed even without a new filing. When eligible disclosures change holdings, rebalance the updated basket, trimming existing positions to fund new allocations even when individual drift is small. Sell removed or overweight holdings before dependent buys and verify fills and "
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
                "Return the full report-derived stock ledger as of as_of. Read chosen reports completely, "
                "audit every eligible post-snapshot stock purchase/exercise against the ledger, including additions absent from the annual stock section; resolve ticker aliases, check new disclosures and cite "
                "sources. The trader will calculate valuation estimates and target allocations."
            ),
            context={"as_of": as_of, "your_last_answer": previous},
        )
        self.vars.set("last_research", research.summary)
        self.agents["trader"].run(
            task_prompt="Calculate the estimated basket from the source ledger and match the account, including initial purchases.",
            context={"as_of": as_of, "research": research.summary, "previous_research": previous},
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
