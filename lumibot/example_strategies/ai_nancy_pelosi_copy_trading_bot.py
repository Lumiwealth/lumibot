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
                "Reconcile amendments, gifts and splits. Verify current listings and ticker changes; return current tickers and historical aliases. "
                "Keep pre-snapshot trades in reconciliation notes, not in applied share changes. "
                "Inspect every transaction row: separate stock and option purchases can share an issuer. "
                "Include listed partnership units; "
                "exclude funds, bonds, private assets and property. "
                "Return the FULL source ledger: each stock and call, baseline asset-value range, remaining "
                "reported contract quantities, dated share/contract changes, contract terms, sources, applied "
                "report IDs and latest filing checked. The trader handles valuation and sizing from this ledger. "
                "Check indexes for newly eligible filings each day. Without new evidence, copy the FULL prior ledger with refreshed source status. "
                "A lookup failure is not a sale: retain supported prior findings, label stale evidence and explain "
                "gaps. Never substitute a favorite-stock basket. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Use the source ledger to calculate an approximate model portfolio as of as_of. For unknown "
                "baseline shares, divide the reported asset-value range midpoint by its snapshot-date price. "
                "Apply only post-snapshot share changes and mark remaining shares as of as_of. If shares turn "
                "negative, recheck dates and double counting; do not clip a contradictory estimate to zero. "
                "Retain a labeled baseline-value proxy if quantity reconciliation remains unresolved. Check "
                "ticker changes if a quote is missing; otherwise use a clearly labeled stale midpoint proxy. "
                "Keep calls separate, estimate their remaining values from reported quantities and quotes or disclosed ranges, and explain assumptions. "
                "Mirror the full estimated portfolio as of as_of, including initial purchases from cash. Replication includes small target positions and requires no separate conviction thesis. "
                "Size the basket jointly using 99% of capital, scaling buys to available cash rather than dropping whole constituents. Complete all feasible orders, reserving pending orders' cash for independent targets. "
                "For immediate stock execution during the open session, use market orders and orders_wait_for_terminal. Submit one order at a time; after each mutation or ORDER_READINESS_REQUIRED, refresh account_portfolio and complete account_positions plus orders_open_orders pagination, then continue the basket. Put "
                "options_share of this account in her calls and the rest in stocks, each proportional to its "
                "estimated value. Use the same underlying and expiration for calls: her strike if one contract "
                "fits its allocation, otherwise the nearest higher strike that fits. Use the contract multiplier, "
                "disclose substitutions and leave unaffordable or unpriced targets in cash. An empty sleeve stays "
                "in cash. Buy missing affordable holdings; drift tolerance must not skip initial small positions. "
                "On quiet days leave established positions within 2 percentage points of target alone. When eligible disclosures change holdings, rebalance the updated basket and trim existing positions to fund new allocations even when individual drift is small. Verify sales and cash "
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
                "Return the full report-derived stocks-and-calls ledger as of as_of. Read chosen reports "
                "completely; audit each eligible post-snapshot purchase, exercise and sale against the ledger, including additions absent from the annual inventory; resolve ticker aliases, retain contract identities, check new "
                "disclosures and cite sources. The trader will calculate estimates and allocations."
            ),
            context={"as_of": as_of, "your_last_answer": previous},
        )
        self.vars.set("last_research", research.summary)
        self.agents["trader"].run(
            task_prompt="Calculate the estimated basket from the source ledger and match the account, including initial purchases.",
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
