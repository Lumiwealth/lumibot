"""Estimate and mirror the publicly disclosed household stock portfolio.
Disclosures are delayed and use value ranges, so this is an approximate basket.
"""
from lumibot.strategies import Strategy


class NancyPelosiTradingBot(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher", model="openai/gpt-6-luna", reasoning_effort="high",
            allow_trading=False, allow_network=True,
            model_request_timeout_seconds=0, run_timeout_seconds=0,
            system_prompt=(
                "Reconstruct Nancy Pelosi's full publicly disclosed household listed-stock and listed-partnership "
                "portfolio as of as_of. This is an estimate, not verification of her exact account. Find the "
                "official yearly indexes at https://disclosures-clerk.house.gov/public_disc/financial-pdfs/<year>FD.zip. "
                "Check the as_of year and earlier years needed for the baseline. Indexed transaction PDFs use "
                "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/<filing-year>/<DocID>.pdf; annual reports "
                "use financial-pdfs/<report-year>/<DocID>.pdf under the same root. Use actual indexed IDs. "
                "Start with the newest eligible annual snapshot; use its reporting year-end when implicit and "
                "disclose that convention. Apply eligible transactions AFTER that valuation date once, including "
                "transactions filed before the annual. Earlier transactions are already reflected in the baseline. "
                "Never use a disclosure known to be after as_of. Read every asset-section text chunk, including "
                "wrapped issuer names, tickers, type labels and value ranges. Return the actual numeric lower and "
                "upper ASSET VALUE bounds for every included annual issuer, with report ID and page or text chunk. "
                "None ASSET VALUE is zero modeled baseline exposure; None INCOME does not erase a positive value. "
                "Audit names against the full numeric ledger and separate zero exclusions. Resolve discrepancies "
                "rather than inventing a holding or dropping a wrapped row. Include later stock additions, "
                "including shares from exercised calls. Exclude unexercised options, funds, bonds, private assets "
                "and property. Distinguish full sales from partial sales: an explicit full exit removes that "
                "holding, not merely a share subtraction from an uncertain baseline estimate. Partial sales, "
                "gifts and contributions reduce shares; exercises add shares. Reconcile amendments, splits, "
                "spinoffs, mergers and delistings from eligible evidence. Do not apply an estimated spinoff again "
                "when a later filing confirms it. Resolve current listings and historical ticker aliases. "
                "Return one compact FULL ledger with every included issuer, numeric baseline bounds, valuation "
                "date, dated share changes or full-exit status, sources, applied IDs and latest filing checked. "
                "Check new filings every iteration. Without new evidence retain the full prior ledger with "
                "refreshed source status; a lookup failure is not a sale. Label stale or approximate evidence. "
                "The trader handles valuation and sizing. Do not trade."
            ),
        )
        self.agents.create(
            name="trader", model="openai/gpt-6-luna", reasoning_effort="high",
            allow_trading=True, model_request_timeout_seconds=0, run_timeout_seconds=0,
            system_prompt=(
                "Mirror the full source-ledger stock and listed-partnership basket, scaled to this account. "
                "Estimate unknown baseline shares from the asset-value midpoint and the snapshot-date price, "
                "apply eligible later share changes once, and mark remaining shares at current prices. An "
                "explicit full sale sets the holding to zero; do not invent a residual from midpoint uncertainty. "
                "Recheck contradictory quantities and dates. Use a labeled baseline-value proxy only when "
                "quantity reconciliation or an eligible historical price genuinely remains unavailable. Check "
                "issuer, listing, USD currency and consistent split/share basis; use historical ticker aliases "
                "when needed. Normalize estimated remaining values into portfolio weights. "
                "Target 99% of current account capital with a joint cash-aware allocation. Buy the complete "
                "feasible basket initially, without waiting for a new disclosure. Small positions are part of "
                "replication, not discretionary conviction bets. For a positive allocation smaller than one "
                "share, prefer one whole share when the joint budget can fund it; fund that small tracking "
                "difference from larger allocations and disclose rounding. Never short, overspend or use margin. "
                "Reconcile every positive source name against targets and actual holdings. On quiet days leave "
                "established holdings within 2 percentage points of target alone, after all feasible constituents "
                "are held. Complete missing holdings with funding trims when needed. New eligible disclosures "
                "require rebalancing the updated whole basket, including additions and full exits even when "
                "individual drift is small. Leave genuinely unpriced or unaffordable allocations in cash while "
                "matching the supported remainder. Preserve supported holdings if research fails; nothing new "
                "is not a liquidation signal. Report complete target versus actual weights, fills, cash and "
                "specific unresolved exceptions."
            ),
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime().isoformat()
        previous = self.vars.get("last_research", "none yet")
        research = self.agents["researcher"].run(
            task_prompt="Return the complete numeric source ledger, inventory audit, eligible changes and newly checked filings.",
            context={"as_of": as_of, "your_last_answer": previous},
        )
        self.vars.set("last_research", research.summary)
        self.agents["trader"].run(
            task_prompt="Value the full disclosed basket and complete every feasible target position change in this account.",
            context={"as_of": as_of, "research": research.summary, "previous_research": previous},
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
