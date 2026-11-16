"""Estimate and mirror the publicly disclosed household stock portfolio.
Disclosures are delayed and use value ranges, so this is an approximate basket.
"""
from lumibot.strategies import Strategy


class NancyPelosiTradingBot(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher", model="openai/gpt-6-luna", reasoning_effort="high",
            allow_trading=False, allow_network=True, model_request_timeout_seconds=0, run_timeout_seconds=0,
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
                "Never use a disclosure known to be after as_of. Read every continuation chunk of each selected annual and transaction report until next_start is null. Do not treat a first page as the complete report. Read every asset-section text chunk, including "
                "wrapped issuer names, tickers, type labels and value ranges. Return the actual numeric lower and "
                "upper ASSET VALUE bounds for every included annual issuer, with report ID and page or text chunk. "
                "None ASSET VALUE is zero modeled baseline exposure; None INCOME does not erase a positive value. "
                "Audit names against the full numeric ledger and separate zero exclusions. Resolve discrepancies "
                "rather than inventing a holding or dropping a wrapped row. Include later stock and listed-partnership "
                "additions: unit purchases count regardless of bracket type labels, as do shares from exercised calls. "
                "Treat each security row separately: an issuer can have both share and option purchases. Classify "
                "each transaction from its own description and share or contract quantity, not another row for that issuer. "
                "Exclude unexercised options, funds, bonds, private assets and property. Distinguish full sales from partial sales: an explicit full exit removes that "
                "holding, not merely a share subtraction from an uncertain baseline estimate. House transaction type S means a full sale; S (partial) means a partial sale. Preserve the row's own type and transaction date. Partial sales, "
                "gifts and contributions reduce shares; exercises add shares. Corporate actions may change "
                "share counts, listings or holdings before a personal filing. Reconcile splits, spinoffs, mergers "
                "and delistings from dated issuer or exchange evidence. Return BOTH snapshot and as_of tickers; "
                "a renamed ticker is not an exit. For an effective distribution without confirmed shares, give "
                "its parent ticker and ratio. Later confirmations replace that estimate rather than adding it again. "
                "Return compact JSON with as_of, snapshot_date, ledger, zero_exclusions, source_checks and limitations. "
                "The ledger's issuers are the union of positive annual stock/unit rows and later share additions, "
                "including new issuers created by exercises, purchases or spinoffs. Each issuer row gives "
                "snapshot_ticker, as_of_ticker, baseline_lower, baseline_upper, baseline_kind, share_changes, corporate_actions and full_exit. "
                "Each share_changes entry gives its transaction_date, signed numeric shares, kind and source_id. "
                "A new issuer has a modeled zero baseline and its known positive share changes; include it even "
                "when the annual lists only options. An exercise label without the numeric share count is incomplete. "
                "Keep all positive additions in this one ledger. Include applied IDs and latest filing checked. "
                "Check new filings every iteration. Without new evidence retain the full prior ledger with "
                "refreshed source status; a lookup failure is not a sale. Label stale or approximate evidence. The trader handles valuation and sizing. Do not trade."
            ),
        )
        self.agents.create(
            name="trader", model="openai/gpt-6-luna", reasoning_effort="high",
            allow_trading=True, model_request_timeout_seconds=0, run_timeout_seconds=0,
            system_prompt=(
                "Mirror the full source-ledger stock and listed-partnership basket, scaled to this account. "
                "Reuse a verified baseline price only for the identical snapshot date and ticker. When the annual snapshot changes, refresh every baseline price. If a historical ticker has no row on that date, query the successor ticker history for that exact date; never substitute a close from an earlier snapshot. Estimate unknown baseline shares from the asset-value midpoint and the snapshot-date price, "
                "apply eligible later share changes once, and mark remaining shares at current prices. Apply "
                "corporate-action ratios only when not already reflected in the price/share basis; reconcile "
                "later confirmed quantities without adding an estimated distribution twice. Polygon and Yahoo historical bars are split-adjusted: midpoint divided by an adjusted snapshot close already yields shares on that adjusted basis. Never multiply those shares by the split again when current prices use the same adjusted basis. Label adjusted backtest units separately from literal historical shares. An "
                "explicit full sale sets the holding to zero; do not invent a residual from midpoint uncertainty. "
                "Recheck contradictory quantities and dates. Use a labeled baseline-value proxy only when "
                "quantity reconciliation or an eligible historical price genuinely remains unavailable. Check "
                "issuer, listing, USD currency and consistent split/share basis; use historical ticker aliases "
                "when needed. Normalize estimated remaining values into portfolio weights. Target 99% of current account capital with a joint cash-aware allocation. Buy the complete "
                "feasible basket initially, without waiting for a new disclosure. Small positions are part of "
                "replication, not discretionary conviction bets. For a positive allocation smaller than one "
                "share, prefer one whole share using unspent joint cash above the reserve first; trim larger "
                "allocations only if funding requires it and disclose rounding. Never short, overspend or use margin. "
                "Reconcile every positive source name against targets and actual holdings. On quiet days leave "
                "established holdings within 2 percentage points of target alone, after all feasible constituents "
                "are held. Complete missing holdings with funding trims when needed. New eligible disclosures "
                "require rebalancing the updated whole basket, including additions and full exits even when "
                "individual drift is small. Use the researched as_of listing for current quotes and snapshot ticker for old prices. "
                "For a still-unpriced issuer, use its baseline value midpoint as a labeled denominator proxy and reserve "
                "that estimated allocation in cash; complete the supported remainder. Preserve supported holdings if research fails; nothing new "
                "is not a liquidation signal. Report complete target versus actual weights, fills, cash and specific unresolved exceptions."
            ),
        )

    def on_trading_iteration(self):
        as_of = self.get_datetime().isoformat()
        previous = self.vars.get("last_research", "none yet")
        inventory = self.agents["researcher"].run(
            task_prompt="Update the complete numeric inventory from new eligible filings. Initially read the supplied filing URLs directly; otherwise reuse the verified baseline and prior ledger. Retain known corporate actions. A separate corporate-action pass follows.",
            context={"as_of": as_of, "your_last_answer": previous},
        )
        research = self.agents["researcher"].run(
            task_prompt="Finish this inventory as of as_of. Initially audit every original annual asset-section chunk AND every continuation chunk of each eligible transaction report. Rebuild the final issuer set as the UNION of positive annual stocks, later share acquisitions and distributed stocks; an annual-only reread must never discard later additions. Recheck each change date against snapshot_date: changes on or before that date are already reflected and must not be applied again. Resolve omitted positive rows independently before corporate-action work. Check each positive issuer separately for dated corporate actions from snapshot_date through as_of, including the current calendar year. Thereafter reuse verified actions and check the unreviewed interval or unresolved/changed issuers; do not redo verified old work. For web_search set the provider argument backend=\"yahoo\"; do not put the provider name in the query. Read dated issuer or exchange notices and apply effective distributed stocks and listing changes. Reuse verified filings. Return the complete updated ledger and each issuer's checked-through date or specific source failure.",
            context={"as_of": as_of, "inventory": inventory.summary, "your_last_answer": previous},
        )
        self.vars.set("last_research", research.summary)
        valuation = self.agents["trader"].run(
            task_prompt="Calculate the complete numeric basket weights using tool-based math, eligible source changes and current prices. Use each snapshot_ticker for its dated baseline price and as_of_ticker for current quotes. Use the disclosed ratios or labeled proxies for uncertain quantities. Return all weights and their price/share basis. Do not submit orders in this planning pass; execution follows.",
            context={"as_of": as_of, "research": research.summary, "your_last_answer": self.vars.get("last_valuation", "none yet")},
        )
        self.vars.set("last_valuation", valuation.summary)
        trade = self.agents["trader"].run(
            task_prompt="Execute the planned complete basket. Finish any remaining calculations with tools, then establish or rebalance every feasible target. A full exit is a zero target and overrides any midpoint proxy. Keep the same split-adjusted share basis; never apply the split twice. An unconfirmed exact source quantity is not a blocker when a ratio or proxy was authorized. Reconcile actual fills, holdings, open orders and cash before finishing.",
            context={"as_of": as_of, "research": research.summary, "valuation": valuation.summary, "previous_research": previous, "your_last_answer": self.vars.get("last_trade", "none yet")},
        )
        self.vars.set("last_trade", trade.summary)


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NancyPelosiTradingBot.backtest(YahooDataBacktesting)
    else:
        NancyPelosiTradingBot().run_live()
