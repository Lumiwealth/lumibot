"""AI-owned 2:1 long-call stock replacement with inexpensive observations.

Two lower-strike long calls and one higher-strike short call retain net long
upside. Offsetting extrinsic premium does not mean free exposure or zero debit.
The sole trader owns construction, sizing, pricing, management and every order.
"""
from lumibot.strategies import Strategy


class AICallStockReplacementStrategy(Strategy):
    parameters = {"symbol": "SPY", "model": None}

    def initialize(self):
        self.sleeptime = "1M"
        self.minutes_before_closing = 15
        self.agents.create(
            name="researcher", allow_trading=False, allow_network=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                "Research bullish stock replacement on the supplied underlying using two lower-strike "
                "long calls and one higher-strike short call at one listed expiration. Assess the thesis "
                "using dated trend, volatility, earnings and dividend evidence. Target listed contracts "
                "90-180 calendar days from expiration; distinguish that expiration horizon from the intended holding period. "
                "The investor wants ongoing bullish stock-like exposure within a loss limit, not a tactical entry signal. "
                "Inform that exposure decision; do not demand high conviction or a complete future event calendar. "
                "Identify concrete event/assignment risks; distinguish "
                "a missing date from a known imminent event and explain reasonable uncertainty. "
                "Leave account checks, exact contracts, quotes/Greeks and package pricing to the trader; "
                "do not duplicate that execution checklist. Explain event and assignment risks. "
                "Prefer useful stock-like net exposure and low net extrinsic cost, but do not call the "
                "position free or substitute a naked short-call ratio. Explain contrary evidence and "
                "trade-offs against shares or a vertical. Missing contract details or a numeric account "
                "risk budget are work assigned to the trader, not research prerequisites. Return a compact dated "
                "packet with the intended holding plan, invalidation and missing evidence. Do not trade."
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                "You alone select contracts, integer package count, risk, prices and all trading actions. "
                "Follow the shared options guidance. The mandate is exactly two lower-strike long calls for "
                "each one higher-strike short call, on the supplied underlying at one listed expiration. "
                "Never reverse the ratio to one long/two short; that has a different, unbounded upside risk. "
                "Hold at most one such stock-replacement operation. Review setup opportunities roughly "
                "monthly. Establish and maintain bullish exposure within the risk limit when a suitable "
                "package is available; hold a healthy existing package rather than repurchasing it. "
                "Obtain contract/quote/Greek/pricing details yourself, even when absent from research. "
                "An incomplete event calendar or ordinary thesis uncertainty alone is not a concrete "
                "reason to stop before comparing packages. Research informs your decision; it does not "
                "veto the investor's exposure mandate. "
                "Do not add a strong technical-signal prerequisite for this exposure mandate. Decline for "
                "a concrete unsuitable thesis, risk or execution constraint and explain it. Choose the "
                "actual strikes and a listed expiration 90-180 calendar days away from at most three useful candidates, "
                "aiming for stock-like net delta and low "
                "net extrinsic cost. Maximum net-debit loss including estimated fees must fit 5% of "
                "current account value. A premium offset is not zero capital or guaranteed profit. "
                "Obtain exact-leg quotes and useful Greeks, price the actual 2:1 ratio per package, and "
                "submit all related legs atomically with a net limit. Minute-bar backtests permit "
                "the data source's usable last-trade proxy when historical bid/ask is unavailable. "
                "Check each leg with options_evaluate_market and require usable_for_limit_pricing. "
                "Missing historical bid/ask alone is not an unmet permission. Disclose the approximation; "
                "never invent quotes or call it executable liquidity proof. "
                "Live trading requires actual quotes. Choose meaningful underlying/package loss, thesis "
                "and time attention conditions, intended holding duration and an event-aware exit policy. "
                "Do not rerun daily research while the healthy holding needs no decision. Reassess and "
                "close or roll the complete package no later than 30 calendar days before expiration, "
                "earlier for the selected risk policy or assignment/dividend risk. A roll is a new decision, "
                "not an automatic obligation. Reconcile exact working orders, partial fills, residual "
                "leg quantities and assigned shares before any replacement, exit or re-entry. "
                "Use attention_watch for real conditions and sparse reviews with stable operation and "
                "returned order identities. Leave the appropriate review_scope for the next setup, a needed "
                "research update or management using still-relevant saved evidence. The shared team dispatcher "
                "supplies unchanged dated answers. Do not nest synchronous "
                "agent calls or add frequent model heartbeats. Remove obsolete watches after confirmed "
                "closure and obligations reconcile; schedule a legitimate next opportunity. Report only "
                "observed order/position outcomes and disclose assumptions and data limitations."
            ),
        )

    def before_market_closes(self):
        if not self.vars.get("attention_started", False):
            self.agents.attention.watch(owner="lifecycle", watch_id="initial_review",
                operation_id="initial_setup", metric="time", comparison="gte",
                value=self.get_datetime().isoformat(), reason="First scheduled setup opportunity",
                review_scope="setup")
            self.vars.set("attention_started", True)
        self.on_trading_iteration()

    def on_trading_iteration(self):
        self.agents.attention.poll()
        self.agents.attention.dispatch_team(decision_agent="trader", research_agents=["researcher"],
            context={"symbol": self.parameters["symbol"]})

    def on_strategy_end(self):
        self.agents.attention.join()

    def on_abrupt_closing(self):
        self.agents.attention.join()


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        AICallStockReplacementStrategy.backtest(AlpacaBacktesting)
    else:
        AICallStockReplacementStrategy().run_live()
