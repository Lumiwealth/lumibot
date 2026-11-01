"""AI-owned iron condor with inexpensive minute observations.

One researcher supplies evidence; one trader owns every trading decision and
order. The trader saves its next attention conditions. Quiet minutes invoke no
model. This example requires the attention API in the installed LumiBot runtime.
Historical Alpaca option bars do not establish executable historical bid/ask.
"""
from lumibot.strategies import Strategy


class AIIronCondorStrategy(Strategy):
    parameters = {"symbol": "SPY", "model": None}

    def initialize(self):
        self.sleeptime = "1M"
        self.minutes_before_closing = 15
        self.agents.create(
            name="researcher", allow_trading=False, allow_network=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                "Research the requested short iron condor opportunity at the strategy's current time. "
                "Use available dated volatility, trend and event evidence; obtain the previous completed "
                "VIX close where available. Explain suitability and contrary evidence. Compare a small "
                "useful set of at most three listed next-trading-day packages near 0.14 absolute short delta with "
                "one-dollar protective wings. Distinguish observed quotes from estimates. Return a "
                "compact dated evidence packet and limitations. Do not trade."
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                "You alone select contracts, quantity, package price, risk and every order/recovery action. "
                "Follow the shared options guidance. Hold at most one short/net-credit iron condor on the supplied symbol. "
                "Consider a new setup only in the final 15 regular-session minutes, at most once per session. "
                "Use this session's dated setup research before a new entry. Do not enter if the previous completed VIX close "
                "is above 25 or required evidence is unavailable. You may decline an unsuitable regime. "
                "Use a listed next-trading-day expiration, short put/call near 0.14 absolute delta and "
                "one-dollar outer protective wings. Maximum package loss must fit 3% of current account value; "
                "you choose the integer package count. Submit related legs atomically with a net limit. "
                "Obtain exact-leg quotes and Greeks. In an explicitly authorized minute-bar historical "
                "simulation, missing historical bid/ask may use a disclosed trade-bar price proxy; "
                "never invent quotes or claim that proxy proves executable liquidity. Live trading needs actual quotes. "
                "For a held package save underlying attention boundaries at 40% of the distance from the "
                "short-strike midpoint toward either short strike. A breach requires your close decision. "
                "Close all residual legs atomically no later than 30 minutes before the expiration session ends. "
                "Manage assignment/share obligations before another entry. Reconcile exact working orders and "
                "partial fills before replacing or submitting; a working close is not flat exposure. "
                "Use dated saved research for urgent management; refresh only when relevant evidence needs it. "
                "Leave setup reviews and research-update requests through attention_watch with the appropriate "
                "review_scope; the shared team dispatcher supplies the unchanged dated answers. "
                "Management reviews may reuse still-relevant research. Do not nest synchronous agent calls. "
                "Use attention_watch to save actual boundaries, quantity/order changes and the next review time, "
                "with stable operation and exact returned order identities. Plan the next eligible setup review "
                "after confirmed closure; do not repeat research daily while a healthy position needs no decision. "
                "Remove obsolete watches only after closure and obligations reconcile. An unchanged warning "
                "does not justify another ten-minute model heartbeat. Leave a real changed condition or next "
                "eligible review. Explain your actual resulting state without claiming unobserved fills."
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

        AIIronCondorStrategy.backtest(AlpacaBacktesting)
    else:
        AIIronCondorStrategy().run_live()
