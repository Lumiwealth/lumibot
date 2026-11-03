"""AI-owned iron condor with inexpensive minute observations.

One researcher supplies evidence; one trader owns every trading decision and
order. The trader saves its next attention conditions. Quiet minutes invoke no
model. This example requires the attention API in the installed LumiBot runtime.
Historical Alpaca option bars do not establish executable historical bid/ask.
"""
from datetime import timedelta

from lumibot.strategies import Strategy


class AIIronCondorStrategy(Strategy):
    parameters = {"symbol": "SPY", "model": None, "estimated_contract_fee": 0.65}

    def initialize(self):
        self.sleeptime = "1M"
        self.minutes_before_closing = 0
        self.agents.create(
            name="researcher", allow_trading=False, allow_network=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                (
                'Research the supplied underlying for a short iron condor held overnight through the next '
                'trading session. Use dated volatility, trend and event evidence, including the previous '
                'completed VIX close. Focus on material events inside that holding window. Distinguish known '
                'events from unavailable calendar evidence; give the trader a compact dated assessment, '
                'contrary evidence and uncertainty. Do not trade. '
            )
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, model=self.parameters["model"],
            reasoning_effort="high", system_prompt=(
                (
                'Trade at most one short/net-credit iron condor on the supplied underlying. Consider a new '
                'setup only in the final 15 regular-session minutes, at most once per session, using this '
                "session's setup research. Require the previous completed VIX close at or below 25. Use next- "
                'trading-day expiry, short strikes near 0.14 absolute delta and one-dollar outer wings. '
                'Compare at most three candidates. You choose integer package count; maximum loss including '
                'estimated round-trip per-contract fees must fit 3% of current account value. The supplied '
                'fee is an estimate, not a broker fee claim. '
                'The mandate is routine defined-risk premium collection when the priced range is reasonable. '
                'Compare candidate premiums and risk before deciding; decline for a concrete limit, known '
                'imminent major release or demonstrated breakout/gap risk. Ordinary mixed news or a missing '
                'calendar is not itself a veto. '
                'Set underlying review boundaries 40% of the distance from the short-strike midpoint toward '
                'either short strike; a breach requires your close decision. Close remaining exposure no '
                'later than 30 minutes before the expiration session ends. Leave the next eligible setup '
                "review after closure. Follow LumiBot's shared options and attention guidance; you own every "
                'trading decision. '
            )
            ),
        )

    def before_starting_trading(self):
        if not self.vars.get("attention_started", False):
            self.agents.attention.watch(owner="lifecycle", watch_id="initial_review",
                operation_id="initial_setup", metric="time", comparison="gte",
                value=(self.get_datetime() + timedelta(seconds=max(
                    0, (self.get_time_to_close() or 0) - 15 * 60))).isoformat(),
                reason="Initial setup review; verify entry eligibility under your policy",
                review_scope="setup")
            self.vars.set("attention_started", True)
        self.on_trading_iteration()

    def on_trading_iteration(self):
        self.agents.attention.poll()
        self.agents.attention.dispatch_team(decision_agent="trader", research_agents=["researcher"],
            context={"symbol": self.parameters["symbol"],
                     "estimated_contract_fee": self.parameters.get("estimated_contract_fee", 0.65),
                     "estimated_contract_fee_units": "USD per contract per side"})

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
