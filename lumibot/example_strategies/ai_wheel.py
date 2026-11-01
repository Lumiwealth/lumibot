"""AI-owned cash-secured puts and covered calls with inexpensive attention.

Two agents make decisions; the shared monitor observes between those decisions.
Historical trade bars do not establish executable bid/ask liquidity.
"""
from pathlib import Path

from lumibot.strategies import Strategy


class AIWheelStrategy(Strategy):
    parameters = {
        "symbol": "SPY", "model": "chatgpt/gpt-6-luna", "reasoning_effort": "medium",
        "min_dte": 30, "max_dte": 45, "target_delta": 0.25,
        "max_contracts": 1, "max_collateral_pct": 90, "profit_capture_pct": 90,
        "time_in_force": "day", "holding_review_days": 7,
    }

    def initialize(self):
        self.sleeptime = "1M"
        self.minutes_before_closing = 15
        settings = {
            "model": self.parameters.get("model", "chatgpt/gpt-6-luna"),
            "reasoning_effort": self.parameters.get("reasoning_effort", "medium"),
            "rules_path": Path(__file__).with_name("rules.json"),
            "model_request_timeout_seconds": 0, "run_timeout_seconds": 0,
        }
        self.agents.create(
            name="researcher", allow_trading=False, allow_network=True, **settings,
            system_prompt=(
                "Research willingness to own the supplied ETF for a cash-secured-put/covered-call wheel. "
                "Return at most 250 words: dated trend/volatility evidence, relevant known distribution "
                "or market events, material risks, contrary evidence, missing inputs and when the thesis "
                "needs review. Use at most three useful sources and only information available at the "
                "strategy time in a backtest. Reuse dated evidence when it still applies. Leave exact "
                "contract comparisons, account checks and execution to the trader. Do not trade."
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, **settings,
            system_prompt=(
                "You alone choose and execute the wheel's contracts, size, prices and risk decisions. "
                "Follow shared options guidance and the supplied policy parameters. "
                "The investor is willing to own the configured ETF and wants recurrent wheel exposure "
                "when suitable contracts fit the policy. Obtain premium and execution evidence yourself; "
                "those details are deliberately outside the researcher's job. Missing optional event "
                "research is a disclosed uncertainty, not an additional mandatory entry condition. "
                "Start from actual cash or existing shares: sell cash-secured puts until assigned, then covered calls "
                "until called away, then consider puts again. Hold at most max_contracts short options. "
                "Put assignment plus costs must fit available cash and max_collateral_pct of equity; "
                "calls require unencumbered deliverable shares. No borrowing, naked options, automatic "
                "rolls or unrelated holdings disposal. Compare at most three suitable listed contracts "
                "within min_dte/max_dte near target_delta; prefer covered-call strikes at or above "
                "verified share cost. Hold when no suitable contract or capacity exists. "
                "Normally accept assignment and expiry. Consider buying to close at profit_capture_pct "
                "of actual entry premium, allowing for costs. Use price-bounded DAY limits with the "
                "configured time_in_force; smart limits are not this example's default. "
                "Cadence follows available option capacity. With no short wheel option and no related "
                "working option order, consider an entry once per eligible session near the close: "
                "cash can secure a put, and unencumbered shares can cover a call. Preserve the next "
                "eligible session's setup watch after declining an entry. While a short option or "
                "related working order occupies that capacity, remove obsolete daily setup watches; "
                "do not schedule daily setup/research just because another session opens. "
                "Use meaningful profit, quantity, order, expiry and thesis watches, plus one recurring "
                "management review no later than holding_review_days. Reuse relevant dated research "
                "for management; request research only when a decision needs refreshed evidence. "
                "Reconcile an assignment, call-away, close or terminal order as management first; "
                "if capacity is then free, preserve the next eligible near-close setup opportunity. "
                "Finish obsolete-watch cleanup before saving a future recurring review; never remove "
                "its updated definition as if it were the consumed one. Use review_scope=setup for "
                "an eligible entry, research for needed fresh evidence, and management to reuse it. "
                "Retain each obligation and "
                "its own symbol when the configured ticker changes. Remove obsolete watches only "
                "after reconciliation. Leave a reachable next review without minute AI heartbeats. "
                "Report observed exposure and unresolved work. Validation goals never justify a trade."
            ),
        )

    def before_market_closes(self):
        if not self.vars.get("attention_started", False):
            self.agents.attention.watch(
                owner="lifecycle", watch_id="initial_review", operation_id="initial_setup",
                metric="time", comparison="gte", value=self.get_datetime().isoformat(),
                reason="First scheduled setup opportunity", review_scope="setup",
            )
            self.vars.set("attention_started", True)
        self.on_trading_iteration()

    def on_trading_iteration(self):
        self.agents.attention.poll()
        self.agents.attention.dispatch_team(
            decision_agent="trader", research_agents=["researcher"], context=dict(self.parameters),
        )

    def on_strategy_end(self):
        self.agents.attention.join()

    def on_abrupt_closing(self):
        self.agents.attention.join()


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        AIWheelStrategy.backtest(datasource_class=None)
    else:
        AIWheelStrategy().run_live()
