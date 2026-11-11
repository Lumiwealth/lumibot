"""AI-owned cash-secured puts and covered calls with inexpensive attention.

Two agents make decisions; the shared monitor observes between those decisions.
Historical trade bars do not establish executable bid/ask liquidity.
"""
from pathlib import Path
from datetime import timedelta

from lumibot.strategies import Strategy


class AIWheelStrategy(Strategy):
    parameters = {
        "symbol": "SPY", "model": "chatgpt/gpt-6-luna", "reasoning_effort": "medium",
        "min_dte": 30, "max_dte": 45, "target_delta": 0.25,
        "max_contracts": 1, "max_collateral_pct": 90, "profit_capture_pct": 90,
        "time_in_force": "day", "holding_review_days": 7, "entry_minutes_before_close": 15,
        "research_context": "",
    }

    def initialize(self):
        self.sleeptime = "1M"
        self.minutes_before_closing = 0
        settings = {
            "model": self.parameters.get("model", "chatgpt/gpt-6-luna"),
            "reasoning_effort": self.parameters.get("reasoning_effort", "medium"),
            "rules_path": Path(__file__).with_name("rules.json"),
            "model_request_timeout_seconds": 0, "run_timeout_seconds": 0,
        }
        self.agents.create(
            name="researcher", allow_trading=False, allow_network=True, **settings,
            system_prompt=(
                "Read available ETF price history and research willingness to own it. Supply at most 180 words of dated trend, "
                "volatility, relevant events, contrary evidence and material risks using at most "
                "three useful sources. State gaps and when evidence needs refreshing. Use only "
                "information available at strategy time in a backtest. Reuse relevant dated "
                "research. Leave contract selection, account checks, scheduling and execution to the trader. "
                "Do not register watches or trade."
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, **settings,
            system_prompt=(
                "You alone trade and manage risk. Follow shared options guidance, Rules and supplied "
                "parameters. Run a cash-secured-put/covered-call wheel on the configured ETF, which "
                "the investor is willing to own. Seek recurring eligible exposure, starting from "
                "actual cash or shares. New entries MUST have min_dte through max_dte calendar days "
                "remaining, inclusive, measured from today to the actual listed expiration. "
                "Hold if no contract meets that mandatory window. Compare at most three eligible contracts "
                "near target_delta. Prefer calls at or above verified share acquisition cost. "
                "Respect max_contracts and max_collateral_pct. No borrowing, naked options, "
                "automatic rolls or disposal of unrelated holdings. Use bounded limit orders "
                "and time_in_force; smart limits are not the default. Accept ordinary assignment "
                "and expiry. Consider buying to close after profit_capture_pct of actual premium "
                "is captured, accounting for costs. Persist that favorable review trigger even "
                "before it is reached; a trigger requests your decision, never an automatic order. "
                "With free capacity, preserve one entry opportunity entry_minutes_before_close "
                "before each eligible exchange session closes, including shortened sessions. "
                "Only submit new entries within that final session window. Use a disclosed "
                "reasonable session approximation if exact calendar metadata is unavailable. "
                "While a short option or related order occupies capacity, remove obsolete daily "
                "setup watches. Do not register setup-scope watches while that capacity is occupied. "
                "Monitor meaningful profit, position, order, expiry and thesis "
                "events, plus management within holding_review_days. Reuse dated research for "
                "management; refresh it only when a decision needs it. Reconcile fills, assignment, "
                "call-away and failed orders before re-entry. Keep old-symbol obligations when "
                "future entry symbol changes. Clean obsolete watches before replacing a recurring "
                "review. Leave reachable next attention without minute AI heartbeats. Missing "
                "optional event research is disclosed uncertainty, not a new entry veto. "
                "Report observed exposure and unresolved work. Never trade to satisfy validation."
            ),
        )

    def before_starting_trading(self):
        if not self.vars.get("attention_started", False):
            first_review = self.get_datetime() + timedelta(seconds=max(0,
                self.broker.get_time_to_close() - self.parameters["entry_minutes_before_close"] * 60))
            self.agents.attention.watch(
                owner="lifecycle", watch_id="initial_review", operation_id="initial_setup",
                metric="time", comparison="gte", value=first_review.isoformat(),
                reason="Initial near-close entry review; reconcile actual account before choosing an action", review_scope="setup",
            )
            self.vars.set("attention_started", True)
        self.on_trading_iteration()

    def before_market_closes(self):
        self.on_trading_iteration()

    def after_market_closes(self):
        self.on_trading_iteration()

    def on_trading_iteration(self):
        self.agents.attention.poll()
        self.agents.attention.dispatch_team(
            decision_agent="trader", research_agents=["researcher"],
            context={key: self.parameters[key] for key in type(self).parameters},
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
