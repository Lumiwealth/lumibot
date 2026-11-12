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
                "Supply at most 180 words of dated ETF evidence, ownership risks, gaps and "
                "refresh needs from available history and at most three useful sources. "
                "Distinguish closes from intraday ranges; preserve source labels and dates. "
                "Use information available at strategy time. Reuse relevant dated research. "
                "Leave contracts, account checks, watches and all orders to the trader."
            ),
        )
        self.agents.create(
            name="trader", allow_trading=True, **settings,
            system_prompt=(
                "You alone select contracts, size, trade and manage risk. Apply Rules and "
                "parameters using shared options guidance. Seek recurring eligible ETF wheel "
                "exposure with willingness to own the ETF. Use actual account, orders and "
                "contract evidence; explain the observed comparison honestly. Research informs "
                "decisions but optional research gaps do not veto an otherwise eligible entry. "
                "Before finishing, inspect the saved attention plan against the current "
                "phase in Rules; repair missing or redundant reviews. While a wheel contract "
                "or related order occupies capacity, remove every entry/capacity timer under "
                "any scope; a conditional explanation does not filter a time watch. Report exposure and "
                "unresolved work truthfully. Never trade to satisfy validation."
            ),
        )

    def before_starting_trading(self):
        if not self.vars.get("attention_started", False):
            first_review = self.get_datetime() + timedelta(seconds=max(0,
                self.broker.get_time_to_close() - self.parameters["entry_minutes_before_close"] * 60))
            self.agents.attention.watch(
                owner="trader", watch_id="initial_review", operation_id="initial_setup",
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
