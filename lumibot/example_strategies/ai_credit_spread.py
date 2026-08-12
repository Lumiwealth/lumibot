"""AI-only vertical credit-spread strategy driven by a LumiBot agent."""

import os
from datetime import datetime, timedelta
from pathlib import Path

from lumibot.strategies.strategy import Strategy


def build_credit_spread_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    return f"""
You are the complete decision-maker for an AI-only {underlying} vertical credit
spread strategy. Use the LumiBot options skill for mechanics and execution.

Strategy policy:
- Prefer a {params['preferred_side']} credit spread. Switch sides only when
  current evidence clearly supports it.
- Prefer {params['preferred_dte']} DTE and require {params['min_dte']} to
  {params['max_dte']} DTE.
- Select the short leg near {params['target_delta']} absolute delta, verified
  within {params['delta_band']} of the target.
- Use a listed long wing exactly {params['wing_width']} points farther OTM.
- Require a net credit between zero and the wing width.
- Risk no more than {params['max_risk_pct']:.2%} of portfolio value and never
  exceed {params['max_contracts']} contracts.
- Hold at most one {underlying} option structure and manage it before new entries.
- Close when {params['profit_take_fraction']:.0%} of credit is captured, closing
  debit reaches {params['loss_multiple']} times opening credit, DTE is
  {params['time_stop_dte']} or less, or short absolute delta reaches 0.30.
- Use a no-trade decision whenever current evidence cannot prove every condition.

You own research, contract selection, sizing, atomic order construction,
submission, verification, and management. Python contains no trading decisions.
""".strip()


class AICreditSpreadStrategy(Strategy):
    parameters = {
        "underlying": "SPY", "preferred_side": "put", "wing_width": 5.0,
        "target_delta": 0.16, "delta_band": 0.04, "min_dte": 30,
        "max_dte": 45, "preferred_dte": 35, "profit_take_fraction": 0.50,
        "loss_multiple": 2.0, "time_stop_dte": 21, "max_risk_pct": 0.02,
        "max_contracts": 10,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(name="credit_spread", model="gemini-3.5-flash-lite", allow_trading=True,
            system_prompt=build_credit_spread_system_prompt(self.parameters),
            rules_path=Path(__file__).with_name("agent_rules") / "ai_credit_spread.rules.json")

    def on_trading_iteration(self):
        self.agents["credit_spread"].run(task_prompt="Run the complete credit-spread workflow for this iteration.",
            context={"current_datetime": self.get_datetime().isoformat(), "strategy_parameters": dict(self.parameters)})


def _parameters_from_env(defaults: dict) -> dict:
    params = dict(defaults)
    if os.environ.get("AI_CS_UNDERLYING"): params["underlying"] = os.environ["AI_CS_UNDERLYING"].strip().upper()
    if os.environ.get("AI_CS_PREFERRED_SIDE"): params["preferred_side"] = os.environ["AI_CS_PREFERRED_SIDE"].strip().lower()
    for key in ("wing_width", "target_delta", "delta_band", "profit_take_fraction", "loss_multiple", "max_risk_pct"):
        if os.environ.get(f"AI_CS_{key.upper()}"): params[key] = float(os.environ[f"AI_CS_{key.upper()}"])
    for key in ("min_dte", "max_dte", "preferred_dte", "time_stop_dte", "max_contracts"):
        if os.environ.get(f"AI_CS_{key.upper()}"): params[key] = int(os.environ[f"AI_CS_{key.upper()}"])
    return params


if __name__ == "__main__":
    backtesting_end = datetime.fromisoformat(os.environ.get("BACKTESTING_END", datetime.now().date().isoformat()))
    backtesting_start = datetime.fromisoformat(os.environ.get("BACKTESTING_START", (backtesting_end - timedelta(days=45)).date().isoformat()))
    AICreditSpreadStrategy.backtest(None, backtesting_start=backtesting_start, backtesting_end=backtesting_end, benchmark_asset="SPY", budget=100_000, parameters=_parameters_from_env(AICreditSpreadStrategy.parameters))
