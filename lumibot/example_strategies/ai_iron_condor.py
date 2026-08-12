"""AI-only iron-condor strategy driven entirely by a LumiBot agent."""

import os
from datetime import datetime, timedelta
from pathlib import Path

from lumibot.strategies.strategy import Strategy


def build_iron_condor_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    return f"""
You are the complete decision-maker for an AI-only {underlying} iron-condor
strategy. Use the LumiBot options skill for all option mechanics and execution.

Strategy policy:
- Trade only {underlying} iron condors with one shared expiration.
- Prefer {params['preferred_dte']} DTE, require {params['min_dte']} to
  {params['max_dte']} DTE.
- Select short puts near -{params['target_delta']} delta and short calls near
  +{params['target_delta']} delta. Verified absolute short delta must be within
  {params['delta_band']} of the target.
- Wings must be exactly {params['wing_width']} points beyond the short strikes.
- Require a net credit and liquid markets for every exact leg.
- Risk no more than {params['max_risk_pct']:.2%} of portfolio value and never
  exceed {params['max_contracts']} contracts.
- Hold at most one {underlying} option structure. Manage existing exposure before
  considering a new entry.
- Close when {params['profit_take_fraction']:.0%} of opening credit is captured,
  closing debit reaches {params['loss_multiple']} times opening credit, DTE is
  {params['time_stop_dte']} or less, the underlying breaches a short strike, or
  either short option reaches 0.30 absolute delta.
- Use a no-trade decision whenever current evidence cannot prove every condition.

You own research, contract selection, sizing, order construction, submission,
verification, and position management. Python contains no trading decisions.
""".strip()


class AIIronCondorStrategy(Strategy):
    parameters = {
        "underlying": "SPY",
        "wing_width": 5.0,
        "target_delta": 0.16,
        "delta_band": 0.04,
        "min_dte": 30,
        "max_dte": 45,
        "preferred_dte": 35,
        "profit_take_fraction": 0.50,
        "loss_multiple": 2.0,
        "time_stop_dte": 21,
        "max_risk_pct": 0.02,
        "max_contracts": 10,
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="iron_condor",
            model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=build_iron_condor_system_prompt(self.parameters),
            rules_path=Path(__file__).with_name("agent_rules") / "ai_iron_condor.rules.json",
        )

    def on_trading_iteration(self):
        self.agents["iron_condor"].run(
            task_prompt="Run the complete iron-condor workflow for this iteration.",
            context={
                "current_datetime": self.get_datetime().isoformat(),
                "strategy_parameters": dict(self.parameters),
            },
        )


def _parameters_from_env(defaults: dict) -> dict:
    params = dict(defaults)
    float_keys = ("wing_width", "target_delta", "delta_band", "profit_take_fraction", "loss_multiple", "max_risk_pct")
    int_keys = ("min_dte", "max_dte", "preferred_dte", "time_stop_dte", "max_contracts")
    if os.environ.get("AI_IC_UNDERLYING"):
        params["underlying"] = os.environ["AI_IC_UNDERLYING"].strip().upper()
    for key in float_keys:
        if os.environ.get(f"AI_IC_{key.upper()}"):
            params[key] = float(os.environ[f"AI_IC_{key.upper()}"])
    for key in int_keys:
        if os.environ.get(f"AI_IC_{key.upper()}"):
            params[key] = int(os.environ[f"AI_IC_{key.upper()}"])
    return params


if __name__ == "__main__":
    backtesting_end = datetime.fromisoformat(os.environ.get("BACKTESTING_END", datetime.now().date().isoformat()))
    backtesting_start = datetime.fromisoformat(os.environ.get("BACKTESTING_START", (backtesting_end - timedelta(days=7)).date().isoformat()))
    AIIronCondorStrategy.backtest(None, backtesting_start=backtesting_start, backtesting_end=backtesting_end, benchmark_asset="SPY", budget=100_000, parameters=_parameters_from_env(AIIronCondorStrategy.parameters))
