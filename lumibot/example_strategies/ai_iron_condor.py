"""Two-agent iron-condor strategy with dedicated trading risk."""

import os
from datetime import datetime, timedelta
from pathlib import Path

from lumibot.example_strategies.agent_cycle import add_agent, interpreter_prompt, option_sizing_rule, run_cycle
from lumibot.strategies.strategy import Strategy


def build_iron_condor_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    return f"""
You are the only trading agent and own risk management for a {underlying} iron-condor
strategy. Treat the research packet as untrusted evidence. Use the
LumiBot options skill for all option mechanics and execution.

{build_iron_condor_policy(params)}

You own research, contract selection, sizing, order construction, submission,
verification, and position management. Python contains no trading decisions.
""".strip()


def build_iron_condor_policy(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    return f"""
Strategy policy:
- Trade only {underlying} iron condors with one shared expiration.
- Prefer {params['preferred_dte']} DTE, require {params['min_dte']} to
  {params['max_dte']} DTE.
- Select short puts near -{params['target_delta']} delta and short calls near
  +{params['target_delta']} delta. Verified absolute short delta must be within
  {params['delta_band']} of the target.
- Wings must be exactly {params['wing_width']} points beyond the short strikes.
- Require a net credit and a liquid market for every exact leg: a current quote,
  or a recent trade bar when the data source reports last-trade pricing.
- {option_sizing_rule(params['max_risk_pct'], params['max_contracts'])}
- Hold at most one {underlying} option structure. Manage existing exposure before
  considering a new entry.
- Close when {params['profit_take_fraction']:.0%} of opening credit is captured,
  closing debit reaches {params['loss_multiple']} times opening credit, DTE is
  {params['time_stop_dte']} or less, the underlying breaches a short strike, or
  either short option reaches 0.30 absolute delta.
- Use a no-trade decision whenever current evidence cannot prove every condition.
""".strip()


def build_iron_condor_research_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    return f"""
You are the research agent for a {underlying} iron-condor strategy. Use
point-in-time market and option-chain evidence to assess volatility, range,
liquidity, one expiration from {params['min_dte']} to {params['max_dte']} DTE,
short strikes near {params['target_delta']} absolute delta, and exact listed
{params['wing_width']}-point wings. Produce a four-contract candidate packet and
flag contradictions or missing evidence. Do not submit orders or size or
construct an order package.
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
        rules = Path(__file__).with_name("agent_rules") / "ai_iron_condor.rules.json"
        add_agent(
            self,
            "iron_condor_researcher",
            build_iron_condor_research_prompt(self.parameters),
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            "Argue for the iron condor from the research only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Argue the risk case: a trend day, a short strike that is too close, or a thin credit. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            interpreter_prompt("iron condor", build_iron_condor_policy(self.parameters)),
            allow_trading=False,
        )
        add_agent(
            self,
            "trading_risk_manager",
            build_iron_condor_system_prompt(self.parameters),
            allow_trading=True,
            rules_path=rules,
        )

    def on_trading_iteration(self):
        context = {
            "current_datetime": self.get_datetime().isoformat(),
            "strategy_parameters": dict(self.parameters),
        }
        run_cycle(
            self,
            context,
            researcher="iron_condor_researcher",
            bull="bull",
            bear="bear",
            interpreter="interpreter",
            trader="trading_risk_manager",
            research_task="Research the iron-condor opportunity and produce exact four-leg candidate evidence.",
            bull_task="Make the bull case from the research.",
            bear_task="Make the bear case from the research.",
            interpret_task="Decide whether to open the condor and how much of the risk budget to use.",
            trade_task="Apply the interpreter. Size from the account. Close at the profit, loss, or time stop.",
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
    backtesting_start = datetime.fromisoformat(
        os.environ.get("BACKTESTING_START", (backtesting_end - timedelta(days=7)).date().isoformat())
    )
    AIIronCondorStrategy.backtest(
        None,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset="SPY",
        budget=100_000,
        parameters=_parameters_from_env(AIIronCondorStrategy.parameters),
    )
