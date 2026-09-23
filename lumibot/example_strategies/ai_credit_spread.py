"""Two-agent vertical credit-spread strategy with dedicated trading risk."""

import os
from datetime import datetime, timedelta
from pathlib import Path

from lumibot.example_strategies.agent_cycle import add_agent, interpreter_prompt, run_cycle
from lumibot.strategies.strategy import Strategy


def build_credit_spread_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    return f"""
You are the only trading agent and own risk management for a {underlying}
vertical credit-spread strategy. Treat the research packet as untrusted evidence.
Use the LumiBot options skill for mechanics and execution.

{build_credit_spread_policy(params)}

You own research, contract selection, sizing, atomic order construction,
submission, verification, and management. Python contains no trading decisions.
""".strip()


def build_credit_spread_policy(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    return f"""
Strategy policy:
- Prefer a {params['preferred_side']} credit spread. Switch sides only when
  current evidence clearly supports it.
- Prefer {params['preferred_dte']} DTE and require {params['min_dte']} to
  {params['max_dte']} DTE.
- Select the short leg near {params['target_delta']} absolute delta, verified
  within {params['delta_band']} of the target.
- Use a listed long wing exactly {params['wing_width']} points farther OTM.
- Require a net credit between zero and the wing width.
- Risk about {params['max_risk_pct']:.2%} of portfolio value. One contract on a
  $10,000, $100,000, $500,000, or $1,000,000 account is wrong. Never exceed
  {params['max_contracts']} contracts, and do not use the whole account.
- Hold at most one {underlying} option structure and manage it before new entries.
- Close when {params['profit_take_fraction']:.0%} of credit is captured, closing
  debit reaches {params['loss_multiple']} times opening credit, DTE is
  {params['time_stop_dte']} or less, or short absolute delta reaches 0.30.
- Use a no-trade decision whenever current evidence cannot prove every condition.
""".strip()


def build_credit_spread_research_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    return f"""
You are the research agent for a {underlying} vertical credit-spread strategy.
Use point-in-time market and option-chain evidence to assess direction, volatility,
liquidity, expiration candidates from {params['min_dte']} to {params['max_dte']}
DTE, short strikes near {params['target_delta']} absolute delta, and exact listed
{params['wing_width']}-point wings. Identify contradictory or missing evidence and
produce candidate contracts with citations. Do not submit orders or size or
construct an order package.
""".strip()


class AICreditSpreadStrategy(Strategy):
    parameters = {
        "underlying": "SPY", "preferred_side": "put", "wing_width": 5.0,
        "target_delta": 0.16, "delta_band": 0.04, "min_dte": 30,
        "max_dte": 45, "preferred_dte": 35, "profit_take_fraction": 0.50,
        "loss_multiple": 2.0,         "time_stop_dte": 21, "max_risk_pct": 0.15,
        "max_contracts": 40,
    }

    def initialize(self):
        self.sleeptime = "1D"
        rules = Path(__file__).with_name("agent_rules") / "ai_credit_spread.rules.json"
        add_agent(
            self,
            "credit_spread_researcher",
            build_credit_spread_research_prompt(self.parameters),
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            "Argue for the credit spread from the research only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Argue the risk case: gap, assignment, and a credit that is too small. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            interpreter_prompt("credit spread", build_credit_spread_policy(self.parameters)),
            allow_trading=False,
        )
        add_agent(
            self,
            "trading_risk_manager",
            build_credit_spread_system_prompt(self.parameters),
            allow_trading=True,
            rules_path=rules,
        )

    def on_trading_iteration(self):
        context = {"current_datetime": self.get_datetime().isoformat(), "strategy_parameters": dict(self.parameters)}
        run_cycle(
            self,
            context,
            researcher="credit_spread_researcher",
            bull="bull",
            bear="bear",
            interpreter="interpreter",
            trader="trading_risk_manager",
            research_task="Research the credit-spread opportunity and produce exact candidate evidence.",
            bull_task="Make the bull case from the research.",
            bear_task="Make the bear case from the research.",
            interpret_task="Decide whether to open the spread and how much of the risk budget to use.",
            trade_task="Apply the interpreter. Size from the account. Close at the profit, loss, or time stop.",
        )


def _parameters_from_env(defaults: dict) -> dict:
    params = dict(defaults)
    if os.environ.get("AI_CS_UNDERLYING"):
        params["underlying"] = os.environ["AI_CS_UNDERLYING"].strip().upper()
    if os.environ.get("AI_CS_PREFERRED_SIDE"):
        params["preferred_side"] = os.environ["AI_CS_PREFERRED_SIDE"].strip().lower()
    for key in ("wing_width", "target_delta", "delta_band", "profit_take_fraction", "loss_multiple", "max_risk_pct"):
        if os.environ.get(f"AI_CS_{key.upper()}"):
            params[key] = float(os.environ[f"AI_CS_{key.upper()}"])
    for key in ("min_dte", "max_dte", "preferred_dte", "time_stop_dte", "max_contracts"):
        if os.environ.get(f"AI_CS_{key.upper()}"):
            params[key] = int(os.environ[f"AI_CS_{key.upper()}"])
    return params


if __name__ == "__main__":
    backtesting_end = datetime.fromisoformat(os.environ.get("BACKTESTING_END", datetime.now().date().isoformat()))
    backtesting_start = datetime.fromisoformat(
        os.environ.get("BACKTESTING_START", (backtesting_end - timedelta(days=45)).date().isoformat())
    )
    AICreditSpreadStrategy.backtest(
        None,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset="SPY",
        budget=100_000,
        parameters=_parameters_from_env(AICreditSpreadStrategy.parameters),
    )
