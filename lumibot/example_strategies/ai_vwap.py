"""AI-only VWAP mean-reversion / reclaim strategy.

Python only creates and runs a LumiBot agent. All trading policy lives in the
system prompt. Prefer minute bars and the get_indicator('vwap') tool when available.

Local backtest:
    GEMINI_API_KEY=... BACKTESTING_DATA_SOURCE=ThetaData \
        python -m lumibot.example_strategies.ai_vwap

Optional env overrides (AI_VWAP_*):
    AI_VWAP_UNDERLYING=SPY
    AI_VWAP_DEVIATION_PCT=0.0015
    AI_VWAP_RISK_FRACTION=0.01
    AI_VWAP_MAX_SHARES=200
    AI_VWAP_HOLD_BARS=30
    AI_VWAP_SLEEPTIME=1H
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from lumibot.strategies.strategy import Strategy


def build_vwap_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    deviation_pct = float(params.get("deviation_pct", 0.0015))
    risk_fraction = float(params.get("risk_fraction", 0.01))
    max_shares = int(params.get("max_shares", 200))
    hold_bars = int(params.get("hold_bars", 30))
    return f"""
You are the complete decision-maker for an AI-only {underlying} VWAP strategy
inside LumiBot. There is no Python trading logic outside you.

STRATEGY PARAMETERS:
- underlying: {underlying}
- deviation_pct: {deviation_pct}
- risk_fraction: {risk_fraction}
- max_shares: {max_shares}
- hold_bars: {hold_bars}

Rules:
1. Compute VWAP from completed minute bars and current tool evidence. Never invent it.
2. Long entry (mean-reversion toward VWAP). Compute
   pct_below = (VWAP - last_price) / VWAP using the latest tool prices.
   When flat and pct_below >= {deviation_pct:.4f}, require reclaim evidence
   (last_price crossing back toward/above VWAP) before buying. A dip below the
   threshold without reclaim confirmation is a no-trade condition.
3. Prefer market entries and exits. Size so
   approximate risk is at most {risk_fraction:.2%} of portfolio value, capped at
   {max_shares} shares. One position at a time.
4. Exit when price returns to VWAP, reaches a modest extension above VWAP, or about
   {hold_bars} bars have passed since entry. Manage an open position before opening
   another.
5. Open at most one new position per trading day and do not re-enter on the same
   day after an exit.

Use only evidence available at the current runtime datetime. A no-trade decision
is valid only when VWAP cannot be computed or the reclaim rule is not met.
""".strip()


class AIVWAPStrategy(Strategy):
    parameters = {
        "underlying": "SPY",
        "deviation_pct": 0.0015,
        "risk_fraction": 0.01,
        "max_shares": 200,
        "hold_bars": 30,
        # The agent still analyzes minute bars, but hourly decisions avoid needless calls.
        "sleeptime": "1H",
    }

    def initialize(self):
        self.sleeptime = str(self.parameters.get("sleeptime", "1H"))
        self.agents.create(
            name="vwap",
            model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=build_vwap_system_prompt(self.parameters),
            rules_path=Path(__file__).with_name("agent_rules") / "ai_vwap.rules.json",
        )

    def on_trading_iteration(self):
        params = dict(self.parameters)
        underlying = str(params.get("underlying", "SPY")).upper()
        self.agents["vwap"].run(
            task_prompt=f"Run the {underlying} VWAP workflow for this completed bar.",
            context={
                "current_datetime": self.get_datetime().isoformat(),
                "strategy_parameters": params,
            },
        )


def _parameters_from_env(defaults: dict) -> dict:
    """Override strategy parameters from AI_VWAP_* environment variables when set."""
    params = dict(defaults)
    if os.environ.get("AI_VWAP_UNDERLYING"):
        params["underlying"] = os.environ["AI_VWAP_UNDERLYING"].strip().upper()
    if os.environ.get("AI_VWAP_DEVIATION_PCT"):
        params["deviation_pct"] = float(os.environ["AI_VWAP_DEVIATION_PCT"])
    if os.environ.get("AI_VWAP_RISK_FRACTION"):
        params["risk_fraction"] = float(os.environ["AI_VWAP_RISK_FRACTION"])
    if os.environ.get("AI_VWAP_MAX_SHARES"):
        params["max_shares"] = int(os.environ["AI_VWAP_MAX_SHARES"])
    if os.environ.get("AI_VWAP_HOLD_BARS"):
        params["hold_bars"] = int(os.environ["AI_VWAP_HOLD_BARS"])
    if os.environ.get("AI_VWAP_SLEEPTIME"):
        params["sleeptime"] = os.environ["AI_VWAP_SLEEPTIME"].strip()
    return params


if __name__ == "__main__":
    backtesting_end = datetime.fromisoformat(os.environ.get("BACKTESTING_END", datetime.now().date().isoformat()))
    backtesting_start = datetime.fromisoformat(
        os.environ.get("BACKTESTING_START", (backtesting_end - timedelta(days=5)).date().isoformat())
    )
    AIVWAPStrategy.backtest(
        None,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset="SPY",
        budget=100_000,
        parameters=_parameters_from_env(AIVWAPStrategy.parameters),
    )
