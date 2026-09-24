"""Two-agent VWAP mean-reversion / reclaim strategy.

Python coordinates a research agent and a dedicated trading/risk agent. All
trading policy lives in their prompts. Prefer minute bars and the
get_indicator('vwap') tool when available.

Local backtest:
    OPENAI_API_KEY=... BACKTESTING_DATA_SOURCE=alpaca \
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

from lumibot.example_strategies.agent_cycle import (
    add_agent,
    run_cycle,
    session_minutes_elapsed,
    trader_prompt,
)
from lumibot.strategies.strategy import Strategy


def build_vwap_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    deviation_pct = float(params.get("deviation_pct", 0.0015))
    risk_fraction = float(params.get("risk_fraction", 0.01))
    max_shares = int(params.get("max_shares", 200))
    hold_bars = int(params.get("hold_bars", 1))
    sleeptime = str(params.get("sleeptime", "1H"))
    return f"""
You are the research agent for a {underlying} VWAP strategy. Evaluate the setup
from point-in-time evidence and produce a precise research packet. Do not submit orders.

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
   This strategy is evaluated once per {sleeptime}. The entry signal is current
   when the threshold dip and the reclaim both formed on completed bars since the
   previous evaluation ({sleeptime}) and price is still at or above VWAP now. Do
   not reject such a reclaim because several one-minute bars have passed;
   hold_bars counts bars after entry, not the age of the signal.
3. Prefer market entries and exits. Put the stop just below the dip's lowest
   completed bar and size so stop risk is at most {risk_fraction:.2%} of portfolio
   value, capped at {max_shares} shares. One position at a time.
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
        "hold_bars": 1,
        "min_session_minutes": 30,
        "sleeptime": "1H",
    }

    def initialize(self):
        self.sleeptime = str(self.parameters.get("sleeptime", "1H"))
        rules = Path(__file__).with_name("agent_rules") / "ai_vwap.rules.json"
        underlying = str(self.parameters.get("underlying", "SPY")).upper()
        deviation_pct = float(self.parameters.get("deviation_pct", 0.0015))
        risk_fraction = float(self.parameters.get("risk_fraction", 0.01))
        max_shares = int(self.parameters.get("max_shares", 200))
        add_agent(
            self,
            "vwap_researcher",
            build_vwap_system_prompt(self.parameters),
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            f"Argue the long reclaim case for {underlying} from the research only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            f"Argue the risk case against the {underlying} VWAP entry. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            "Read both cases. Say whether the reclaim is real and where the dip low stop sits. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "trading_risk_manager",
            trader_prompt(
                book_rule=(
                    f"Trade only {underlying}. Buy only after price dipped at least "
                    f"{deviation_pct:.2%} below VWAP and reclaim confirmation. "
                    "A dip and reclaim that formed on completed bars since the previous "
                    "evaluation is a current signal while price is still at or above VWAP."
                ),
                exit_rule=(
                    "If a position was opened on an earlier bar, sell it with the order tool "
                    "before any new buy. Also sell when price is back at VWAP or the hold is over."
                ),
                cash_rule=(
                    "Size from the stop: put the stop just below the dip's lowest completed bar, so "
                    f"stop risk is at most {risk_fraction:.2%} of portfolio value, capped at {max_shares} shares. "
                    "Shares are that risk budget divided by the distance from entry to stop. "
                    "Do not size to a fraction of account value."
                ),
            ),
            allow_trading=True,
            rules_path=rules,
        )

    def on_trading_iteration(self):
        params = dict(self.parameters)
        # Session VWAP is undefined until completed session bars exist.
        if session_minutes_elapsed(self) < int(params.get("min_session_minutes", 30)):
            return
        underlying = str(params.get("underlying", "SPY")).upper()
        context = {
            "current_datetime": self.get_datetime().isoformat(),
            "strategy_parameters": params,
        }
        run_cycle(
            self,
            context,
            researcher="vwap_researcher",
            bull="bull",
            bear="bear",
            interpreter="interpreter",
            trader="trading_risk_manager",
            research_task=f"Research the {underlying} VWAP setup for this completed bar.",
            bull_task="Make the bull case from the research.",
            bear_task="Make the bear case from the research.",
            interpret_task="Decide whether the reclaim is real and how much of the account to use.",
            trade_task="Apply the interpreter. Size from the stop risk. Exit an older position before a new buy.",
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
