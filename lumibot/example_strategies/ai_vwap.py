"""AI-only VWAP mean-reversion / reclaim strategy.

Python only creates and runs a LumiBot agent. All trading policy lives in the
system prompt. Prefer minute bars and the get_indicator('vwap') tool when available.

Local backtest:
    GEMINI_API_KEY=... BACKTESTING_DATA_SOURCE=ThetaData \
        python -m lumibot.example_strategies.ai_vwap
"""

import os
from datetime import datetime, timedelta

from lumibot.strategies.strategy import Strategy


def build_vwap_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    deviation_pct = float(params.get("deviation_pct", 0.003))
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
1. Each iteration call account_portfolio, account_positions, orders_open_orders,
   and market_last_price for {underlying}.
2. Prefer get_indicator(symbol='{underlying}', indicator='vwap', timestep='minute')
   and/or market_load_history_table with timestep='minute'. If only daily data is
   available, use daily VWAP cautiously and state the limitation.
3. Long bias when price is at least {deviation_pct:.2%} below VWAP and shows reclaim
   evidence (price crossing back above VWAP or stabilizing). Exit when price
   returns to VWAP, reaches a modest extension above VWAP, or {hold_bars} bars pass.
4. Size so approximate risk is at most {risk_fraction:.2%} of portfolio value, capped
   at {max_shares} shares. One position at a time.
5. Never invent VWAP, prices, or fills. A no-trade decision is valid when VWAP
   cannot be computed from available bars.
6. After any order submission, call orders_get_status on returned identifiers,
   re-read account_positions, and never claim a fill unless is_filled is true.

Use only evidence available at the current runtime datetime.
""".strip()


class AIVWAPStrategy(Strategy):
    parameters = {
        "underlying": "SPY",
        "deviation_pct": 0.003,
        "risk_fraction": 0.01,
        "max_shares": 200,
        "hold_bars": 30,
    }

    def initialize(self):
        self.sleeptime = "1M"
        self.agents.create(
            name="vwap",
            model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=build_vwap_system_prompt(self.parameters),
        )

    def on_trading_iteration(self):
        params = dict(self.parameters)
        underlying = str(params.get("underlying", "SPY")).upper()
        self.agents["vwap"].run(
            task_prompt=(
                f"Run the {underlying} VWAP workflow for this bar. "
                "Prefer the vwap indicator on minute data when available. "
                "After any submission, verify status with orders_get_status."
            ),
            context={
                "current_datetime": self.get_datetime().isoformat(),
                "strategy_parameters": params,
            },
        )


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
    )
