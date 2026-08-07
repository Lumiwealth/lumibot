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
    AI_VWAP_SLEEPTIME=15M
"""

import os
from datetime import datetime, timedelta

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
1. Each iteration call account_portfolio, account_positions, orders_open_orders,
   and market_last_price for {underlying}.
2. Prefer get_indicator(symbol='{underlying}', indicator='vwap', timestep='minute')
   and/or market_load_history_table with timestep='minute'. If only daily data is
   available, use daily VWAP cautiously and state the limitation. Never invent VWAP.
3. Long entry (mean-reversion toward VWAP). Compute
   pct_below = (VWAP - last_price) / VWAP using the latest tool prices.
   When flat and pct_below >= {deviation_pct:.4f}, you SHOULD market-buy in this
   same iteration (buy the dip under VWAP). Prefer entries that also show reclaim
   evidence (last_price crossing back toward/above VWAP), but do not skip a clear
   dip that already meets the deviation threshold. Missing a valid dip entry is
   worse than sitting flat all session.
4. Prefer order_type='market' for entries and exits so the order can fill in
   backtests and live. Do not attach limit_price on market orders. Size so
   approximate risk is at most {risk_fraction:.2%} of portfolio value, capped at
   {max_shares} shares. One position at a time.
5. Exit when price returns to VWAP, reaches a modest extension above VWAP, or about
   {hold_bars} bars have passed since entry. Manage an open position before opening
   another. Before any close submit, call market_last_price again in this same run.
6. After any orders_submit_order call, capture identifiers, call orders_get_status
   (and orders_wait_for_terminal with a short timeout if still open), re-read
   account_positions, and never claim a fill unless is_filled is true.

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
        # Prefer minute bars; use a coarser sleeptime in short backtests to bound agent calls.
        "sleeptime": "15M",
    }

    def initialize(self):
        self.sleeptime = str(self.parameters.get("sleeptime", "15M"))
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
                f"Run the {underlying} VWAP reclaim workflow for this bar. "
                "Compute pct_below versus VWAP. If the dip threshold was met and price "
                "has reclaimed VWAP, submit a market buy now and verify fills with "
                "orders_get_status / orders_wait_for_terminal."
            ),
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
