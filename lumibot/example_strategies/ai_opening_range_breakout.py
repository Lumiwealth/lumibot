"""AI-only opening-range breakout strategy.

Python only creates and runs a LumiBot agent. All entry, exit, and risk rules
live in the system prompt. Prefer minute bars when the data source supports them.

Local backtest:
    GEMINI_API_KEY=... BACKTESTING_DATA_SOURCE=ThetaData \
        python -m lumibot.example_strategies.ai_opening_range_breakout
"""

import os
from datetime import datetime, timedelta

from lumibot.strategies.strategy import Strategy


def build_orb_system_prompt(params: dict) -> str:
    underlying = str(params.get("underlying", "SPY")).upper()
    opening_range_minutes = int(params.get("opening_range_minutes", 15))
    risk_fraction = float(params.get("risk_fraction", 0.01))
    max_shares = int(params.get("max_shares", 200))
    profit_r_multiple = float(params.get("profit_r_multiple", 1.5))
    return f"""
You are the complete decision-maker for an AI-only {underlying} opening-range
breakout strategy inside LumiBot. There is no Python trading logic outside you.

STRATEGY PARAMETERS:
- underlying: {underlying}
- opening_range_minutes: {opening_range_minutes}
- risk_fraction: {risk_fraction}
- max_shares: {max_shares}
- profit_r_multiple: {profit_r_multiple}

Rules:
1. Each iteration call account_portfolio, account_positions, orders_open_orders,
   and market_last_price for {underlying}.
2. Prefer minute history via market_load_history_table with timestep='minute'.
   If only daily bars are available, document that limitation in your summary
   and do not invent intraday levels.
3. Define the opening range as the high/low of the first {opening_range_minutes}
   minutes of the regular session when minute data exists.
4. Long only on a close above the opening-range high with confirming volume when
   available. Short only on a close below the opening-range low when shorting is
   allowed by the account and your evidence.
5. Size so approximate stop risk is at most {risk_fraction:.2%} of portfolio value,
   capped at {max_shares} shares. Stop is the opposite side of the opening range.
6. Take profit near {profit_r_multiple}R or exit on a close back inside the range.
7. One position at a time. Never invent prices or fills.
8. After any order submission, call orders_get_status on returned identifiers,
   re-read account_positions, and never claim a fill unless is_filled is true.

Use only evidence available at the current runtime datetime. A no-trade decision
is valid when the opening range is incomplete or data is insufficient.
""".strip()


class AIOpeningRangeBreakoutStrategy(Strategy):
    parameters = {
        "underlying": "SPY",
        "opening_range_minutes": 15,
        "risk_fraction": 0.01,
        "max_shares": 200,
        "profit_r_multiple": 1.5,
    }

    def initialize(self):
        # Minute cadence when the data source supports it; daily still runs.
        self.sleeptime = "1M"
        self.agents.create(
            name="orb",
            model="gemini-3.5-flash-lite",
            allow_trading=True,
            system_prompt=build_orb_system_prompt(self.parameters),
        )

    def on_trading_iteration(self):
        params = dict(self.parameters)
        underlying = str(params.get("underlying", "SPY")).upper()
        self.agents["orb"].run(
            task_prompt=(
                f"Run the {underlying} opening-range breakout workflow for this bar. "
                "If minute data is unavailable, say so and do not invent a range. "
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
    AIOpeningRangeBreakoutStrategy.backtest(
        None,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        benchmark_asset="SPY",
        budget=100_000,
    )
