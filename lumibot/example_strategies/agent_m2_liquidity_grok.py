"""
M2 Liquidity Strategy - AI Agent Demo (xAI Grok)
------------------------------------------------
Same strategy intent as agent_m2_liquidity.py, but uses xAI's Grok model
instead of Google's Gemini. This demonstrates Lumibot's multi-provider
AI agent support via the LiteLLM bridge.

The only differences vs the Gemini version:
    - default_model is an xAI id ("xai/grok-4.20-0309-reasoning")
    - XAI_API_KEY or GROK_API_KEY is required instead of GEMINI_API_KEY
    - The litellm package must be installed (it ships with Lumibot)

Model choice note:
    xai/grok-4.20-0309-reasoning is Grok 4.20 (= Grok 4.2) with chain-of-
    thought reasoning enabled. 2M context, $2/M input / $6/M output. If you
    want a cheaper/faster variant, try xai/grok-4-1-fast-reasoning-latest
    ($0.20/M input / $0.50/M output, same 2M context). The plain
    xai/grok-4-latest alias is older and more expensive.

All other agent mechanics (built-in tools, replay cache, backtesting
safety rules, base system prompt) are identical across providers. The
cache key includes the model string, so swapping providers on the same
backtest produces fresh runs rather than stale cross-model replays.

Requirements:
    - XAI_API_KEY or GROK_API_KEY (get one at https://console.x.ai/)
    - FRED_API_KEY (for official FRED/ALFRED macro data)

Usage:
    export XAI_API_KEY='your-xai-key'
    export FRED_API_KEY='your-fred-key'
    python agent_m2_liquidity_grok.py
"""

import os

from lumibot.components.agents import agent_tool
from lumibot.strategies.strategy import Strategy

IS_BACKTESTING = True


class M2LiquidityGrokStrategy(Strategy):

    @agent_tool(
        name="get_fred_series",
        description=(
            "Fetch economic data from FRED (Federal Reserve Economic Data). "
            "Common series: M2SL (M2 money supply), FEDFUNDS (fed funds rate), "
            "CPIAUCSL (CPI), UNRATE (unemployment), GDP (gross domestic product), "
            "T10Y2Y (10Y-2Y yield spread), BOGMBASE (monetary base). "
            "Returns date-value pairs. Use start_date and end_date in YYYY-MM-DD format."
        ),
    )
    def get_fred_series(
        self, series_id: str, start_date: str = "2020-01-01", end_date: str = ""
    ) -> dict:
        """Fetch a FRED series through Lumibot's point-in-time macro helper."""
        return self.macro.get_series(
            series_id,
            start=start_date,
            end=end_date or None,
        )

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.iteration_count = 0
        self.agents.create(
            name="m2_analyst",
            default_model="xai/grok-4.20-0309-reasoning",
            system_prompt=(
                "You must be fully invested at all times. Never leave cash idle. "
                "Fetch M2 money supply data (M2SL) and check if liquidity is expanding "
                "or contracting. Compare recent values to values from 3-6 months ago. "
                "If M2 is growing (recent values higher than earlier), buy TQQQ. "
                "If M2 is flat or shrinking, buy SHV. "
                "You can also check FEDFUNDS and T10Y2Y for confirmation. "
                "Always hold either TQQQ or SHV. Sell one before buying the other."
            ),
            tools=[self.get_fred_series],
        )

    def on_trading_iteration(self):
        # Run agent every 20 trading days
        self.vars.iteration_count += 1
        if self.vars.iteration_count != 1 and self.vars.iteration_count % 20 != 0:
            return
        result = self.agents["m2_analyst"].run()
        self.log_message(f"[agent] {result.summary}")


if __name__ == "__main__":
    if not os.environ.get("XAI_API_KEY") and not os.environ.get("GROK_API_KEY"):
        print("ERROR: XAI_API_KEY or GROK_API_KEY environment variable is required.")
        print("Get an API key at https://console.x.ai/")
        print("Then set it: export XAI_API_KEY='your-key-here'")
        raise SystemExit(1)

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import Asset, TradingFee

    if IS_BACKTESTING:
        trading_fee = TradingFee(percent_fee=0.001)
        M2LiquidityGrokStrategy.backtest(
            YahooDataBacktesting,
            backtesting_start=datetime(2024, 1, 1),
            backtesting_end=datetime(2025, 1, 1),
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            quiet_logs=False,
        )
