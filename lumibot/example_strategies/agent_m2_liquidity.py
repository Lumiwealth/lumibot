"""
M2 Liquidity Strategy - AI Agent Demo
--------------------------------------
This strategy uses @agent_tool to fetch real M2 money supply data
from FRED (Federal Reserve Economic Data) and lets the AI decide
between TQQQ (risk-on) and SHV (risk-off) based on whether
liquidity is expanding or contracting.

Requirements:
    - GOOGLE_API_KEY (for Gemini model)
    - No additional API keys needed (FRED CSV endpoint is public)

Usage:
    export GOOGLE_API_KEY='your-google-key'
    python agent_m2_liquidity.py
"""

import csv
import io
import os
import requests

from lumibot.components.agents import agent_tool
from lumibot.strategies.strategy import Strategy

IS_BACKTESTING = True


class M2LiquidityStrategy(Strategy):

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
        """Fetch a FRED series using the public CSV endpoint (no API key needed).

        Args:
            series_id: FRED series identifier (e.g., M2SL, FEDFUNDS, CPIAUCSL,
                UNRATE, GDP, T10Y2Y, BOGMBASE)
            start_date: Start date in YYYY-MM-DD format (default '2020-01-01')
            end_date: End date in YYYY-MM-DD format. Must not exceed the
                current datetime during backtests.

        Returns:
            dict: A dictionary with 'series_id', 'count', and 'observations'
                keys. Each observation contains 'date' and 'value'.
        """
        params = {"id": series_id}
        if start_date:
            params["cosd"] = start_date
        if end_date:
            params["coed"] = end_date
        try:
            resp = requests.get(
                "https://fred.stlouisfed.org/graph/fredgraph.csv",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            observations = []
            for row in reader:
                date = row.get("observation_date", "")
                value = row.get(series_id, ".")
                if value and value != ".":
                    observations.append({"date": date, "value": float(value)})
            return {
                "series_id": series_id,
                "count": len(observations),
                "observations": observations,
            }
        except Exception as e:
            return {"error": str(e), "series_id": series_id}

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.iteration_count = 0
        self.agents.create(
            name="m2_analyst",
            default_model="gemini-3.1-flash-lite-preview",
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
    import os

    if not os.environ.get("GOOGLE_API_KEY"):
        print("ERROR: GOOGLE_API_KEY environment variable is required.")
        print("Get a free API key from https://aistudio.google.com/apikey")
        print("Then set it: export GOOGLE_API_KEY='your-key-here'")
        raise SystemExit(1)

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import Asset, TradingFee

    if IS_BACKTESTING:
        trading_fee = TradingFee(percent_fee=0.001)
        M2LiquidityStrategy.backtest(
            YahooDataBacktesting,
            backtesting_start=datetime(2024, 1, 1),
            backtesting_end=datetime(2025, 1, 1),
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            quiet_logs=False,
        )
