"""
Macro Risk Strategy - AI Agent Demo
------------------------------------
This strategy uses @agent_tool to call the Alpaca market data API
for historical price bars and market movers, then lets the AI decide
between TQQQ (risk-on) and SHV (risk-off) based on market trends.

Requirements:
    - GOOGLE_API_KEY (for Gemini model)
    - ALPACA_API_KEY and ALPACA_API_SECRET (for Alpaca market data API)

Usage:
    export GOOGLE_API_KEY='your-google-key'
    export ALPACA_API_KEY='your-alpaca-key'
    export ALPACA_API_SECRET='your-alpaca-secret'
    python agent_macro_risk.py
"""

import os
import requests

from lumibot.components.agents import agent_tool
from lumibot.strategies.strategy import Strategy

IS_BACKTESTING = True


class MacroRiskStrategy(Strategy):

    @agent_tool(
        name="get_stock_bars",
        description="Get historical daily price bars for a stock from Alpaca.",
    )
    def get_stock_bars(
        self, symbol: str, start: str = "", end: str = "", limit: int = 30
    ) -> dict:
        """Get historical OHLCV bars from the Alpaca market data API.

        Args:
            symbol: Stock ticker symbol (e.g., TQQQ, SPY, QQQ)
            start: Start date in YYYY-MM-DD or ISO format
            end: End date in YYYY-MM-DD or ISO format (must not exceed
                current datetime in backtests)
            limit: Maximum number of bars to return (default 30)

        Returns:
            dict: A dictionary with 'symbol', 'count', and 'bars' keys.
                Each bar contains 'date', 'close', 'high', 'low', and 'volume'.
        """
        api_key = os.environ.get("ALPACA_API_KEY", "")
        api_secret = os.environ.get("ALPACA_API_SECRET", "")
        headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
        params = {"timeframe": "1Day", "limit": limit, "sort": "desc"}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        try:
            resp = requests.get(
                f"https://data.alpaca.markets/v2/stocks/{symbol}/bars",
                headers=headers, params=params, timeout=15,
            )
            resp.raise_for_status()
            bars = resp.json().get("bars", [])
            return {
                "symbol": symbol,
                "count": len(bars),
                "bars": [
                    {"date": b["t"][:10], "close": b["c"], "high": b["h"], "low": b["l"], "volume": b["v"]}
                    for b in bars[:15]
                ],
            }
        except Exception as e:
            return {"error": str(e), "symbol": symbol}

    @agent_tool(
        name="get_market_movers",
        description="Get the top gaining and losing stocks from Alpaca.",
    )
    def get_market_movers(self, top: int = 10) -> dict:
        """Get market movers (top gainers and losers) from Alpaca.

        Args:
            top: Number of top movers to return per category (default 10).
                Results are capped at 5 gainers and 5 losers in the response.

        Returns:
            dict: A dictionary with 'gainers' and 'losers' keys. Each entry
                contains 'symbol' and 'change_pct'.
        """
        api_key = os.environ.get("ALPACA_API_KEY", "")
        api_secret = os.environ.get("ALPACA_API_SECRET", "")
        headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
        try:
            resp = requests.get(
                "https://data.alpaca.markets/v1beta1/screener/stocks/movers",
                headers=headers, params={"top": top}, timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "gainers": [
                    {"symbol": g.get("symbol"), "change_pct": g.get("percent_change")}
                    for g in (data.get("gainers") or [])[:5]
                ],
                "losers": [
                    {"symbol": l.get("symbol"), "change_pct": l.get("percent_change")}
                    for l in (data.get("losers") or [])[:5]
                ],
            }
        except Exception as e:
            return {"error": str(e)}

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.iteration_count = 0
        self.agents.create(
            name="macro_allocator",
            default_model="gemini-3.1-flash-lite-preview",
            system_prompt=(
                "You must be fully invested at all times. Never leave cash idle. "
                "Check TQQQ and SPY price trends using historical bars. "
                "If TQQQ is trending up (recent closes higher than earlier), "
                "buy TQQQ with all available capital. "
                "If TQQQ is trending down, buy SHV with all capital. "
                "Sell your current position before buying the other. "
                "Always hold either TQQQ or SHV, never cash."
            ),
            tools=[self.get_stock_bars, self.get_market_movers],
        )

    def on_trading_iteration(self):
        # Run agent every 20 trading days
        self.vars.iteration_count += 1
        if self.vars.iteration_count != 1 and self.vars.iteration_count % 20 != 0:
            return
        result = self.agents["macro_allocator"].run()
        self.log_message(f"[agent] {result.summary}")


if __name__ == "__main__":
    import os

    if not os.environ.get("GOOGLE_API_KEY"):
        print("ERROR: GOOGLE_API_KEY environment variable is required.")
        print("Get a free API key from https://aistudio.google.com/apikey")
        print("Then set it: export GOOGLE_API_KEY='your-key-here'")
        raise SystemExit(1)

    if not os.environ.get("ALPACA_API_KEY") or not os.environ.get("ALPACA_API_SECRET"):
        print("ERROR: ALPACA_API_KEY and ALPACA_API_SECRET environment variables are required.")
        print("Get free API keys from https://app.alpaca.markets/signup")
        print("Then set them:")
        print("  export ALPACA_API_KEY='your-key-here'")
        print("  export ALPACA_API_SECRET='your-secret-here'")
        raise SystemExit(1)

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import Asset, TradingFee

    if IS_BACKTESTING:
        trading_fee = TradingFee(percent_fee=0.001)
        MacroRiskStrategy.backtest(
            YahooDataBacktesting,
            backtesting_start=datetime(2024, 1, 1),
            backtesting_end=datetime(2025, 1, 1),
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            quiet_logs=False,
        )
