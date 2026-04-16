"""
Momentum Allocator Strategy - AI Agent Demo
---------------------------------------------
This strategy uses @agent_tool to call the Alpaca market data API
for price bars and news, then lets the AI decide between
TQQQ (risk-on) and SHV (risk-off) based on price momentum and sentiment.

Requirements:
    - GOOGLE_API_KEY (for Gemini model)
    - ALPACA_API_KEY and ALPACA_API_SECRET (for Alpaca market data and news APIs)

Usage:
    export GOOGLE_API_KEY='your-google-key'
    export ALPACA_API_KEY='your-alpaca-key'
    export ALPACA_API_SECRET='your-alpaca-secret'
    python agent_momentum_allocator.py
"""

import os
import requests

from lumibot.components.agents import agent_tool
from lumibot.strategies.strategy import Strategy

IS_BACKTESTING = True


class MomentumAllocatorStrategy(Strategy):

    @agent_tool(
        name="get_stock_bars",
        description=(
            "Get recent daily price bars for a stock symbol from Alpaca. "
            "Returns OHLCV data. Use this to check price trends and momentum "
            "for TQQQ, SPY, QQQ, or any US stock."
        ),
    )
    def get_stock_bars(self, symbol: str, start: str = "", end: str = "", limit: int = 20) -> dict:
        """Call Alpaca bars API for historical price data.

        Args:
            symbol: Stock ticker symbol (e.g., TQQQ, SPY, QQQ)
            start: Start date in YYYY-MM-DD or ISO format
            end: End date in YYYY-MM-DD or ISO format (must not exceed
                current datetime in backtests)
            limit: Maximum number of bars to return (default 20)

        Returns:
            dict: A dictionary with 'symbol', 'count', and 'bars' keys.
                Each bar contains 'date', 'open', 'high', 'low', 'close',
                and 'volume'.
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
                headers=headers,
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            bars = data.get("bars", [])
            return {
                "symbol": symbol,
                "count": len(bars),
                "bars": [
                    {
                        "date": b.get("t", ""),
                        "open": b.get("o"),
                        "high": b.get("h"),
                        "low": b.get("l"),
                        "close": b.get("c"),
                        "volume": b.get("v"),
                    }
                    for b in bars[:10]
                ],
            }
        except Exception as e:
            return {"error": str(e), "symbol": symbol}

    @agent_tool(
        name="search_news",
        description=(
            "Search recent stock market news from Alpaca. Returns headlines and symbols. "
            "Use this to check market sentiment and find catalysts."
        ),
    )
    def search_news(self, start: str = "", end: str = "", limit: int = 5) -> dict:
        """Call the Alpaca News API for recent headlines.

        Args:
            start: Start timestamp in ISO format (e.g., '2024-06-01T00:00:00Z')
            end: End timestamp in ISO format. Must not exceed the current
                datetime during backtests.
            limit: Maximum number of articles to return (default 5)

        Returns:
            dict: A dictionary with 'count' and 'articles' keys. Each article
                contains 'headline' and 'symbols'.
        """
        api_key = os.environ.get("ALPACA_API_KEY", "")
        api_secret = os.environ.get("ALPACA_API_SECRET", "")
        headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
        params = {"limit": limit, "sort": "desc"}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        try:
            resp = requests.get(
                "https://data.alpaca.markets/v1beta1/news",
                headers=headers,
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            articles = resp.json().get("news", [])
            return {
                "count": len(articles),
                "articles": [
                    {"headline": a.get("headline", ""), "symbols": a.get("symbols", [])}
                    for a in articles
                ],
            }
        except Exception as e:
            return {"error": str(e)}

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.iteration_count = 0
        self.agents.create(
            name="momentum_analyst",
            default_model="gemini-3.1-flash-lite-preview",
            system_prompt=(
                "You must be fully invested at all times. Never leave cash idle. "
                "Check TQQQ price momentum and recent news. "
                "If TQQQ is trending up (recent closes are higher than earlier closes) "
                "and news sentiment is not terrible, buy TQQQ with all capital. "
                "If TQQQ is trending down or news is very negative, buy SHV with all capital. "
                "Sell your current position before buying the other. "
                "Always hold either TQQQ or SHV, never cash."
            ),
            tools=[self.get_stock_bars, self.search_news],
        )

    def on_trading_iteration(self):
        # Run agent every 20 trading days
        self.vars.iteration_count += 1
        if self.vars.iteration_count != 1 and self.vars.iteration_count % 20 != 0:
            return
        result = self.agents["momentum_analyst"].run()
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
        MomentumAllocatorStrategy.backtest(
            YahooDataBacktesting,
            backtesting_start=datetime(2024, 1, 1),
            backtesting_end=datetime(2025, 1, 1),
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            quiet_logs=False,
        )
