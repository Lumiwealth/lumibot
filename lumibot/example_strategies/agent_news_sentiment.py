"""
News Sentiment Strategy - AI Agent Demo
----------------------------------------
This strategy uses an @agent_tool to call the Alpaca News API for
historical news headlines, then lets the AI decide what to trade.

Requirements:
    - GOOGLE_API_KEY (for Gemini model)
    - ALPACA_API_KEY and ALPACA_API_SECRET (for Alpaca News API)

Usage:
    export GOOGLE_API_KEY='your-google-key'
    export ALPACA_API_KEY='your-alpaca-key'
    export ALPACA_API_SECRET='your-alpaca-secret'
    python agent_news_sentiment.py
"""

import os
import requests

from lumibot.components.agents import agent_tool
from lumibot.strategies.strategy import Strategy

IS_BACKTESTING = True


class NewsSentimentStrategy(Strategy):

    @agent_tool(
        name="search_news",
        description=(
            "Search recent stock market news from Alpaca. Returns headlines, summaries, "
            "and associated stock symbols. Pass start and end as ISO timestamps to get "
            "news for a specific date range."
        ),
    )
    def search_news(
        self, start: str = "", end: str = "", symbols: str = "", limit: int = 10
    ) -> dict:
        """Call the Alpaca News API for historical news.

        Args:
            start: Start timestamp in ISO format (e.g., '2024-01-15T00:00:00Z')
            end: End timestamp in ISO format (e.g., '2024-01-16T00:00:00Z').
                Must not exceed the current datetime during backtests.
            symbols: Comma-separated stock symbols to filter by (e.g., 'AAPL,MSFT')
            limit: Maximum number of articles to return (default 10)

        Returns:
            dict: A dictionary with 'count' and 'articles' keys. Each article
                contains 'headline', 'summary', 'symbols', and 'created_at'.
        """
        api_key = os.environ.get("ALPACA_API_KEY", "")
        api_secret = os.environ.get("ALPACA_API_SECRET", "")
        headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
        params = {"limit": limit, "sort": "desc"}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if symbols:
            params["symbols"] = symbols
        try:
            resp = requests.get(
                "https://data.alpaca.markets/v1beta1/news",
                headers=headers, params=params, timeout=15,
            )
            resp.raise_for_status()
            articles = resp.json().get("news", [])
            return {
                "count": len(articles),
                "articles": [
                    {
                        "headline": a.get("headline", ""),
                        "summary": a.get("summary", "")[:200],
                        "symbols": a.get("symbols", []),
                        "created_at": a.get("created_at", ""),
                    }
                    for a in articles
                ],
            }
        except Exception as e:
            return {"error": str(e)}

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.iteration_count = 0
        self.agents.create(
            name="news_scout",
            default_model="gemini-3.1-flash-lite-preview",
            system_prompt=(
                "You must deploy all capital on every run. Never leave cash idle. "
                "Search for recent stock news using the current date. "
                "If any headlines mention strong catalysts (earnings beats, upgrades, "
                "product launches, deals), buy those stocks. Spread capital across 2-4 "
                "names. Only buy well-known US-listed stocks (like AAPL, MSFT, GOOGL, "
                "AMZN, NVDA, TSLA, META, JPM, etc). Do not buy obscure or penny stocks. "
                "If news is negative or unclear, buy SHV as a defensive position. "
                "Always be fully invested."
            ),
            tools=[self.search_news],
        )

    def on_trading_iteration(self):
        # Run agent every 5 trading days to keep backtest reasonable
        self.vars.iteration_count += 1
        if self.vars.iteration_count != 1 and self.vars.iteration_count % 5 != 0:
            return
        result = self.agents["news_scout"].run()
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
        NewsSentimentStrategy.backtest(
            YahooDataBacktesting,
            backtesting_start=datetime(2024, 1, 1),
            backtesting_end=datetime(2025, 1, 1),
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            quiet_logs=False,
        )
