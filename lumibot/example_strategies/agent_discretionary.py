"""
Discretionary AI Trader - Maximum Discretion Agent Demo
--------------------------------------------------------
This strategy gives the AI agent maximum discretion: minimal user prompt,
broad tool surface, no asset whitelist, no thesis in the system prompt.

The agent decides what to buy, what to sell, when to short, when to hold
cash, and which instruments make sense. Its only mandate is to grow the
account. Everything else (risk discipline, look-ahead safety, tool-use
guidance, position sizing) comes from LumiBot's base system prompt.

This is the demo used for honest multi-provider model comparison. The
exact same code runs against any provider via the AGENT_MODEL env var:

    AGENT_MODEL="gemini-3.1-pro-preview"           # needs GOOGLE_API_KEY
    AGENT_MODEL="openai/gpt-5.4"                    # needs OPENAI_API_KEY
    AGENT_MODEL="xai/grok-4.20-0309-reasoning"      # needs XAI_API_KEY
    AGENT_MODEL="anthropic/claude-opus-4-7"         # needs ANTHROPIC_API_KEY

Data source: Yahoo Finance (US-listed stocks + ETFs). Fundamentals come
from yfinance (already a LumiBot dependency). News comes from Alpaca.
Macro comes from FRED.

Requirements (only the key matching AGENT_MODEL is strictly required):
    - GOOGLE_API_KEY / OPENAI_API_KEY / XAI_API_KEY / ANTHROPIC_API_KEY
    - ALPACA_API_KEY and ALPACA_API_SECRET (for the news tool; optional)

Usage:
    export AGENT_MODEL="xai/grok-4.20-0309-reasoning"
    export XAI_API_KEY='your-xai-key'
    export BACKTESTING_START='2026-03-01'
    export BACKTESTING_END='2026-03-31'
    python agent_discretionary.py
"""

import csv
import io
import os
import requests

from lumibot.components.agents import agent_tool
from lumibot.strategies.strategy import Strategy

IS_BACKTESTING = True


class DiscretionaryTraderStrategy(Strategy):

    @agent_tool(
        name="get_fred_series",
        description=(
            "Fetch economic data from FRED (Federal Reserve Economic Data). "
            "Common series: M2SL (M2 money supply), FEDFUNDS (fed funds rate), "
            "CPIAUCSL (CPI), UNRATE (unemployment), GDP, T10Y2Y (10Y-2Y yield "
            "spread), BOGMBASE (monetary base), DGS10 (10Y Treasury), VIXCLS "
            "(VIX close), DCOILWTICO (crude oil WTI), GOLDAMGBD228NLBM (gold)."
        ),
    )
    def get_fred_series(
        self, series_id: str, start_date: str = "2020-01-01", end_date: str = ""
    ) -> dict:
        """Fetch a FRED series using the public CSV endpoint.

        Args:
            series_id: FRED series identifier.
            start_date: Start date in YYYY-MM-DD format.
            end_date: End date in YYYY-MM-DD format. Must not exceed the
                current simulated datetime during backtests.

        Returns:
            dict with series_id, count, and observations (each {date, value}).
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
                "observations": observations[-60:],
            }
        except Exception as e:
            return {"error": str(e), "series_id": series_id}

    @agent_tool(
        name="search_news",
        description=(
            "Search recent US stock market news from Alpaca. Returns headlines, "
            "summaries, and associated stock symbols. Pass start/end as ISO "
            "timestamps to scope to a time range. Filter by symbols if you "
            "already have a target name in mind."
        ),
    )
    def search_news(
        self, start: str = "", end: str = "", symbols: str = "", limit: int = 10
    ) -> dict:
        """Call the Alpaca News API.

        Args:
            start: Start timestamp in ISO format (e.g. '2025-06-01T00:00:00Z').
            end: End timestamp in ISO format. Must not exceed the current
                simulated datetime during backtests.
            symbols: Comma-separated tickers to filter by (e.g. 'AAPL,MSFT').
            limit: Max articles to return (default 10, cap 50).

        Returns:
            dict with count and articles (each {headline, summary, symbols,
            created_at}).
        """
        api_key = os.environ.get("ALPACA_API_KEY", "")
        api_secret = os.environ.get("ALPACA_API_SECRET", "")
        if not api_key or not api_secret:
            return {"error": "ALPACA_API_KEY/SECRET not set; news unavailable", "count": 0, "articles": []}
        headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
        params = {"limit": min(max(int(limit or 10), 1), 50), "sort": "desc"}
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
                        "summary": (a.get("summary") or "")[:300],
                        "symbols": a.get("symbols", []),
                        "created_at": a.get("created_at", ""),
                    }
                    for a in articles
                ],
            }
        except Exception as e:
            return {"error": str(e), "count": 0, "articles": []}

    @agent_tool(
        name="get_fundamentals",
        description=(
            "Get fundamentals snapshot for any US-listed stock or ETF from "
            "Yahoo Finance. Returns trailing P/E, forward P/E, market cap, "
            "profit margin, revenue growth, earnings date, analyst mean target, "
            "short interest, 52-week high/low, sector, and industry. Useful for "
            "quick due diligence on any name before sizing a position."
        ),
    )
    def get_fundamentals(self, symbol: str) -> dict:
        """Fetch a fundamentals snapshot via yfinance.

        Args:
            symbol: Ticker symbol (e.g. 'AAPL', 'SPY', 'NVDA').

        Returns:
            dict with fundamentals fields. Keys are None if unavailable.
        """
        try:
            import yfinance as yf
            t = yf.Ticker(symbol)
            info = t.info or {}
            keys = [
                "shortName", "sector", "industry",
                "trailingPE", "forwardPE", "pegRatio",
                "marketCap", "enterpriseValue",
                "profitMargins", "operatingMargins", "revenueGrowth", "earningsGrowth",
                "currentPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
                "dividendYield", "beta",
                "shortRatio", "shortPercentOfFloat",
                "targetMeanPrice", "recommendationMean", "numberOfAnalystOpinions",
                "earningsTimestamp", "earningsTimestampStart", "earningsTimestampEnd",
            ]
            snapshot = {k: info.get(k) for k in keys if k in info}
            snapshot["symbol"] = symbol
            return snapshot
        except Exception as e:
            return {"error": str(e), "symbol": symbol}

    def initialize(self):
        self.sleeptime = "1D"
        self.vars.iteration_count = 0
        model_id = os.environ.get("AGENT_MODEL", "gemini-3.1-pro-preview")
        self.agents.create(
            name="trader",
            default_model=model_id,
            system_prompt="Make as much money as you possibly can.",
            tools=[self.get_fred_series, self.search_news, self.get_fundamentals],
        )
        self.log_message(f"DiscretionaryTraderStrategy initialized with model={model_id}", color="yellow")

    def on_trading_iteration(self):
        # Weekly cadence: run the agent every 5 trading days.
        self.vars.iteration_count += 1
        if self.vars.iteration_count != 1 and self.vars.iteration_count % 5 != 0:
            return
        result = self.agents["trader"].run()
        self.log_message(f"[trader] {result.summary}", color="yellow")


if __name__ == "__main__":
    model_id = os.environ.get("AGENT_MODEL", "gemini-3.1-pro-preview")
    # Auth precheck: only the key for the selected provider is required.
    prefix_key = {
        "openai/": ("OPENAI_API_KEY", "https://platform.openai.com/api-keys"),
        "xai/": ("XAI_API_KEY", "https://console.x.ai/"),
        "anthropic/": ("ANTHROPIC_API_KEY", "https://console.anthropic.com/"),
    }
    required_key = None
    sign_up_url = None
    if model_id.startswith("gemini-") or model_id.startswith("models/gemini"):
        required_key, sign_up_url = "GOOGLE_API_KEY", "https://aistudio.google.com/apikey"
    else:
        for prefix, (k, url) in prefix_key.items():
            if model_id.startswith(prefix):
                required_key, sign_up_url = k, url
                break
    if required_key and not os.environ.get(required_key):
        print(f"ERROR: {required_key} is required for AGENT_MODEL={model_id!r}.")
        print(f"Get one at {sign_up_url}")
        print(f"Then export it: export {required_key}='your-key'")
        raise SystemExit(1)

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import Asset, TradingFee

    if IS_BACKTESTING:
        trading_fee = TradingFee(percent_fee=0.001)
        DiscretionaryTraderStrategy.backtest(
            YahooDataBacktesting,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            budget=10000,
            quiet_logs=False,
        )
