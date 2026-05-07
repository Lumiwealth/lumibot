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

    AGENT_MODEL="gemini-3.1-pro-preview"           # needs GEMINI_API_KEY
    AGENT_MODEL="openai/gpt-5.4"                    # needs OPENAI_API_KEY
    AGENT_MODEL="xai/grok-4.20-0309-reasoning"      # needs XAI_API_KEY or GROK_API_KEY
    AGENT_MODEL="anthropic/claude-opus-4-7"         # needs ANTHROPIC_API_KEY

Data source: Yahoo Finance (US-listed stocks + ETFs). Fundamentals come
from yfinance (already a LumiBot dependency). News comes from Alpaca.
Macro comes from FRED.

Requirements (only the key matching AGENT_MODEL is strictly required):
    - GEMINI_API_KEY / OPENAI_API_KEY / XAI_API_KEY or GROK_API_KEY / ANTHROPIC_API_KEY
    - Active Alpaca broker credentials or ALPACA_NEWS_API_KEY / ALPACA_NEWS_API_SECRET for the news tool; optional

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

from lumibot.components.agents import BuiltinTools, agent_tool
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
            "(VIX close), DCOILWTICO (crude oil WTI), GOLDAMGBD228NLBM (gold). "
            "These are economic series ids, not tradable tickers; do not pass them to market price/history tools."
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
            tools=[self.get_fred_series, BuiltinTools.news.alpaca_news(), self.get_fundamentals],
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
        "xai/": (("XAI_API_KEY", "GROK_API_KEY"), "https://console.x.ai/"),
        "anthropic/": ("ANTHROPIC_API_KEY", "https://console.anthropic.com/"),
    }
    required_key = None
    sign_up_url = None
    if model_id.startswith("gemini-") or model_id.startswith("models/gemini"):
        required_key, sign_up_url = "GEMINI_API_KEY", "https://aistudio.google.com/apikey"
    else:
        for prefix, (k, url) in prefix_key.items():
            if model_id.startswith(prefix):
                required_key, sign_up_url = k, url
                break
    if isinstance(required_key, tuple):
        missing_key = not any(os.environ.get(key) for key in required_key)
        key_label = " or ".join(required_key)
    else:
        missing_key = bool(required_key) and not os.environ.get(required_key)
        key_label = required_key
    if missing_key:
        print(f"ERROR: {key_label} is required for AGENT_MODEL={model_id!r}.")
        print(f"Get one at {sign_up_url}")
        first_key = required_key[0] if isinstance(required_key, tuple) else required_key
        print(f"Then export it: export {first_key}='your-key'")
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
