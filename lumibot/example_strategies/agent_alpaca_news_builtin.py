"""
Alpaca News Built-in Strategy - AI Agent Demo
---------------------------------------------
This demo shows the recommended built-in-tool pattern for news-driven AI
agents. It uses BuiltinTools.news.alpaca_news() instead of writing a custom
Alpaca wrapper in the strategy.

The agent is instructed to:
    1. Scan broad-market news with include_content=False.
    2. Fetch full article content with include_content=True before trading on
       any important story.
    3. Compare article timestamps to the current simulated datetime.

Requirements:
    - GEMINI_API_KEY, or set AGENT_MODEL to another provider and provide its key
    - ALPACA_API_KEY and ALPACA_API_SECRET (or APCA_API_KEY_ID/APCA_API_SECRET_KEY)

Usage:
    export GEMINI_API_KEY='your-gemini-key'
    export ALPACA_API_KEY='your-alpaca-key'
    export ALPACA_API_SECRET='your-alpaca-secret'
    export BACKTESTING_START='2025-04-21'
    export BACKTESTING_END='2025-04-28'
    python -m lumibot.example_strategies.agent_alpaca_news_builtin
"""

import os

from lumibot.strategies.strategy import Strategy


IS_BACKTESTING = True


class AlpacaNewsBuiltinStrategy(Strategy):
    def initialize(self):
        from lumibot.components.agents import BuiltinTools

        self.sleeptime = "1D"
        self.vars.iteration_count = 0
        model_id = os.environ.get("AGENT_MODEL", "gemini-3.1-flash-lite-preview")
        self.agents.create(
            name="news_trader",
            default_model=model_id,
            system_prompt=(
                "Use Alpaca news and market tools to decide whether to hold SPY, QQQ, or a defensive ETF. "
                "For news, first call alpaca_news with symbols='SPY,QQQ,DIA,IWM', include_content=False, "
                "and limit=30 to scan broad-market headlines and summaries. Before making a trade or no-trade "
                "decision, pick the most relevant article from the scan and call alpaca_news again for the same "
                "or narrower window with include_content=True and exclude_contentless=True to read the full article. If next_page_token is "
                "returned and the first page is not enough, use page_token "
                "to fetch another page. Always compare article timestamps to the current simulated datetime."
            ),
            tools=[BuiltinTools.news.alpaca_news()],
        )
        self.log_message(f"AlpacaNewsBuiltinStrategy initialized with model={model_id}", color="yellow")

    def on_trading_iteration(self):
        self.vars.iteration_count += 1
        if self.vars.iteration_count != 1 and self.vars.iteration_count % 5 != 0:
            return

        result = self.agents["news_trader"].run(
            task_prompt=(
                "Research current broad-market news using alpaca_news. First scan headlines and summaries. "
                "Then fetch full content for the most relevant article with include_content=True and exclude_contentless=True before your final decision. "
                "If evidence is bullish, buy SPY or QQQ. If evidence is negative or unclear, buy a defensive ETF. "
                "Keep position sizing reasonable."
            ),
            context={"current_datetime": self.get_datetime().isoformat()},
        )
        self.log_message(f"[news_trader] {result.summary}", color="yellow")


def _has_model_key(model_id: str) -> bool:
    lower = model_id.lower()
    if lower.startswith("gemini-") or lower.startswith("models/gemini"):
        return bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"))
    if lower.startswith("openai/"):
        return bool(os.environ.get("OPENAI_API_KEY"))
    if lower.startswith("xai/"):
        return bool(os.environ.get("XAI_API_KEY") or os.environ.get("GROK_API_KEY"))
    if lower.startswith("anthropic/"):
        return bool(os.environ.get("ANTHROPIC_API_KEY"))
    return True


if __name__ == "__main__":
    model_id = os.environ.get("AGENT_MODEL", "gemini-3.1-flash-lite-preview")
    if not _has_model_key(model_id):
        print(f"ERROR: missing model-provider API key for AGENT_MODEL={model_id!r}.")
        raise SystemExit(1)

    has_alpaca_key = bool(os.environ.get("ALPACA_API_KEY") or os.environ.get("APCA_API_KEY_ID"))
    has_alpaca_secret = bool(os.environ.get("ALPACA_API_SECRET") or os.environ.get("APCA_API_SECRET_KEY"))
    if not (has_alpaca_key and has_alpaca_secret):
        print("ERROR: Alpaca news credentials are required.")
        print("Set ALPACA_API_KEY and ALPACA_API_SECRET, or APCA_API_KEY_ID and APCA_API_SECRET_KEY.")
        raise SystemExit(1)

    from datetime import datetime

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import Asset, TradingFee

    if IS_BACKTESTING:
        trading_fee = TradingFee(percent_fee=0.001)
        AlpacaNewsBuiltinStrategy.backtest(
            YahooDataBacktesting,
            backtesting_start=datetime(2025, 4, 21),
            backtesting_end=datetime(2025, 4, 28),
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            budget=10000,
            quiet_logs=False,
        )
