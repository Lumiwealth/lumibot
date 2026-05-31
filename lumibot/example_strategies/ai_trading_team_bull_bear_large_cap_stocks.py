"""Bull/bear large-cap stock AI trading team example.

Set GEMINI_API_KEY plus Alpaca credentials, then run paper trading:
    python ai_trading_team_bull_bear_large_cap_stocks.py

Set IS_BACKTESTING=True in the runner to run the historical example instead.
"""

import os
from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamBullBearLargeCapStocksStrategy(Strategy):
    parameters = {
        "universe": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO", "COST", "JPM", "V", "MA", "LLY", "UNH", "XOM"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        model = os.environ.get("AI_TRADING_TEAM_MODEL", "gemini-3.1-flash-lite")
        self.agents.create(
            name="researcher",
            model=model,
            allow_trading=False,
            system_prompt="Rank the large-cap stocks by upside. Be direct.",
        )
        self.agents.create(
            name="bull",
            model=model,
            allow_trading=False,
            system_prompt="Argue for the strongest money-making stock.",
        )
        self.agents.create(
            name="bear",
            model=model,
            allow_trading=False,
            system_prompt="Point out the biggest risk, briefly.",
        )
        self.agents.create(
            name="trader",
            model=model,
            allow_trading=True,
            system_prompt="Buy one stock from the universe aggressively. Use nearly all cash.",
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
        }
        research = self.agents["researcher"].run(task_prompt="Pick the strongest stock.", context=context)
        bull = self.agents["bull"].run(task_prompt="Make the bull case.", context={**context, "research": research.summary})
        bear = self.agents["bear"].run(task_prompt="Make the bear case.", context={**context, "research": research.summary, "bull": bull.summary})
        self.agents["trader"].run(
            task_prompt="Sell anything that is not the pick, then buy the best stock with nearly all available cash.",
            context={**context, "research": research.summary, "bull": bull.summary, "bear": bear.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = False

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBullBearLargeCapStocksStrategy.backtest(
            YahooDataBacktesting,
            datetime(2026, 4, 7),
            datetime(2026, 5, 22),
        )
    else:
        from lumibot.brokers import Alpaca
        from lumibot.traders import Trader

        ALPACA_CONFIG = {
            "API_KEY": os.environ["ALPACA_API_KEY"],
            "API_SECRET": os.environ["ALPACA_API_SECRET"],
            "PAPER": os.environ.get("ALPACA_IS_PAPER", "true").lower() != "false",
        }

        broker = Alpaca(ALPACA_CONFIG)
        strategy = AITradingTeamBullBearLargeCapStocksStrategy(broker=broker)

        trader = Trader()
        trader.add_strategy(strategy)
        trader.run_all()
