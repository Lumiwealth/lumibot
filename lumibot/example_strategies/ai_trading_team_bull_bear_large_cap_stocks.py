"""Bull/bear large-cap stock AI trading team example.

Set GEMINI_API_KEY, then run:
    python ai_trading_team_bull_bear_large_cap_stocks.py
"""

from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamBullBearLargeCapStocksStrategy(Strategy):
    parameters = {
        "universe": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO", "COST", "JPM", "V", "MA", "LLY", "UNH", "XOM"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Rank the large-cap stocks by upside. Be direct.",
        )
        self.agents.create(
            name="bull",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Argue for the strongest money-making stock.",
        )
        self.agents.create(
            name="bear",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Point out the biggest risk, briefly.",
        )
        self.agents.create(
            name="trader",
            model="gemini-3.1-flash-lite",
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
    from lumibot.backtesting import YahooDataBacktesting

    AITradingTeamBullBearLargeCapStocksStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 7),
        datetime(2026, 5, 22),
    )
