"""Bill Ackman / Pershing Square-inspired concentrated AI trading team example.

This example is inspired by public descriptions of concentrated, high-quality
large-cap investing. It is not affiliated with or endorsed by Bill Ackman,
Pershing Square, or related companies.

Set GEMINI_API_KEY, then run:
    python ai_trading_team_bill_ackman_concentrated.py
"""

import os
from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamBillAckmanConcentratedStrategy(Strategy):
    parameters = {
        "universe": ["GOOGL", "CMG", "HLT", "QSR", "UBER", "CP", "LOW", "MDLZ", "BKNG", "MSFT"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        model = os.environ.get("AI_TRADING_TEAM_MODEL", "gemini-3.1-flash-lite")
        self.agents.create(
            name="quality_researcher",
            model=model,
            allow_trading=False,
            system_prompt="Find the best high-quality, large-cap business with durable free cash flow and clear upside.",
        )
        self.agents.create(
            name="activist_bull",
            model=model,
            allow_trading=False,
            system_prompt="Argue for the most concentrated high-conviction position. Focus on catalysts, pricing power, and value creation.",
        )
        self.agents.create(
            name="short_seller_bear",
            model=model,
            allow_trading=False,
            system_prompt="Attack the thesis like a short seller. Find leverage, governance, accounting, competition, and valuation risk.",
        )
        self.agents.create(
            name="portfolio_manager",
            model=model,
            allow_trading=True,
            system_prompt="Build one concentrated position from the universe if the bull case survives. Use nearly all cash in the best idea.",
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
        }
        quality = self.agents["quality_researcher"].run(task_prompt="Pick the best high-quality large-cap candidate.", context=context)
        bull = self.agents["activist_bull"].run(task_prompt="Make the concentrated bull case.", context={**context, "quality": quality.summary})
        bear = self.agents["short_seller_bear"].run(
            task_prompt="Attack the concentrated thesis.",
            context={**context, "quality": quality.summary, "bull": bull.summary},
        )
        self.agents["portfolio_manager"].run(
            task_prompt="Sell anything that is not the surviving best idea, then buy the best stock with nearly all available cash.",
            context={**context, "quality": quality.summary, "bull": bull.summary, "bear": bear.summary},
        )


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    AITradingTeamBillAckmanConcentratedStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 7),
        datetime(2026, 5, 22),
    )
