"""Citadel / Surveyor-inspired sector-pod AI trading team example.

This example is inspired by public descriptions of sector-specialist equities
teams. It is not affiliated with or endorsed by Citadel, Surveyor Capital, or
related companies.

Set GEMINI_API_KEY, then run:
    python ai_trading_team_citadel_sector_pods.py
"""

import os
from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamCitadelSectorPodsStrategy(Strategy):
    parameters = {
        "universe": ["XLK", "XLF", "XLV", "XLE", "XLY", "XLI", "XLP", "XLU", "XLB", "XLRE", "XLC"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        model = os.environ.get("AI_TRADING_TEAM_MODEL", "gemini-3.1-flash-lite")
        self.agents.create(
            name="cyclical_pod",
            model=model,
            allow_trading=False,
            system_prompt="Rank cyclical and growth sectors: technology, consumer discretionary, industrials, materials, energy, and communications.",
        )
        self.agents.create(
            name="defensive_pod",
            model=model,
            allow_trading=False,
            system_prompt="Rank defensive and rate-sensitive sectors: healthcare, staples, utilities, real estate, financials, and cash sensitivity.",
        )
        self.agents.create(
            name="risk_pod",
            model=model,
            allow_trading=False,
            system_prompt="Challenge the sector picks. Focus on factor crowding, drawdown risk, macro exposure, and reversal risk.",
        )
        self.agents.create(
            name="portfolio_manager",
            model=model,
            allow_trading=True,
            system_prompt="Choose the best sector ETF from the universe. Use nearly all cash and rotate decisively.",
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
        }
        cyclical = self.agents["cyclical_pod"].run(task_prompt="Pick the best cyclical or growth sector ETF.", context=context)
        defensive = self.agents["defensive_pod"].run(task_prompt="Pick the best defensive or rate-sensitive sector ETF.", context=context)
        risk = self.agents["risk_pod"].run(
            task_prompt="Compare both sector picks and identify the biggest risks.",
            context={**context, "cyclical": cyclical.summary, "defensive": defensive.summary},
        )
        self.agents["portfolio_manager"].run(
            task_prompt="Sell anything that is not the strongest sector ETF, then buy the strongest ETF with nearly all available cash.",
            context={**context, "cyclical": cyclical.summary, "defensive": defensive.summary, "risk": risk.summary},
        )


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    AITradingTeamCitadelSectorPodsStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 7),
        datetime(2026, 5, 22),
    )
