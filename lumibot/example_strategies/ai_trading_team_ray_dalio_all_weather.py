"""Ray Dalio / Bridgewater-inspired all-weather AI trading team example.

This example is inspired by public descriptions of All Weather and risk
balancing. It is not affiliated with or endorsed by Ray Dalio or Bridgewater.

Set GEMINI_API_KEY, then run:
    python ai_trading_team_ray_dalio_all_weather.py
"""

import os
from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamRayDalioAllWeatherStrategy(Strategy):
    parameters = {
        "universe": ["SPY", "QQQ", "TLT", "IEF", "TIP", "GLD", "DBC", "VNQ", "SHY"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        model = os.environ.get("AI_TRADING_TEAM_MODEL", "gemini-3.1-flash-lite")
        self.agents.create(
            name="growth_regime",
            model=model,
            allow_trading=False,
            system_prompt="Classify the growth regime and rank assets that benefit if growth beats expectations.",
        )
        self.agents.create(
            name="inflation_regime",
            model=model,
            allow_trading=False,
            system_prompt="Classify inflation and rate risk. Favor assets that diversify equity and bond shocks.",
        )
        self.agents.create(
            name="risk_balancer",
            model=model,
            allow_trading=False,
            system_prompt="Balance the cases. Prefer resilient allocation across growth, inflation, rates, and liquidity regimes.",
        )
        self.agents.create(
            name="portfolio_manager",
            model=model,
            allow_trading=True,
            system_prompt="Allocate to the best one or two ETFs from the universe. Use nearly all cash and avoid tiny positions.",
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
        }
        growth = self.agents["growth_regime"].run(task_prompt="Rank the universe for the current growth regime.", context=context)
        inflation = self.agents["inflation_regime"].run(
            task_prompt="Rank the universe for inflation, rates, and macro shock protection.",
            context={**context, "growth_view": growth.summary},
        )
        balance = self.agents["risk_balancer"].run(
            task_prompt="Choose the most resilient allocation from the two regime views.",
            context={**context, "growth_view": growth.summary, "inflation_view": inflation.summary},
        )
        self.agents["portfolio_manager"].run(
            task_prompt="Sell anything outside the final allocation, then buy the selected ETF or ETFs with nearly all available cash.",
            context={**context, "growth_view": growth.summary, "inflation_view": inflation.summary, "balanced_view": balance.summary},
        )


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    AITradingTeamRayDalioAllWeatherStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 7),
        datetime(2026, 5, 22),
    )
