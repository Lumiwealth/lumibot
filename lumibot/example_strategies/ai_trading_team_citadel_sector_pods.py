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
            name="technology_pod",
            model=model,
            allow_trading=False,
            system_prompt="Rank technology and communications sector ETFs. Use exact symbols from the universe only. Do not invent symbols or table names.",
        )
        self.agents.create(
            name="financials_pod",
            model=model,
            allow_trading=False,
            system_prompt="Rank financial and rate-sensitive sector ETFs. Use exact symbols from the universe only. Do not invent symbols or table names.",
        )
        self.agents.create(
            name="healthcare_pod",
            model=model,
            allow_trading=False,
            system_prompt="Rank healthcare and defensive growth sector ETFs. Use exact symbols from the universe only. Do not invent symbols or table names.",
        )
        self.agents.create(
            name="energy_pod",
            model=model,
            allow_trading=False,
            system_prompt="Rank energy and commodity-sensitive sector ETFs. Use exact symbols from the universe only. Do not invent symbols or table names.",
        )
        self.agents.create(
            name="consumer_pod",
            model=model,
            allow_trading=False,
            system_prompt="Rank consumer discretionary, staples, and housing-sensitive sector ETFs. Use exact symbols from the universe only. Do not invent symbols or table names.",
        )
        self.agents.create(
            name="risk_manager",
            model=model,
            allow_trading=False,
            system_prompt="Compare the pod picks. Challenge crowding, factor exposure, drawdown risk, and reversal risk.",
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
        technology = self.agents["technology_pod"].run(task_prompt="Pick the best technology or communications ETF.", context=context)
        financials = self.agents["financials_pod"].run(task_prompt="Pick the best financial or rate-sensitive ETF.", context=context)
        healthcare = self.agents["healthcare_pod"].run(task_prompt="Pick the best healthcare or defensive-growth ETF.", context=context)
        energy = self.agents["energy_pod"].run(task_prompt="Pick the best energy or commodity-sensitive ETF.", context=context)
        consumer = self.agents["consumer_pod"].run(task_prompt="Pick the best consumer or housing-sensitive ETF.", context=context)
        risk = self.agents["risk_manager"].run(
            task_prompt="Compare all pod picks and identify the biggest risks.",
            context={**context, "technology": technology.summary, "financials": financials.summary, "healthcare": healthcare.summary, "energy": energy.summary, "consumer": consumer.summary},
        )
        self.agents["portfolio_manager"].run(
            task_prompt="Sell anything that is not the strongest sector ETF, then buy the strongest ETF with nearly all available cash.",
            context={**context, "technology": technology.summary, "financials": financials.summary, "healthcare": healthcare.summary, "energy": energy.summary, "consumer": consumer.summary, "risk": risk.summary},
        )


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    AITradingTeamCitadelSectorPodsStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 7),
        datetime(2026, 5, 22),
    )
