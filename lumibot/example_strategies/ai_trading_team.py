"""Copy-paste AI trading team example.

Set GEMINI_API_KEY, then run:
    python ai_trading_team.py
"""

import os
from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamStrategy(Strategy):
    parameters = {
        # Bull/bear leveraged ETFs across broad indexes, sectors, rates, and gold miners.
        "universe": ["TQQQ", "SQQQ", "UPRO", "SPXU", "UDOW", "SDOW", "TNA", "TZA", "TECL", "TECS", "SOXL", "SOXS", "WEBL", "WEBS", "FAS", "FAZ", "LABU", "LABD", "ERX", "ERY", "GUSH", "DRIP", "DRN", "DRV", "TMF", "TMV", "NUGT", "DUST"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        model = os.environ.get("AI_TRADING_TEAM_MODEL", "gemini-3.1-flash-lite")
        # The first three agents are read-only. They can reason, but cannot trade.
        self.agents.create(
            name="researcher",
            model=model,
            allow_trading=False,
            system_prompt="Rank the ETFs by upside. Be direct.",
        )
        self.agents.create(
            name="bull",
            model=model,
            allow_trading=False,
            system_prompt="Argue for the strongest money-making trade.",
        )
        self.agents.create(
            name="bear",
            model=model,
            allow_trading=False,
            system_prompt="Point out the biggest risk, briefly.",
        )
        # Only this final agent can submit orders through Lumibot.
        self.agents.create(
            name="trader",
            model=model,
            allow_trading=True,
            system_prompt="Buy one ETF from the universe aggressively. Use nearly all cash.",
        )

    def on_trading_iteration(self):
        # Each trading day, pass the same market context through the team.
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
        }
        research = self.agents["researcher"].run(task_prompt="Pick the strongest ETF.", context=context)
        bull = self.agents["bull"].run(task_prompt="Make the bull case.", context={**context, "research": research.summary})
        bear = self.agents["bear"].run(task_prompt="Make the bear case.", context={**context, "research": research.summary, "bull": bull.summary})
        self.agents["trader"].run(
            task_prompt="Sell anything that is not the pick, then buy the best ETF with nearly all available cash.",
            context={**context, "research": research.summary, "bull": bull.summary, "bear": bear.summary},
        )


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    AITradingTeamStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 7),
        datetime(2026, 5, 22),
    )
