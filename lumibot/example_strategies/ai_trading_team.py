"""Copy-paste AI trading team example.

Set GEMINI_API_KEY, then run:
    python ai_trading_team.py
"""

from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamStrategy(Strategy):
    parameters = {
        "universe": ["TQQQ", "SQQQ", "SOXL", "SOXS", "UPRO", "SPXU", "TECL", "TECS"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        # Three agents debate. The trader agent is the only one allowed to place orders.
        self.agents.create(
            name="researcher",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Rank the ETFs by upside. Be direct.",
        )
        self.agents.create(
            name="bull",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Argue for the strongest money-making trade.",
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
            system_prompt="Buy one ETF from the universe aggressively. Use nearly all cash.",
        )

    def on_trading_iteration(self):
        # Keep the handoff tiny so the example is easy to read.
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
