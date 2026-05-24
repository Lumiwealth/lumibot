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
        for name, prompt, can_trade in [
            ("researcher", "Rank the ETFs by upside. Be direct.", False),
            ("bull", "Argue for the strongest money-making trade.", False),
            ("bear", "Point out the biggest risk, briefly.", False),
            ("trader", "Buy one ETF from the universe aggressively. Use nearly all cash.", True),
        ]:
            self.agents.create(
                name=name,
                model="gemini-3.1-flash-lite",
                allow_trading=can_trade,
                system_prompt=prompt,
            )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
        }
        research = self.agents["researcher"].run("Pick the strongest ETF.", context=context)
        bull = self.agents["bull"].run("Make the bull case.", context={**context, "research": research.summary})
        bear = self.agents["bear"].run("Make the bear case.", context={**context, "research": research.summary, "bull": bull.summary})
        self.agents["trader"].run(
            "Sell anything that is not the pick, then buy the best ETF with nearly all available cash.",
            context={**context, "research": research.summary, "bull": bull.summary, "bear": bear.summary},
        )


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    AITradingTeamStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 7),
        datetime(2026, 5, 22),
    )
