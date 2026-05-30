"""Ray Dalio / Bridgewater-inspired idea-meritocracy AI trading team example.

Set GEMINI_API_KEY, then run:
    python ai_trading_team_ray_dalio_idea_meritocracy.py
"""

from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamRayDalioIdeaMeritocracyStrategy(Strategy):
    parameters = {
        "universe": ["SPY", "QQQ", "IWM", "TLT", "IEF", "TIP", "GLD", "DBC", "VNQ", "UUP", "FXI", "EEM"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="growth_agent",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Argue which ETFs win if growth improves. Be direct and expose weak assumptions.",
        )
        self.agents.create(
            name="inflation_agent",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Argue which ETFs win or lose if inflation and rates surprise. Be direct.",
        )
        self.agents.create(
            name="debt_liquidity_agent",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Argue from debt, liquidity, currency, and policy pressure. Be direct.",
        )
        self.agents.create(
            name="thoughtful_disagreement",
            model="gemini-3.1-flash-lite",
            allow_trading=False,
            system_prompt="Challenge all views with thoughtful disagreement. Identify the best idea after stress testing.",
        )
        self.agents.create(
            name="trader",
            model="gemini-3.1-flash-lite",
            allow_trading=True,
            system_prompt="Choose the best macro idea after the disagreement. Buy one ETF aggressively with nearly all cash.",
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
        }
        growth = self.agents["growth_agent"].run(task_prompt="Pick the strongest ETF from a growth-regime view.", context=context)
        inflation = self.agents["inflation_agent"].run(task_prompt="Pick the strongest ETF from an inflation-and-rates view.", context=context)
        liquidity = self.agents["debt_liquidity_agent"].run(task_prompt="Pick the strongest ETF from a debt-and-liquidity view.", context=context)
        disagreement = self.agents["thoughtful_disagreement"].run(
            task_prompt="Challenge the growth, inflation, and liquidity views. Pick the best idea.",
            context={**context, "growth": growth.summary, "inflation": inflation.summary, "liquidity": liquidity.summary},
        )
        self.agents["trader"].run(
            task_prompt="Sell anything that is not the best idea, then buy the best ETF with nearly all available cash.",
            context={**context, "growth": growth.summary, "inflation": inflation.summary, "liquidity": liquidity.summary, "disagreement": disagreement.summary},
        )


if __name__ == "__main__":
    from lumibot.backtesting import YahooDataBacktesting

    AITradingTeamRayDalioIdeaMeritocracyStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 7),
        datetime(2026, 5, 22),
    )
