"""Ray Dalio / Bridgewater-inspired idea-meritocracy AI trading team example.

Set GEMINI_API_KEY plus Alpaca credentials, then run paper trading:
    python ai_trading_team_ray_dalio_idea_meritocracy.py

Set IS_BACKTESTING=True in the runner to run the historical example instead.
"""

import os
from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamRayDalioIdeaMeritocracyStrategy(Strategy):
    parameters = {
        "universe": ["SPY", "QQQ", "IWM", "TLT", "IEF", "TIP", "GLD", "DBC", "VNQ", "UUP", "FXI", "EEM"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        model = os.environ.get("AI_TRADING_TEAM_MODEL", "gemini-3.1-flash-lite")
        self.agents.create(
            name="growth_agent",
            model=model,
            allow_trading=False,
            system_prompt="Argue which ETFs win if growth improves. Be direct and expose weak assumptions.",
        )
        self.agents.create(
            name="inflation_agent",
            model=model,
            allow_trading=False,
            system_prompt="Argue which ETFs win or lose if inflation and rates surprise. Be direct.",
        )
        self.agents.create(
            name="debt_liquidity_agent",
            model=model,
            allow_trading=False,
            system_prompt="Argue from debt, liquidity, currency, and policy pressure. Be direct.",
        )
        self.agents.create(
            name="thoughtful_disagreement",
            model=model,
            allow_trading=False,
            system_prompt="Challenge all views with thoughtful disagreement. Identify the best idea after stress testing.",
        )
        self.agents.create(
            name="trader",
            model=model,
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
    IS_BACKTESTING = False

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamRayDalioIdeaMeritocracyStrategy.backtest(
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
        strategy = AITradingTeamRayDalioIdeaMeritocracyStrategy(broker=broker)

        trader = Trader()
        trader.add_strategy(strategy)
        trader.run_all()
