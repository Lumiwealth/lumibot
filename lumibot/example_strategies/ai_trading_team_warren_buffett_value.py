"""Warren Buffett-inspired annual-report value AI trading team example.

This example is inspired by public Berkshire Hathaway shareholder letters and
value-investing principles. It is not affiliated with or endorsed by Warren
Buffett, Berkshire Hathaway, or related companies.

Set GEMINI_API_KEY plus Alpaca credentials, then run paper trading:
    python ai_trading_team_warren_buffett_value.py

Set IS_BACKTESTING=True in the runner to run the historical example instead.
"""

import os
from datetime import datetime

from lumibot.strategies.strategy import Strategy


class AITradingTeamWarrenBuffettValueStrategy(Strategy):
    parameters = {
        "universe": ["AAPL", "MSFT", "GOOGL", "COST", "V", "MA", "KO", "AXP", "JPM", "PG"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        model = os.environ.get("AI_TRADING_TEAM_MODEL", "gemini-3.1-flash-lite")
        self.agents.create(
            name="annual_report_reader",
            model=model,
            allow_trading=False,
            system_prompt="Find the best business quality from filings, fundamentals, cash flow, balance sheet strength, and durability.",
        )
        self.agents.create(
            name="valuation_skeptic",
            model=model,
            allow_trading=False,
            system_prompt="Challenge the business-quality case. Require a margin of safety and reject weak or overpriced ideas.",
        )
        self.agents.create(
            name="portfolio_manager",
            model=model,
            allow_trading=True,
            system_prompt="Buy the best long-term compounder from the universe when quality and margin of safety are acceptable. Use nearly all cash.",
        )

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
        }
        report = self.agents["annual_report_reader"].run(task_prompt="Pick the highest-quality business.", context=context)
        skeptic = self.agents["valuation_skeptic"].run(
            task_prompt="Challenge the valuation and business-quality case. Require margin of safety.",
            context={**context, "report": report.summary},
        )
        self.agents["portfolio_manager"].run(
            task_prompt="Sell anything that is not the best long-term compounder, then buy the best stock with nearly all available cash.",
            context={**context, "report": report.summary, "skeptic": skeptic.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = False

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamWarrenBuffettValueStrategy.backtest(
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
        strategy = AITradingTeamWarrenBuffettValueStrategy(broker=broker)

        trader = Trader()
        trader.add_strategy(strategy)
        trader.run_all()
