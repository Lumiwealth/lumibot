"""Warren Buffett-inspired value team.

This example is inspired by public Berkshire Hathaway shareholder letters.
It is not affiliated with or endorsed by Warren Buffett or Berkshire Hathaway.

Python only creates the agents and runs them. Bull and bear run together.
The trader is the only order path.
"""

import os
from datetime import datetime

from lumibot.example_strategies.agent_cycle import add_agent, run_cycle, trader_prompt
from lumibot.strategies.strategy import Strategy

_BOOK = (
    "Own the businesses the interpreter accepts, from this universe only, when quality "
    "and a margin of safety both survive the bear case. Split the account by those weights."
)
_EXIT = (
    "Sell a holding with the order tool when the interpreter says the margin of safety is gone, "
    "or when the position was opened on an earlier session and today's weights no longer include it."
)


class AITradingTeamWarrenBuffettValueStrategy(Strategy):
    parameters = {
        "universe": ["AAPL", "MSFT", "GOOGL", "COST", "V", "MA", "KO", "AXP", "JPM", "PG"],
        "max_position_pct": 1.0,
    }

    def initialize(self):
        self.sleeptime = "1D"
        add_agent(
            self,
            "researcher",
            "Read public filings and fundamentals. Rank business quality. For each name, compute "
            "valuation as of the current date from the latest statements and the current price: "
            "earnings yield, free cash flow yield, P/E, and net debt. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            "Argue the quality and compounding case from the research only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Challenge price, debt, and the margin of safety from the research only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            "Read both cases. Do not require a full intrinsic value model. Judge the margin of "
            "safety by comparing earnings yield, free cash flow yield, and net debt across the "
            "universe, and keep the names where quality and price both hold best. "
            "Weight only symbols in the universe, and make the weights sum near 100%. "
            "Do not submit orders.",
            allow_trading=False,
        )
        add_agent(self, "trader", trader_prompt(book_rule=_BOOK, exit_rule=_EXIT), allow_trading=True)

    def on_trading_iteration(self):
        context = {
            "date": self.get_datetime().date().isoformat(),
            "universe": self.parameters["universe"],
            "max_position_pct": self.parameters["max_position_pct"],
        }
        run_cycle(
            self,
            context,
            researcher="researcher",
            bull="bull",
            bear="bear",
            interpreter="interpreter",
            trader="trader",
            research_task="Pick the highest-quality businesses in the universe.",
            bull_task="Make the bull case from the research.",
            bear_task="Make the bear case from the research.",
            interpret_task="Decide which names clear a margin of safety and assign weights.",
            trade_task="Apply the interpreter weights. Size from the account. Exit any name that left the book.",
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
