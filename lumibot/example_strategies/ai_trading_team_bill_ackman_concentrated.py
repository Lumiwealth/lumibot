"""Bill Ackman-inspired concentrated team.

This example is inspired by public descriptions of concentrated large-cap investing.
It is not affiliated with or endorsed by Bill Ackman or Pershing Square.

Python only creates the agents and runs them. Bull and bear run together.
The trader is the only order path.
"""

import os
from datetime import datetime

from lumibot.example_strategies.agent_cycle import add_agent, run_cycle, trader_prompt
from lumibot.strategies.strategy import Strategy

_BOOK = (
    "Own a concentrated book from this universe only. A few names can take most of the "
    "account when the interpreter keeps them. Weights still sum near 100%."
)
_EXIT = (
    "Sell a holding with the order tool when the bear case wins that name, or when the "
    "position was opened on an earlier session and today's weights no longer include it."
)


class AITradingTeamBillAckmanConcentratedStrategy(Strategy):
    parameters = {
        "universe": ["GOOGL", "CMG", "HLT", "QSR", "UBER", "CP", "LOW", "MDLZ", "BKNG", "MSFT"],
        "max_position_pct": 1.0,
    }

    def initialize(self):
        self.sleeptime = "1D"
        add_agent(
            self,
            "researcher",
            "Find high-quality large-cap businesses with durable cash flow. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            "Argue the concentrated bull case from the research only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Attack leverage, governance, competition, and valuation from the research only. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            "Read both cases. Keep only names that survive the attack. Weight only symbols in the universe. Assign concentrated weights. Do not submit orders.",
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
            research_task="Pick the best high-quality candidates.",
            bull_task="Make the bull case from the research.",
            bear_task="Make the bear case from the research.",
            interpret_task="Keep the names that survive and assign concentrated weights.",
            trade_task="Apply the interpreter weights. Size from the account. Exit any name that left the book.",
        )


if __name__ == "__main__":
    IS_BACKTESTING = False

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBillAckmanConcentratedStrategy.backtest(
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
        strategy = AITradingTeamBillAckmanConcentratedStrategy(broker=broker)

        trader = Trader()
        trader.add_strategy(strategy)
        trader.run_all()
