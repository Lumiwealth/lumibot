"""Bull and bear leveraged ETF team.

Python only creates the agents and runs them. Bull and bear run together.
The interpreter reads both. The trader is the only order path.
"""

import os
from datetime import datetime

from lumibot.example_strategies.agent_cycle import add_agent, run_cycle, trader_prompt
from lumibot.strategies.strategy import Strategy

_BOOK = (
    "Own the leveraged ETFs the interpreter ranks, from this universe only. "
    "Respect the long or inverse direction. Split the account by the interpreter weights."
)
_EXIT = (
    "If a position was opened on an earlier session, sell it with the order tool "
    "before any new buy. Then open the new book if the interpreter still wants it."
)


class AITradingTeamBullBearLeveragedETFStrategy(Strategy):
    parameters = {
        "universe": [
            "TQQQ",
            "SQQQ",
            "UPRO",
            "SPXU",
            "UDOW",
            "SDOW",
            "TNA",
            "TZA",
            "TECL",
            "TECS",
            "SOXL",
            "SOXS",
            "WEBL",
            "WEBS",
            "FAS",
            "FAZ",
            "LABU",
            "LABD",
            "ERX",
            "ERY",
            "GUSH",
            "DRIP",
            "DRN",
            "DRV",
            "TMF",
            "TMV",
            "NUGT",
            "DUST",
        ],
        "max_position_pct": 1.0,
    }

    def initialize(self):
        self.sleeptime = "1D"
        add_agent(
            self,
            "researcher",
            "Rank the leveraged ETF universe from point-in-time prices. Note which names are inverse. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bull",
            "Argue the long case from the research only. Do not read the bear case. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "bear",
            "Argue the risk case from the research only. Do not read the bull case. Do not submit orders.",
            allow_trading=False,
        )
        add_agent(
            self,
            "interpreter",
            "Read the bull case and the bear case. Weight only symbols in the universe. Assign weights that sum near 100% of the account. Do not submit orders.",
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
            research_task="Rank the ETF universe for this session.",
            bull_task="Make the bull case from the research.",
            bear_task="Make the bear case from the research.",
            interpret_task="Turn the bull case and the bear case into account weights.",
            trade_task="Apply the interpreter weights. Size from the account. Exit yesterday's book first if it is still open.",
        )


if __name__ == "__main__":
    IS_BACKTESTING = False

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBullBearLeveragedETFStrategy.backtest(
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
        strategy = AITradingTeamBullBearLeveragedETFStrategy(broker=broker)

        trader = Trader()
        trader.add_strategy(strategy)
        trader.run_all()


AITradingTeamStrategy = AITradingTeamBullBearLeveragedETFStrategy
