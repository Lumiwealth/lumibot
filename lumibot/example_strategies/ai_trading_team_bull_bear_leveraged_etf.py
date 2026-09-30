"""TQQQ Strategy AI Trading Bot.

A bull AI and a bear AI debate the market, then the bot picks leveraged ETFs like
TQQQ (3x Nasdaq up) or SQQQ (3x Nasdaq down). A research agent ranks the ETFs.
The bull and bear agents argue both sides at the same time. A trading agent
judges the debate and holds only one direction per index.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AITradingTeamBullBearLeveragedETFStrategy(Strategy):
    parameters = {
        "universe": ["TQQQ", "SQQQ", "UPRO", "SPXU", "UDOW", "SDOW", "TNA", "TZA", "TECL", "TECS",
                     "SOXL", "SOXS", "FAS", "FAZ", "LABU", "LABD", "ERX", "ERY", "TMF", "TMV"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt="Rank the leveraged ETFs in the universe from recent prices and trends. Do not trade.",
        )
        self.agents.create(
            name="bull", allow_trading=False, system_prompt="Argue for the ETFs most likely to rise. Do not trade."
        )
        self.agents.create(
            name="bear", allow_trading=False, system_prompt="Argue the biggest risks in each ETF. Do not trade."
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You are the judge and the only agent that trades. Weigh the bull and bear cases and split "
                "the account across the winning ETFs from the universe, near 100% invested. Never hold an "
                "ETF and its opposite on the same index, like TQQQ with SQQQ; keep only the stronger side. "
                "When you switch sides, sell the old side before you buy."
            ),
        )

    def on_trading_iteration(self):
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Rank the ETFs.", context=facts)
        facts = {**facts, "research": research.summary}
        debate = self.agents.run_together(
            [("bull", "Make the bull case.", facts), ("bear", "Make the bear case.", facts)]
        )
        self.agents["trader"].run(
            task_prompt="Judge the debate and rebalance.",
            context={**facts, "bull": debate["bull"].summary, "bear": debate["bear"].summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBullBearLeveragedETFStrategy.backtest(YahooDataBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 16))
    else:
        AITradingTeamBullBearLeveragedETFStrategy().run_live()
