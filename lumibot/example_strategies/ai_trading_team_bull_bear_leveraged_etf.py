"""TQQQ Strategy AI Trading Bot.

A bull AI and a bear AI debate the market, then the bot picks leveraged ETFs like
TQQQ (3x the Nasdaq-100 up) or SQQQ (3x down). A research agent ranks the ETFs,
the bull and bear agents argue at the same time, and a judge agent trades,
holding only one side of each index.
"""

from lumibot.strategies import Strategy


class AITradingTeamBullBearLeveragedETFStrategy(Strategy):
    parameters = {
        "universe": ["TQQQ", "SQQQ", "UPRO", "SPXU", "UDOW", "SDOW", "TNA", "TZA", "SOXL", "SOXS", "TMF", "TMV"]
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=("Rank the ETFs in the universe from recent prices and trends. Do not trade."),
        )
        self.agents.create(
            name="bull",
            allow_trading=False,
            system_prompt=("Argue for the ETFs most likely to rise. Do not trade."),
        )
        self.agents.create(
            name="bear",
            allow_trading=False,
            system_prompt=("Argue the biggest risks in each ETF. Do not trade."),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You are the judge. Weigh the bull and bear cases and split the account across the winning "
                "ETFs. Never hold an ETF and its opposite on the same index, like TQQQ and SQQQ. Sell the old "
                "side before you switch."
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
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBullBearLeveragedETFStrategy.backtest(YahooDataBacktesting)
    else:
        AITradingTeamBullBearLeveragedETFStrategy().run_live()
