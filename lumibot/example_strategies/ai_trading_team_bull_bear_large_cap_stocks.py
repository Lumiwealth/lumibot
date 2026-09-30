"""Bull vs Bear AI Stock Trading Bot.

Two AI agents argue about the biggest US stocks before any money moves. A
research agent ranks the stocks. A bull agent makes the case for buying and a
bear agent makes the case against, at the same time. A judge agent weighs both
sides and splits the account across the stocks that win the debate.
"""

from lumibot.strategies import Strategy


class AITradingTeamBullBearLargeCapStocksStrategy(Strategy):
    parameters = {
        "universe": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO", "COST", "JPM", "V", "LLY", "XOM"]
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=("Rank the stocks in the universe from recent prices, trends, and news. Do not trade."),
        )
        self.agents.create(
            name="bull",
            allow_trading=False,
            system_prompt=("Argue for buying the strongest stocks. Do not trade."),
        )
        self.agents.create(
            name="bear",
            allow_trading=False,
            system_prompt=("Argue the biggest risks in each stock. Do not trade."),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You are the judge. Weigh the bull and bear cases, pick the stocks that win the debate, and "
                "split the account across them. Sell the stocks that lose."
            ),
        )

    def on_trading_iteration(self):
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Rank the stocks.", context=facts)
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

        AITradingTeamBullBearLargeCapStocksStrategy.backtest(YahooDataBacktesting)
    else:
        AITradingTeamBullBearLargeCapStocksStrategy().run_live()
