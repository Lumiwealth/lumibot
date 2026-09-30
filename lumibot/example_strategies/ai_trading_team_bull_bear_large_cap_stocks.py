"""Bull vs Bear AI Stock Trading Bot.

Two AI agents argue about the biggest US stocks before any money moves. A
research agent ranks the stocks. A bull agent makes the case for buying and a
bear agent makes the case against, at the same time. A trading agent weighs both
sides and splits the account across the stocks that win the debate.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AITradingTeamBullBearLargeCapStocksStrategy(Strategy):
    parameters = {
        "universe": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "AVGO",
                     "COST", "JPM", "V", "MA", "LLY", "UNH", "XOM"],
    }

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt="Rank the universe from recent prices, trends, and news. Do not trade.",
        )
        self.agents.create(
            name="bull", allow_trading=False, system_prompt="Argue for buying the strongest stocks. Do not trade."
        )
        self.agents.create(
            name="bear", allow_trading=False, system_prompt="Argue the biggest risks in each stock. Do not trade."
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You are the judge and the only agent that trades. Weigh the bull and bear cases, pick the "
                "stocks from the universe that win the debate, and split the account across them, near "
                "100% invested. Sell stocks that lost the debate."
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
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBullBearLargeCapStocksStrategy.backtest(YahooDataBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 16))
    else:
        AITradingTeamBullBearLargeCapStocksStrategy().run_live()
