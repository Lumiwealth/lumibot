"""Bill Ackman Portfolio AI Trading Bot.

Invests the way Bill Ackman describes his style: own just a few simple,
high-quality companies and put real money behind them. A research agent studies
each company and attacks its weak spots. A trading agent holds the 3 to 5 best,
with most of the account in the top ideas.
Not affiliated with or endorsed by Bill Ackman or Pershing Square.
"""

from datetime import datetime

from lumibot.strategies import Strategy


class AITradingTeamBillAckmanConcentratedStrategy(Strategy):
    parameters = {"universe": ["GOOGL", "CMG", "HLT", "QSR", "UBER", "CP", "LOW", "MDLZ", "BKNG", "MSFT"]}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            system_prompt=(
                "You research companies the way Bill Ackman does. For each stock in the universe, check its "
                "financial statements and recent price. Look for simple, predictable businesses that make "
                "lots of cash. Then attack each one: too much debt, weak management, strong rivals, or a "
                "price that is too high. Rank the stocks that survive. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "You invest like Bill Ackman. Own the 3 to 5 best stocks from the research, with bigger "
                "weights on the best ideas, near 100% of the account. Only buy stocks in the universe. "
                "Sell a stock when the research no longer ranks it in the top 5."
            ),
        )

    def on_trading_iteration(self):
        facts = {"universe": self.parameters["universe"]}
        research = self.agents["researcher"].run(task_prompt="Rank the universe.", context=facts)
        self.agents["trader"].run(
            task_prompt="Hold the best few ideas.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = True  # Set to False to trade with the broker in your .env file

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AITradingTeamBillAckmanConcentratedStrategy.backtest(YahooDataBacktesting, datetime(2026, 1, 5), datetime(2026, 1, 16))
    else:
        AITradingTeamBillAckmanConcentratedStrategy().run_live()
