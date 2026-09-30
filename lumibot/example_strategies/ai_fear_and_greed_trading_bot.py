"""Fear and Greed Index Trading Bot.

Buys the S&P 500 when investors are scared and sells when they are greedy. A
research agent opens a real web browser and reads CNN's Fear & Greed Index. A
trading agent then sets how much of the account sits in SPY: more on fear, less
on greed.
"""

from lumibot.strategies import Strategy


class FearAndGreedTradingBot(Strategy):
    parameters = {"symbol": "SPY"}

    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="researcher",
            allow_trading=False,
            allow_network=True,
            system_prompt=(
                "Find the CNN Fear & Greed Index score from 0 to 100 for the most recent day before today. "
                "Use a web browser. Today's score is at https://www.cnn.com/markets/fear-and-greed. Past "
                "scores are listed day by day at https://production.dataviz.cnn.io/index/fearandgreed/graphdata/ "
                "followed by a start date, such as 2026-01-01. Report the score and its date. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Set how much of the account is in the symbol from the Fear & Greed score: 100% below 25, 75% "
                "from 25 to 44, 50% from 45 to 55, 25% from 56 to 75, and 0% above 75. Keep the rest in cash. "
                "If there is no score from the last few days, do nothing."
            ),
        )

    def on_trading_iteration(self):
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(task_prompt="Read the Fear & Greed Index.", context=facts)
        self.agents["trader"].run(
            task_prompt="Set the position from the score.", context={**facts, "research": research.summary}
        )


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        FearAndGreedTradingBot.backtest(YahooDataBacktesting)
    else:
        FearAndGreedTradingBot().run_live()
