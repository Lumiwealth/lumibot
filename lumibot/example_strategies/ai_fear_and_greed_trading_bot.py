"""Fear and Greed Index Trading Bot.

Buys the S&P 500 when investors are scared and sells when they are greedy. A
research agent opens a real web browser, goes to CNN's Fear & Greed Index page,
and reads today's score. A trading agent then sets how much of the account sits
in SPY: more on fear, less on greed.
"""

from datetime import datetime

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
                "Open a browser session, go to https://www.cnn.com/markets/fear-and-greed, and read the "
                "Fear & Greed Index score (0 to 100), its label, and the 'Last updated' date. Report all "
                "three. If the page date is not today's date, say the score is not from today. Do not trade."
            ),
        )
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Trade only the symbol in the context. Set its weight from today's Fear & Greed score: "
                "below 25 (extreme fear) 100%, 25 to 44 (fear) 75%, 45 to 55 (neutral) 50%, "
                "56 to 75 (greed) 25%, above 75 (extreme greed) 0%. Keep the rest in cash. "
                "If the score is missing or not from today, do nothing."
            ),
        )

    def on_trading_iteration(self):
        facts = {"symbol": self.parameters["symbol"]}
        research = self.agents["researcher"].run(
            task_prompt="Read today's Fear & Greed Index.", context=facts
        )
        self.agents["trader"].run(
            task_prompt="Set the position from the score.",
            context={**facts, "research": research.summary},
        )


if __name__ == "__main__":
    IS_BACKTESTING = False  # The website only shows today's score, so run this live or on paper

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        FearAndGreedTradingBot.backtest(YahooDataBacktesting, datetime(2026, 9, 21), datetime(2026, 9, 26))
    else:
        FearAndGreedTradingBot().run_live()
