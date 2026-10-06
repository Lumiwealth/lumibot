"""News Sentiment AI Trading Bot.

Buys well-known stocks with strong good news, like earnings beats, upgrades, or
big deals. One AI agent reads today's stock news and invests the whole account in
the 2 to 4 best stories, or in SHV when the news is weak.
"""

from lumibot.strategies import Strategy


class NewsSentimentStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Read today's stock market news. Buy the 2 to 4 well-known US stocks with the strongest good "
                "news, such as earnings beats, upgrades, product launches, or deals, and split the account "
                "between them. Skip small or penny stocks. If the news is weak or negative, hold SHV instead. "
                "Stay fully invested."
            ),
        )

    def on_trading_iteration(self):
        self.agents["trader"].run(task_prompt="Read today's news and invest.")


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        NewsSentimentStrategy.backtest(YahooDataBacktesting)
    else:
        NewsSentimentStrategy().run_live()
