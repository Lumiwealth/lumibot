"""Momentum and News AI Trading Bot.

Holds TQQQ when its price is rising and the news is not bad, and SHV, a short-term
Treasury fund, otherwise. One AI agent checks the trend and the news each day.
"""

from lumibot.strategies import Strategy


class MomentumAllocatorStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Hold either TQQQ or SHV with the whole account. Each day, check TQQQ's recent price trend "
                "and the latest market news. If the trend is up and the news is not very negative, hold TQQQ. "
                "Otherwise hold SHV. Sell one before buying the other."
            ),
        )

    def on_trading_iteration(self):
        self.agents["trader"].run(task_prompt="Check momentum and news, then hold TQQQ or SHV.")


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        MomentumAllocatorStrategy.backtest(YahooDataBacktesting)
    else:
        MomentumAllocatorStrategy().run_live()
