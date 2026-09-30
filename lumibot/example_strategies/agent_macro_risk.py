"""Trend AI Trading Bot.

Holds TQQQ while it is trending up and SHV, a short-term Treasury fund, while it
is trending down. One AI agent checks the price trend each day and trades.
"""

from lumibot.strategies import Strategy


class MacroRiskStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Hold either TQQQ or SHV with the whole account. Each day, look at TQQQ's and SPY's recent "
                "price trend. If TQQQ is trending up, hold TQQQ. If it is trending down, hold SHV. Sell one "
                "before buying the other."
            ),
        )

    def on_trading_iteration(self):
        self.agents["trader"].run(task_prompt="Check the trend and hold TQQQ or SHV.")


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        MacroRiskStrategy.backtest(YahooDataBacktesting)
    else:
        MacroRiskStrategy().run_live()
