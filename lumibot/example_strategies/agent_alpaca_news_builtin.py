"""Market News AI Trading Bot.

Reads the day's broad market news and holds SPY, QQQ, or SHV, a short-term
Treasury fund, depending on what the news says. One AI agent reads the headlines,
opens the most important story, and trades.
"""

from lumibot.strategies import Strategy


class AlpacaNewsBuiltinStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Read today's broad market news about the S&P 500, the Nasdaq, the Dow, and small caps. Open "
                "and read the most important story in full. If the news is good, hold SPY or QQQ, whichever "
                "fits better. If it is bad, hold SHV. Use only news published before now."
            ),
        )

    def on_trading_iteration(self):
        self.agents["trader"].run(task_prompt="Read today's market news and decide.")


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        AlpacaNewsBuiltinStrategy.backtest(YahooDataBacktesting)
    else:
        AlpacaNewsBuiltinStrategy().run_live()
