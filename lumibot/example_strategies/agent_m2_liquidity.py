"""M2 Liquidity AI Trading Bot.

Holds TQQQ when the money supply (M2) is growing and SHV, a short-term Treasury
fund, when it is shrinking. One AI agent reads the Federal Reserve's M2 data each
day and trades.
"""

from lumibot.strategies import Strategy


class M2LiquidityStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=(
                "Hold either TQQQ or SHV with the whole account. Each day, check whether the M2 money supply "
                "is growing compared with 3 to 6 months ago, using official Federal Reserve data. If it is "
                "growing, hold TQQQ. If it is flat or shrinking, hold SHV. Sell one before buying the other."
            ),
        )

    def on_trading_iteration(self):
        self.agents["trader"].run(task_prompt="Check M2 and hold TQQQ or SHV.")


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        M2LiquidityStrategy.backtest(YahooDataBacktesting)
    else:
        M2LiquidityStrategy().run_live()
