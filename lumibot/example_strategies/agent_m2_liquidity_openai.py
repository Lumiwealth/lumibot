"""M2 Liquidity AI Trading Bot (OpenAI).

Holds TQQQ when the money supply (M2) is growing and SHV, a short-term Treasury
fund, when it is shrinking. One AI agent reads the Federal Reserve's M2 data each
day and trades. This copy runs on OpenAI's GPT-6 Luna.
"""

from lumibot.strategies import Strategy


class M2LiquidityOpenAIStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="trader",
            allow_trading=True,
            model="openai/gpt-6-luna",
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

        M2LiquidityOpenAIStrategy.backtest(YahooDataBacktesting)
    else:
        M2LiquidityOpenAIStrategy().run_live()
