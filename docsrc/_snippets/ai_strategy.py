from datetime import datetime
from lumibot.backtesting import YahooDataBacktesting
from lumibot.strategies import Strategy


class MyAIStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="trader",
            default_model="openai/gpt-6-luna",
            reasoning_effort="medium",
            allow_trading=True,
            system_prompt=(
                "Trade SPY using its last 20 completed daily bars. "
                "Buy when the latest completed close is above their average; "
                "sell an existing position when it is below. Otherwise hold. "
                "Keep the position at or below 10% of portfolio value. "
                "Check cash, positions and open orders before acting. "
                "No shorting or leverage. Do not duplicate pending orders. "
                "Use only information available as of the strategy time."
            ),
        )

    def on_trading_iteration(self):
        self.agents["trader"].run(
            task_prompt="Review SPY, apply your rules and verify any orders.",
            context={"as_of": self.get_datetime().isoformat()},
        )


if __name__ == "__main__":
    MyAIStrategy.backtest(
        YahooDataBacktesting,
        datetime(2026, 4, 6),
        datetime(2026, 4, 11),
        budget=100_000,
        benchmark_asset="SPY",
    )
