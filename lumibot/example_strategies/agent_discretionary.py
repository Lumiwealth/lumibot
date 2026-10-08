"""Make Me Money AI Trading Bot.

Gives one AI agent a one-sentence goal and lets it decide everything else: what
to research, what to buy, and when to sell.
"""

from lumibot.strategies import Strategy


class DiscretionaryTraderStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        self.agents.create(
            name="trader",
            allow_trading=True,
            system_prompt=("Make as much money as you possibly can."),
        )

    def on_trading_iteration(self):
        self.agents["trader"].run(task_prompt="Decide what to do today.")


if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        DiscretionaryTraderStrategy.backtest(YahooDataBacktesting)
    else:
        DiscretionaryTraderStrategy().run_live()
