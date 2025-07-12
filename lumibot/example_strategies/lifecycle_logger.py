import logging
import datetime

from lumibot.strategies.strategy import Strategy

logger = logging.getLogger(__name__)


class LifecycleLogger(Strategy):

    parameters = {
        "sleeptime": "10s",
        "market": "24/7",
    }

    def initialize(self, symbol=""):
        self.sleeptime = self.parameters["sleeptime"]
        self.set_market(self.parameters["market"])

    def before_market_opens(self):
        dt = self.get_datetime()
        logger.info(f"{dt} before_market_opens called")

    def before_starting_trading(self):
        dt = self.get_datetime()
        logger.info(f"{dt} before_starting_trading called")

    def on_trading_iteration(self):
        dt = self.get_datetime()
        logger.info(f"{dt} on_trading_iteration called")

    def before_market_closes(self):
        dt = self.get_datetime()
        logger.info(f"{dt} before_market_closes called")

    def after_market_closes(self):
        dt = self.get_datetime()
        logger.info(f"{dt} after_market_closes called")


if __name__ == "__main__":
    IS_BACKTESTING = True

    if IS_BACKTESTING:
        from lumibot.backtesting import YahooDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime.datetime(2023, 1, 1)
        backtesting_end = datetime.datetime(2024, 9, 1)

        results = LifecycleLogger.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            # show_progress_bar=False,
            # quiet_logs=False,
        )

        # Print the results
        print(results)
    else:
        from lumibot.credentials import ALPACA_CONFIG
        from lumibot.brokers import Alpaca

        broker = Alpaca(ALPACA_CONFIG)
        strategy = LifecycleLogger(broker=broker)
        strategy.run_live()
