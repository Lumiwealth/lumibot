import logging

from lumibot.strategies.strategy import Strategy

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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

