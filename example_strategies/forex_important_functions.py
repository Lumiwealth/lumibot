from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4

from credentials import INTERACTIVE_BROKERS_CONFIG
from lumibot.brokers import InteractiveBrokers
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class ForexImportantFunctions(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the time between trading iterations
        self.sleeptime = "30S"

        self.set_market("24/7")

    def on_trading_iteration(self):
        self.base = Asset(
            symbol="EUR",
            asset_type="forex",
        )
        qty = 100000

        ############
        # Orders
        ############

        # Place a market order to buy 1 contract of the base asset
        order = self.create_order(
            asset=self.base,
            quantity=qty,
            side="buy",
        )
        self.submit_order(order)

        # Place a limit order to sell 1 contract of the base asset
        order = self.create_order(
            asset=self.base,
            quantity=qty,
            side="sell",
            limit_price=Decimal("100"),
        )

        ############
        # Positions
        ############

        positions = self.get_positions()
        for position in positions:
            self.log_message(f"Position: {position}")


if __name__ == "__main__":
    trader = Trader()
    broker = InteractiveBrokers(INTERACTIVE_BROKERS_CONFIG)

    strategy = ForexImportantFunctions(
        broker=broker,
    )

    trader.add_strategy(strategy)
    strategy_executors = trader.run_all()
