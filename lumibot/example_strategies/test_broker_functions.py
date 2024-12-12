import logging
from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy
from datetime import timedelta

# from lumiwealth_tradier import Tradier as _Tradier

class BrokerTest(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the time between trading iterations
        self.sleeptime = "20S"

        # Set the market to 24/7 since those are the hours for the crypto market
        self.set_market("24/7")

        # Record the last trade time
        self.last_trade_time = None

        self.strike = 520

    def on_trading_iteration(self):
        ## historical data
        stock_asset = Asset(
            symbol="SPY",
            asset_type=Asset.AssetType.STOCK
        )
        self.get_historical_prices(stock_asset, 10, timestep="1 day", quote=stock_asset, include_after_hours=True, timeshift=timedelta(days=2))

        ## Get Positions
        positions = self.get_positions()
        logging.info(positions)

        ## Get Orders
        orders = self.get_orders()
        logging.info(orders)

        if self.first_iteration:
            # Get the current time
            current_time = self.get_datetime()

            asset = Asset(symbol="SPY")
            #self.broker.get_chains(asset=asset)
            # Get expiration (tomorrow, or the next trading day)
            expiration = self.get_next_trading_day(current_time.date())

            option_asset_1 = Asset(
                symbol="SPY",
                asset_type=Asset.AssetType.OPTION,
                strike=550,
                right=Asset.OptionRight.PUT,
                expiration=expiration,
            )
            quote_option_1 = self.get_quote(option_asset_1)
            logging.info(quote_option_1)
            order_1 = self.create_order(option_asset_1, 2, Order.OrderSide.BUY_TO_OPEN)

            option_asset_2 = Asset(
                symbol="SPY",
                asset_type=Asset.AssetType.OPTION,
                strike=550,
                right=Asset.OptionRight.CALL,
                expiration=expiration,
            )
            quote_option_2 = self.get_quote(option_asset_2)
            order_2 = self.create_order(option_asset_2, 2, Order.OrderSide.BUY_TO_OPEN)

            option_asset_3 = Asset(
                symbol="SPY",
                asset_type=Asset.AssetType.OPTION,
                strike=560,
                right=Asset.OptionRight.PUT,
                expiration=expiration,
            )
            quote_option_3 = self.get_quote(option_asset_3)
            order_3 = self.create_order(option_asset_3, 2, Order.OrderSide.SELL_TO_OPEN)

            option_asset_4 = Asset(
                symbol="SPY",
                asset_type=Asset.AssetType.OPTION,
                strike=560,
                right=Asset.OptionRight.CALL,
                expiration=expiration,
            )
            quote_option_4 = self.get_quote(option_asset_4)
            order_4 = self.create_order(option_asset_4, 2, Order.OrderSide.SELL_TO_OPEN)

            option_asset_5 = Asset(
                symbol="SPY",
                asset_type=Asset.AssetType.OPTION,
                strike=570,
                right=Asset.OptionRight.PUT,
                expiration=expiration
            )
            quote_option_5 = self.get_quote(option_asset_5)
            order_5 = self.create_order(option_asset_5, 2, Order.OrderSide.BUY_TO_OPEN)

            # Submit one of the orders
            # result = self.submit_order(order_5)
            
            # Submit all of the orders as a multileg order
            result = self.submit_orders([order_1, order_4], is_multileg=True, price=2)
            logging.info(result)

            ## stock order
            stock_asset = Asset(
                symbol="SPY",
                asset_type=Asset.AssetType.STOCK
            )
            order = self.create_order(asset=stock_asset, quantity=2, side=Order.OrderSide.BUY_TO_OPEN)

            self.submit_order(order)

            self.cancel_orders(result)
        return

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"Filled order: {order}, {position}, {price}, {quantity}, {multiplier}")

    def on_new_order(self, order):
        self.log_message(f"New order: {order}")


if __name__ == "__main__":
    # broker = ExampleBroker()

    strategy = BrokerTest()
    strategy.run_live()

