from decimal import Decimal

from credentials import (
    ALPACA_CONFIG_PAPER,
    COINBASE_CONFIG,
    COINBASEPRO_CUSTOM_ETF,
    KRAKEN_CONFIG_LIVE,
    KUCOIN_CONFIG_LIVE,
    KUCOIN_CONFIG_MEXICO,
    KUCOIN_LIVE,
)
from lumibot.brokers import Alpaca, Ccxt, Tradovate
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class CryptoImportantFunctions(Strategy):
    parameters = {"base": Asset(symbol="BTC", asset_type="crypto")}
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the time between trading iterations
        self.sleeptime = "30S"

        # Set the market to 24/7 since those are the hours for the crypto market
        self.set_market("24/7")
        # self.set_market("us_futures")

    def on_trading_iteration(self):
        # return

        # Define the base and quote assets for our transactions
        base = self.parameters["base"]
        quote = self.quote_asset

        ###########################
        # Orders
        ###########################

        # Market Order
        order = self.create_order(base, 0.011, "sell", quote=quote)
        self.submit_order(order)

        # Market Order
        order = self.create_order(
            base, 0.00051, "buy", quote=quote, custom_params={"leverage": 5}
        )
        self.submit_order(order)

        # # Limit Buy Order
        order = self.create_order(base, 0.00011, "buy", quote=quote, limit_price=40000)
        self.submit_order(order)

        # Limit Sell Order
        order = self.create_order(base, 0.00011, "sell", quote=quote, limit_price=30000)
        self.submit_order(order)

        # Stop Order
        order = self.create_order(
            base, 0.1, "buy", quote=quote, stop_price=Decimal(30000) - Decimal(1)
        )
        self.submit_order(order)

        # OCO Order
        order = self.create_order(
            base,
            0.1,
            "buy",
            quote=quote,
            take_profit_price=Decimal(30000) - Decimal(1),
            stop_loss_price=Decimal(30000) + Decimal(1),
            position_filled=True,
        )
        self.submit_order(order)

        ###########################
        # Getting Current Prices
        ###########################

        # # Get the current (last) price for the base/quote pair
        last_price = self.get_last_price(base, quote=quote)
        self.log_message(f"Last price for {base}/{quote} was {last_price}")

        # # Get the current (last) price for a few bases
        last_prices = self.get_last_prices([base], quote=quote)
        self.log_message(f"Last price for {base}/{quote} was {last_prices}")

        # ###########################
        # # Getting Historical Data
        # ###########################

        # # Get the historical prices for our base/quote pair
        bars = self.get_historical_prices(base, 100, "minute", quote=quote)
        if bars is not None:
            df = bars.df

        ###########################
        # Positions and Orders
        ###########################

        # Get all the positions that we own, including cash
        positions = self.get_positions()
        for position in positions:
            self.log_message(f"Position: {position}")
            # Do whatever you need to do with the position

        # Get one specific position
        asset_to_get = Asset(symbol="BTC", asset_type="crypto")
        position = self.get_position(asset_to_get)

        # Get all of the outstanding orders
        orders = self.get_orders()
        for order in orders:
            self.log_message(f"Order: {order}")
            # Do whatever you need to do with the order

        # # Get one specific order
        if order is not None:
            found_order = self.get_order(order.identifier)
            self.log_message(f"Found Order: {found_order}")
            # Do whatever you need to do with the order

        # Get a selling order for a specific positon
        quote_position = self.get_position(quote)
        sell_order = self.get_selling_order(quote_position)
        if sell_order is not None:
            self.submit_order(sell_order)

        # Sell all of the positions
        self.sell_all()

        ###########################
        # Other Useful Functions
        ###########################

        dt = self.get_datetime()
        self.log_message(f"The current datetime is {dt}")

        # Get the value of the entire portfolio/account, including positions and cash
        portfolio_value = self.portfolio_value
        # or
        portfolio_value = self.get_portfolio_value()

        # Get the amount of cash in the account (the amount in the quote_asset)
        cash = self.cash
        # or
        cash = self.get_cash()

        self.log_message("done")


if __name__ == "__main__":
    trader = Trader()
    # broker = Ccxt(COINBASEPRO_CUSTOM_ETF)
    # broker = Ccxt(KUCOIN_CONFIG_LIVE)
    broker = Ccxt(KRAKEN_CONFIG_LIVE)
    # broker = Ccxt(COINBASE_CONFIG)
    # broker = Alpaca(ALPACA_CONFIG_PAPER)
    # broker = InteractiveBrokersRest(ALPACA_CONFIG_PAPER)

    strategy = CryptoImportantFunctions(
        broker=broker,
        # quote_asset=Asset(symbol="USDT", asset_type="crypto"),
        quote_asset=Asset(symbol="USD", asset_type="forex"),
        # parameters={"base": Asset(symbol="ETH", asset_type="crypto")},
    )

    trader.add_strategy(strategy)
    strategy_executors = trader.run_all()
