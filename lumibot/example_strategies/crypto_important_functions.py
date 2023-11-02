import datetime

from lumibot.brokers import Ccxt
from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class ImportantFunctions(Strategy):
    def initialize(self):
        # Set the time between trading iterations
        self.sleeptime = "30S"

        # Set the market to 24/7 since those are the hours for the crypto market
        self.set_market("24/7")

    def on_trading_iteration(self):
        ###########################
        # Placing an Order
        ###########################

        # Define the base and quote assets for our transactions
        base = Asset(symbol="BTC", asset_type="crypto")
        quote = self.quote_asset

        # Market Order for 0.1 BTC
        mkt_order = self.create_order(base, 0.1, "buy", quote=quote)
        self.submit_order(mkt_order)

        # Limit Order for 0.1 BTC at a limit price of $10,000
        lmt_order = self.create_order(base, 0.1, "buy", quote=quote, limit_price=10000)
        self.submit_order(lmt_order)

        ###########################
        # Getting Historical Data
        ###########################

        # Get the historical prices for our base/quote pair for the last 100 minutes
        bars = self.get_historical_prices(base, 100, "minute", quote=quote)
        if bars is not None:
            df = bars.df
            max_price = df["close"].max()
            self.log_message(f"Max price for {base} was {max_price}")

            ############################
            # TECHNICAL ANALYSIS
            ############################

            # Use pandas_ta to calculate the 20 period RSI
            rsi = df.ta.rsi(length=20)
            current_rsi = rsi.iloc[-1]
            self.log_message(f"RSI for {base} was {current_rsi}")

            # Use pandas_ta to calculate the MACD
            macd = df.ta.macd()
            current_macd = macd.iloc[-1]
            self.log_message(f"MACD for {base} was {current_macd}")

            # Use pandas_ta to calculate the 55 EMA
            ema = df.ta.ema(length=55)
            current_ema = ema.iloc[-1]
            self.log_message(f"EMA for {base} was {current_ema}")

        ###########################
        # Positions and Orders
        ###########################

        # Get all the positions that we own, including cash
        positions = self.get_positions()
        for position in positions:
            self.log_message(f"Position: {position}")

            # Get the asset of the position
            asset = position.asset

            # Get the quantity of the position
            quantity = position.quantity

            # Get the symbol from the asset
            symbol = asset.symbol

            self.log_message(f"we own {quantity} shares of {symbol}")

        # Get one specific position
        asset_to_get = Asset(symbol="BTC", asset_type="crypto")
        position = self.get_position(asset_to_get)

        # Get all of the outstanding orders
        orders = self.get_orders()
        for order in orders:
            self.log_message(f"Order: {order}")
            # Do whatever you need to do with the order

        # Get one specific order
        order = self.get_order(mkt_order.identifier)

        ###########################
        # Other Useful Functions
        ###########################

        # Get the current (last) price for the base/quote pair
        last_price = self.get_last_price(base, quote=quote)
        self.log_message(
            f"Last price for {base}/{quote} was {last_price}", color="green"
        )

        dt = self.get_datetime()
        self.log_message(f"The current datetime is {dt}")
        self.log_message(f"The current time is {dt.time()}")

        # If you want to check if it's after a certain time, you can do this (eg. trading only after 9:30am)
        if dt.time() > datetime.time(hour=9, minute=30):
            self.log_message("It's after 9:30am")

        # Get the value of the entire portfolio, including positions and cash
        portfolio_value = self.portfolio_value
        # Get the amount of cash in the account (the amount in the quote_asset)
        cash = self.cash

        self.log_message(f"The current value of your account is {portfolio_value}")
        # Note: Cash is based on the quote asset
        self.log_message(f"The current amount of cash in your account is {cash}")


if __name__ == "__main__":
    trader = Trader()

    KRAKEN_CONFIG = {
        "exchange_id": "kraken",
        "apiKey": "YOUR_API_KEY",
        "secret": "YOUR_SECRET_KEY",
        "margin": True,
        "sandbox": False,
    }

    # Check that the user has filled in the API keys
    if KRAKEN_CONFIG["apiKey"] == "YOUR_API_KEY":
        raise Exception("Please fill in your API key")
    if KRAKEN_CONFIG["secret"] == "YOUR_SECRET_KEY":
        raise Exception("Please fill in your secret key")

    broker = Ccxt(KRAKEN_CONFIG)

    strategy = ImportantFunctions(
        broker=broker,
    )

    trader.add_strategy(strategy)
    strategy_executors = trader.run_all()
