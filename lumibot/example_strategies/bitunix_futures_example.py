import time
import logging

from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy
from lumibot.brokers import Bitunix
from lumibot.credentials import BITUNIX_CONFIG  # Assuming Bitunix config is in credentials

class BitunixFuturesExample(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the time between trading iterations
        self.sleeptime = "1s"  # Run every minute initially, adjust as needed

        # Set the market to 24/7 for crypto futures
        self.set_market("24/7")


    def on_trading_iteration(self):
        if not self.first_iteration:
            self.log_message("Demo already completed in the first iteration.")
            return

        # Get the last price of BTC/USDT (or your quote asset)
        try:
            btc_asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
            last_price = self.get_last_price(btc_asset)
            self.log_message(f"Last price for {btc_asset.symbol}: {last_price}")
        except Exception as e:
            self.log_message(f"Could not get last price for BTC: {e}", color="red")

        self.log_message("Starting Bitunix Futures Demo...")

        cash = self.get_cash()
        self.log_message(f"Current cash {cash}")

        TEST_SYMBOL = "HBARUSDT"  # Use a symbol available on Bitunix Futures

        # ---------------------- 1) Place Test Market Order ----------------------
        asset = Asset(TEST_SYMBOL, Asset.AssetType.CRYPTO_FUTURE)
        asset.leverage = 10  # Example: Set leverage to 10x for this order
        try:
            quantity_to_trade = 100

            self.log_message(f"Attempting to place BUY LIMIT order for {quantity_to_trade} {asset} at 10x leverage...")
            order = self.create_order(
                asset=asset,
                quantity=quantity_to_trade,
                side=Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET, # can also do limits
            )
            submitted_order = self.submit_order(order)

            if submitted_order:
                self.log_message(f"Placed order: ID={submitted_order.identifier}, Status={submitted_order.status}")
                self.log_message("Waiting 10 seconds for order processing/fill...")
                time.sleep(10)
                # Refresh order status after waiting
                order = self.get_order(submitted_order.identifier)
                if order:
                    self.log_message(f"Order status after wait: ID={order.identifier}, Status={order.status}")
                else:
                    self.log_message(f"Could not retrieve order {submitted_order.identifier} after wait.")
            else:
                self.log_message("Failed to submit order.", color="red")
                order = None

        except Exception as e:
            self.log_message(f"Error placing order: {e}", color="red")
            order = None

        # ---------------------- 2) Close Position (if opened) ----------------------
        try:
            self.log_message("Waiting 10 seconds before attempting to close position...")
            time.sleep(10)
            self.close_position(asset)

        except Exception as e:
            self.log_message(f"Error closing position: {e}", color="red")

        self.log_message("Demo complete.")
        self.log_message(f"Will sleep for {self.sleeptime}...")


if __name__ == "__main__":
    # Ensure Bitunix credentials are set in .env or environment variables
    # BITUNIX_CONFIG should contain API_KEY, API_SECRET
    if not BITUNIX_CONFIG or not BITUNIX_CONFIG.get("API_KEY") or not BITUNIX_CONFIG.get("API_SECRET"):
        print("Error: Bitunix API Key or Secret not found in credentials. Please set them in your .env file or environment variables.")
    elif BITUNIX_CONFIG.get("TRADING_MODE", "").upper() != "FUTURES":
         print(f"Error: BITUNIX_TRADING_MODE is not set to FUTURES in credentials. Current mode: {BITUNIX_CONFIG.get('TRADING_MODE')}")
    else:
        print("Attempting to run Bitunix Futures Example live...")
        broker = Bitunix(BITUNIX_CONFIG)
        strategy = BitunixFuturesExample(broker=broker)
        strategy.run_live()

