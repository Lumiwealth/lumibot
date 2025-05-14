import time
import logging

from lumibot.entities import Asset, Order, Position
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
        self.iter = 0


    def on_trading_iteration(self):
        self.iter += 1
        TEST_SYMBOL = "HBARUSDT"
        asset = Asset(TEST_SYMBOL, Asset.AssetType.CRYPTO_FUTURE)
        asset.leverage = 10

        if self.iter == 1:
            # 1st iteration: open market position
            try:
                qty = 100
                self.log_message(f"Iteration 1: placing BUY market order for {qty} {asset.symbol} at 10x")
                order = self.create_order(asset=asset, quantity=qty, side=Order.OrderSide.BUY, order_type=Order.OrderType.MARKET, secondary_limit_price=0.5, secondary_stop_price=0.01)
                sub = self.submit_order(order)
                if sub:
                    self.log_message(f"Opened position ID={sub.identifier}, status={sub.status}")
                else:
                    self.log_message("Failed to open position.", color="red")
            except Exception as e:
                self.log_message(f"Open error: {e}", color="red")

        elif self.iter == 2:
            # 2nd iteration: close position
            try:
                self.log_message("Iteration 2: closing 50% then remainder")
                self.close_position(asset, fraction=0.5)
                time.sleep(10)
                self.close_position(asset)
                self.log_message("Position fully closed.")
            except Exception as e:
                self.log_message(f"Close error: {e}", color="red")

        else:
            # no further actions
            self.log_message("Demo complete. No further actions.")

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

