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
            # Only run the demo logic once
            self.log_message("Demo already completed in the first iteration.")
            # You might want to stop the strategy after the first run for a demo
            # self.stop_backtest() # Uncomment if running in backtest and want to stop
            return

        # Get the last price of BTC/USDT (or your quote asset)
        try:
            # Ensure the base asset type is correct (crypto or future depending on broker needs)
            btc_asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
            last_price = self.get_last_price(btc_asset)
            self.log_message(f"Last price for {btc_asset.symbol}: {last_price}")
        except Exception as e:
            self.log_message(f"Could not get last price for BTC: {e}", color="red")

        self.log_message("Starting Bitunix Futures Demo...")

        positions = self.get_positions()
        self.log_message(f"Current positions: {positions}")

        cash = self.get_cash()
        self.log_message(f"Current cash {cash}")


        """
        Demo: Low-Risk BitUnix Futures Order Placement & Cleanup
        """
        # Small test quantity on HBARUSDT (~1 HBAR ≈ $0.1)
        TEST_SYMBOL   = "HBAR" # Use a symbol available on Bitunix Futures

        # ---------------------- 1) Place Test Market Order ----------------------
        try:
            # Use Asset.AssetType.FUTURE for futures trading
            asset = Asset(TEST_SYMBOL, Asset.AssetType.FUTURE)
            # Adjust quantity based on contract size/minimums if needed
            # Bitunix HBARUSDT perpetual seems to have minimum 1 HBAR
            quantity_to_trade = 100 # Example: trade 10 HBAR

            self.log_message(f"Attempting to place BUY MARKET order for {quantity_to_trade} {asset}...")
            order = self.create_order(
                asset=asset,
                quantity=quantity_to_trade,
                side=Order.OrderSide.BUY,
                order_type=Order.OrderType.LIMIT,
                limit_price=0.19
            )
            submitted_order = self.submit_order(order)

            if submitted_order:
                self.log_message(f"Placed order: ID={submitted_order.identifier}, Status={submitted_order.status}")
                # Give the exchange a moment to register the order and potential fill
                self.log_message("Waiting 5 seconds for order processing...")
                time.sleep(5)
                # Refresh order status after waiting
                order = self.get_order(submitted_order.identifier)
                if order:
                     self.log_message(f"Order status after wait: ID={order.identifier}, Status={order.status}")
                else:
                     self.log_message(f"Could not retrieve order {submitted_order.identifier} after wait.")

            else:
                self.log_message("Failed to submit order.", color="red")
                order = None # Ensure order is None if submission failed

        except Exception as e:
            self.log_message(f"Error placing order: {e}", color="red")
            order = None # Ensure order is None on exception


        # ---------------------- 2) Cancel Test Order (if still open) ----------------------
        if order and order.status in [Order.OrderStatus.SUBMITTED, Order.OrderStatus.PENDING, Order.OrderStatus.PARTIALLY_FILLED]:
            try:
                self.log_message(f"Attempting to cancel order: ID={order.identifier}")
                self.cancel_order(order)
                self.log_message(f"Cancellation request sent for order: ID={order.identifier}")
                # Wait for cancellation to propagate
                self.log_message("Waiting 5 seconds for cancellation processing...")
                time.sleep(5)
                # Verify cancellation
                cancelled_order = self.get_order(order.identifier)
                if cancelled_order and cancelled_order.status == Order.OrderStatus.CANCELLED:
                    self.log_message(f"Order successfully cancelled: ID={cancelled_order.identifier}")
                elif cancelled_order:
                    self.log_message(f"Order status after cancellation attempt: {cancelled_order.status}", color="yellow")
                else:
                    self.log_message(f"Could not retrieve order {order.identifier} after cancellation attempt.", color="yellow")

            except Exception as e:
                self.log_message(f"Error cancelling order {order.identifier}: {e}", color="red")
        elif order:
             self.log_message(f"Order ID={order.identifier} is not in a cancellable state (Status: {order.status}). Skipping cancellation.")


        # ---------------------- 3) Close Any Residual Position ----------------------
        try:
            self.log_message("Checking for residual positions...")
            # Fetch current positions again to ensure we have the latest state
            positions = self.get_positions()
            self.log_message(f"Positions before cleanup: {positions}")
            found_position_to_close = False
            for pos in positions:
                # Ensure we are checking the correct asset type (FUTURE)
                if pos.asset.symbol == TEST_SYMBOL and pos.asset.asset_type == Asset.AssetType.FUTURE and abs(pos.quantity) > 0:
                    found_position_to_close = True
                    self.log_message(f"Found residual position: {pos.quantity} of {pos.asset}")
                    # Determine side needed to close (sell if long, buy if short)
                    close_side = Order.OrderSide.SELL if pos.quantity > 0 else Order.OrderSide.BUY
                    close_quantity = abs(pos.quantity)

                    self.log_message(f"Attempting to close position with {close_side} MARKET order for {close_quantity} {pos.asset}...")
                    close_order_req = self.create_order(
                        asset=pos.asset, # Use the asset from the position object
                        quantity=close_quantity,
                        side=close_side,
                        order_type=Order.OrderType.MARKET
                    )
                    submitted_close_order = self.submit_order(close_order_req)

                    if submitted_close_order:
                        self.log_message(f"Submitted close order: ID={submitted_close_order.identifier}, Status={submitted_close_order.status}")
                        self.log_message("Waiting 5 seconds for close order processing...")
                        time.sleep(5)
                        # Verify position closed
                        final_positions = self.get_positions()
                        pos_closed = True
                        for final_pos in final_positions:
                             if final_pos.asset == pos.asset and abs(final_pos.quantity) > 0:
                                 pos_closed = False
                                 self.log_message(f"Position NOT fully closed. Remaining: {final_pos.quantity}", color="yellow")
                                 break
                        if pos_closed:
                             self.log_message(f"Position for {pos.asset} appears closed.")

                    else:
                        self.log_message(f"Failed to submit close order for {pos.asset}.", color="red")

            if not found_position_to_close:
                 self.log_message(f"No residual position found for {TEST_SYMBOL} {Asset.AssetType.FUTURE}.")

        except Exception as e:
            self.log_message(f"Error closing position: {e}", color="red")

        self.log_message("Demo complete.")
        # Consider stopping the strategy after the demo run
        # self.stop_backtest() # Uncomment if in backtest
        # Or just let it sleep if running live
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

