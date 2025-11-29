from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from lumibot.entities import Asset, Order, TradingFee
from lumibot.backtesting import YahooDataBacktesting
from lumibot.credentials import IS_BACKTESTING

from datetime import timedelta
import math

"""
Strategy Description
--------------------
Drawdown-Recovery Strategy for MELI (MercadoLibre)

This code was generated based on the user prompt: 'Buy the MELI stock every time it has a drawdown of 25% or more and sell it when it recovers by 40% or more.'
"""

class MELIDrawdownRecovery(Strategy):
    # User-tunable parameters kept here for convenience
    parameters = {
        "symbol": "MELI",                 # The stock to trade
        "drawdown_threshold": 0.25,        # Buy when price is down 25% or more from peak
        "recovery_threshold": 0.40,        # Sell when price is up 40% or more from the lowest price seen while in position
        "allocation_pct": 0.99,            # Use 99% of available cash for each buy to avoid cash rounding issues
        "history_days_for_peak": 756,      # About 3 years of daily data to seed an initial peak
        "sleeptime": "1D"                  # Run once per trading day
    }

    def initialize(self):
        # Run the logic on end-of-day cadence for stocks
        self.sleeptime = self.parameters.get("sleeptime", "1D")

        # Persistent state stored in self.vars (safe across restarts/iterations)
        # These are easy-to-understand state variables used by traders
        self.vars.in_position = False            # Are we currently holding MELI?
        self.vars.peak_price = None              # Highest observed price used to measure drawdown (while in cash)
        self.vars.dd_low_price = None            # Lowest price seen since entry (used to measure recovery)
        self.vars.initialized_peak = False       # Track whether we've seeded peak from history

        # Friendly label for logs
        self.vars.strategy_label = "MELI Drawdown-Recovery"

    def _get_meli_asset(self):
        # Helper to provide the Asset object for MELI stock
        return Asset(self.parameters.get("symbol", "MELI"), asset_type=Asset.AssetType.STOCK)

    def _safe_last_price(self, asset):
        # Pull the most recent price; if missing, try to fall back to the latest daily bar close
        price = self.get_last_price(asset)
        if price is None:
            bars = self.get_historical_prices(asset, 1, "day")
            if bars is not None and hasattr(bars, "df") and not bars.df.empty:
                price = float(bars.df["close"].iloc[-1])
        return price

    def _seed_initial_peak(self, asset, last_price):
        # On first run, look back over history to seed a more realistic peak rather than just today's price
        # This helps the strategy catch drawdowns already in progress when the bot starts
        if self.vars.initialized_peak:
            return
        bars = self.get_historical_prices(asset, self.parameters.get("history_days_for_peak", 756), "day")
        if bars is not None and hasattr(bars, "df") and not bars.df.empty:
            peak_from_history = float(bars.df["close"].max())
            if math.isfinite(peak_from_history):
                self.vars.peak_price = peak_from_history
                self.log_message(f"Seeded initial peak from history: {peak_from_history:.2f}", color="blue")
            else:
                self.vars.peak_price = last_price
                self.log_message("Historical peak not finite; using last price as peak.", color="yellow")
        else:
            self.vars.peak_price = last_price
            self.log_message("No historical data; using last price as peak.", color="yellow")
        self.vars.initialized_peak = True

    def on_trading_iteration(self):
        asset = self._get_meli_asset()
        last_price = self._safe_last_price(asset)

        if last_price is None or not math.isfinite(last_price):
            self.log_message("Price data unavailable for MELI; skipping this iteration.", color="red")
            return

        # Add a line for MELI's current price so we can visualize the price evolution
        self.add_line("MELI", float(last_price), color="black", width=2, detail_text="MELI Price")

        # Seed initial peak once at startup using history
        if not self.vars.initialized_peak:
            self._seed_initial_peak(asset, last_price)

        # If peak_price hasn't been set for any reason, fallback to last price
        if self.vars.peak_price is None:
            self.vars.peak_price = last_price

        drawdown_threshold = float(self.parameters.get("drawdown_threshold", 0.25))
        recovery_threshold = float(self.parameters.get("recovery_threshold", 0.40))
        allocation_pct = float(self.parameters.get("allocation_pct", 0.99))

        # Trading logic splits into two modes: in cash vs in position
        if not self.vars.in_position:
            # Update the peak price while in cash; we want the most recent high to measure new drawdowns
            if last_price > self.vars.peak_price:
                self.vars.peak_price = last_price
                self.log_message(f"New peak observed while in cash: {self.vars.peak_price:.2f}", color="blue")

            # Compute drawdown from that peak
            if self.vars.peak_price and self.vars.peak_price > 0:
                drawdown = (self.vars.peak_price - last_price) / self.vars.peak_price
            else:
                drawdown = 0.0

            # Plot helpful reference lines: Peak and the 25% drawdown level from the current peak
            peak_to_plot = float(self.vars.peak_price) if self.vars.peak_price else float(last_price)
            self.add_line("Peak", peak_to_plot, color="purple", detail_text="Tracked Peak")
            dd25_level = peak_to_plot * (1.0 - drawdown_threshold)
            self.add_line("DD25 Level", dd25_level, color="orange", detail_text="25% DD Trigger")

            self.log_message(
                f"Mode: CASH | Last: {last_price:.2f}, Peak: {peak_to_plot:.2f}, Drawdown: {drawdown:.2%}",
                color="white"
            )

            # If drawdown meets or exceeds threshold, buy using available cash
            if drawdown >= drawdown_threshold:
                cash = self.get_cash()
                if cash is None or cash <= 0:
                    self.log_message("Insufficient cash to buy MELI; holding.", color="yellow")
                    return

                # Calculate shares using available cash (integer shares for stocks)
                shares = int((cash * allocation_pct) // last_price)
                if shares <= 0:
                    self.log_message(
                        f"Calculated 0 shares with cash={cash:.2f} and price={last_price:.2f}; holding.",
                        color="yellow"
                    )
                    return

                order = self.create_order(asset, shares, Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
                submitted = self.submit_order(order)
                if submitted is not None:
                    self.vars.in_position = True
                    # Initialize dd_low with current price; it may go lower after entry and will be updated
                    self.vars.dd_low_price = last_price
                    self.add_marker("DD Buy", float(last_price), color="green", symbol="arrow-up", size=10, detail_text="Drawdown Buy")
                    self.log_message(
                        f"BUY {shares} MELI at ~{last_price:.2f} due to drawdown {drawdown:.2%} >= {drawdown_threshold:.2%}.",
                        color="green"
                    )
                else:
                    self.log_message("Order submission failed; staying in cash.", color="red")
            else:
                self.log_message(
                    f"Drawdown {drawdown:.2%} below threshold {drawdown_threshold:.2%}; waiting in cash.",
                    color="white"
                )

        else:
            # In position: keep track of the lowest price since entry to measure recovery from the trough
            if self.vars.dd_low_price is None:
                self.vars.dd_low_price = last_price
            else:
                if last_price < self.vars.dd_low_price:
                    self.vars.dd_low_price = last_price
                    self.log_message(f"New post-entry low recorded: {self.vars.dd_low_price:.2f}", color="blue")

            dd_low_plot = float(self.vars.dd_low_price)
            self.add_line("DD Low", dd_low_plot, color="brown", detail_text="Post-Entry Low")

            # Compute recovery from the lowest price seen since entry
            if self.vars.dd_low_price and self.vars.dd_low_price > 0:
                recovery = (last_price - self.vars.dd_low_price) / self.vars.dd_low_price
            else:
                recovery = 0.0

            self.log_message(
                f"Mode: IN POSITION | Last: {last_price:.2f}, Trough: {self.vars.dd_low_price:.2f}, Recovery: {recovery:.2%}",
                color="white"
            )

            # If recovery meets or exceeds the threshold, sell everything
            if recovery >= recovery_threshold:
                pos = self.get_position(asset)
                if pos is None or pos.quantity is None or pos.quantity <= 0:
                    self.log_message("No MELI position size found; cannot sell. Will reset state to cash.", color="red")
                    # Reset to cash state defensively
                    self.vars.in_position = False
                    self.vars.dd_low_price = None
                    # Reset the peak to the last price so new drawdowns are measured from here
                    self.vars.peak_price = last_price
                    return

                quantity = int(pos.quantity)
                order = self.create_order(asset, quantity, Order.OrderSide.SELL, order_type=Order.OrderType.MARKET)
                submitted = self.submit_order(order)
                if submitted is not None:
                    self.vars.in_position = False
                    self.add_marker("Recovery Sell", float(last_price), color="red", symbol="arrow-down", size=10, detail_text="Recovery Exit")
                    self.log_message(
                        f"SELL {quantity} MELI at ~{last_price:.2f} due to recovery {recovery:.2%} >= {recovery_threshold:.2%}.",
                        color="green"
                    )

                    # After exiting, reset trough and set a fresh peak to current price, then we will update it while in cash
                    self.vars.dd_low_price = None
                    self.vars.peak_price = last_price
                else:
                    self.log_message("Sell order submission failed; keeping position and monitoring.", color="red")
            else:
                self.log_message(
                    f"Recovery {recovery:.2%} below threshold {recovery_threshold:.2%}; holding position.",
                    color="white"
                )


if __name__ == "__main__":
    # Backtesting vs Live is controlled by environment; LumiBot sets IS_BACKTESTING accordingly
    if IS_BACKTESTING:
        # Backtesting path using Yahoo data for stocks
        trading_fee = TradingFee(percent_fee=0.001)  # 10 bps example fee; adjust as needed

        # Note: backtesting_start/end are controlled by env vars unless explicitly provided
        results = MELIDrawdownRecovery.backtest(
            datasource_class=YahooDataBacktesting,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            parameters=None,  # Use class defaults; override by passing a dict here
        )
    else:
        # Live trading path (broker is chosen by environment configuration outside of this script)
        trader = Trader()
        strategy = MELIDrawdownRecovery(
            quote_asset=Asset("USD", Asset.AssetType.FOREX),  # Keep quote in USD by default
        )
        trader.add_strategy(strategy)
        trader.run_all()
