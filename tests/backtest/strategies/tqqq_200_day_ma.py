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
import pandas as pd

"""
TQQQ 200-Day Moving Average Strategy
------------------------------------
This strategy buys the triple-leveraged NASDAQ ETF (TQQQ) when its closing
price is ABOVE its 200-day simple moving average (SMA-200) and sells when the
price dips BELOW the SMA-200.  

The logic is intentionally very simple so that traders who are new to LumiBot
can follow along:  
1. Once a day, fetch the last 200 trading days of data.  
2. Calculate the SMA-200 from that data.  
3. Compare the latest closing price to the SMA-200.  
   • Price > SMA-200  → be IN the market (buy if not already long).  
   • Price < SMA-200  → be OUT of the market (sell if currently long).  

Visual aids:  
• A continuous black line plots TQQQ’s closing price.  
• A continuous blue line plots the SMA-200.  
• Green upward arrows mark BUY signals.  
• Red downward arrows mark SELL signals.

No guarantee of future performance.  Historical results do not assure future
returns.  Use at your own risk.

This code was generated based on the user prompt: 'Make a bot that trades TQQQ based on a 200 day moving average filter. Buy TQQQ when the price is above the 200 day moving average, and sell when it is below.'
"""


class TQQQ200DayMAStrategy(Strategy):
    # Parameters could be made configurable; hard-coded here for simplicity.
    parameters = {
        "symbol": "SPY",               # Use a dividend-paying ETF for validation
        "sma_window": 200,              # Length of the moving average
        "sleeptime": "1D",            # Run the logic once per trading day
        "capital_allocation": 0.98     # Use 98 % of available cash when buying
    }

    def initialize(self):
        """Runs once when the bot starts."""
        # Friendly reminder for later debugging
        self.log_message("Initializing TQQQ 200-Day MA strategy …", color="blue")
        # Store the asset we will trade (TQQQ is an equity/ETF)
        self.tqqq = Asset(self.parameters["symbol"], Asset.AssetType.STOCK)
        # How often should on_trading_iteration run? (Once a day is enough)
        self.sleeptime = self.parameters["sleeptime"]
        # A helper variable so we don’t spam orders if the signal doesn’t change
        self.vars.last_signal = None  # Can be "LONG" or "FLAT"

    def on_trading_iteration(self):
        """This method is triggered every self.sleeptime interval."""
        # 1) Get the most recent price. If price is missing, we skip this round.
        price = self.get_last_price(self.tqqq)
        if price is None:
            self.log_message("Price data for TQQQ unavailable – skipping this iteration.", color="red")
            return

        # 2) Fetch 200 days of historical data to compute the SMA-200
        bars = self.get_historical_prices(self.tqqq, self.parameters["sma_window"], "day")
        if bars is None or bars.df.empty:
            self.log_message("Historical data unavailable – cannot calculate SMA-200.", color="red")
            return

        df: pd.DataFrame = bars.df
        sma_200 = df["close"].mean()

        # ----- Visualize price & moving average on the chart -----
        self.add_line("TQQQ", price, color="black", width=2)
        self.add_line("SMA_200", sma_200, color="blue", width=2)

        # 3) Determine current position status
        position = self.get_position(self.tqqq)  # None if we are flat
        in_market = position is not None and position.quantity > 0

        # 4) Trading rules
        if price > sma_200:
            signal = "LONG"
            if not in_market:
                # We’re not in but should be → BUY signal
                cash = self.get_cash()
                allocation = cash * self.parameters["capital_allocation"]
                qty = int(allocation // price)  # Whole shares only
                if qty <= 0:
                    self.log_message("Not enough cash to buy TQQQ.", color="yellow")
                else:
                    order = self.create_order(self.tqqq, qty, Order.OrderSide.BUY)
                    self.submit_order(order)
                    self.add_marker("Buy", price, color="green", symbol="arrow-up", size=10,
                                    detail_text="Price crossed above SMA-200")
                    self.log_message(f"BUY {qty} shares TQQQ @ ~{price:.2f}", color="green")
        else:
            signal = "FLAT"
        #     if in_market:
        #         # We’re in but should be out → SELL signal
        #         qty = position.quantity
        #         order = self.create_order(self.tqqq, qty, Order.OrderSide.SELL)
        #         self.submit_order(order)
        #         self.add_marker("Sell", price, color="red", symbol="arrow-down", size=10,
        #                         detail_text="Price crossed below SMA-200")
        #         self.log_message(f"SELL {qty} shares TQQQ @ ~{price:.2f}", color="red")

        # 5) Update last signal to avoid redundant trades & log state
        if self.vars.last_signal != signal:
            self.vars.last_signal = signal
            self.log_message(f"Signal changed to {signal}.", color="white")
        else:
            # Helpful trace when no action is taken
            self.log_message(f"No trade – signal remains {signal}.", color="white")


if __name__ == "__main__":
    if IS_BACKTESTING:
        # ----------------------------
        # Backtest path
        # ----------------------------
        trading_fee = TradingFee(percent_fee=0.001)  # 0.1 % assumed commission
        results = TQQQ200DayMAStrategy.backtest(
            YahooDataBacktesting,                 # Data source for stocks/ETFs
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX)
        )
    else:
        # ----------------------------
        # Live trading path
        # ----------------------------
        trader = Trader()
        strategy = TQQQ200DayMAStrategy(
            quote_asset=Asset("USD", Asset.AssetType.FOREX)
        )
        trader.add_strategy(strategy)
        trader.run_all()
