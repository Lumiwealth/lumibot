################################################################################
# Must Be Imported First If Run Locally
if True:
    import os
    import sys

    myPath = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")
    sys.path.insert(
        0,
        "/Users/robertgrzesik/Development/lumiwealth_tradier/",
    )
    sys.path.insert(0, "/Users/robertgrzesik/Development/quantstats_lumi/")
################################################################################

import pandas as pd

from lumibot.credentials import IS_BACKTESTING
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

"""
TQQQ 200-day SMA Strategy
-------------------------
This code was generated based on the user prompt: 'make a trading bot that trades tqqq using a 200 day moving average, if we are above that ma then buy, otherwise hold cash'

Simple strategy: if TQQQ's daily close is above its 200-day moving average buy using nearly all available cash; if price is below the 200-day moving average, exit to cash.
"""


class TqqqSma200Strategy(Strategy):
    parameters = {
        "symbol": "TQQQ",
        "sma_window": 200,
        # keep allocation near full cash use but leave a small buffer for fees
        "cash_buffer_pct": 0.01,
    }

    def initialize(self):
        # Called once at the start. Set how often the bot runs and which market hours to use.
        # We use daily iterations for a long-term moving average strategy.
        self.sleeptime = "1D"
        # Make the market normal US stock hours
        self.set_market("stock")

        # persistent variable to avoid re-logging unchanged state too often
        self.vars.last_signal = None

    def on_trading_iteration(self):
        # This runs every trading iteration (daily in this setup).
        # 1) Build the asset object for TQQQ
        symbol = self.parameters.get("symbol")
        asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)

        # 2) Get enough historical daily prices to compute a 200-day SMA
        bars = self.get_historical_prices(asset, self.parameters.get("sma_window") + 20, "day")
        if bars is None:
            # If we don't have data, log and skip this iteration
            self.log_message("Historical data for TQQQ unavailable. Skipping iteration.", color="red")
            return

        df = bars.df
        if df is None or df.empty or len(df) < self.parameters.get("sma_window"):
            self.log_message("Not enough data to compute 200-day moving average. Skipping.", color="red")
            return

        # 3) Calculate the 200-day simple moving average and get the last close price
        df = df.copy()
        df["SMA_200"] = df["close"].rolling(window=self.parameters.get("sma_window")).mean()
        last_row = df.iloc[-1]
        last_close = float(last_row["close"]) if pd.notnull(last_row["close"]) else None
        sma_200 = float(last_row["SMA_200"]) if pd.notnull(last_row["SMA_200"]) else None

        # Add continuous lines for charting: price and SMA
        if last_close is not None:
            self.add_line(symbol, last_close, color="black", width=2, detail_text=f"{symbol} Price")
        if sma_200 is not None:
            self.add_line("SMA_200", sma_200, color="blue", width=2, detail_text="200-day SMA")

        if last_close is None or sma_200 is None:
            self.log_message("Price or SMA is None, skipping.", color="yellow")
            return

        # 4) Determine current position for TQQQ
        position = self.get_position(asset)
        position_qty = 0
        if position is not None:
            try:
                position_qty = float(position.quantity)
            except Exception:
                position_qty = position.quantity if position is not None else 0

        # 5) Trading logic: buy when price > SMA_200, otherwise close position and hold cash
        # Simple state variable to avoid repeated identical log messages
        if last_close > sma_200:
            # Signal: bullish — buy or hold position
            if position_qty == 0:
                # We currently have no position, attempt to buy using most of the cash
                cash = self.get_cash()
                if cash is None or cash <= 0:
                    self.log_message("No available cash to buy TQQQ.", color="red")
                    return

                # Use nearly all cash but leave a small buffer for fees
                allocation = cash * (1.0 - float(self.parameters.get("cash_buffer_pct", 0.01)))
                shares_to_buy = int(allocation // last_close)

                if shares_to_buy <= 0:
                    self.log_message("Calculated zero shares to buy based on cash and price.", color="yellow")
                    return

                # Create and send a market buy order for the integer number of shares
                order = self.create_order(asset, shares_to_buy, Order.OrderSide.BUY)
                self.submit_order(order)

                # Add a marker for the buy event and log it
                self.add_marker(
                    "Buy",
                    last_close,
                    color="green",
                    symbol="arrow-up",
                    size=8,
                    detail_text=f"Bought {shares_to_buy} shares",
                )
                self.log_message(f"Buying {shares_to_buy} shares of {symbol} at price {last_close:.2f}", color="green")
                self.vars.last_signal = "buy"
            else:
                # We already hold the position — do nothing but log occasionally
                if self.vars.last_signal != "buy":
                    self.log_message(f"Holding existing {symbol} position: {position_qty} shares.", color="blue")
                    self.vars.last_signal = "buy"
        else:
            # Signal: bearish or neutral — move to cash by selling any position
            if position_qty > 0:
                # Create and submit a sell order for the full position quantity
                sell_order = self.create_order(asset, position_qty, Order.OrderSide.SELL)
                self.submit_order(sell_order)
                # Add a marker and log the sell
                self.add_marker(
                    "Sell",
                    last_close,
                    color="red",
                    symbol="arrow-down",
                    size=8,
                    detail_text=f"Sold {position_qty} shares",
                )
                self.log_message(
                    f"Selling {position_qty} shares of {symbol} at price {last_close:.2f} — moving to cash.",
                    color="red",
                )
                self.vars.last_signal = "sell"
            else:
                # Already in cash — log occasionally
                if self.vars.last_signal != "cash":
                    self.log_message("Price below 200-day average. Holding cash.", color="yellow")
                    self.vars.last_signal = "cash"


if __name__ == "__main__":
    # Entry point differentiates between backtesting and live trading
    if IS_BACKTESTING:
        # ----------------------
        # Backtesting path
        # ----------------------
        from lumibot.backtesting import YahooDataBacktesting

        # Use a small percent fee for buys and sells to more closely mimic real trading costs
        trading_fee = TradingFee(percent_fee=0.001)

        # Run the backtest using Yahoo daily data. SPY is used as the default benchmark.
        result = TqqqSma200Strategy.backtest(
            YahooDataBacktesting,
            benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
            parameters=None,  # No extra parameters passed; uses defaults defined in the strategy
            budget=100000,  # default budget (can be overridden by environment if desired)
        )

        # Print or log a short summary so users running the script see something in stdout
        try:
            print(result)
        except Exception:
            pass
    else:
        # ----------------------
        # Live trading path
        # ----------------------
        # Create a trader, instantiate the strategy, add it and run.
        trader = Trader()
        strategy = TqqqSma200Strategy(
            quote_asset=Asset("USD", Asset.AssetType.FOREX),
        )
        trader.add_strategy(strategy)
        strategies = trader.run_all()
