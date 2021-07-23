import logging
import random

import pandas as pd

from lumibot.strategies.strategy import Strategy

"""
This Dev strategy is being used to set up csv ingestion into Lumibot. 
"""


class Dev(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the initial variables or constants

        # 0 will default to day time frame.
        self.sleeptime = 1
        self.minutes_before_closing = 15

        # Create specific dataframe for entry.
        # Index is localized datetime
        # Columns: Open, High, Low, Close, Volume, Dividends, Stock Splits
        # For now, if missing any columns add them, add in exact column names.
        # Is possible to now add in another indicator column(s)
        self.asset = self.create_asset("SPY", asset_type="stock")
        df = pd.read_csv("data/minute_data.csv")
        df["Date"] = pd.to_datetime(df["Date"])
        df["Date"] = df["Date"].dt.tz_localize(tz=self.data_source.DEFAULT_PYTZ)
        df["SMA15"] = df["Close"].rolling(15).mean()
        df["SMA100"] = df["Close"].rolling(100).mean()
        self.load_pandas(self.asset, df)

    def on_trading_iteration(self):
        # What to do each iteration
        bars = self.get_symbol_bars(self.asset, length=1, timestep="day")
        sma15 = bars.df["SMA15"][0]
        sma100 = bars.df["SMA100"][0]
        if not sma15:
            return
        current_value = self.get_last_price(self.asset)

        # print(
        #     f"\n{self.get_datetime()}: last price: {current_value}\n "
        #     f"{bars.df[['close', 'SMA15', 'SMA100']]}"
        # )

        # print(
        #     f"{self.get_datetime()}: Symbol: {self.asset.symbol}, Close: {current_value:7.2f}, "
        #     f"sma15: {sma15:7.2f}, sma100: {sma100:7.2f}"
        # )

        logging.info(f"Program thinks it is {self.get_datetime()}")
        logging.info(
            f"The value of {self.asset.symbol} is {current_value}, sma15: {sma15}, sma100: {sma100}"
        )

        all_positions = self.get_tracked_positions()
        if len(all_positions) > 0 and sma15 < sma100:
            for position in all_positions:
                sell_text = (
                    f"\n{self.get_datetime()}: "
                    f"*********SELL*****************\n"
                    f"We own {position.quantity} of {position.symbol}, about to sell"
                )
                print(sell_text)
                logging.info(sell_text)
                selling_order = position.get_selling_order()
                self.submit_order(selling_order)
        elif len(all_positions) > 0:
            logging.info(f"We have open positions waiting for sell signal.")
        else:
            logging.info(f"We have no open positions")

        # We can also do this to sell all our positions:
        # self.sell_all()

        if len(all_positions) == 0 and sma15 > sma100:
            print(f"\n{self.get_datetime()}: *********PURCHASE*****************")
            purchase_order = self.create_order(self.asset, 10, "buy")
            self.submit_order(purchase_order)


    def before_market_closes(self):
        print(f"\nMarket Closing: Sell all if positions.")
        all_positions = self.get_tracked_positions()
        if len(all_positions) > 0:
            self.sell_all()

        # Wait until the end of the day
        self.await_market_to_close()

    def on_abrupt_closing(self):
        self.sell_all()

    def trace_stats(self, context, snapshot_before):
        random_number = random.randint(0, 100)
        row = {"my_custom_stat": random_number}

        return row
