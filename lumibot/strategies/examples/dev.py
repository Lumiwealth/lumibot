import logging
import random

import pandas as pd

from lumibot.strategies.strategy import Strategy

"""
This Dev strategy is being used to set up csv ingestion into Lumibot. 
"""


class Dev(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self, assets=None):
        # Set the initial variables or constants

        # 0 will default to day time frame.
        self.sleeptime = 1
        self.minutes_before_closing = 15

        self.kwarg_assets = assets
        self.symbol = assets[0]
        getassets = self.data_source.get_assets()
        self.symbol = getassets[0]
        getassets = self.data_source.get_assets()

        r=1


    def on_trading_iteration(self):
        # What to do each iteration
        bars = self.get_symbol_bars(self.symbol, length=1, timestep="minute")
        sma15 = bars.df["SMA15"][0]
        sma100 = bars.df["SMA100"][0]
        if not sma15:
            return
        current_value = self.get_last_price(self.symbol)

        print(
            f"{self.get_datetime()}: Symbol: {self.symbol}, Close: {current_value:7.2f}, "
            f"sma15: {sma15:7.2f}, sma100: {sma100:7.2f}"
        )

        logging.info(f"Program thinks it is {self.get_datetime()}")
        logging.info(
            f"The value of {self.symbol} is {current_value}, sma15: {sma15}, sma100: {sma100}"
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
            purchase_order = self.create_order(self.symbol, 10, "buy")
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
