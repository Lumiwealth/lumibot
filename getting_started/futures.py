import datetime
import logging
from time import perf_counter, time

import pandas as pd

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies.strategy import Strategy

"""
This is a sample module used for demonstrating how to implement futures 
from csv.
"""


class Futures(Strategy):
    def initialize(self, assets, sleep_time=1):
        # Set the initial variables or constants
        self.assets = assets

        # Built in Variables
        self.minutes_before_closing = 0
        self.sleeptime = sleep_time

        # Our Own Variables
        self.purchase_order = None
        self.traded = False


    def on_trading_iteration(self):
        # For this example, the futures contracts do not overlap.
        # Cycle through the assets (futures contracts) and find
        # the currently active contract within a user difined data range.
        dt = self.get_datetime()
        self.trading_asset = None
        for asset in self.assets:
            net_days = asset.expiration - dt.date()
            # User defined.
            if datetime.timedelta(days=3) < net_days < datetime.timedelta(days=45):
                self.trading_asset = asset
                self.log_message(
                    f"New iteration: {dt}, asset: {self.trading_asset} "
                    f"{self.get_last_price(self.trading_asset)}"
                )

        all_positions = self.get_tracked_positions()
        self.log_message(f"{dt}:  {all_positions}")

        if len(all_positions) > 0:
            for position in all_positions:
                logging.info(
                    f"We own {position.quantity} of {position.symbol}, about to sell"
                )
                selling_order = position.get_selling_order()
                self.submit_order(selling_order)
                self.traded = False
        else:
            logging.info(f"We have no open positions")

        # Market
        if len(all_positions) == 0 and self.trading_asset and not self.traded:
            self.purchase_order = self.create_order(self.trading_asset, 1, "buy")
            self.submit_order(self.purchase_order)
            self.traded = True
            self.log_message(f"\nOrder created: {dt} for {self.purchase_order.asset.symbol}.")

    def on_canceled_order(self, order):
        self.log_message(
            f"ORDER CANCEL: {self.get_datetime()}, Quantity:     0, price:      0, "
            f"side: {order.side}"
        )

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(
            f"ORDER FILLED: {self.get_datetime()}, Quantity: {quantity:5.0f}, price:"
            f" {price:5.2f}, side: {order.side}"
        )

    def on_abrupt_closing(self):
        self.sell_all()


if __name__ == "__main__":

    logfile = "logs/test.log"
    backtesting_start = datetime.datetime(2020, 10, 8)
    backtesting_end = datetime.datetime(2021, 8, 10)

    trading_hours_start = datetime.time(9, 30)
    trading_hours_end = datetime.time(16, 0)

    strategy_class = Futures

    backtesting_datasource = PandasDataBacktesting

    symbols = {
        ("ES", "Z", 20): {
            "data_start_date": "2020-09-13",
            "data_end_date": "2020-12-14",
            "expiry": datetime.date(2020, 12, 14),
        },
        ("ES", "H", 21): {
            "data_start_date": "2020-12-14",
            "data_end_date": "2021-03-15",
            "expiry": datetime.date(2021, 3, 15),
        },
        ("ES", "M", 21): {
            "data_start_date": "2021-03-14",
            "data_end_date": "2021-06-11",
            "expiry": datetime.date(2021, 6, 11),
        },
        ("ES", "U", 21): {
            "data_start_date": "2021-06-13",
            "data_end_date": "2021-08-17",
            "expiry": datetime.date(2021, 9, 17),
        },
    }

    pandas_data = dict()
    for symbol, dates in symbols.items():
        asset = Asset(
            symbol=symbol[0],
            asset_type="future",
            expiration=dates["expiry"],
            multiplier=50,
        )
        df = pd.read_csv(
            f"data/futures/{''.join([str(s) for s in symbol])}.csv",
            parse_dates=True,
            index_col=0,
            header=0,
            names=["datetime", "high", "low", "open", "close", "volume"],
        )
        df = df[["open", "high", "low", "close", "volume"]]
        df.index = df.index.tz_localize("America/New_York")

        pandas_data[asset] = Data(
            asset,
            df,
            date_start=datetime.datetime.strptime(dates["data_start_date"], "%Y-%m-%d"),
            date_end=datetime.datetime.strptime(dates["data_end_date"], "%Y-%m-%d"),
            trading_hours_start=trading_hours_start,
            trading_hours_end=trading_hours_end,
            timestep="minute",
        )

    kwargs = {
        "assets": list(pandas_data),
        "sleep_time": 30,
    }
    stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"

    ####
    # Run the strategy
    ####
    tic = perf_counter()
    strategy_class.backtest(
        backtesting_datasource,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
        stats_file=stats_file,
        logfile=logfile,
        **kwargs,
    )
    toc = perf_counter()
    print("Elapsed time:", toc - tic)
