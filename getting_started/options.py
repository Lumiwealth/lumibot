import datetime
from pathlib import Path
from time import perf_counter, time

import pandas as pd

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies.strategy import Strategy

"""
This is a sample module used for demonstrating how to implement options 
from csv.

Options data for this backtest is in `/data/options/AAPL` folder. The 
underlying 'AAPL' is in `/data`.
"""


class Options(Strategy):
    def initialize(self, assets, sleep_time=1):
        # Set the initial variables or constants
        self.assets = assets
        self.underlying_asset = assets[0]
        self.option_assets = assets[1:]

        # Built in Variables
        self.minutes_before_closing = 0
        self.sleeptime = sleep_time

        # Our Own Variables
        self.purchase_order = None
        self.traded = False

        # Used to make a single order.
        self.call_option_to_buy = None

        # How many bars?
        self.length = 10

        self.count = -1

    def on_trading_iteration(self):
        self.count += 1
        if self.count <= self.length / self.sleeptime:
            return

        # Get server timestamp
        self.log_message(self.get_datetime())

        # Get the last price for `SPY`
        last_price = self.get_last_price(self.underlying_asset)
        self.log_message(f"This is the last price: {last_price}")

        # Run a basic call for bars on `AAPL`.
        bar = self.get_symbol_bars(self.underlying_asset, self.length)
        self.log_message(bar)

        # Get the dataframe from the `bar` and create new columns.
        self.log_message(f"Trading bars for {self.underlying_asset.symbol} are \n{bar}.")
        if bar is not None:
            df = bar.df
            df["range"] = df["high"] - df["low"]
            df["ma"] = df["close"].rolling(2).mean()
            self.log_message(df[["open", "close", "range", "ma"]])

        # Get an options quote, data is only available after 19th.
        # So `None` is returned before then.
        self.option = self.create_asset(
            symbol="AAPL",
            asset_type="option",
            expiration=datetime.date(2021, 10, 15),
            strike=140,
            right="call",
            multiplier=100,
        )
        option_bar = self.get_symbol_bars(self.option, self.length)
        self.log_message(f"Option bars: {option_bar}")

        # Retrieve the option chains.
        chains = self.get_chains(self.underlying_asset)
        for ex, chain in chains.items():
            self.log_message(f"Full option chains: \n,{ex}, {chains}")

        # Get a single option chain for `SMART` in backtesting this will
        # be the same as `chains` as there are not multiple exchanges.
        chain = self.get_chain(chains, exchange="SMART")
        self.log_message(f"Single option chain: \n{chain}")

        # Get expiration dates for the chain above.
        expirations = self.get_expiration(chains, exchange="SMART")
        self.log_message(f"Expirations:{expirations}")

        # Retrieve the multiplier for this chain.
        multiplier = self.get_multiplier(chains)
        self.log_message("Multiplier:{multiplier}")

        # Strikes are retrieved by searching one expiry date and one right.
        # Create an option asset without strikes.
        strike_asset = self.create_asset(
            symbol="AAPL",
            asset_type="option",
            expiration=datetime.date(2021, 10, 15),
            right="CALL",
            multiplier=100,
            currency="USD",
        )
        strikes = self.get_strikes(strike_asset)
        self.log_message(
            f"These are the strikes for: {strike_asset.symbol} {strike_asset.expiration} "
            f"{strike_asset.right}: {strikes}"
        )

        # Create a buy order for an option and submit. Only once.
        # If there is no data, the order will be cancelled.
        if self.call_option_to_buy:
            return

        self.call_option_to_buy = self.create_asset(
            symbol="AAPL",
            asset_type="option",
            expiration=datetime.date(2021, 9, 24),
            right="CALL",
            strike=144,
            multiplier=100,
            currency="USD",
        )
        option_order = self.create_order(self.call_option_to_buy, 10, "buy")
        self.log_message(f"Order for {option_order} submitted.")
        self.submit_order(option_order)

    def on_canceled_order(self, order):
        self.log_message(
            f"ORDER CANCEL: {self.get_datetime()}, "
            f"Quantity:     0, price:      0, "
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

    # AAPL data is minute from the 15th to the 20th, 0930 - 1559.
    # 18th/19th are weekend.
    # Recommended to have extra data after and before test or test will
    # fail.
    backtesting_start = datetime.datetime(2021, 9, 17)  # Earliest is 15th
    backtesting_end = datetime.datetime(2021, 9, 20)  # up to 20th

    trading_hours_start = datetime.time(9, 30)
    trading_hours_end = datetime.time(16, 00)

    strategy_class = Options

    backtesting_datasource = PandasDataBacktesting

    # Stores all of the assets/datas.
    pandas_data = dict()

    # Store the underlying asset `AAPL`.
    asset = Asset(
        symbol="AAPL",
        asset_type="stock",
    )

    df = pd.read_csv(
        "data/AAPL.csv",
        parse_dates=True,
        index_col=0,
        header=0,
        names=["datetime", "high", "low", "open", "close", "volume"],
    )

    pandas_data[asset] = Data(
        asset,
        df,
        trading_hours_start=trading_hours_start,
        trading_hours_end=trading_hours_end,
        timestep="minute",
    )

    # Load the options data.
    files = Path("data/options/AAPL").glob("*.csv")
    for file in [file for file in files if file.suffix == ".csv"]:
        fn = file.name.split(".")[0]
        filepath = file
        fn_params = fn.split("_")
        symbol = fn_params[0]
        expiry = datetime.datetime.strptime(fn_params[1], "%Y-%m-%d").date()
        right = fn_params[2][:-1].upper()
        strike = fn_params[3]

        asset = Asset(
            symbol=symbol,
            asset_type="option",
            expiration=expiry,
            right=right.upper(),
            strike=strike,
            multiplier=100,
        )

        df = pd.read_csv(
            filepath,
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
            trading_hours_start=trading_hours_start,
            trading_hours_end=trading_hours_end,
            timestep="minute",
        )

    kwargs = {
        "assets": list(pandas_data),
        "sleep_time": 10,
    }
    stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"

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
