import datetime
from datetime import date
from operator import itemgetter
from pathlib import Path

import pandas as pd

from lumibot.entities import Asset, Data
from lumibot.brokers import InteractiveBrokers
from lumibot.backtesting import PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

from credentials import InteractiveBrokersConfig


# noinspection PyMethodOverriding
class Singles(Strategy):
    """Strategy description: Exploring Greeks.

    Description
        Exploring greeks with some simple single option transactions.
        The objective is to be able to place single options contract
        orders and establish positions without coupling to other orders
        or having the impact of the underlying.

        With this module you can do the following:
        - use greeks
        - select a contract using greeks, expiration, strikes.
        - sell based bracket order.
        - buy or sell an option,
        - set the quantity
        - can enter a range for expirations, strikes,
        - can sort ascending or descending.

    """

    IS_BACKTESTABLE = True

    def initialize(
        self,
        symbol,
        interval="15S",
        right="CALL",
        side="buy",
        expiry_low=date(2021, 11, 1),
        expiry_high=date(2021, 11, 15),
        expiry_reverse=False,
        strike_low=135,
        strike_high=150,
        strike_reverse=False,
        quantity=1,
    ):
        self.sleeptime = interval
        self.asset = self.create_asset(symbol)
        self.right = right
        self.side = side
        self.quantity = quantity

        self.expiry_low = expiry_low
        self.expiry_high = expiry_high
        self.expiry_reverse = expiry_reverse
        self.strike_low = strike_low
        self.strike_high = strike_high
        self.strike_reverse = strike_reverse

        self.chains = None

    def on_trading_iteration(self):
        positions = self.get_tracked_positions()
        # Generate log of greeks for one option held only.
        if len(positions) > 0:
            op = [
                position
                for position in positions
                if position.asset.asset_type == "option"
            ]
            if len(op) == 1:
                op = op[0]
                op_greeks = self.get_greeks(op.asset)
                op_greeks = {
                    k: round(v, 4) for k, v in op_greeks.items() if v is not None
                }
                self.log_message(f"{op},{ op_greeks}")

        # Selling criteria.
        pass

        # Get chains
        if not self.chains:
            self.chains = self.get_chains(self.asset)

        # Get expiries.
        self.expiries = self.get_expiration(self.chains, "SMART")

        selected_expiries = [
            exp
            for exp in self.expiries
            if ((exp > self.expiry_low) and (exp < self.expiry_high))
        ]
        selected_expiries.sort(reverse=self.expiry_reverse)
        self.log_message(f"Selected expiries:{selected_expiries}")
        max_delta = 0
        trading_option = None
        track = {}
        for exp in selected_expiries:
            contract = self.create_asset(
                symbol=self.asset.symbol,
                asset_type="option",
                expiration=exp,
                right=self.right,
                multiplier=100,
            )
            strikes = self.get_strikes(contract)
            selected_strikes = [
                strike
                for strike in strikes
                if (strike > self.strike_low) and (strike < self.strike_high)
            ]

            self.log_message(
                f"Expiry and strikes: {self.asset}, {exp},{selected_strikes}"
            )

            # Do not purchase more than one options.
            if len(positions) > 0:
                return

            # Get greeks
            for strike in selected_strikes:
                option_asset = self.create_asset(
                    symbol=self.asset.symbol,
                    asset_type="option",
                    expiration=exp,
                    right=self.right,
                    strike=strike,
                    multiplier=100,
                )
                greek = self.get_greeks(option_asset)
                track[option_asset] = itemgetter("delta", "theta")(greek)
                delta = greek["delta"]
                vega = greek["vega"]
                iv = greek["implied_volatility"]
                if abs(delta) > max_delta:
                    max_delta = delta
                    trading_option = option_asset
                self.log_message(f"{delta} {vega} {iv} ")
        for k, v in track.items():
            self.log_message(f"{k}, {v}")
        order = self.create_order(trading_option, self.quantity, side=self.side)
        self.submit_order(order)


    def before_market_closes(self):
        self.sell_all()

    def on_abrupt_closing(self):
        self.sell_all()


# noinspection PyUnboundLocalVariable
def main(backtest=False):
    strategy_class = Singles

    symbol = "AAPL"

    if backtest:
        backtesting_start = datetime.datetime(2021, 9, 15)  # Earliest is 5th
        backtesting_end = datetime.datetime(2021, 9, 21)  # up to 20th

        trading_hours_start = datetime.time(9, 30)
        trading_hours_end = datetime.time(16, 00)

        backtesting_datasource = PandasDataBacktesting

        # Stores all of the assets/datas.
        pandas_data = dict()

        # Store the underlying stock asset.
        asset = Asset(
            symbol=symbol,
            asset_type="stock",
        )
        datadir = Path("data")
        df = pd.read_csv(
            datadir / f"{symbol}.csv",
            parse_dates=True,
            index_col=0,
            header=0,
            names=["datetime", "high", "low", "open", "close", "volume"],
        )

        pandas_data[asset] = Data(
            asset,
            df,
            date_start=backtesting_start,
            date_end=backtesting_end,
            trading_hours_start=trading_hours_start,
            trading_hours_end=trading_hours_end,
            timestep="minute",
        )

        # Load the options data.
        filesdir = Path(f"data/options/{symbol}")
        files = sorted(list(filesdir.glob("*.csv")))
        for file in files:  # [file for file in files if file.suffix == ".csv"]:
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
            # df.index = df.index.tz_localize("America/New_York")

            pandas_data[asset] = Data(
                asset,
                df,
                date_start=backtesting_start,
                date_end=backtesting_end,
                trading_hours_start=trading_hours_start,
                trading_hours_end=trading_hours_end,
                timestep="minute",
            )
    # optimization
    kwargs = {
        "symbol": symbol,
        "interval": 30,
        "right": "CALL",
        "quantity": 10,
        "side": "buy",
        "expiry_low": date(2021, 11, 2),
        "expiry_high": date(2021, 11, 15),
        "expiry_reverse": False,
        "strike_low": 152,
        "strike_high": 156,
        "strike_reverse": False,
    }

    if not backtest:
        # Initialize all our classes
        trader = Trader()
        broker = InteractiveBrokers(InteractiveBrokersConfig)

        strategy_class = Singles(
            broker=broker, **kwargs
        )
        trader.add_strategy(strategy_class)
        trader.run_all()

    elif backtest:
        strategy_class.backtest(
            backtesting_datasource,
            backtesting_start,
            backtesting_end,
            pandas_data=pandas_data,
            **kwargs,
        )


if __name__ == "__main__":
    main(backtest=True)
