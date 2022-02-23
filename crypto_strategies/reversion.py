import datetime
from decimal import Decimal
import pandas as pd
import pandas_ta as ta
from pathlib import Path

from lumibot.backtesting import PandasDataBacktesting
from lumibot.brokers.ccxt import Ccxt
from lumibot.entities import Asset, Data
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

from credentials import CcxtConfig


class Momentum(Strategy):
    def initialize(self, df=None, crypto=None):
        if self.is_backtesting:
            self.set_parameter_defaults(
                {
                    "crypto": crypto,
                    "target_units": Decimal("1.0"),
                    "bb_period": 30,
                    "bb_std": 2,
                    "rsi_period": 10,
                    "rsi_high": 75,
                    "rsi_low": 25,
                }
            )
        else:
            self.set_parameter_defaults(
                {
                    "base": self.create_asset(
                        symbol=crypto.split("-")[0], asset_type="crypto"
                    ),
                    "quote": self.create_asset(
                        symbol=crypto.split("-")[1], asset_type="crypto"
                    ),
                    "target_units": Decimal("1.0"),
                }
            )
            self.parameters["crypto"] = (
                self.parameters["base"],
                self.parameters["quote"],
            )

        self.sleeptime = 1
        self.set_market("NASDAQ")

        self.signal = self.create_indicators(df)


    def on_trading_iteration(self):
        """ Buy with signal 1, sell with signal -1, hold 0
        oco orders.
        """
        x=1
        if self.get_orders():
            return
        date = self.get_datetime()

        signal = self.signal.loc[date][0]

        if len(self.get_positions()) > 0:
            if signal == -1:
                self.sell_all()
            else:
                return

        if signal != 1:
            return

        last_price = self.get_last_price(self.parameters['crypto'])
        cash = self.cash * .99
        quantity = Decimal(cash / last_price).quantize(Decimal("0.00000001"))

        order = self.create_order(self.parameters['crypto'], quantity, "buy")
        self.submit_order(order)


    def create_indicators(self, df):
        """Precalculate the trading signals."""

        ReversionStrategy = ta.Strategy(
            name="Reversion",
            ta=[
                {
                    "kind": "rsi",
                    "length": self.parameters["rsi_period"],
                    "col_names": ("rsi",),
                },
                {
                    "kind": "bbands",
                    "length": self.parameters["bb_period"],
                    "std": self.parameters["bb_std"],
                    "col_names": ("lower", "mid", "upper", "bandwidth", "percent"),
                },
            ],
        )
        df = df.copy()
        df.ta.strategy(ReversionStrategy)

        # Calculate RSI
        df['rsi_signal'] = 0
        df.loc[df['rsi'] > self.parameters['rsi_high'], 'rsi_signal'] = -1
        df.loc[df['rsi'] < self.parameters['rsi_low'], 'rsi_signal'] = 1

        # Calculate Bollinger Bands
        df['bb_signal'] = 0
        df.loc[df['close'] > df['upper'], 'bb_signal'] = -1
        df.loc[df['close'] < df['lower'], 'bb_signal'] = 1

        df = df.drop(['bandwidth', 'percent'], axis=1)
        # df = df.dropna()

        # Trade signal when to signals in a row.
        df["signal"] = 0
        df["combined_signal"] = df["rsi_signal"] + df["bb_signal"]
        df["signal_shift"] = df['combined_signal'] + df['combined_signal'].shift()
        df = df.dropna()
        df.loc[df['signal_shift'] == 4, "signal"] = 1
        df.loc[df['signal_shift'] == -4, "signal"] = -1

        return df[["signal"]]


def run(live=False):
    strategy = Momentum

    if not live:
        # Backtest this strategy
        backtesting_start = datetime.datetime(2020, 6, 1)
        backtesting_end = datetime.datetime(2020, 10, 31)

        backtesting_datasource = PandasDataBacktesting

        # Development: Minute Data

        # symbols = ["BTC", "ETH", "BNB", "NEO", "LTC"]
        symbol = "BTC"
        pandas_data = dict()
        asset = Asset(symbol=symbol, asset_type="stock")

        data_dir = Path("/media/runout/run-out-ssd/data/crypto_data")
        filename = f"{symbol}-USDT.parquet"
        filepath = str(data_dir / filename)
        df = pd.read_parquet(filepath)
        df.index.name = "datetime"
        df.index = df.index.tz_localize("UTC")
        df = df.sort_index()
        df = df[["open", "high", "low", "close", "volume"]]
        df = df.dropna()

        pandas_data[asset] = Data(
            asset,
            df,
            date_start=backtesting_start,
            date_end=backtesting_end,
            timestep="minute",
        )

        strategy.backtest(
            backtesting_datasource,
            backtesting_start,
            backtesting_end,
            df=df.loc[backtesting_start - datetime.timedelta(days=5): backtesting_end, :],
            pandas_data=pandas_data,
            benchmark_asset="BTC-USD",
            budget=50000,
            name="Mean Reversion",
            crypto=asset,
            show_plot=True,
            show_tearsheet=True,
        )

    else:
        trader = Trader()
        exchange_id = "coinbasepro_bitcoin"
        broker = Ccxt(CcxtConfig.EXCHANGE_KEYS[exchange_id])

        strategy = Momentum(broker, crypto="BTC-USD")

        trader.add_strategy(strategy)
        trader.run_all()


if __name__ == "__main__":
    run(live=False)
