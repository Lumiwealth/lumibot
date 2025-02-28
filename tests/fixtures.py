from typing import List, Any
import os
from zoneinfo import ZoneInfo

import pytest
import logging
import datetime
from pathlib import Path

import pandas as pd

from lumibot import LUMIBOT_SOURCE_PATH
from lumibot.entities import Data, Asset
from lumibot.backtesting import PolygonDataBacktesting
from lumibot.strategies import Strategy

logger = logging.getLogger(__name__)


@pytest.fixture
def polygon_data_backtesting():
    datetime_start = datetime.datetime(2023, 1, 1)
    datetime_end = datetime.datetime(2023, 2, 1)
    api_key = "fake_api_key"
    pandas_data = []
    
    polygon_data_instance = PolygonDataBacktesting(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        pandas_data=pandas_data,
        api_key=api_key,
    )
    
    return polygon_data_instance


@pytest.fixture(scope="function")
def pandas_data_fixture():
    """
    Get a dictionary of Lumibot Data objects from the test data in tests/data folder
    """
    pandas_data = []
    symbols = ["SPY", "TLT", "GLD"]
    quote = Asset(symbol='USD', asset_type="forex")

    lumibot_git_dir = Path(LUMIBOT_SOURCE_PATH).parent
    data_dir = lumibot_git_dir / "data"
    # print(data_dir)

    for symbol in symbols:
        csv_path = data_dir / f"{symbol}.csv"
        asset = Asset(
            symbol=symbol,
            asset_type="stock",
        )

        df = pd.read_csv(
            csv_path,
            parse_dates=True,
            index_col=0,
            header=0,
        )

        df = df.rename(
            columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividend",
            }
        )
        df = df[["open", "high", "low", "close", "volume", "dividend"]]
        df.index.name = "datetime"

        data = Data(
            asset,
            df,
            date_start=datetime.datetime(2019, 1, 2),
            date_end=datetime.datetime(2019, 12, 31),
            timestep="day",
            quote=quote,
        )
        pandas_data.append(data)
    return pandas_data


@pytest.fixture(scope="function")
def pandas_data_fixture_amzn_day():
    return load_pandas_data_from_alpaca_cached_data(
        symbol="AMZN",
        filename="AMZN_DAY.csv",
        timestep="day"
    )


@pytest.fixture(scope="function")
def pandas_data_fixture_amzn_hour():
    return load_pandas_data_from_alpaca_cached_data(
        symbol="AMZN",
        filename="AMZN_HOUR.csv",
        timestep="minute"
    )


@pytest.fixture(scope="function")
def pandas_data_fixture_amzn_minute():
    return load_pandas_data_from_alpaca_cached_data(
        symbol="AMZN",
        filename="AMZN_MINUTE.csv",
        timestep="minute"
    )


@pytest.fixture(scope="function")
def pandas_data_fixture_btc_day():
    return load_pandas_data_from_alpaca_cached_data(
        symbol="BTC",
        filename="BTC-USD_DAY.csv",
        timestep="day",
        asset_type="crypto"
    )


@pytest.fixture(scope="function")
def pandas_data_fixture_btc_hour():
    return load_pandas_data_from_alpaca_cached_data(
        symbol="BTC",
        filename="BTC-USD_HOUR.csv",
        timestep="minute",
        asset_type="crypto"
    )


@pytest.fixture(scope="function")
def pandas_data_fixture_btc_minute():
    return load_pandas_data_from_alpaca_cached_data(
        symbol="BTC",
        filename="BTC-USD_MINUTE.csv",
        timestep="minute",
        asset_type="crypto"
    )


def load_pandas_data_from_alpaca_cached_data(
        symbol: str,
        filename: str,
        timestep: str,
        asset_type: str = "stock"
) -> List[Data]:
    pandas_data = []
    quote = Asset(symbol='USD', asset_type="forex")

    lumibot_git_dir = Path(LUMIBOT_SOURCE_PATH).parent
    csv_path = lumibot_git_dir / "data" / filename
    # print(csv_path)

    asset = Asset(
        symbol=symbol,
        asset_type=asset_type,
    )

    if asset_type == "crypto":
        timezone = ZoneInfo("America/Chicago")
    else:
        timezone = ZoneInfo("America/New_York")

    df = pd.read_csv(
        csv_path,
        parse_dates=True,
        index_col=0,
        header=0,
    )

    df = df[["open", "high", "low", "close", "volume"]]

    data = Data(
        asset,
        df,
        date_start=df.index[0],
        date_end=df.index[-1],
        timestep=timestep,
        quote=quote,
        timezone=timezone,
    )
    pandas_data.append(data)
    return pandas_data


class BuyOneShareTestStrategy(Strategy):

    # Set the initial values for the strategy
    # noinspection PyAttributeOutsideInit
    def initialize(self, parameters: Any = None) -> None:
        self.set_market(self.parameters.get("market", "NYSE"))
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        self.symbol = self.parameters.get("symbol", "AMZN")
        self.market_opens = []
        self.market_closes = []
        self.tracker = {}
        self.num_trading_iterations = 0

    def before_market_opens(self):
        self.log_message(f"Before market opens called at {self.get_datetime().isoformat()}")
        self.market_opens.append(self.get_datetime())

    def after_market_closes(self):
        self.log_message(f"After market closes called at {self.get_datetime().isoformat()}")
        self.market_closes.append(self.get_datetime())
        orders = self.get_orders()
        self.log_message(f"AlpacaBacktestTestStrategy: {len(orders)} orders executed today")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"AlpacaBacktestTestStrategy: Filled Order: {order}")
        self.tracker["filled_at"] = self.get_datetime()
        self.tracker["avg_fill_price"] = order.avg_fill_price

    def on_new_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: New Order: {order}")
        self.tracker["submitted_at"] = self.get_datetime()

    def on_canceled_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: Canceled Order: {order}")

    # noinspection PyAttributeOutsideInit
    def on_trading_iteration(self):
        self.num_trading_iterations += 1
        if self.first_iteration:
            now = self.get_datetime()
            asset = Asset(self.parameters["symbol"])
            current_asset_price = self.get_last_price(asset)

            # Buy 1 shares of the asset for the test
            qty = 1
            self.log_message(f"Buying {qty} shares of {asset} at {current_asset_price} @ {now}")
            order = self.create_order(asset, quantity=qty, side="buy")
            submitted_order = self.submit_order(order)
            self.tracker = {
                "symbol": self.symbol,
                "iteration_at": self.get_datetime(),
                "last_price": current_asset_price,
                "order_id": submitted_order.identifier,
            }

        # Not the 1st iteration, cancel orders.
        else:
            self.cancel_open_orders()
