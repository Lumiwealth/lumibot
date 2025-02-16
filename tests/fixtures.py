from typing import Dict, List
import os
import pytest
import logging
import datetime

import pandas as pd

from lumibot.entities import Data, Asset
from lumibot.backtesting import PolygonDataBacktesting

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
def pandas_data_fixture() -> List[Data]:
    """
    Get a dictionary of Lumibot Data objects from the test data in tests/data folder
    """
    symbols = ["SPY", "TLT", "GLD"]
    pandas_data = []
    data_dir = os.getcwd() + "/data"
    quote = Asset(symbol='USD', asset_type="forex")
    print(data_dir)
    for symbol in symbols:
        csv_path = data_dir + f"/{symbol}.csv"
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
def pandas_data_fixture_amzn_day() -> List[Data]:
    return pandas_data_from_alpaca_cached_data(
        symbol="AMZN",
        filename="AMZN_1D.csv",
        timestep="day"
    )


@pytest.fixture(scope="function")
def pandas_data_fixture_amzn_minute() -> List[Data]:
    return pandas_data_from_alpaca_cached_data(
        symbol="AMZN",
        filename="AMZN_1M.csv",
        timestep="minute"
    )


def pandas_data_from_alpaca_cached_data(symbol: str, filename: str, timestep: str) -> List[Data]:
    pandas_data = []
    data_dir = os.getcwd() + "/data"
    quote = Asset(symbol='USD', asset_type="forex")
    csv_path = data_dir + f"/" + filename
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

    df = df[["open", "high", "low", "close", "volume"]]

    data = Data(
        asset,
        df,
        date_start=df.index[0],
        date_end=df.index[-1],
        timestep=timestep,
        quote=quote,
    )
    pandas_data.append(data)
    return pandas_data
