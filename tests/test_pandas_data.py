from datetime import datetime, timedelta

import pandas as pd

from lumibot.data_sources import PandasData
from lumibot.entities import Asset
from lumibot.entities.data import Data

from tests.fixtures import pandas_data_fixture


class TestPandasData:

    def test_pandas_data_fixture(self, pandas_data_fixture):
        assert pandas_data_fixture is not None

    def test_spy_has_dividends(self, pandas_data_fixture):
        spy = pandas_data_fixture[0]
        expected_columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividend",
        ]
        assert spy.df.columns.tolist() == expected_columns

    def test_get_start_datetime_and_ts_unit(self):
        start = datetime(2023, 3, 25)
        end = datetime(2023, 4, 5)
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        length = 30
        timestep = '1day'
        start_datetime, ts_unit = data_source.get_start_datetime_and_ts_unit(
            length,
            timestep,
            start,
            start_buffer=timedelta(days=0)  # just test our math
        )
        extra_padding_days = (length // 5) * 3
        expected_datetime = datetime(2023, 3, 25) - timedelta(days=length + extra_padding_days)
        assert start_datetime == expected_datetime

    def test_data_get_quote_handles_missing_bid_ask(self):
        idx = pd.date_range("2024-01-01", periods=1, freq="D")
        df = pd.DataFrame(
            {
                "open": [1.0],
                "high": [1.2],
                "low": [0.9],
                "close": [1.1],
                "volume": [1000],
            },
            index=idx,
        )
        asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
        data = Data(asset=asset, df=df, quote=quote, timestep="day")

        quote_dict = data.get_quote(data.datetime_start)

        assert quote_dict["bid"] is None
        assert quote_dict["ask"] is None
        assert quote_dict["open"] == 1.0
