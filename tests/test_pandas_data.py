from datetime import datetime, timedelta

from lumibot.data_sources import PandasData
from lumibot.entities import Asset

from tests.fixtures import (
    pandas_data_fixture,
    pandas_data_fixture_amzn_day,
    pandas_data_fixture_amzn_minute,
    pandas_data_fixture_btc_hourly
)


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

    def test_pandas_data_fixture_amzn_day(self, pandas_data_fixture_amzn_day):
        assert pandas_data_fixture_amzn_day is not None
        data = pandas_data_fixture_amzn_day[0]
        assert data.asset.symbol == 'AMZN'
        assert data.timestep == 'day'
        assert data.df.index[0].isoformat() == '2025-01-13T00:00:00-05:00'
        assert data.df.index[-1].isoformat() == '2025-01-17T00:00:00-05:00'

    def test_pandas_data_fixture_amzn_minute(self, pandas_data_fixture_amzn_minute):
        assert pandas_data_fixture_amzn_minute is not None
        data = pandas_data_fixture_amzn_minute[0]
        assert data.asset.symbol == 'AMZN'
        assert data.timestep == 'minute'
        assert data.df.index[0].isoformat() == '2025-01-13T04:00:00-05:00'
        assert data.df.index[-1].isoformat() == '2025-01-17T19:59:00-05:00'

    def test_pandas_data_fixture_btc_hourly_in_pandas_data(self, pandas_data_fixture_btc_hourly):
        backtesting_start = datetime(2021, 3, 26)
        backtesting_end = datetime(2021, 4, 25)
        data_source = PandasData(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture_btc_hourly
        )

        data = pandas_data_fixture_btc_hourly[0]
        bars = data_source.get_bars(
            assets=data.asset,
            length=5,
            timestep="minute",
            quote=Asset("USD", asset_type='forex')
        )
        assert len(bars) == 1
        assert bars[data.asset] is not None
        assert bars[data.asset].asset.symbol == 'BTC'
        assert bars[data.asset].df is not None
        assert len(bars[data.asset].df) == 5

        # Test getting the same data with a different key
        asset_tuple = (Asset("BTC", asset_type='crypto'), Asset("USD", asset_type='forex'))
        bars = data_source.get_bars(
            assets=asset_tuple,
            length=5,
            timestep="minute",
        )
        assert len(bars) == 1
        assert bars[asset_tuple] is not None
        assert bars[asset_tuple].asset.symbol == 'BTC'
        assert bars[asset_tuple].df is not None
        assert len(bars[asset_tuple].df) == 5



