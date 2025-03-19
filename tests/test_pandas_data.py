from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import pytest
import pytz

import pandas as pd
from lumibot.data_sources import PandasData
from lumibot.entities import Asset

from tests.fixtures import (
    pandas_data_fixture,
    pandas_data_fixture_amzn_day,
    pandas_data_fixture_amzn_hour,
    pandas_data_fixture_amzn_minute,
    pandas_data_fixture_btc_day,
    pandas_data_fixture_btc_hour,
    pandas_data_fixture_btc_minute,
    BaseDataSourceTester
)


class TestPandasData(BaseDataSourceTester):

    def test_pandas_data_fixture(self, pandas_data_fixture):
        assert pandas_data_fixture is not None
        assert pandas_data_fixture[0] is not None

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
        assert data.df.index[0].isoformat() == '2021-01-04T00:00:00-05:00'
        assert data.df.index[-1].isoformat() == '2021-12-31T00:00:00-05:00'

    def test_pandas_data_fixture_amzn_hour(self, pandas_data_fixture_amzn_hour):
        assert pandas_data_fixture_amzn_hour is not None
        data = pandas_data_fixture_amzn_hour[0]
        assert data.asset.symbol == 'AMZN'
        assert data.timestep == 'minute'  # hourly data is minute timestep but missing 59 out of 60 bars.
        assert data.df.index[0].isoformat() == '2020-12-31T19:00:00-05:00'
        assert data.df.index[-1].isoformat() == '2021-12-31T17:00:00-05:00'

    def test_pandas_data_fixture_amzn_minute(self, pandas_data_fixture_amzn_minute):
        assert pandas_data_fixture_amzn_minute is not None
        data = pandas_data_fixture_amzn_minute[0]
        assert data.asset.symbol == 'AMZN'
        assert data.timestep == 'minute'
        assert data.df.index[0].isoformat() == '2020-12-31T19:01:00-05:00'
        assert data.df.index[-1].isoformat() == '2021-01-08T17:58:00-05:00'

    def test_pandas_data_fixture_btc_day(self, pandas_data_fixture_btc_day):
        assert pandas_data_fixture_btc_day is not None
        data = pandas_data_fixture_btc_day[0]
        assert data.asset.symbol == 'BTC'
        assert data.timestep == 'day'
        assert data.df.index[0].isoformat() == '2021-01-01T00:00:00-06:00'
        assert data.df.index[-1].isoformat() == '2021-12-31T00:00:00-06:00'

    def test_pandas_data_fixture_btc_hour(self, pandas_data_fixture_btc_hour):
        assert pandas_data_fixture_btc_hour is not None
        data = pandas_data_fixture_btc_hour[0]
        assert data.asset.symbol == 'BTC'
        assert data.timestep == 'minute'
        assert data.df.index[0].isoformat() == '2021-01-01T00:00:00-06:00'
        assert data.df.index[-1].isoformat() == '2021-12-31T17:00:00-06:00'

    def test_pandas_data_fixture_btc_minute(self, pandas_data_fixture_btc_minute):
        assert pandas_data_fixture_btc_minute is not None
        data = pandas_data_fixture_btc_minute[0]
        assert data.asset.symbol == 'BTC'
        assert data.timestep == 'minute'
        assert data.df.index[0].isoformat() == '2021-01-01T00:00:00-06:00'
        assert data.df.index[-1].isoformat() == '2021-01-04T17:59:00-06:00'

    def test_pandas_data_fixture_amzn_day_in_pandas_data(self, pandas_data_fixture_amzn_day):
        data = pandas_data_fixture_amzn_day[0]
        assert data.df.index[-1].isoformat() == '2021-12-31T00:00:00-05:00'

        # start at the last day of data in the fixture and
        datetime_start = data.df.index[-1]

        # end it sometime after (it doesn't matter, we're not doing a backtest)
        datetime_end = datetime_start + timedelta(days=1)

        data_source = PandasData(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data_fixture_amzn_day
        )

        length = 2
        bars = data_source.get_bars(
            assets=data.asset,
            length=length,
            timestep="day",
            quote=Asset("USD", asset_type='forex')
        )
        assert len(bars) == 1
        assert bars[data.asset] is not None
        assert bars[data.asset].asset.symbol == 'AMZN'
        assert bars[data.asset].df is not None
        assert len(bars[data.asset].df) == length
        df = bars[data.asset].df
        assert df.index[0].isoformat() == '2021-12-29T00:00:00-05:00'
        assert df.index[-1].isoformat() == '2021-12-30T00:00:00-05:00'

        # Check that the last day of bars data is the day before the datetime_start
        assert df.index[-1] == datetime_start - timedelta(days=1)

    def test_pandas_data_fixture_btc_day_in_pandas_data(self, pandas_data_fixture_btc_day):
        data = pandas_data_fixture_btc_day[0]

        # start at the last day of data in the fixture and
        datetime_start = data.df.index[-1]

        # end it sometime after (it doesn't matter, we're not doing a backtest)
        datetime_end = datetime_start + timedelta(days=1)
        data_source = PandasData(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data_fixture_btc_day
        )

        length = 5
        bars = data_source.get_bars(
            assets=data.asset,
            length=length,
            timestep="day",
            quote=Asset("USD", asset_type='forex')
        )
        assert len(bars) == 1
        assert bars[data.asset] is not None
        assert bars[data.asset].asset.symbol == 'BTC'
        assert bars[data.asset].df is not None
        assert len(bars[data.asset].df) == length

        df = bars[data.asset].df
        assert df.index[0].isoformat() == '2021-12-26T00:00:00-06:00'
        assert df.index[-1].isoformat() == '2021-12-30T00:00:00-06:00'

        # Check that the last day of bars data is the day before the datetime_start
        assert df.index[-1] == datetime_start - timedelta(days=1)

        # Test getting the same data with a different key
        asset_tuple = (Asset("BTC", asset_type='crypto'), Asset("USD", asset_type='forex'))
        bars = data_source.get_bars(
            assets=asset_tuple,
            length=5,
            timestep="day",
        )
        assert len(bars) == 1
        assert bars[asset_tuple] is not None
        assert bars[asset_tuple].asset.symbol == 'BTC'
        assert bars[asset_tuple].df is not None
        assert len(bars[asset_tuple].df) == 5

    def test_pandas_data_fixture_spy_day_in_pandas_data_issue_test(self, pandas_data_fixture):
        data = pandas_data_fixture[0]
        datetime_start = datetime(2019, 6, 18)
        datetime_end = datetime(2019, 6, 22)
        data_source = PandasData(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data_fixture
        )

        length = 30
        bars = data_source.get_bars(
            assets=data.asset,
            length=length,
            timestep="day",
            quote=Asset("USD", asset_type='forex')
        )
        assert len(bars) == 1
        assert bars[data.asset] is not None
        assert bars[data.asset].asset.symbol == 'SPY'
        assert bars[data.asset].df is not None
        assert len(bars[data.asset].df) == length

        df = bars[data.asset].df
        assert df.index[-1].isoformat() == '2019-06-17T00:00:00-04:00'

        # Check that the last day of bars data is the day before the datetime_start
        datetime_start_ny = datetime_start.replace(tzinfo=ZoneInfo("America/New_York"))
        assert df.index[-1] == datetime_start_ny - timedelta(days=1)

    def test_pandas_data_fixture_btc_hour_in_pandas_data(self, pandas_data_fixture_btc_hour):
        data = pandas_data_fixture_btc_hour[0]

        # start at the last day of data in the fixture and
        datetime_start = data.df.index[-1]

        # end it sometime after (it doesn't matter, we're not doing a backtest)
        datetime_end = datetime_start + timedelta(days=1)
        data_source = PandasData(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data_fixture_btc_hour
        )

        data = pandas_data_fixture_btc_hour[0]
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

        df = bars[data.asset].df
        assert df.index[-1].isoformat() == '2021-12-31T16:00:00-06:00'

        # Check that the last bar of data is the bar before the datetime_start
        assert df.index[-1] == datetime_start - timedelta(hours=1)

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

    def test_empty_pandas_data(self):
        datetime_start = datetime(2019, 6, 18)
        datetime_end = datetime(2019, 6, 22)
        pandas_data = PandasData(datetime_start, datetime_end)
        assert pandas_data

    # @pytest.mark.skip()
    def test_pandas_backtesting_data_source_get_historical_prices_daily(
            self,
            pandas_data_fixture
    ):
        tzinfo = pytz.timezone('America/New_York')
        datetime_start = tzinfo.localize(datetime(2019, 1, 2))
        datetime_end = tzinfo.localize(datetime(2019, 12, 31))
        asset = Asset("SPY")
        timestep = "day"

        data_source = PandasData(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data_fixture
        )

        # First trading day after MLK day
        now = tzinfo.localize(datetime(2019, 1, 22)).replace(hour=9, minute=30)
        data_source._datetime = now

        for length in [1, 10]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            # TODO: PandasData doesn't return the incomplete daily bar for the day like most live data sources.
            # self.check_daily_bars(
            #     bars=bars,
            #     now=now,
            #     data_source_tz=data_source._tzinfo,
            #     time_check=time(0, 0),
            # )

    # @pytest.mark.skip()
    def test_pandas_backtesting_data_source_get_historical_prices_daily_bars_for_backtesting_broker(
            self,
            pandas_data_fixture
    ):
        tzinfo = pytz.timezone('America/New_York')
        datetime_start = tzinfo.localize(datetime(2019, 3, 26))
        datetime_end = tzinfo.localize(datetime(2019, 4, 25))

        asset = Asset("SPY")
        timestep = "day"

        data_source = PandasData(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data_fixture
        )

        now = tzinfo.localize(datetime(2019, 4, 25))
        data_source._datetime = now

        # Test getting 2 bars into the future (which is what the backtesting does when trying to fill orders
        # for the next trading day)
        length = 2
        timeshift = -length  # negative length gets future bars
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, timeshift=timeshift)
        self.check_length(bars=bars, length=length)
        self.check_columns(bars=bars)
        self.check_index(bars=bars, data_source_tz=data_source._tzinfo)