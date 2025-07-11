import logging
import pytest
import math
from datetime import datetime, timedelta, time
import pytz

import pandas as pd

from lumibot.data_sources import YahooData
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset
from lumibot.tools import get_trading_days
from tests.fixtures import BaseDataSourceTester

logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_display_precision()


# @pytest.mark.skip()
class TestYahooData(BaseDataSourceTester):

    def _create_data_source(self):
        return YahooData()

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_get_historical_prices_daily_bars_over_long_weekend(self):
        tzinfo = pytz.timezone('America/New_York')
        datetime_start = tzinfo.localize(datetime(2019, 1, 2))
        datetime_end = tzinfo.localize(datetime(2019, 12, 31))
        asset = Asset("SPY")
        timestep = "day"

        length = 10
        data_source = YahooData(datetime_start, datetime_end)

        # First trading day after MLK day
        now = tzinfo.localize(datetime(2019, 1, 22)).replace(hour=9, minute=30)
        data_source._datetime = now
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        self.check_length(bars=bars, length=length)
        self.check_columns(bars=bars)
        self.check_index(bars=bars)
        # TODO: YahooData doesn't return the incomplete daily bar for the day like most live data sources.
        # self.check_daily_bars(
        #     bars=bars,
        #     now=now,
        #     data_source_tz=data_source.tzinfo,
        #     time_check=time(0, 0),
        # )

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_get_historical_prices_daily_bars_for_backtesting_broker(self):
        # Simulate what the backtesting broker does for yahoo
        tzinfo = pytz.timezone('America/New_York')
        datetime_start = tzinfo.localize(datetime(2019, 1, 2))
        datetime_end = tzinfo.localize(datetime(2019, 12, 31))
        asset = Asset("SPY")
        timestep = "day"
        length = 1
        timeshift = timedelta(
            days=-1
        )

        data_source = YahooData(datetime_start, datetime_end)
        now = tzinfo.localize(datetime(2019, 4, 1, 9, 30))
        data_source._datetime = now

        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, timeshift=timeshift)
        self.check_length(bars=bars, length=length)
        self.check_columns(bars=bars)
        self.check_index(bars=bars)
        # TODO: yahoo data is indexed at 4pm, so even this, which correctly gets the current bar,
        # fails the check that the bar is before now and not from the future.
        # self.check_daily_bars(
        #     bars=bars,
        #     now=now,
        #     data_source_tz=data_source.tzinfo,
        #     time_check=time(0, 0),
        # )

    def test_format_index_symbol(self):
        """Test that index symbols are properly formatted for Yahoo Finance"""
        data_source = YahooData()

        # Test SPX -> ^SPX
        symbols = data_source._format_index_symbol("SPX")
        assert "^SPX" in symbols
        assert "SPX" in symbols

        # Test ^SPX -> ^SPX (already formatted)
        symbols = data_source._format_index_symbol("^SPX")
        assert "^SPX" in symbols
        assert "SPX" in symbols

        # Test other common indexes
        symbols = data_source._format_index_symbol("DJI")
        assert "^DJI" in symbols
        assert "DJI" in symbols

    @pytest.mark.xfail(reason="Depends on external Yahoo Finance API")
    def test_get_historical_prices_index_asset(self):
        """Test getting historical prices for index assets"""
        tzinfo = pytz.timezone('America/New_York')
        datetime_start = tzinfo.localize(datetime(2019, 1, 2))
        datetime_end = tzinfo.localize(datetime(2019, 12, 31))

        # Test with SPX index
        asset = Asset("SPX", asset_type="index")
        timestep = "day"
        length = 10

        data_source = YahooData(datetime_start, datetime_end)

        # Set the datetime to a valid trading day
        now = tzinfo.localize(datetime(2019, 1, 22)).replace(hour=9, minute=30)
        data_source._datetime = now

        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)

        # Basic validation
        self.check_length(bars=bars, length=length)
        self.check_columns(bars=bars)
        self.check_index(bars=bars)

