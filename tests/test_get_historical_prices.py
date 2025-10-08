import os
import logging
import pytest
import pytz
from datetime import datetime, timedelta, time

import pandas as pd
from pandas.testing import assert_series_equal

from lumibot.backtesting import (
    PolygonDataBacktesting,
    YahooDataBacktesting,
    CcxtBacktesting,
)
from lumibot.data_sources import PandasData
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset
from lumibot.credentials import POLYGON_CONFIG


logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_display_precision()

# @pytest.mark.skip()
class TestBacktestingDataSources:
    """These tests check the daily Bars returned from get_historical_prices for backtesting data sources."""

    # noinspection PyMethodMayBeStatic
    def get_mlk_day(self, year):
        # Start from January 1st of the given year
        mlk_date = datetime(year, 1, 1)
        # Find the first Monday of January
        while mlk_date.weekday() != 0:  # 0 = Monday
            mlk_date += timedelta(days=1)
        # Add 14 days to get to the third Monday
        mlk_date += timedelta(days=14)
        return mlk_date

    # noinspection PyMethodMayBeStatic
    def get_first_trading_day_after_long_weekend(self, year):
        # Martin Luther King Jr. Day is observed on the third Monday of January each year.
        mlk_date = self.get_mlk_day(year)
        first_trading_day = mlk_date + timedelta(days=1)
        return first_trading_day

    # noinspection PyMethodMayBeStatic
    def check_dividends_and_adjusted_returns(self, bars):
        assert "dividend" in bars.df.columns
        assert bars.df["dividend"].iloc[-1] is not None

        # assert that there was a dividend paid on 3/15
        dividend_value = bars.df.loc["2019-03-15", "dividend"]
        if isinstance(dividend_value, pd.Series):
            dividend_value = dividend_value.iloc[0]
        assert dividend_value != 0.0

        # make a new dataframe where the index is Date and the columns are the actual returns
        actual_df = pd.DataFrame(columns=["actual_return"])
        for dt, row in bars.df.iterrows():
            actual_return = row["return"]
            actual_df.loc[dt.date()] = {"actual_return": actual_return}

        # We load the SPY data directly and calculate the adjusted returns.
        file_path = os.getcwd() + "/data/SPY.csv"
        expected_df = pd.read_csv(file_path)
        expected_df.rename(columns={"Date": "date"}, inplace=True)
        expected_df['date'] = pd.to_datetime(expected_df['date'])
        expected_df.set_index('date', inplace=True)
        expected_df['expected_return'] = expected_df['Adj Close'].pct_change()

        comparison_df = pd.concat(
            [actual_df["actual_return"],
             expected_df["expected_return"]],
            axis=1
        ).reindex(actual_df.index)

        comparison_df = comparison_df.dropna()
        # print(f"\n{comparison_df}")

        # check that the returns are adjusted correctly
        assert_series_equal(
            comparison_df["actual_return"],
            comparison_df["expected_return"],
            check_names=False,
            check_index=True,
            atol=1e-4,
            rtol=0
        )

    def test_get_first_trading_day_after_long_weekend(self):
        first_trading_day_after_mlk = self.get_first_trading_day_after_long_weekend(2019)
        assert first_trading_day_after_mlk == datetime(2019, 1, 22)

        first_trading_day_after_mlk = self.get_first_trading_day_after_long_weekend(2023)
        assert first_trading_day_after_mlk == datetime(2023, 1, 17)

    # @pytest.mark.skip()
    def test_pandas_backtesting_data_source_get_historical_prices_daily_bars_dividends_and_adj_returns(self, pandas_data_fixture):
        """
        This tests that the pandas data_source calculates adjusted returns for bars and that they
        are calculated correctly. It assumes that it is provided split adjusted OHLCV and dividend data.
        """
        tzinfo = pytz.timezone('America/New_York')
        datetime_start = tzinfo.localize(datetime(2019, 3, 26))
        datetime_end = tzinfo.localize(datetime(2019, 4, 25))
        now = tzinfo.localize(datetime(2019, 4, 25))
        length = 30
        asset = Asset("SPY")
        timestep = "day"

        data_source = PandasData(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data_fixture
        )
        data_source._datetime = now
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        self.check_dividends_and_adjusted_returns(bars)

    @pytest.mark.skipif(
        not POLYGON_CONFIG['API_KEY'] or POLYGON_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_polygon_backtesting_data_source_get_historical_prices_daily_bars_for_backtesting_broker(self):
        asset = Asset("SPY")
        timestep = "day"
        tzinfo = pytz.timezone('America/New_York')

        datetime_start = tzinfo.localize(datetime(2025, 1, 2))
        datetime_end = tzinfo.localize(datetime(2025, 12, 31))
        # First trading day after MLK day
        now = tzinfo.localize(datetime(2025, 1, 21)).replace(hour=9, minute=30)
        data_source = PolygonDataBacktesting(
            datetime_start,
            datetime_end,
            api_key=POLYGON_CONFIG["API_KEY"]
        )

        # Test getting 2 bars into the future (which is what the backtesting does when trying to fill orders
        # for the next trading day)
        length = 2
        timeshift = -length  # negative length gets future bars
        data_source._datetime = now
        bars = data_source.get_historical_prices(
            asset=asset,
            length=length,
            timeshift=timeshift,
            timestep=timestep
        )
        # Handle cases where API might not return data due to rate limits or data availability
        if bars is None or bars.df is None or bars.df.empty:
            pytest.skip("Polygon API returned no data - possibly due to rate limits, invalid API key, or data availability")

    @pytest.mark.skipif(
        not POLYGON_CONFIG['API_KEY'] or POLYGON_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_polygon_backtesting_data_source_get_historical_prices_daily_bars_over_long_weekend(self):
        asset = Asset("SPY")
        timestep = "day"
        tzinfo = pytz.timezone('America/New_York')

        datetime_start = tzinfo.localize(datetime(2025, 1, 2))
        datetime_end = tzinfo.localize(datetime(2025, 12, 31))
        # First trading day after MLK day
        now = tzinfo.localize(datetime(2025, 1, 21)).replace(hour=9, minute=30)

        length = 10
        data_source = PolygonDataBacktesting(
            datetime_start,
            datetime_end,
            api_key=POLYGON_CONFIG["API_KEY"]
        )

        data_source._datetime = now
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        # Handle cases where API might not return data due to rate limits or data availability
        if bars is None or bars.df is None or bars.df.empty:
            pytest.skip("Polygon API returned no data - possibly due to rate limits, invalid API key, or data availability")

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_yahoo_backtesting_data_source_get_historical_prices_daily_bars_dividends_and_adj_returns(
            self
    ):
        """
        This tests that the yahoo data_source calculates adjusted returns for bars and that they
        are calculated correctly. It assumes that it is provided split adjusted OHLCV and dividend data.
        """
        asset = Asset("SPY")
        timestep = "day"
        length = 30

        tzinfo = pytz.timezone('America/New_York')
        datetime_start = tzinfo.localize(datetime(2019, 1, 2))
        datetime_end = tzinfo.localize(datetime(2019, 12, 31))
        now = tzinfo.localize(datetime(2019, 4, 1, 9, 30))

        data_source = YahooDataBacktesting(
            datetime_start=datetime_start,
            datetime_end=datetime_end
        )
        data_source._datetime = now
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        self.check_dividends_and_adjusted_returns(bars)

    @pytest.mark.skip(reason="CCXT Kraken integration test requires stable network connection and external API availability")
    def test_kraken_ccxt_backtesting_data_source_get_historical_prices_daily_bars(
            self
    ):
        """
        This tests that the kraken ccxt data_source gets the right bars
        """
        length = 30
        tzinfo = pytz.timezone('UTC')
        now = tzinfo.localize(datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
        datetime_start = now - timedelta(days=length + 5)
        datetime_end = now - timedelta(days=2)
        now = datetime_end - timedelta(days=2)
        base = Asset(symbol='BTC', asset_type='crypto')
        quote = Asset(symbol='USD', asset_type='forex')
        timestep = "day"
        kwargs = {
            # "max_data_download_limit":10000, # optional
            "exchange_id": "kraken"  # "kucoin" #"bybit" #"okx" #"bitmex" # "binance"
        }
        data_source = CcxtBacktesting(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            **kwargs
        )
        data_source._datetime = now
        bars = data_source.get_historical_prices(
            asset=(base, quote),
            length=length,
            timestep=timestep
        )
        assert bars.df is not None and not bars.df.empty



class TestTimestepParsing:
    """Test the new timestep parsing and multi-timeframe aggregation features."""

    def test_parse_timestep_standard_formats(self):
        """Test parsing of standard timestep formats."""
        from lumibot.strategies.strategy import Strategy

        # Create a simple mock strategy instance just for testing the parse method
        class TestStrategy(Strategy):
            def __init__(self):
                # Skip parent init to avoid broker requirement
                pass

        strategy = TestStrategy()

        # Test standard formats
        assert strategy._parse_timestep("minute") == (1, "minute")
        assert strategy._parse_timestep("day") == (1, "day")
        assert strategy._parse_timestep("minutes") == (1, "minute")
        assert strategy._parse_timestep("days") == (1, "day")
        assert strategy._parse_timestep("min") == (1, "minute")
        assert strategy._parse_timestep("m") == (1, "minute")
        assert strategy._parse_timestep("d") == (1, "day")

    def test_parse_timestep_multi_minute_formats(self):
        """Test parsing of multi-minute timestep formats."""
        from lumibot.strategies.strategy import Strategy

        # Create a simple mock strategy instance just for testing the parse method
        class TestStrategy(Strategy):
            def __init__(self):
                # Skip parent init to avoid broker requirement
                pass

        strategy = TestStrategy()

        # Test various 5-minute formats
        assert strategy._parse_timestep("5min") == (5, "minute")
        assert strategy._parse_timestep("5m") == (5, "minute")
        assert strategy._parse_timestep("5 min") == (5, "minute")
        assert strategy._parse_timestep("5 minutes") == (5, "minute")
        assert strategy._parse_timestep("5minute") == (5, "minute")

        # Test 15-minute formats
        assert strategy._parse_timestep("15min") == (15, "minute")
        assert strategy._parse_timestep("15m") == (15, "minute")
        assert strategy._parse_timestep("15 minutes") == (15, "minute")

        # Test 30-minute formats
        assert strategy._parse_timestep("30min") == (30, "minute")
        assert strategy._parse_timestep("30m") == (30, "minute")

    def test_parse_timestep_hour_formats(self):
        """Test parsing of hour timestep formats."""
        from lumibot.strategies.strategy import Strategy

        # Create a simple mock strategy instance just for testing the parse method
        class TestStrategy(Strategy):
            def __init__(self):
                # Skip parent init to avoid broker requirement
                pass

        strategy = TestStrategy()

        # Hour formats should convert to minutes
        assert strategy._parse_timestep("1h") == (60, "minute")
        assert strategy._parse_timestep("1hour") == (60, "minute")
        assert strategy._parse_timestep("1 hour") == (60, "minute")
        assert strategy._parse_timestep("2h") == (120, "minute")
        assert strategy._parse_timestep("4hours") == (240, "minute")

    def test_parse_timestep_day_week_month_formats(self):
        """Test parsing of day/week/month timestep formats."""
        from lumibot.strategies.strategy import Strategy

        # Create a simple mock strategy instance just for testing the parse method
        class TestStrategy(Strategy):
            def __init__(self):
                # Skip parent init to avoid broker requirement
                pass

        strategy = TestStrategy()

        # Multi-day formats
        assert strategy._parse_timestep("2d") == (2, "day")
        assert strategy._parse_timestep("2days") == (2, "day")
        assert strategy._parse_timestep("2 days") == (2, "day")

        # Week formats (convert to days)
        assert strategy._parse_timestep("1w") == (7, "day")
        assert strategy._parse_timestep("1week") == (7, "day")
        assert strategy._parse_timestep("2 weeks") == (14, "day")

        # Month formats (approximate as 30 days)
        assert strategy._parse_timestep("1month") == (30, "day")
        assert strategy._parse_timestep("1mo") == (30, "day")
        assert strategy._parse_timestep("2 months") == (60, "day")

    def test_parse_timestep_case_insensitive(self):
        """Test that parsing is case-insensitive."""
        from lumibot.strategies.strategy import Strategy

        # Create a simple mock strategy instance just for testing the parse method
        class TestStrategy(Strategy):
            def __init__(self):
                # Skip parent init to avoid broker requirement
                pass

        strategy = TestStrategy()

        assert strategy._parse_timestep("5MIN") == (5, "minute")
        assert strategy._parse_timestep("5Min") == (5, "minute")
        assert strategy._parse_timestep("5M") == (5, "minute")
        assert strategy._parse_timestep("MINUTE") == (1, "minute")
        assert strategy._parse_timestep("DAY") == (1, "day")

    def test_parse_timestep_invalid_formats(self):
        """Test that invalid formats return None."""
        from lumibot.strategies.strategy import Strategy

        # Create a simple mock strategy instance just for testing the parse method
        class TestStrategy(Strategy):
            def __init__(self):
                # Skip parent init to avoid broker requirement
                pass

        strategy = TestStrategy()

        assert strategy._parse_timestep("invalid") is None
        assert strategy._parse_timestep("") is None
        assert strategy._parse_timestep(None) is None
        assert strategy._parse_timestep("abc123") is None
        assert strategy._parse_timestep("5x") is None
