import os
from datetime import datetime, timedelta
import logging

import pytest

import pandas as pd
import pytz
from pandas.testing import assert_series_equal

from lumibot.backtesting import PolygonDataBacktesting
from lumibot.data_sources import AlpacaData, TradierData, YahooData, PandasData
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_precision
from lumibot.entities import Asset

# Global parameters
# API Key for testing Polygon.io
from lumibot.credentials import POLYGON_API_KEY
from lumibot.credentials import TRADIER_CONFIG, ALPACA_CONFIG


logger = logging.getLogger(__name__)
# print_full_pandas_dataframes()
# set_pandas_float_precision(precision=15)


class TestDatasourceDailyBars:
    """These tests check that the Bars returned from get_historical_prices.

     They test:
        - the index is a timestamp
        - they contain returns for the different data sources.
        - they return the right number of bars
        - returns are calculated correctly
        - certain data_sources contain dividends

     """

    length = 30
    ticker = "SPY"
    asset = Asset("SPY")
    timestep = "day"
    expected_df = None
    backtesting_start = datetime(2019, 3, 1)
    backtesting_end = datetime(2019, 3, 31)

    @classmethod
    def setup_class(cls):
        # We load the SPY data directly and calculate the adjusted returns.
        file_path = os.getcwd() + "/data/SPY.csv"
        df = pd.read_csv(file_path)
        df.rename(columns={"Date": "date"}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df['expected_return'] = df['Adj Close'].pct_change()
        cls.expected_df = df

    # @pytest.mark.skip()
    @pytest.mark.skipif(not ALPACA_CONFIG['API_KEY'], reason="This test requires an alpaca API key")
    @pytest.mark.skipif(ALPACA_CONFIG['API_KEY'] == '<your key here>', reason="This test requires an alpaca API key")
    def test_alpaca_data_source_daily_bars(self):
        """
        Among other things, this tests that the alpaca data_source calculates SIMPLE returns for bars.
        Since we don't get dividends with alpaca, we are not going to check if the returns are adjusted correctly.
        """
        data_source = AlpacaData(ALPACA_CONFIG)
        prices = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)

        assert isinstance(prices.df.index[0], pd.Timestamp)
        # assert prices.df.index[0].tzinfo.zone == "America/New_York"  # Note, this is different from all others
        assert prices.df.index[0].tzinfo == pytz.timezone("UTC")
        assert len(prices.df) == self.length

        assert isinstance(prices.df.index[0], pd.Timestamp)

        # assert that the last row has a return value
        assert prices.df["return"].iloc[-1] is not None

        # check that there is no dividend column... This test will fail when dividends are added. We hope that's soon.
        assert "dividend" not in prices.df.columns

    # @pytest.mark.skip()
    def test_yahoo_data_source_daily_bars(self):
        """
        This tests that the yahoo data_source calculates adjusted returns for bars and that they
        are calculated correctly.
        """
        start = self.backtesting_start + timedelta(days=25)
        end = self.backtesting_end + timedelta(days=25)
        data_source = YahooData(datetime_start=start, datetime_end=end)
        prices = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)

        assert isinstance(prices.df.index[0], pd.Timestamp)
        assert prices.df.index[0].tzinfo.zone == "America/New_York"
        assert len(prices.df) == self.length

        # assert that the last row has a return value
        assert prices.df["return"].iloc[-1] is not None

        # check that there is a dividend column.
        assert "dividend" in prices.df.columns

        # assert that there was a dividend paid on 3/15
        assert prices.df["dividend"].loc["2019-03-15"] != 0.0

        # make a new dataframe where the index is Date and the columns are the actual returns
        actual_df = pd.DataFrame(columns=["actual_return"])
        for dt, row in prices.df.iterrows():
            actual_return = row["return"]
            actual_df.loc[dt.date()] = {
                "actual_return": actual_return,
            }

        comparison_df = pd.concat(
            [actual_df["actual_return"],
             self.expected_df["expected_return"]],
            axis=1).reindex(actual_df.index)

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

    # @pytest.mark.skip()
    def test_pandas_data_source_daily_bars(self, pandas_data_fixture):
        """
        This tests that the pandas data_source calculates adjusted returns for bars and that they
        are calculated correctly. It assumes that it is provided split adjusted OHLCV and dividend data.
        """
        start = self.backtesting_start + timedelta(days=25)
        end = self.backtesting_end + timedelta(days=25)
        data_source = PandasData(
            datetime_start=start,
            datetime_end=end,
            pandas_data=pandas_data_fixture
        )
        prices = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)
        assert isinstance(prices.df.index[0], pd.Timestamp)
        assert prices.df.index[0].tzinfo.zone == "America/New_York"
        assert len(prices.df) == self.length
        assert prices.df["return"].iloc[-1] is not None

        # check that there is a dividend column.
        assert "dividend" in prices.df.columns

        # assert that there was a dividend paid on 3/15
        assert prices.df["dividend"].loc["2019-03-15"] != 0.0

        # make a new dataframe where the index is Date and the columns are the actual returns
        actual_df = pd.DataFrame(columns=["actual_return"])
        for dt, row in prices.df.iterrows():
            actual_return = row["return"]
            actual_df.loc[dt.date()] = {
                "actual_return": actual_return,
            }

        comparison_df = pd.concat(
            [actual_df["actual_return"],
             self.expected_df["expected_return"]],
            axis=1).reindex(actual_df.index)

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

    # @pytest.mark.skip()
    @pytest.mark.skipif(POLYGON_API_KEY == '<your key here>', reason="This test requires a Polygon.io API key")
    def test_polygon_data_source_daily_bars(self):
        """
        This tests that the po broker calculates SIMPLE returns for bars. Since we don't get dividends with
        alpaca, we are not going to check if the returns are adjusted correctly.
        """
        # get data from  3 months ago, so we can use the free Polygon.io data
        start = datetime.now() - timedelta(days=90)
        end = datetime.now() - timedelta(days=60)
        tzinfo = pytz.timezone("America/New_York")
        start = start.astimezone(tzinfo)
        end = end.astimezone(tzinfo)

        data_source = PolygonDataBacktesting(
            start, end, api_key=POLYGON_API_KEY
        )
        prices = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)

        assert isinstance(prices.df.index[0], pd.Timestamp)
        assert prices.df.index[0].tzinfo.zone == "America/New_York"
        assert len(prices.df) == self.length

        # assert that the last row has a return value
        assert prices.df["return"].iloc[-1] is not None

        assert isinstance(prices.df.index[0], pd.Timestamp)

    @pytest.mark.skipif(not TRADIER_CONFIG['ACCESS_TOKEN'], reason="No Tradier credentials provided.")
    def test_tradier_data_source_generates_simple_returns(self):
        """
        This tests that the po broker calculates SIMPLE returns for bars. Since we don't get dividends with
        tradier, we are not going to check if the returns are adjusted correctly.
        """
        data_source = TradierData(
                account_number=TRADIER_CONFIG["ACCOUNT_NUMBER"],
                access_token=TRADIER_CONFIG["ACCESS_TOKEN"],
                paper=TRADIER_CONFIG["PAPER"],
        )

        prices = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)

        assert isinstance(prices.df.index[0], pd.Timestamp)
        assert prices.df.index[0].tzinfo.zone == "America/New_York"
        assert len(prices.df) == self.length

        # This shows a bug. The index a datetime.date but should be a timestamp
        assert isinstance(prices.df.index[0], pd.Timestamp)

        # assert that the last row has a return value
        assert prices.df["return"].iloc[-1] is not None
