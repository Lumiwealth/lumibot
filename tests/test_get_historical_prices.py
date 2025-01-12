import os
from datetime import datetime, timedelta
import logging

import pytest

import pandas as pd
import pytz
from pandas.testing import assert_series_equal

from lumibot.backtesting import PolygonDataBacktesting, YahooDataBacktesting
from lumibot.data_sources import AlpacaData, TradierData, PandasData
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset, Bars
from lumibot.tools import get_trading_days

# Global parameters
# API Key for testing Polygon.io
from lumibot.credentials import POLYGON_API_KEY
from lumibot.credentials import TRADIER_CONFIG, ALPACA_CONFIG


logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_display_precision()


def check_bars(
        *,
        bars: Bars,
        length: int = 30,
        check_timezone: bool = True,
):
    """
     This tests:
        - the right number of bars are retrieved
        - the index is a timestamp
        - optionally checks the timezone of the index (alpaca is incorrect)
        - the bars contain returns
    """
    assert len(bars.df) == length
    assert isinstance(bars.df.index[-1], pd.Timestamp)

    if check_timezone:
        assert bars.df.index[-1].tzinfo.zone == "America/New_York"

    assert bars.df["return"].iloc[-1] is not None


class TestDatasourceBacktestingGetHistoricalPricesDailyData:
    """These tests check the daily Bars returned from get_historical_prices for backtesting data sources."""

    length = 30
    ticker = "SPY"
    asset = Asset("SPY")
    timestep = "day"

    @classmethod
    def setup_class(cls):
        pass
        
    # noinspection PyMethodMayBeStatic
    def check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            self, bars: Bars,
            backtesting_start: datetime
    ):
        # The current behavior of the backtesting data sources is to return the data for the
        # last trading day before now. In this case, "now" is the backtesting_start date.
        # So based on the backtesting_start date, the last bar should be the bar from the previous trading day.
        previous_trading_day_date = get_trading_days(
            market="NYSE",
            start_date=backtesting_start - timedelta(days=5),
            end_date=backtesting_start - timedelta(days=1)
        ).index[-1].date()
        assert bars.df.index[-1].date() == previous_trading_day_date

    # noinspection PyMethodMayBeStatic
    def check_dividends_and_adjusted_returns(self, bars):
        assert "dividend" in bars.df.columns
        assert bars.df["dividend"].iloc[-1] is not None

        # assert that there was a dividend paid on 3/15
        assert bars.df["dividend"].loc["2019-03-15"] != 0.0

        # make a new dataframe where the index is Date and the columns are the actual returns
        actual_df = pd.DataFrame(columns=["actual_return"])
        for dt, row in bars.df.iterrows():
            actual_return = row["return"]
            actual_df.loc[dt.date()] = {
                "actual_return": actual_return,
            }

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

    def test_pandas_backtesting_data_source_get_historical_prices_daily_bars(self, pandas_data_fixture):
        """
        This tests that the pandas data_source calculates adjusted returns for bars and that they
        are calculated correctly. It assumes that it is provided split adjusted OHLCV and dividend data.
        """
        backtesting_start = datetime(2019, 3, 26)
        backtesting_end = datetime(2019, 4, 25)
        data_source = PandasData(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )
        bars = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)
        check_bars(bars=bars, length=self.length)
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(bars, backtesting_start=backtesting_start)
        self.check_dividends_and_adjusted_returns(bars)

    @pytest.mark.skip(reason="This test exposes a possible bug in data.py that we have not investigated yet.")
    @pytest.mark.skipif(POLYGON_API_KEY == '<your key here>', reason="This test requires a Polygon.io API key")
    def test_polygon_backtesting_data_source_get_historical_prices_daily_bars(self):
        backtesting_end = datetime.now() - timedelta(days=1)
        backtesting_start = backtesting_end - timedelta(days=self.length * 2 + 5)
        data_source = PolygonDataBacktesting(
            backtesting_start, backtesting_end, api_key=POLYGON_API_KEY
        )
        bars = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)
        check_bars(bars=bars, length=self.length)
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(bars, backtesting_start=backtesting_start)

    def test_yahoo_backtesting_data_source_get_historical_prices_daily_bars(self, pandas_data_fixture):
        """
        This tests that the yahoo data_source calculates adjusted returns for bars and that they
        are calculated correctly. It assumes that it is provided split adjusted OHLCV and dividend data.
        """
        backtesting_start = datetime(2019, 3, 25)
        backtesting_end = datetime(2019, 4, 25)
        data_source = YahooDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )
        bars = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)
        check_bars(bars=bars, length=self.length)
        self.check_dividends_and_adjusted_returns(bars)
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(bars, backtesting_start=backtesting_start)


# @pytest.mark.skip()
class TestDatasourceGetHistoricalPricesDailyData:
    """These tests check the daily Bars returned from get_historical_prices for live data sources."""

    length = 30
    ticker = "SPY"
    asset = Asset("SPY")
    timestep = "day"
    now = datetime.now().astimezone(pytz.timezone("America/New_York"))
    today = now.date()
    trading_days = get_trading_days(market="NYSE", start_date=datetime.now() - timedelta(days=7))

    @classmethod
    def setup_class(cls):
        pass

    def check_date_of_last_bar_is_correct_for_live_data_sources(self, bars):
        """
        Weird test: the results depend on the date and time the test is run.
        If you ask for one bar before the market is closed, you should get the bar from the last trading day.
        If you ask for one bar while the market is open, you should get an incomplete bar for the current day.
        If you ask for one bar after the market is closed, you should get a complete bar from the current trading day.
        """

        if self.today in self.trading_days.index.date:
            market_open = self.trading_days.loc[str(self.today), 'market_open']

            if self.now < market_open:
                # if now is before market open, the bar should from previous trading day
                assert bars.df.index[-1].date() == self.trading_days.index[-2].date()
            else:
                # if now is after market open, the bar should be from today
                assert bars.df.index[-1].date() == self.trading_days.index[-1].date()

        else:
            # if it's not a trading day, the last bar the bar should from the last trading day
            assert bars.df.index[-1].date() == self.trading_days.index[-1].date()

    # @pytest.mark.skip()
    @pytest.mark.skipif(not ALPACA_CONFIG['API_KEY'], reason="This test requires an alpaca API key")
    @pytest.mark.skipif(
        ALPACA_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_alpaca_data_source_get_historical_prices_daily_bars(self):
        data_source = AlpacaData(ALPACA_CONFIG)
        bars = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)

        # Alpaca's time zone is UTC. We should probably convert it to America/New_York
        # Alpaca data source does not provide dividends
        check_bars(bars=bars, length=self.length, check_timezone=False)
        self.check_date_of_last_bar_is_correct_for_live_data_sources(bars)

        # TODO: convert the timezones returned by alpaca to America/New_York
        assert bars.df.index[0].tzinfo == pytz.timezone("UTC")

        # This simulates what the call to get_yesterday_dividends does (lookback of 1)
        bars = data_source.get_historical_prices(asset=self.asset, length=1, timestep=self.timestep)
        check_bars(bars=bars, length=1, check_timezone=False)
        self.check_date_of_last_bar_is_correct_for_live_data_sources(bars)

    # @pytest.mark.skip()
    @pytest.mark.skipif(not TRADIER_CONFIG['ACCESS_TOKEN'], reason="No Tradier credentials provided.")
    def test_tradier_data_source_get_historical_prices_daily_bars(self):
        data_source = TradierData(
            account_number=TRADIER_CONFIG["ACCOUNT_NUMBER"],
            access_token=TRADIER_CONFIG["ACCESS_TOKEN"],
            paper=TRADIER_CONFIG["PAPER"],
        )

        bars = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)
        check_bars(bars=bars, length=self.length)
        self.check_date_of_last_bar_is_correct_for_live_data_sources(bars)

        # This simulates what the call to get_yesterday_dividends does (lookback of 1)
        bars = data_source.get_historical_prices(asset=self.asset, length=1, timestep=self.timestep)
        check_bars(bars=bars, length=1)
        self.check_date_of_last_bar_is_correct_for_live_data_sources(bars)
