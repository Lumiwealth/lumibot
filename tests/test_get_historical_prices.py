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
from lumibot.credentials import TRADIER_CONFIG, ALPACA_CONFIG, POLYGON_CONFIG


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
    def check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            self, bars: Bars,
            backtesting_start: datetime
    ):
        # The current behavior of the backtesting data sources is to return the data for the
        # last trading day before now.
        # To simulate this, we set backtesting_start date to what we want "now" to be.
        # So based on the backtesting_start date, the last bar should be the bar from the previous trading day
        # before the backtesting_start date.

        # Get trading days around the backtesting_start date
        trading_days = get_trading_days(
            market="NYSE",
            start_date=backtesting_start - timedelta(days=5),
            end_date=backtesting_start + timedelta(days=5)
        )

        # find the index of the backtesting_start date in the trading_days
        backtesting_start_index = trading_days.index.get_loc(backtesting_start)

        # get the date of the last trading day before the backtesting_start date
        previous_trading_day_date = trading_days.index[backtesting_start_index - 1].date()
        assert bars.df.index[-1].date() == previous_trading_day_date

    # noinspection PyMethodMayBeStatic
    def check_date_of_last_bar_is_date_of_first_trading_date_on_or_after_backtest_start(
            self,
            bars: Bars,
            backtesting_start: datetime
    ):
        # The backtesting broker needs to look into the future to fill orders.
        # To simulate this, we set backtesting_start date to what we want "now" to be.
        # So the first bar should be the backtesting_start date, or if the
        # backtesting_start date is not a trading day, the first trading day after the backtesting_start date.

        # Get trading days around the backtesting_start date
        trading_days = get_trading_days(
            market="NYSE",
            start_date=backtesting_start - timedelta(days=5),
            end_date=backtesting_start + timedelta(days=5)
        )

        # Check if backtesting_start is in trading_days
        if backtesting_start in trading_days.index:
            backtesting_start_index = trading_days.index.get_loc(backtesting_start)
        else:
            # Find the first trading date after backtesting_start
            backtesting_start_index = trading_days.index.get_indexer([backtesting_start], method='bfill')[0]

        # get the date of the first trading day on or after the backtesting_start date
        first_trading_day_date = trading_days.index[backtesting_start_index].date()
        assert bars.df.index[0].date() == first_trading_day_date

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

    def test_get_first_trading_day_after_long_weekend(self):
        first_trading_day_after_mlk = self.get_first_trading_day_after_long_weekend(2019)
        assert first_trading_day_after_mlk == datetime(2019, 1, 22)

        first_trading_day_after_mlk = self.get_first_trading_day_after_long_weekend(2023)
        assert first_trading_day_after_mlk == datetime(2023, 1, 17)

    def test_pandas_backtesting_data_source_get_historical_prices_daily_bars_dividends_and_adj_returns(self, pandas_data_fixture):
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
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )
        self.check_dividends_and_adjusted_returns(bars)

    def test_pandas_backtesting_data_source_get_historical_prices_daily_bars_for_backtesting_broker(
            self,
            pandas_data_fixture
    ):
        # Test getting 2 bars into the future (which is what the backtesting does when trying to fill orders
        # for the next trading day)
        backtesting_start = datetime(2019, 3, 26)
        backtesting_end = datetime(2019, 4, 25)
        length = 2
        data_source = PandasData(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )
        timeshift = -length  # negative length gets future bars
        bars = data_source.get_historical_prices(
            asset=self.asset,
            length=length,
            timeshift=timeshift,
            timestep=self.timestep
        )
        check_bars(bars=bars, length=length)
        self.check_date_of_last_bar_is_date_of_first_trading_date_on_or_after_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    def test_pandas_backtesting_data_source_get_historical_prices_daily_bars_over_long_weekend(
            self,
            pandas_data_fixture
    ):
        # Get MLK day in 2019
        mlk_day = self.get_mlk_day(2019)

        # First trading day after MLK day
        backtesting_start = mlk_day + timedelta(days=1)
        backtesting_end = datetime(2019, 2, 22)

        # get 10 bars starting from backtesting_start (going back in time)
        length = 10
        data_source = PandasData(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )
        bars = data_source.get_historical_prices(asset=self.asset, length=length, timestep=self.timestep)
        check_bars(bars=bars, length=length)
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    @pytest.mark.skipif(
        not POLYGON_CONFIG["API_KEY"],
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        not POLYGON_CONFIG["IS_PAID_SUBSCRIPTION"],
        reason="This test requires a paid Polygon.io API key"
    )
    def test_polygon_backtesting_data_source_get_historical_prices_daily_bars_for_backtesting_broker(self):
        # Test getting 2 bars into the future (which is what the backtesting does when trying to fill orders
        # for the next trading day)
        last_year = datetime.now().year - 1

        # Get MLK day last year which is a non-trading monday
        mlk_day = self.get_mlk_day(last_year)

        # First trading day after MLK day
        backtesting_start = mlk_day + timedelta(days=1)
        backtesting_end = datetime(last_year, 2, 22)

        data_source = PolygonDataBacktesting(
            backtesting_start, backtesting_end, api_key=POLYGON_CONFIG["API_KEY"]
        )
        
        length = 2
        timeshift = -length  # negative length gets future bars
        bars = data_source.get_historical_prices(
            asset=self.asset,
            length=length,
            timeshift=timeshift,
            timestep=self.timestep
        )
        
        check_bars(bars=bars, length=length)
        self.check_date_of_last_bar_is_date_of_first_trading_date_on_or_after_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    @pytest.mark.skipif(
        not POLYGON_CONFIG["API_KEY"],
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        not POLYGON_CONFIG["IS_PAID_SUBSCRIPTION"],
        reason="This test requires a paid Polygon.io API key"
    )
    def test_polygon_backtesting_data_source_get_historical_prices_daily_bars_over_long_weekend(self):
        # Get MLK day for last year
        last_year = datetime.now().year - 1
        mlk_day = self.get_mlk_day(last_year)

        # First trading day after MLK day
        backtesting_start = mlk_day + timedelta(days=1)
        backtesting_end = datetime(last_year, 2, 22)

        # get 10 bars starting from backtesting_start (going back in time)
        length = 10
        data_source = PolygonDataBacktesting(
            backtesting_start, backtesting_end, api_key=POLYGON_CONFIG["API_KEY"]
        )
        bars = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)
        check_bars(bars=bars, length=self.length)
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    def test_yahoo_backtesting_data_source_get_historical_prices_daily_bars_dividends_and_adj_returns(
            self,
            pandas_data_fixture
    ):
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
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    def test_yahoo_backtesting_data_source_get_historical_prices_daily_bars_for_backtesting_broker(
            self,
            pandas_data_fixture
    ):
        # Test getting 2 bars into the future (which is what the backtesting does when trying to fill orders
        # for the next trading day)
        backtesting_start = datetime(2019, 3, 25)
        backtesting_end = datetime(2019, 4, 25)
        data_source = YahooDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )

        length = 2
        timeshift = -length  # negative length gets future bars
        bars = data_source.get_historical_prices(
            asset=self.asset,
            length=length,
            timeshift=timeshift,
            timestep=self.timestep
        )

        check_bars(bars=bars, length=length)
        self.check_date_of_last_bar_is_date_of_first_trading_date_on_or_after_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    def test_yahoo_backtesting_data_source_get_historical_prices_daily_bars_over_long_weekend(
            self,
            pandas_data_fixture
    ):
        # Get MLK day in 2019
        mlk_day = self.get_mlk_day(2019)

        # First trading day after MLK day
        backtesting_start = mlk_day + timedelta(days=1)
        backtesting_end = datetime(2019, 2, 22)

        # get 10 bars starting from backtesting_start (going back in time)
        length = 10
        data_source = YahooDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )
        bars = data_source.get_historical_prices(asset=self.asset, length=self.length, timestep=self.timestep)
        check_bars(bars=bars, length=self.length)
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )


@pytest.mark.skip()
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

    @pytest.mark.skipif(
        not ALPACA_CONFIG['API_KEY'],
        reason="This test requires an alpaca API key"
    )
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
