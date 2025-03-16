import os
import logging
import pytest
import math
from datetime import datetime, timedelta, time
import pytz

import pandas as pd
from pandas.testing import assert_series_equal

from lumibot.backtesting import (
    PolygonDataBacktesting,
    YahooDataBacktesting,
    CcxtBacktesting,
    AlpacaBacktesting
)
from lumibot.data_sources import AlpacaData, TradierData, PandasData
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset, Bars
from lumibot.tools import get_trading_days
from lumibot.credentials import TRADIER_TEST_CONFIG, ALPACA_TEST_CONFIG, POLYGON_CONFIG

logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_display_precision()


def check_bars(
        *,
        bars: Bars,
        length: int = 30,
        data_source_tz: pytz.timezone = None,
        time_check: time | None = None,
):
    """
     This tests:
        - the right number of bars are retrieved
        - the index is a timestamp
        - data_source_tz: pytz.timezone, if set checks that the index's timezone matches
        - if time_check, check the hour and minute of the timestamp
    """
    assert len(bars.df) == length
    assert isinstance(bars.df.index[-1], pd.Timestamp)

    if data_source_tz:
        assert bars.df.index[-1].tzinfo.zone == data_source_tz.zone

    assert bars.df["return"].iloc[-1] is not None

    if time_check:
        timestamp = bars.df.index[-1]
        assert timestamp.hour == time_check.hour
        assert timestamp.minute == time_check.minute


# @pytest.mark.skip()
class TestLiveDataSource:
    """These tests check the daily Bars returned from get_historical_prices for live data sources."""

    # noinspection PyMethodMayBeStatic
    def check_date_of_last_bar_is_correct_for_live_data_sources(self, bars, market='NYSE'):
        now = datetime.now(tz=pytz.timezone("America/New_York"))
        today = now.date()

        trading_days = get_trading_days(
            market=market,
            start_date=today - timedelta(days=7),
            end_date=today + timedelta(days=1),
            tzinfo=pytz.timezone("America/New_York")
        )

        if today in list(trading_days.index.date):
            market_open = trading_days.loc[str(today), 'market_open']
            market_close = trading_days.loc[str(today), 'market_close']

            if now < market_open:
                # Before market open - should get last trading day's bar
                assert bars.df.index[-1].date() == trading_days.index[-2].date()
            elif market_open <= now <= market_close:
                # During market hours - should get incomplete current day bar
                assert bars.df.index[-1].date() == trading_days.index[-1].date()
            else:
                # After market close - should get complete current day bar
                assert bars.df.index[-1].date() == trading_days.index[-1].date()
        else:
            # Non-trading day - should get last trading day's bar
            assert bars.df.index[-1].date() == trading_days.index[-1].date()

    @pytest.mark.skipif(
        not TRADIER_TEST_CONFIG['ACCESS_TOKEN'] or TRADIER_TEST_CONFIG['ACCESS_TOKEN'] == '<your key here>',
        reason="This test requires a Tradier API key"
    )
    def test_tradier_data_source_get_historical_prices_daily_bars(self):
        length = 30
        asset = Asset("SPY")
        timestep = "day"
        data_source = TradierData(
            account_number=TRADIER_TEST_CONFIG["ACCOUNT_NUMBER"],
            access_token=TRADIER_TEST_CONFIG["ACCESS_TOKEN"],
            paper=TRADIER_TEST_CONFIG["PAPER"],
        )

        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars(
            bars=bars,
            length=length,
            data_source_tz=data_source.DEFAULT_PYTZ,
            time_check=time(0, 0)
        )
        self.check_date_of_last_bar_is_correct_for_live_data_sources(bars)

        # This simulates what the call to get_yesterday_dividends does (lookback of 1)
        length = 1
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars(
            bars=bars,
            length=length,
            data_source_tz=data_source.DEFAULT_PYTZ,
            time_check=time(0,0)
        )
        self.check_date_of_last_bar_is_correct_for_live_data_sources(bars)

    @pytest.mark.skipif(
        not TRADIER_TEST_CONFIG['ACCESS_TOKEN'] or TRADIER_TEST_CONFIG['ACCESS_TOKEN'] == '<your key here>',
        reason="This test requires a Tradier API key"
    )
    def test_tradier_data_source_get_last_price_stock(self):
        data_source = TradierData(
            account_number=TRADIER_TEST_CONFIG["ACCOUNT_NUMBER"],
            access_token=TRADIER_TEST_CONFIG["ACCESS_TOKEN"],
            paper=TRADIER_TEST_CONFIG["PAPER"],
        )
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        price = data_source.get_last_price(asset=asset, quote=quote_asset)
        assert price is not None


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
    def check_date_of_last_bar_is_date_of_day_before_backtest_start_for_crypto(
            self, bars: Bars,
            backtesting_start: datetime
    ):
        # The current behavior of the backtesting data sources is to return the data for the
        # last trading day before now.
        # To simulate this, we set backtesting_start date to what we want "now" to be.
        # So based on the backtesting_start date, the last bar should be the bar from the previous day
        # before the backtesting_start date. Since this is crypto, and it trades 24/7, we don't care about
        # trading days.

        previous_day_date = backtesting_start - timedelta(days=1)
        assert bars.df.index[-1].date() == previous_day_date.date()

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
        # backtesting_start_index = trading_days.index.get_loc(backtesting_start)
        # As the index is sorted, use searchsorted to find the relevant day
        backtesting_start_index = trading_days['market_close'].searchsorted(backtesting_start, side='right')

        # get the date of the last trading day before the backtesting_start date
        previous_trading_day_date = trading_days['market_close'][backtesting_start_index - 1].date()
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
        length = 30
        asset = Asset("SPY")
        timestep = "day"
        backtesting_start = datetime(2019, 3, 26)
        backtesting_end = datetime(2019, 4, 25)
        data_source = PandasData(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars(bars=bars, length=length, time_check=time(0,0))
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )
        self.check_dividends_and_adjusted_returns(bars)

    # @pytest.mark.skip()
    def test_pandas_backtesting_data_source_get_historical_prices_daily_bars_for_backtesting_broker(
            self,
            pandas_data_fixture
    ):
        asset = Asset("SPY")
        timestep = "day"
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
            asset=asset,
            length=length,
            timeshift=timeshift,
            timestep=timestep
        )
        check_bars(bars=bars, length=length, time_check=time(0,0))
        self.check_date_of_last_bar_is_date_of_first_trading_date_on_or_after_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    # @pytest.mark.skip()
    def test_pandas_backtesting_data_source_get_historical_prices_daily_bars_over_long_weekend(
            self,
            pandas_data_fixture
    ):
        # Get MLK day in 2019
        mlk_day = self.get_mlk_day(2019)

        # First trading day after MLK day
        backtesting_start = mlk_day + timedelta(days=1)
        backtesting_end = datetime(2019, 2, 22)

        asset = Asset("SPY")
        timestep = "day"
        # get 10 bars starting from backtesting_start (going back in time)
        length = 10
        data_source = PandasData(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars(bars=bars, length=length, time_check=time(0,0))
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
        asset = Asset("SPY")
        timestep = "day"
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
            asset=asset,
            length=length,
            timeshift=timeshift,
            timestep=timestep
        )
        
        check_bars(bars=bars, length=length, time_check=time(0,0))
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
        asset = Asset("SPY")
        timestep = "day"
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
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars(bars=bars, length=length, time_check=time(0,0))
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    # @pytest.mark.skip()
    def test_yahoo_backtesting_data_source_get_historical_prices_daily_bars_dividends_and_adj_returns(
            self,
            pandas_data_fixture
    ):
        """
        This tests that the yahoo data_source calculates adjusted returns for bars and that they
        are calculated correctly. It assumes that it is provided split adjusted OHLCV and dividend data.
        """
        length = 30
        asset = Asset("SPY")
        timestep = "day"
        backtesting_start = datetime(2019, 3, 25)
        backtesting_end = datetime(2019, 4, 25)
        data_source = YahooDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            pandas_data=pandas_data_fixture
        )
        data_source._datetime = datetime.strptime("09:30", "%H:%M").time()
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        # TODO: Yahoo data is indexed at the close (4pm EDT).
        #  Consider changing that to midnight like lumibot expects
        check_bars(bars=bars, length=length, time_check=time(16,0))
        self.check_dividends_and_adjusted_returns(bars)
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    # @pytest.mark.skip()
    def test_yahoo_backtesting_data_source_get_historical_prices_daily_bars_for_backtesting_broker(
            self,
            pandas_data_fixture
    ):
        asset = Asset("SPY")
        timestep = "day"
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
            asset=asset,
            length=length,
            timeshift=timeshift,
            timestep=timestep
        )

        # TODO: Yahoo data is indexed at the close (4pm EDT). Consider changing that to midnight like lumibot expects
        check_bars(bars=bars, length=length, time_check=time(16,0))
        self.check_date_of_last_bar_is_date_of_first_trading_date_on_or_after_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    # @pytest.mark.skip()
    def test_yahoo_backtesting_data_source_get_historical_prices_daily_bars_over_long_weekend(
            self,
            pandas_data_fixture
    ):
        asset = Asset("SPY")
        timestep = "day"
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
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        # TODO: Yahoo data is indexed at the close (4pm EDT). Consider changing that to midnight like lumibot expects
        check_bars(bars=bars, length=length, time_check=time(16,0))
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    def test_kraken_ccxt_backtesting_data_source_get_historical_prices_daily_bars(
            self
    ):
        """
        This tests that the kraken ccxt data_source gets the right bars
        """
        length = 30
        backtesting_start = (datetime.now() - timedelta(days=4)).replace(hour=0, minute=0, second=0, microsecond=0)
        backtesting_end = (datetime.now() - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        base = Asset(symbol='BTC', asset_type='crypto')
        quote = Asset(symbol='USD', asset_type='forex')
        timestep = "day"
        kwargs = {
            # "max_data_download_limit":10000, # optional
            "exchange_id": "kraken"  # "kucoin" #"bybit" #"okx" #"bitmex" # "binance"
        }
        data_source = CcxtBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            **kwargs
        )
        bars = data_source.get_historical_prices(
            asset=(base, quote),
            length=length,
            timestep=timestep
        )
        # TODO: Kraken returns daily data at midnight UTC which is 5pm ET aka 19
        check_bars(bars=bars, length=length, time_check=time(19,0))
        self.check_date_of_last_bar_is_date_of_day_before_backtest_start_for_crypto(
            bars,
            backtesting_start=backtesting_start
        )
        
    # @pytest.mark.skip()
    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_alpaca_backtesting_data_source_get_historical_prices_daily_bars_at_sim_time(self):
        asset = Asset("SPY")
        # Get MLK day for last year
        last_year = datetime.now().year - 1
        mlk_day = self.get_mlk_day(last_year)

        # Get the friday of MLK week
        backtesting_start = mlk_day + timedelta(days=4)
        backtesting_end = datetime(last_year, 2, 22)
        timestep = 'day'
        refresh_cache = False
        length = 3

        data_source = AlpacaBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            config=ALPACA_TEST_CONFIG,
            timestep=timestep,
            refresh_cache=refresh_cache,
            warm_up_trading_days=length
        )

        # the backtester runs daily bars at 9:30am
        data_source._datetime = data_source._datetime.replace(hour=9, minute=30, second=0, microsecond=0)

        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars(bars=bars, length=length, time_check=time(0,0))
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=data_source._datetime
        )


    # @pytest.mark.skip()
    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_alpaca_backtesting_data_source_get_historical_prices_daily_bars_for_backtesting_broker(self):
        asset = Asset("SPY")
        # Test getting 1 bars into the future (which is what the backtesting does when trying to fill orders
        # for the next trading day)
        last_year = datetime.now().year - 1

        # Get MLK day last year which is a non-trading monday
        mlk_day = self.get_mlk_day(last_year)

        # First trading day after MLK day
        backtesting_start = mlk_day + timedelta(days=1)
        backtesting_end = datetime(last_year, 2, 22)
        timestep = 'day'
        refresh_cache = False

        data_source = AlpacaBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            config=ALPACA_TEST_CONFIG,
            timestep=timestep,
            refresh_cache=refresh_cache,
        )

        length = 1
        bars = data_source.get_historical_prices(
            asset=asset,
            length=length,
            timestep=timestep
        )

        check_bars(bars=bars, length=length, time_check=time(0,0))
        self.check_date_of_last_bar_is_date_of_first_trading_date_on_or_after_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    # @pytest.mark.skip()
    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_alpaca_backtesting_data_source_get_historical_prices_daily_bars_over_long_weekend(self):
        asset = Asset("SPY")
        # Get MLK day for last year
        last_year = datetime.now().year - 1
        mlk_day = self.get_mlk_day(last_year)

        # First trading day after MLK day
        backtesting_start = mlk_day + timedelta(days=1)
        backtesting_end = datetime(last_year, 2, 22)
        timestep = 'day'
        refresh_cache = False
        length = 10

        data_source = AlpacaBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            config=ALPACA_TEST_CONFIG,
            timestep=timestep,
            refresh_cache=refresh_cache,
            warm_up_trading_days=length
        )

        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars(bars=bars, length=length, time_check=time(0,0))
        self.check_date_of_last_bar_is_date_of_last_trading_date_before_backtest_start(
            bars,
            backtesting_start=backtesting_start
        )

    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_alpaca_backtesting_data_source_get_historical_daily_prices_when_minute_bars_provided(self):
        length = 3
        warm_up_days = length * 2
        ticker = "SPY"
        asset = Asset(ticker)

        # Get MLK day last year which is a non-trading monday
        last_year = datetime.now().year - 1
        mlk_day = self.get_mlk_day(last_year)

        # First trading day after MLK day
        backtesting_start = mlk_day + timedelta(days=1)
        backtesting_end = backtesting_start + timedelta(days=2)
        timestep = 'minute'  # using minute bars, but asking for daily bars in get_historical_prices
        refresh_cache = False

        data_source = AlpacaBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            config=ALPACA_TEST_CONFIG,
            timestep=timestep,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_days
        )

        bars = data_source.get_historical_prices(
            asset=asset,
            length=length,
            timestep="day"
        )

        check_bars(bars=bars, length=length, time_check=time(0,0))
