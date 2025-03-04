import os
import datetime
import logging
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import pandas as pd
from pandas.testing import assert_series_equal

from lumibot.strategies import Strategy
from lumibot.backtesting import PandasDataBacktesting, YahooDataBacktesting, AlpacaBacktesting
from tests.fixtures import pandas_data_fixture
from lumibot.credentials import ALPACA_TEST_CONFIG


logger = logging.getLogger(__name__)
# print_full_pandas_dataframes()
# set_pandas_float_display_precision(precision=15)


class MomoTester(Strategy):
    """This strategy saves the momentum values calculated each trading iteration, so we can compare them later."""
    symbol: str = ""
    lookback_period: int = 0
    actual_df: pd.DataFrame = None

    parameters = {
        "lookback_period": 2,
    }

    def initialize(self, parameters: Any = None) -> None:
        self.set_market("NYSE")
        self.sleeptime = "1D"
        self.symbol = "SPY"
        self.lookback_period = self.parameters["lookback_period"]

        # build a dataframe to store the date, closing price, and momentum
        self.actual_df = pd.DataFrame(columns=["date", "actual_momo"])

    def on_trading_iteration(self):
        dt = self.get_datetime()
        actual_momo = self.get_momentum(self.symbol, self.lookback_period)
        self.actual_df.loc[len(self.actual_df)] = {
            "date": dt.date(),
            "actual_momo": actual_momo,
        }

    def get_momentum(self, symbol, lookback_period):
        bars = self.get_historical_prices(symbol, lookback_period, timestep="day")
        actual_momo = bars.get_momentum(lookback_period)
        return actual_momo


class TestMomentum:
    df = None
    backtesting_start = datetime.datetime(2019, 3, 1)
    backtesting_end = datetime.datetime(2019, 3, 31)

    @classmethod
    def setup_class(cls):
        # We load the SPY data directly and calculate the adjusted returns.
        file_path = os.getcwd() + "/data/SPY.csv"
        df = pd.read_csv(file_path)
        df.rename(columns={"Date": "date"}, inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        cls.df = df

    # noinspection PyMethodMayBeStatic
    def calculate_expected_momo(self, df_orig, lookback_period) -> pd.DataFrame:
        # Given a dataframe with adjusted close prices, calculate the expected momentum values just like we do
        # in bars.get_momentum. But here were using the Adjusted Close from yahoo. And in bars.get_momentum,
        # we calculated the adjusted returns by using unadjusted close prices and dividends.
        df = df_orig.copy()
        df['expected_momo'] = df['Adj Close'].pct_change(lookback_period-1).shift(1)
        return df

    def build_comparison_df(self, strategy) -> pd.DataFrame:
        # This helper function just gets the dataframe of actual momentum values from the strategy object
        # and the dataframe of expected momentum values calculated from the adjusted close prices,
        # and puts them side by side for comparison.
        actual_df = strategy.actual_df
        actual_df.set_index("date", inplace=True)
        expected_df = self.calculate_expected_momo(self.df, strategy.lookback_period)

        # make a new dataframe with the actual and expected momentum values side by side but for the dates in the actual_df
        comparison_df = pd.concat([actual_df["actual_momo"], expected_df["expected_momo"]], axis=1).reindex(actual_df.index)
        return comparison_df

    def test_momo_pandas_lookback_2(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 2,
        }

        results, strategy = MomoTester.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            pandas_data=pandas_data_fixture,
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )
        comparison_df = self.build_comparison_df(strategy)
        # print(f"\n{comparison_df}")

        assert_series_equal(
            comparison_df["actual_momo"],
            comparison_df["expected_momo"],
            check_names=False,
            atol=1e-4,
            rtol=0
        )

    def test_momo_pandas_lookback_30(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 30,
        }

        results, strategy = MomoTester.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            pandas_data=pandas_data_fixture,
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )
        comparison_df = self.build_comparison_df(strategy)
        # print(f"\n{comparison_df}")

        assert_series_equal(
            comparison_df["actual_momo"],
            comparison_df["expected_momo"],
            check_names=False,
            atol=1e-4,
            rtol=0
        )

    def test_momo_yahoo_lookback_2(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 2,
        }

        results, strategy = MomoTester.run_backtest(
            datasource_class=YahooDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )
        comparison_df = self.build_comparison_df(strategy)
        # print(f"\n{comparison_df}")

        assert_series_equal(
            comparison_df["actual_momo"],
            comparison_df["expected_momo"],
            check_names=False,
            atol=1e-4,
            rtol=0
        )

    def test_momo_yahoo_lookback_30(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 30,
        }

        results, strategy = MomoTester.run_backtest(
            datasource_class=YahooDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            quiet_logs=False
        )
        comparison_df = self.build_comparison_df(strategy)
        # print(f"\n{comparison_df}")

        assert_series_equal(
            comparison_df["actual_momo"],
            comparison_df["expected_momo"],
            check_names=False,
            atol=1e-4,
            rtol=0
        )

    # @pytest.mark.skip()
    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'],
        reason="This test requires an alpaca API key"
    )
    @pytest.mark.skipif(
        ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_momo_alpaca_lookback_30(self):
        tickers = "SPY"
        timestep = 'day'
        refresh_cache = False
        tzinfo = ZoneInfo("America/New_York")
        lookback_period = 30
        market='NYSE'

        results, strategy = MomoTester.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            parameters={
                "lookback_period": lookback_period,
            },
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            quiet_logs=False,

            # AlpacaBacktesting kwargs
            tickers=tickers,
            refresh_cache=refresh_cache,
            timestep=timestep,
            config=ALPACA_TEST_CONFIG,
            warm_up_trading_days=lookback_period,
            tzinfo=tzinfo,
            market=market,
        )
        comparison_df = self.build_comparison_df(strategy)

        # Remove the first row
        # comparison_df = comparison_df.iloc[1:]

        assert_series_equal(
            comparison_df["actual_momo"],
            comparison_df["expected_momo"],
            check_names=False,
            atol=1e-2,  # be more flexible. im comparing data from alpaca with data from yahoo.
            rtol=0
        )
