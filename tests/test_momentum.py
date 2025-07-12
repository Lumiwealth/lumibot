import os
import datetime
import logging
from typing import Any

import pytest

import pandas as pd
from pandas.testing import assert_series_equal

from lumibot.strategies import Strategy
from lumibot.backtesting import PandasDataBacktesting, YahooDataBacktesting, PolygonDataBacktesting
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision


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
        dt, actual_momo = self.get_momentum(self.symbol, self.lookback_period)
        self.actual_df.loc[len(self.actual_df)] = {
            "date": dt.date(),
            "actual_momo": actual_momo,
        }

    def get_momentum(self, symbol, lookback_period):
        bars = self.get_historical_prices(symbol, lookback_period + 2, timestep="day")
        dt = bars.df.index[-1]
        actual_momo = bars.get_momentum(lookback_period)
        return dt, actual_momo


class TestMomentum:
    df = None
    backtesting_start = datetime.datetime(2019, 3, 1)
    backtesting_end = datetime.datetime(2019, 3, 31)

    @classmethod
    def setup_class(cls):
        # Load data from CSV file
        script_dir = os.path.dirname(__file__)
        csv_path = os.path.join(script_dir, "..", "data", "SPY.csv")
        cls.df = pd.read_csv(csv_path)
        cls.df["date"] = pd.to_datetime(cls.df["Date"])
        cls.df.set_index("date", inplace=True)
        # Filter data to match the backtest range (plus some buffer for lookback)
        start_buffer = cls.backtesting_start - datetime.timedelta(days=60) # Buffer for max lookback
        cls.df = cls.df[(cls.df.index >= start_buffer) & (cls.df.index <= cls.backtesting_end)]

    # noinspection PyMethodMayBeStatic
    def calculate_expected_momo(self, df_orig, lookback_period) -> pd.DataFrame:
        # Calculate expected momentum using the 'Adj Close' column from the CSV
        df = df_orig.copy()
        df['expected_momo'] = df['Adj Close'].pct_change(lookback_period)
        return df

    def build_comparison_df(self, strat_obj) -> pd.DataFrame:
        # This helper function just gets the dataframe of actual momentum values from the strategy object
        # and the dataframe of expected momentum values calculated from the adjusted close prices,
        # and puts them side by side for comparison.
        actual_df = strat_obj.actual_df
        # Drop duplicate dates before setting the index, keeping the last entry for each date
        actual_df = actual_df.drop_duplicates(subset=['date'], keep='last')
        actual_df.set_index("date", inplace=True)
        expected_df = self.calculate_expected_momo(self.df, strat_obj.lookback_period)

        # Print actual_df
        print(f"\n{actual_df}")

        # Print expected_df
        print(f"\n{expected_df}")

        # make a new dataframe with the actual and expected momentum values side by side but for the dates in the actual_df
        comparison_df = pd.concat([actual_df["actual_momo"], expected_df["expected_momo"]], axis=1).reindex(actual_df.index)
        return comparison_df

    def test_momo_pandas_lookback_2(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 2,
        }

        results, strat_obj = MomoTester.run_backtest(
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
        comparison_df = self.build_comparison_df(strat_obj)
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

        results, strat_obj = MomoTester.run_backtest(
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
        comparison_df = self.build_comparison_df(strat_obj)
        # print(f"\n{comparison_df}")

        assert_series_equal(
            comparison_df["actual_momo"],
            comparison_df["expected_momo"],
            check_names=False,
            atol=1e-4,
            rtol=0
        )

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_momo_yahoo_lookback_2(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 2,
        }

        results, strat_obj = MomoTester.run_backtest(
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
            quiet_logs=True  # Explicitly set quiet_logs
        )
        comparison_df = self.build_comparison_df(strat_obj)

        # Add print statements for the comparison_df
        print(f"\n{comparison_df}")
        
        # Calculate and print difference
        diff_series = (comparison_df["actual_momo"] - comparison_df["expected_momo"]).abs()
        print(f"\nDifference Series (test_momo_yahoo_lookback_2):\n{diff_series.dropna().head()}")
        print(f"Difference Stats (min/max/mean): {diff_series.min():.6f} / {diff_series.max():.6f} / {diff_series.mean():.6f}")

        assert_series_equal(
            comparison_df["actual_momo"],
            comparison_df["expected_momo"],
            check_names=False,
            atol=1e-3,  # Keep increased tolerance
            rtol=0
        )

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_momo_yahoo_lookback_30(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 30,
        }

        results, strat_obj = MomoTester.run_backtest(
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
            quiet_logs=True  # Explicitly set quiet_logs
        )
        comparison_df = self.build_comparison_df(strat_obj)

        # Calculate and print difference
        diff_series = (comparison_df["actual_momo"] - comparison_df["expected_momo"]).abs()
        print(f"\nDifference Series (test_momo_yahoo_lookback_30):\n{diff_series.dropna().head()}")
        print(f"Difference Stats (min/max/mean): {diff_series.min():.6f} / {diff_series.max():.6f} / {diff_series.mean():.6f}")

        assert_series_equal(
            comparison_df["actual_momo"],
            comparison_df["expected_momo"],
            check_names=False,
            atol=1e-3,  # Keep increased tolerance
            rtol=0
        )