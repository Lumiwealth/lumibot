import os
import datetime
import logging
import pytest

import pandas as pd
from pandas.testing import assert_series_equal

from lumibot.strategies import Strategy
from lumibot.backtesting import PandasDataBacktesting
from tests.backtest.fixtures import pandas_data_fixture
from lumibot.tools.pandas import print_full_pandas_dataframes, set_pandas_float_precision

logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_precision(precision=5)


class MomoTester(Strategy):

    parameters = {
        "lookback_period": 2,
    }

    def initialize(self):
        self.set_market("NYSE")
        self.sleeptime = "1D"
        self.symbol = "SPY"
        self.lookback_period = self.parameters["lookback_period"]

        # build a dataframe to store the datetime, closing price, and momentum
        self.momo_df = pd.DataFrame(columns=["dt", "start_close", "end_close", "actual_momo", "expected_momo"])

    def on_trading_iteration(self):
        dt, start_close, end_close, actual_momo, expected_momo = self.get_momentum(self.symbol, self.lookback_period)
        self.momo_df.loc[len(self.momo_df)] = {
            "dt": dt, 
            "start_close": start_close, 
            "end_close": end_close,
            "actual_momo": actual_momo, 
            "expected_momo": expected_momo
        }

    def get_momentum(self, symbol, lookback_period):
        bars = self.get_historical_prices(symbol, lookback_period + 2, timestep="day")
        dt = self.get_datetime()
        start_close = bars.df["close"].iloc[-lookback_period - 1]
        end_close = bars.df["close"].iloc[-1]
        actual_momo = bars.get_momentum(lookback_period)
        expected_momo = (end_close - start_close) / start_close
        return dt, start_close, end_close, actual_momo, expected_momo


class TestMomentum:
    backtesting_start = datetime.datetime(2019, 3, 1)
    backtesting_end = datetime.datetime(2019, 3, 31)

    # @pytest.mark.skip()
    def test_momo_tester_strategy_lookback_2(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 2,
        }

        results, strat_obj = MomoTester.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            pandas_data=list(pandas_data_fixture.values()),
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )

        momo_df = strat_obj.momo_df
        # print(f"\n{momo_df}")
        assert_series_equal(
            momo_df["actual_momo"],
            momo_df["expected_momo"],
            check_names=False,
            atol=1e-10,
            rtol=0
        )

    # @pytest.mark.skip()
    def test_momo_tester_strategy_lookback_3(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 3,
        }

        results, strat_obj = MomoTester.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            pandas_data=list(pandas_data_fixture.values()),
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )

        momo_df = strat_obj.momo_df
        # print(f"\n{momo_df}")
        assert_series_equal(
            momo_df["actual_momo"],
            momo_df["expected_momo"],
            check_names=False,
            atol=1e-10,
            rtol=0
        )

    # @pytest.mark.skip()
    def test_momo_tester_strategy_lookback_20(self, pandas_data_fixture):
        parameters = {
            "lookback_period": 20,
        }

        results, strat_obj = MomoTester.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            pandas_data=list(pandas_data_fixture.values()),
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
        )

        momo_df = strat_obj.momo_df
        # print(f"\n{momo_df}")
        assert_series_equal(
            momo_df["actual_momo"],
            momo_df["expected_momo"],
            check_names=False,
            atol=1e-10,
            rtol=0
        )

    def test_calculate_adjusted_returns_from_close_and_dividends(self):
        file_path = os.getcwd() + "/data/SPY.csv"
        df = pd.read_csv(file_path, parse_dates=True, index_col=0)
        df = df.sort_index(ascending=True)

        # Adjusted returns  = (current close - previous close + dividends) / previous close
        df['my_adj_returns'] = (df['Close'] - df['Close'].shift(1) + df['Dividends']) / df['Close'].shift(1)

        # For comparison, calculate the adjusted returns using the Adj Close column
        df['adj_returns'] = df['Adj Close'].pct_change()

        # cols_to_print = ['Close', 'Adj Close', 'Dividends', 'adj_returns', 'my_adj_returns']
        cols_to_print = ['Dividends', 'adj_returns', 'my_adj_returns']
        # print(f"\n{df[-15:][cols_to_print]}")

        assert_series_equal(
            df["adj_returns"],
            df["my_adj_returns"],
            check_names=False,
            atol=1e-4,
        )
