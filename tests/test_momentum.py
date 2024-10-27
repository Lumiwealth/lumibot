import os
import datetime
import logging
import pytest

import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np

from lumibot.strategies import Strategy
from lumibot.backtesting import PandasDataBacktesting
from tests.backtest.fixtures import pandas_data_fixture

logger = logging.getLogger(__name__)


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
        self.momo_df = pd.DataFrame(columns=["dt", "start_close", "end_close", "lumi_momo", "my_momo"])

    def on_trading_iteration(self):
        dt, start_close, end_close, lumi_momo, my_momo = self.get_momentum(self.symbol, self.lookback_period)
        self.momo_df.loc[len(self.momo_df)] = {"dt": dt, "start_close": start_close, "end_close": end_close,
                                               "lumi_momo": lumi_momo, "my_momo": my_momo}

    def get_momentum(self, symbol, lookback_period):
        start_date = self.get_round_day(timeshift=lookback_period + 1)
        end_date = self.get_round_day(timeshift=1)
        bars = self.get_historical_prices(symbol, lookback_period + 2, timestep="day")
        dt = self.get_datetime()
        start_close = bars.df["close"].iloc[-lookback_period - 1]
        end_close = bars.df["close"].iloc[-1]
        lumi_momo = bars.get_momentum(start=start_date, end=end_date)
        my_momo = (end_close - start_close) / start_close
        return dt, start_close, end_close, lumi_momo, my_momo


class TestMomentum:
    backtesting_start = datetime.datetime(2019, 3, 1)
    backtesting_end = datetime.datetime(2019, 3, 31)

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

        # convert the momo_tracker dictionary to a DataFrame
        momo_df = strat_obj.momo_df
        # use date as the index
        momo_df.set_index("dt", inplace=True)

        logger.info(f"\n{momo_df}")

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

        # convert the momo_tracker dictionary to a DataFrame
        momo_df = strat_obj.momo_df
        # use date as the index
        momo_df.set_index("dt", inplace=True)

        logger.info(f"\n{momo_df}")

    @pytest.mark.skip()
    def test_momentum_using_adjusted_close_diff_from_unadjusted_momentum(self):
        file_path = os.getcwd() + "/data/SPY.csv"
        df = pd.read_csv(file_path, parse_dates=True, index_col=0)
        lookback_period = 2

        df["adj_momentum"] = df["Adj Close"].pct_change(lookback_period).shift(1)
        df["momentum"] = df["Close"].pct_change(lookback_period).shift(1)

        # Use March 2019 because there is a dividend on March 15th
        # momo_df = df.loc["2019-03-02":"2019-03-31", ["adj_momentum",  "unadj_momentum"]]
        momo_df = df.loc[self.backtesting_start:self.backtesting_end, ["Close", "momentum", "Adj Close", "adj_momentum", ]]
        # momo_df['diff'] = momo_df['adj_momentum'] - momo_df['unadj_momentum']

        # show diff as zeros not in scientific notation
        pd.options.display.float_format = '{:.9f}'.format
        logger.error(f"\n{momo_df}")

        # # get a series for the values of the diff column before the dividend date
        # assert momo_df.loc["2019-03-02":"2019-03-14"]["diff"].abs().max() < 1e-5
        #
        # # assert that the diff increases on the dividend date
        # assert momo_df.loc["2019-03-15"]["diff"] > 0
        #
        # # assert that the diff remains increased after the dividend date
        # assert momo_df.loc["2019-03-16":]["diff"].abs().min() > 1e-5
