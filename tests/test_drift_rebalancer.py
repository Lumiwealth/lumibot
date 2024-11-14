from decimal import Decimal
from typing import Any
import datetime
import pytest

import pandas as pd
import numpy as np

from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.components import DriftCalculationLogic, LimitOrderDriftRebalancerLogic
from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_precision

print_full_pandas_dataframes()
set_pandas_float_precision(precision=5)


class TestDriftRebalancer:

    # Need to start two days after the first data point in pandas for backtesting
    backtesting_start = datetime.datetime(2019, 1, 2)
    backtesting_end = datetime.datetime(2019, 12, 31)

    def test_classic_60_60(self, pandas_data_fixture):

        parameters = {
            "market": "NYSE",
            "sleeptime": "1D",
            "drift_threshold": "0.03",
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "target_weights": {
                "SPY": "0.60",
                "TLT": "0.40"
            },
            "shorting": False
        }

        results, strat_obj = DriftRebalancer.run_backtest(
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
            # quiet_logs=False,
        )

        assert results is not None
        assert np.isclose(results["cagr"], 0.22076538945204272, atol=1e-4)
        assert np.isclose(results["volatility"], 0.06740737779031068, atol=1e-4)
        assert np.isclose(results["sharpe"], 3.051823053251843, atol=1e-4)
        assert np.isclose(results["max_drawdown"]["drawdown"], 0.025697778711759052, atol=1e-4)

    def test_with_shorting(self):
        # TODO
        pass
