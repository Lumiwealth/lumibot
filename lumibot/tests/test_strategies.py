from datetime import datetime

import pandas as pd

from lumibot.backtesting import YahooDataBacktesting
from lumibot.strategies.examples import Diversification, Momentum

TOLERANCE = 0.00001


def test_momentum_strategy():
    try:
        budget = 40000
        backtesting_start = datetime(2020, 1, 1)
        backtesting_end = datetime(2020, 12, 31)

        valid_result = {
            "cagr": 0.09294513134496363,
            "volatility": 0.21715809230245922,
            "sharpe": 0.4269476230976895,
            "max_drawdown": {
                "drawdown": 0.223836022349236,
                "date": pd.Timestamp("2020-03-16 16:00:00-0400", tz="America/New_York"),
            },
            "romad": 0.415237593884454,
        }

        stats = Momentum.backtest(
            YahooDataBacktesting, budget, backtesting_start, backtesting_end
        )
        result = stats[0]
        assert result
        assert abs(result["cagr"] - valid_result["cagr"]) < TOLERANCE
        assert abs(result["volatility"] - valid_result["volatility"]) < TOLERANCE
        assert abs(result["sharpe"] - valid_result["sharpe"]) < TOLERANCE
        assert (
            abs(
                result["max_drawdown"]["drawdown"]
                - valid_result["max_drawdown"]["drawdown"]
            )
            < TOLERANCE
        )
        assert (
            abs(
                result["max_drawdown"]["date"].timestamp()
                - valid_result["max_drawdown"]["date"].timestamp()
            )
            < TOLERANCE
        )
        assert abs(result["romad"] - valid_result["romad"]) < TOLERANCE

    except Exception as e:
        print(e)
        assert False


def test_diversification_strategy():
    try:
        budget = 40000
        backtesting_start = datetime(2020, 1, 1)
        backtesting_end = datetime(2020, 12, 31)

        valid_result = {
            "cagr": 0.1883317786133596,
            "volatility": 0.11211875376518643,
            "sharpe": 1.6777012970310623,
            "max_drawdown": {
                "drawdown": 0.13177980529069402,
                "date": pd.Timestamp("2020-03-18 16:00:00-0400", tz="America/New_York"),
            },
            "romad": 1.429139906512361,
        }

        stats = Diversification.backtest(
            YahooDataBacktesting, budget, backtesting_start, backtesting_end
        )
        result = stats[0]
        assert result
        assert abs(result["cagr"] - valid_result["cagr"]) < TOLERANCE
        assert abs(result["volatility"] - valid_result["volatility"]) < TOLERANCE
        assert abs(result["sharpe"] - valid_result["sharpe"]) < TOLERANCE
        assert (
            abs(
                result["max_drawdown"]["drawdown"]
                - valid_result["max_drawdown"]["drawdown"]
            )
            < TOLERANCE
        )
        assert (
            abs(
                result["max_drawdown"]["date"].timestamp()
                - valid_result["max_drawdown"]["date"].timestamp()
            )
            < TOLERANCE
        )
        assert abs(result["romad"] - valid_result["romad"]) < TOLERANCE

    except Exception as e:
        print(e)
        assert False
