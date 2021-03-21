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
        risk_free_rate = 0

        valid_result = {
            "cagr": 0.06808194998616934,
            "volatility": 0.24666811122617535,
            "sharpe": 0.2760062889675412,
            "max_drawdown": {
                "drawdown": 0.2670117192959624,
                "date": pd.Timestamp("2020-03-16 16:00:00-0400", tz="America/New_York"),
            },
            "romad": 0.2549773851338174,
        }

        stats = Momentum.backtest(
            "momentum",
            budget,
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            risk_free_rate=risk_free_rate,
            auto_adjust=False,
        )
        result = stats.get("momentum")
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
        risk_free_rate = 0

        valid_result = {
            "cagr": 0.17107361844543667,
            "volatility": 0.11194730510551205,
            "sharpe": 1.5281620069745954,
            "max_drawdown": {
                "drawdown": 0.1326274935830035,
                "date": pd.Timestamp("2020-03-18 16:00:00-0400", tz="America/New_York"),
            },
            "romad": 1.2898805053446334,
        }

        stats = Diversification.backtest(
            "diversification",
            budget,
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            risk_free_rate=risk_free_rate,
            auto_adjust=False,
        )
        result = stats.get("diversification")
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
