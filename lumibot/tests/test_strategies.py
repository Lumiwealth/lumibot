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
            "cagr": 0.08491181810900916,
            "volatility": 0.2456740093974997,
            "sharpe": 0.3456280064677991,
            "max_drawdown": {
                "drawdown": 0.2633426221391952,
                "date": pd.Timestamp("2020-03-16 16:00:00-0400", tz="America/New_York"),
            },
            "romad": 0.32243856850535674,
        }

        stats = Momentum.backtest(
            "momentum",
            budget,
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            risk_free_rate=risk_free_rate,
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
            "cagr": 0.18828833851783022,
            "volatility": 0.11210787357378023,
            "sharpe": 1.6795282304049255,
            "max_drawdown": {
                "drawdown": 0.13174368699156755,
                "date": pd.Timestamp("2020-03-18 16:00:00-0400", tz="America/New_York"),
            },
            "romad": 1.4292019816469983,
        }

        stats = Diversification.backtest(
            "diversification",
            budget,
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            risk_free_rate=risk_free_rate,
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
