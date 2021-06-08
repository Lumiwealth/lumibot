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
            "cagr": 0.278191780294768,
            "volatility": 0.19865763919432644,
            "sharpe": 1.400357828790271,
            "max_drawdown": {
                "drawdown": 0.17721582744193054,
                "date": pd.Timestamp("2020-03-19 16:00:00-0400", tz="America/New_York"),
            },
            "romad": 1.569790826871403,
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
                result["max_drawdown"]["date"]
                .to_pydatetime()
                .replace(minute=0, hour=0, second=0)
                .timestamp()
                - valid_result["max_drawdown"]["date"]
                .to_pydatetime()
                .replace(minute=0, hour=0, second=0)
                .timestamp()
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
            "cagr": 0.1840888064781594,
            "volatility": 0.1120555153194634,
            "sharpe": 1.6428357493456125,
            "max_drawdown": {
                "drawdown": 0.13217679191636564,
                "date": pd.Timestamp("2020-03-19 16:00:00-0400", tz="America/New_York"),
            },
            "romad": 1.3927468189320322,
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
                result["max_drawdown"]["date"]
                .to_pydatetime()
                .replace(minute=0, hour=0, second=0)
                .timestamp()
                - valid_result["max_drawdown"]["date"]
                .to_pydatetime()
                .replace(minute=0, hour=0, second=0)
                .timestamp()
            )
            < TOLERANCE
        )
        assert abs(result["romad"] - valid_result["romad"]) < TOLERANCE

    except Exception as e:
        print(e)
        assert False
