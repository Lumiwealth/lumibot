from datetime import datetime

from backtesting import YahooDataBacktesting
from strategies import Diversification, Momentum


def test_momentum_strategy():
    try:
        budget = 40000
        backtesting_start = datetime(2010, 6, 1)
        backtesting_end = datetime(2010, 12, 1)
        Momentum.backtest(
            YahooDataBacktesting, budget, backtesting_start, backtesting_end
        )
        assert True
    except:
        assert False


def test_diversification_strategy():
    try:
        budget = 40000
        backtesting_start = datetime(2010, 6, 1)
        backtesting_end = datetime(2010, 12, 1)
        Diversification.backtest(
            YahooDataBacktesting, budget, backtesting_start, backtesting_end
        )
        assert True
    except:
        assert False
