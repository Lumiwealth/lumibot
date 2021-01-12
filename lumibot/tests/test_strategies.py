from datetime import datetime

from lumibot.backtesting import YahooDataBacktesting
from lumibot.strategies.examples import Diversification, Momentum


def test_momentum_strategy():
    try:
        budget = 40000
        backtesting_start = datetime(2010, 6, 1)
        backtesting_end = datetime(2010, 12, 1)
        result = Momentum.backtest(
            YahooDataBacktesting, budget, backtesting_start, backtesting_end
        )
        assert result
    except:
        assert False


def test_diversification_strategy():
    try:
        budget = 40000
        backtesting_start = datetime(2010, 6, 1)
        backtesting_end = datetime(2010, 12, 1)
        result = Diversification.backtest(
            YahooDataBacktesting, budget, backtesting_start, backtesting_end
        )
        assert result
    except:
        assert False
