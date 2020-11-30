import pytest
from datetime import datetime
from backtesting import BacktestingBroker, YahooDataBacktesting
from strategies import Diversification, Momentum
from tests import trader

@pytest.fixture(scope="session")
def backtesting():
    def func_result(datasource, backtesting_start, backtesting_end):
        if datasource == "yahoo":
            backtesting_source = YahooDataBacktesting(backtesting_start, backtesting_end)
        else:
            raise ValueError("Unknown datasource %s" % datasource)

        return BacktestingBroker(backtesting_source)

    return func_result

def test_momentum_strategy(trader, backtesting):
    try:
        budget = 40000
        backtesting_start = datetime(2010, 6, 1)
        backtesting_end = datetime(2010, 12, 1)
        backtesting_broker = backtesting("yahoo", backtesting_start, backtesting_end)
        momentum = Momentum(budget=budget, broker=backtesting_broker)
        trader.add_strategy(momentum)
        trader.run_all()
        assert True
    except:
        assert False

def test_diversification_strategy(trader, backtesting):
    try:
        budget = 40000
        backtesting_start = datetime(2010, 6, 1)
        backtesting_end = datetime(2010, 12, 1)
        backtesting_broker = backtesting("yahoo", backtesting_start, backtesting_end)
        momentum = Diversification(budget=budget, broker=backtesting_broker)
        trader.add_strategy(momentum)
        trader.run_all()
        assert True
    except:
        assert False
