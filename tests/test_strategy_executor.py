from datetime import datetime

import pytest
from lumibot.backtesting import PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


# Skip all the tests in this file
# pytestmark = pytest.mark.skip("all tests still WIP")
pytest.skip("all tests still WIP", allow_module_level=True)


class MyStrategy(Strategy):
    def initialize(self):
        pass
    
    def on_trading_iteration(self):
        pass 


class TestStrategyExecutor:
    def test_calculate_strategy_trigger(self):
        backtest_broker = PandasDataBacktesting(
                datetime_start=datetime(2021, 1, 1),
                datetime_end=datetime(2021, 1, 2),
            )
        strategy = MyStrategy(broker=backtest_broker)
        
        strategy_executor = StrategyExecutor(strategy=strategy)
        
        res = strategy_executor.calculate_strategy_trigger()
        
        assert res == 1
        # assert Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc').side == 'buy'
        # assert Order(asset=Asset("SPY"), quantity=10, side="sell", strategy='abc').side == 'sell'

        # with pytest.raises(ValueError):
        #     Order(asset=Asset("SPY"), quantity=10, side="unknown", strategy='abc')
