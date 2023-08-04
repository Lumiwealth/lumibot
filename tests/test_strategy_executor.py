import pytest
from lumibot.strategies.strategy_executor import StrategyExecutor


class TestStrategyExecutor:
    def test_calculate_strategy_trigger(self):
        strategy_executor = StrategyExecutor()
        
        res = strategy_executor.calculate_strategy_trigger()
        
        assert res == 1
        # assert Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc').side == 'buy'
        # assert Order(asset=Asset("SPY"), quantity=10, side="sell", strategy='abc').side == 'sell'

        # with pytest.raises(ValueError):
        #     Order(asset=Asset("SPY"), quantity=10, side="unknown", strategy='abc')

