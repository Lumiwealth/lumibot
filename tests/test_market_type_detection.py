from datetime import datetime

import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


class _NoopStrategy(Strategy):
    """Minimal strategy stub for StrategyExecutor tests."""

    def initialize(self):
        self.set_market("us_futures")

    def on_trading_iteration(self):
        pass


@pytest.fixture
def strategy_executor():
    broker = PandasDataBacktesting(
        datetime_start=datetime(2025, 10, 28),
        datetime_end=datetime(2025, 11, 6),
    )
    backtesting_broker = BacktestingBroker(data_source=broker)
    strat = _NoopStrategy(broker=backtesting_broker)
    return StrategyExecutor(strategy=strat)


def test_us_futures_treated_as_non_continuous(strategy_executor):
    """us_futures closes over the weekend; it must not be flagged as continuous."""
    assert strategy_executor._is_continuous_market("us_futures") is False


def test_true_continuous_markets_remain_continuous(strategy_executor):
    """24/7 markets should still be recognised as continuous."""
    assert strategy_executor._is_continuous_market("24/7") is True
