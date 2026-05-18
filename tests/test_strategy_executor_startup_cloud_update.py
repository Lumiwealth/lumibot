import logging
from datetime import datetime

from lumibot.strategies.strategy_executor import StrategyExecutor


class _BrokerStub:
    def should_continue(self):
        return True

    def is_market_open(self):
        return True


class _StrategyStub:
    is_backtesting = False

    def __init__(self):
        self.broker = _BrokerStub()
        self.logger = logging.getLogger("tests.startup_cloud_update")
        self.events = []

    def send_update_to_cloud(self):
        self.events.append("cloud")
        return True

    def await_market_to_open(self, timedelta=None):
        self.events.append(("await", timedelta))

    def _update_cash_with_dividends(self):
        self.events.append("dividends")

    def get_datetime(self):
        return datetime(2026, 5, 18, 9, 30)


def _executor(strategy):
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.strategy = strategy
    executor.broker = strategy.broker
    executor.lifecycle_last_date = {}
    executor._before_starting_trading = lambda: strategy.events.append("before_starting")
    executor._before_market_opens = lambda: strategy.events.append("before_open")
    return executor


def test_setup_market_session_sends_startup_cloud_update_before_market_wait():
    strategy = _StrategyStub()
    executor = _executor(strategy)

    assert executor._setup_market_session(has_data_source=False) is True

    assert strategy.events[0] == "cloud"
    assert strategy.events[1] == ("await", None)
    assert "before_starting" in strategy.events
    assert hasattr(executor, "_last_updated_cloud")


def test_startup_cloud_update_failure_does_not_block_market_wait():
    strategy = _StrategyStub()

    def fail_cloud_update():
        strategy.events.append("cloud")
        raise RuntimeError("snapshot failed")

    strategy.send_update_to_cloud = fail_cloud_update
    executor = _executor(strategy)

    assert executor._setup_market_session(has_data_source=False) is True

    assert strategy.events[0] == "cloud"
    assert strategy.events[1] == ("await", None)
    assert not hasattr(executor, "_last_updated_cloud")
