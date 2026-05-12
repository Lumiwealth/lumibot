from __future__ import annotations

from types import SimpleNamespace

from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


class _BrokerActiveOrdersStub:
    IS_BACKTESTING_BROKER = False

    def __init__(self):
        self.name = "stub"
        self.get_active_tracked_orders = None
        self.get_tracked_orders = None


class _MinimalStrategy(Strategy):
    """Strategy stub that avoids Strategy.__init__ (no broker side effects)."""

    def __init__(self, broker):
        self.broker = broker
        self._name = "unit"
        self.logger = SimpleNamespace(error=lambda *_a, **_k: None)

    def log_message(self, *_args, **_kwargs):
        return

    def _validate_order(self, _order):  # noqa: SLF001 - intentional override for unit tests
        return True

    def on_trading_iteration(self):
        return


def test_smart_limit_processing_uses_active_orders_fast_path(mocker):
    broker = _BrokerActiveOrdersStub()
    broker.get_active_tracked_orders = mocker.Mock(return_value=[])
    broker.get_tracked_orders = mocker.Mock(side_effect=AssertionError("Should not scan full tracked order history"))

    strategy = _MinimalStrategy(broker)
    executor = StrategyExecutor(strategy=strategy)

    executor._process_smart_limit_orders()

    broker.get_active_tracked_orders.assert_called_once_with(strategy=strategy.name)
    broker.get_tracked_orders.assert_not_called()
