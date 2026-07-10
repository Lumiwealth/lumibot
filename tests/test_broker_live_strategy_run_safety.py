from types import SimpleNamespace

from tests.test_broker_live_strategy_run_apitest import (
    _cancel_order_until_terminal,
    _ensure_strategy_order_terminal,
)


class _BrokerWithDelayedCancellation:
    def __init__(self):
        self.cancel_calls = 0
        self.statuses = iter(["open", "pending_cancel", "canceled"])

    def _pull_broker_order(self, identifier):
        return {"id": identifier, "status": next(self.statuses, "canceled")}

    def cancel_order(self, order):
        self.cancel_calls += 1


def test_cancel_order_until_terminal_retries_pending_cancellation():
    broker = _BrokerWithDelayedCancellation()
    order = SimpleNamespace(identifier="paper-order-1")

    status = _cancel_order_until_terminal(broker, order, timeout=1, retry_interval=0)

    assert status == "canceled"
    assert broker.cancel_calls == 2


def test_strategy_safety_cancel_skips_terminal_order():
    broker = _BrokerWithDelayedCancellation()
    strategy = SimpleNamespace(
        submitted_order=SimpleNamespace(identifier="paper-order-1"),
        submitted_identifier="paper-order-1",
        status_after_cancel="canceled",
        cancel_requested=True,
        cancel_error=None,
    )

    _ensure_strategy_order_terminal(strategy, broker)

    assert broker.cancel_calls == 0


def test_strategy_safety_cancel_preserves_prior_error():
    broker = _BrokerWithDelayedCancellation()
    broker.statuses = iter(["canceled"])
    strategy = SimpleNamespace(
        submitted_order=SimpleNamespace(identifier="paper-order-1"),
        submitted_identifier="paper-order-1",
        status_after_cancel=None,
        cancel_requested=True,
        cancel_error="initial cancellation failed",
    )

    _ensure_strategy_order_terminal(strategy, broker)

    assert strategy.status_after_cancel == "canceled"
    assert strategy.cancel_error == "initial cancellation failed"
