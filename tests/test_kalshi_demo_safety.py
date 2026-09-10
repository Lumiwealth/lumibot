"""Offline safety checks for the opt-in Demo test cleanup harness."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.test_kalshi_apitest import _cleanup_demo_orders_and_position


def strategy():
    obj = Mock(name="demo-strategy")
    obj.broker._pending_submissions = {}
    obj.get_position.return_value = None
    return obj


def test_empty_demo_cleanup_never_submits():
    demo = strategy()
    _cleanup_demo_orders_and_position(demo, "asset", [], 1)
    demo.broker.sync_orders.assert_called_once()
    demo.submit_order.assert_not_called()


def test_demo_cleanup_cancels_remaining_orders_after_one_cancellation_fails():
    demo = strategy()
    orders = [Mock(identifier="first"), Mock(identifier="second")]
    demo.cancel_order.side_effect = [RuntimeError("temporary"), None]
    demo.get_order.return_value.is_canceled.return_value = True
    with pytest.raises(pytest.fail.Exception, match="cleanup incomplete"):
        _cleanup_demo_orders_and_position(demo, "asset", orders, 2)
    assert demo.cancel_order.call_count == 2
    demo.submit_order.assert_not_called()


def test_demo_cleanup_stops_on_an_uncertain_submission():
    demo = strategy()
    order = Mock(_kalshi_client_order_id="pending")
    order.was_transmitted.return_value = False
    demo.broker._pending_submissions = {"pending": order}
    with pytest.raises(pytest.fail.Exception, match="cleanup incomplete"):
        _cleanup_demo_orders_and_position(demo, "asset", [order], 1)
    demo.submit_order.assert_not_called()
    demo.get_position.assert_not_called()


@pytest.mark.parametrize("quantity", [-1, 2])
def test_demo_cleanup_rejects_unexpected_inventory(quantity):
    demo = strategy()
    demo.get_position.return_value = SimpleNamespace(quantity=quantity)
    with pytest.raises(pytest.fail.Exception, match="Unexpected Demo position"):
        _cleanup_demo_orders_and_position(demo, "asset", [], 1)
    demo.submit_order.assert_not_called()


def test_demo_cleanup_partial_closes_use_only_remaining_position():
    demo = strategy()
    demo.get_position.side_effect = [SimpleNamespace(quantity=1), SimpleNamespace(quantity=0.5), None]
    demo.get_quote.return_value.bid = 0.2
    demo.get_order.return_value.is_filled.return_value = True
    _cleanup_demo_orders_and_position(demo, "asset", [], 1)
    assert [call.args[1] for call in demo.create_order.call_args_list] == [1, 0.5]
    assert demo.submit_order.call_count == 2


def test_demo_cleanup_has_a_finite_retry_bound():
    demo = strategy()
    demo.get_position.return_value = SimpleNamespace(quantity=1)
    demo.get_quote.return_value.bid = 0.2
    demo.get_order.return_value.is_canceled.return_value = True
    with pytest.raises(AssertionError, match="left a position"):
        _cleanup_demo_orders_and_position(demo, "asset", [], 1)
    assert demo.submit_order.call_count == 3


def test_demo_cleanup_never_retries_an_uncertain_close():
    demo = strategy()
    demo.get_position.return_value = SimpleNamespace(quantity=1)
    demo.get_quote.return_value.bid = 0.2
    demo.submit_order.side_effect = RuntimeError("unknown submit outcome")
    with pytest.raises(RuntimeError, match="unknown submit outcome"):
        _cleanup_demo_orders_and_position(demo, "asset", [], 1)
    demo.submit_order.assert_called_once()
