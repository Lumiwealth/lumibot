"""Exact-path regressions from the September Kalshi readiness audit."""

import copy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.brokers import Kalshi
from lumibot.brokers.kalshi import KalshiStream
from lumibot.entities import Order
from lumibot.tools.kalshi_client import KalshiAPIError
from tests.test_kalshi_broker import ContractStrategy, order
from tests.test_kalshi_broker import broker as _broker_fixture
from tests.test_strategy_live_order_accessors import _order, _strategy


@pytest.fixture
def broker():
    yield from _broker_fixture.__wrapped__()


def test_inconsistent_order_does_not_starve_other_fills_or_mark_refresh_complete(broker):
    first, second = broker.submit_order(order()), broker.submit_order(order())
    broker._client.fill(first, 0.5)
    delayed = broker._client.fills.pop()
    broker._client.fill(second, 2)
    for _ in range(3):
        with pytest.raises(KalshiAPIError, match="incomplete"):
            broker.refresh_orders("test", ttl_seconds=999)
        assert second.is_filled()
        assert len(second.transactions) == 1
        assert not first.transactions
        assert "test" not in broker._live_orders_refresh_at
    broker._client.fills.append(delayed)
    broker.refresh_orders("test", ttl_seconds=999)
    assert first.status == Order.OrderStatus.PARTIALLY_FILLED
    assert len(first.transactions) == 1
    assert "test" in broker._live_orders_refresh_at


def test_failed_missing_order_lookup_does_not_starve_another_missing_order(broker):
    first, second = broker.submit_order(order()), broker.submit_order(order())
    broker._client.fill(second, 2)
    broker._pull_broker_all_orders = lambda: []
    pull = broker._pull_broker_order

    def delayed(identifier):
        if identifier == first.identifier:
            raise KalshiAPIError("temporary")
        return pull(identifier)

    broker._pull_broker_order = delayed
    with pytest.raises(KalshiAPIError, match="incomplete"):
        broker.sync_orders("test")
    assert second.is_filled()
    assert len(second.transactions) == 1


def test_stream_refreshes_positions_even_when_order_sync_fails():
    target = SimpleNamespace(
        _strategy_name="test",
        polling_interval=1,
        logger=Mock(),
        sync_orders=Mock(side_effect=KalshiAPIError("temporary")),
        sync_positions=Mock(),
    )
    stream = KalshiStream(target)
    stream._read_websocket = Mock()
    target.sync_positions.side_effect = lambda _: stream._stop_event.set()
    # Bound the broken implementation too, so its failure cannot hang pytest.
    original = target.sync_orders.side_effect
    calls = []

    def fail(_):
        calls.append(1)
        if len(calls) > 1:
            stream._stop_event.set()
        raise original

    target.sync_orders.side_effect = fail
    stream._run()
    target.sync_positions.assert_called_once_with("test")


@pytest.mark.parametrize("filled", [0, 0.5])
def test_unknown_order_recovers_active_bucket_flags_without_duplicate_callbacks(broker, filled):
    obj = broker.submit_order(order())
    if filled:
        broker._client.fill(obj, filled)
        broker._reconcile_order(obj.identifier)
    broker._client.orders[obj.identifier]["status"] = "future_state"
    broker._reconcile_order(obj.identifier)
    assert obj.status == Order.OrderStatus.UNKNOWN
    subscriber = broker._subscribers.get_list()[0]
    subscriber.add_event.reset_mock()
    broker._client.orders[obj.identifier]["status"] = "resting"
    for _ in range(2):
        broker._reconcile_order(obj.identifier)
    expected = Order.OrderStatus.PARTIALLY_FILLED if filled else Order.OrderStatus.NEW
    assert obj.status == expected
    assert broker.get_active_tracked_orders("test") == [obj]
    assert (obj._partial_filled_event if filled else obj._new_event).is_set()
    assert len(obj.transactions) == int(bool(filled))
    subscriber.add_event.assert_not_called()


@pytest.mark.parametrize("terminal", ["executed", "canceled", "rejected"])
def test_terminal_order_does_not_regress_through_unknown_then_resting(broker, terminal):
    obj = broker.submit_order(order())
    if terminal == "executed":
        broker._client.fill(obj, 2)
    else:
        broker._client.orders[obj.identifier].update(status=terminal, remaining_count_fp="0")
    broker._reconcile_order(obj.identifier)
    status, transactions = obj.status, len(obj.transactions)
    for stale_status in ("future_state", "resting"):
        row = copy.deepcopy(broker._client.orders[obj.identifier])
        row["status"] = stale_status
        broker._apply_snapshot(row, "test")
    assert obj.status == status
    assert obj._closed_event.is_set()
    assert not broker.get_active_tracked_orders("test")
    assert len(obj.transactions) == transactions


def test_strategy_can_read_untracked_archived_order_without_old_fill_callbacks(broker):
    original = broker.submit_order(order())
    broker._client.fill(original, 2)
    archived = broker._client.orders.pop(original.identifier)
    pages = broker._client.pages
    broker._client.pages = lambda path, key, **kwargs: (
        iter([archived]) if path == "/historical/orders" else pages(path, key, **kwargs)
    )
    fresh = Kalshi(client=broker._client, connect_stream=False)
    try:
        strategy = ContractStrategy(broker=fresh, analyze_backtest=False, synchronize_broker_on_start=False)
        fresh._on_filled_order = Mock()
        recovered = strategy.get_order(original.identifier)
        assert recovered is not None and recovered.is_filled()
        assert sum(t.quantity for t in recovered.transactions) == 2
        assert fresh.get_tracked_order(original.identifier) is recovered
        assert strategy.get_order(original.identifier, broker_refresh=False) is recovered
        fresh._on_filled_order.assert_not_called()
    finally:
        fresh.cleanup_streams()


@pytest.mark.parametrize("wrong_account", [False, True])
def test_strategy_missing_or_other_account_order_returns_none(broker, wrong_account):
    if wrong_account:
        obj = broker.submit_order(order())
        broker._client.orders[obj.identifier]["subaccount_number"] = 3
        identifier = obj.identifier
    else:
        identifier = "missing"
    broker._pull_broker_all_orders = lambda: []
    fresh = Kalshi(client=broker._client, connect_stream=False)
    fresh._pull_broker_all_orders = lambda: []
    try:
        strategy = ContractStrategy(broker=fresh, analyze_backtest=False, synchronize_broker_on_start=False)
        assert strategy.get_order(identifier) is None
    finally:
        fresh.cleanup_streams()


def test_generic_strategy_missing_local_order_uses_existing_direct_hook():
    strategy, broker = _strategy()
    expected = _order(strategy.name, "direct", Order.OrderStatus.FILLED)
    broker.direct_orders[expected.identifier] = expected
    try:
        assert strategy.get_order(expected.identifier) is expected
    finally:
        broker.cleanup_streams()


@pytest.mark.parametrize("backtesting,refresh", [(True, True), (False, False)])
def test_generic_direct_fallback_respects_no_network_modes(backtesting, refresh):
    strategy, broker = _strategy()
    broker.IS_BACKTESTING_BROKER = backtesting
    broker._pull_order = Mock(side_effect=AssertionError("unexpected direct call"))
    try:
        assert strategy.get_order("missing", broker_refresh=refresh) is None
        broker._pull_order.assert_not_called()
    finally:
        broker.cleanup_streams()


def test_generic_direct_fallback_does_not_leak_other_strategy_order():
    strategy, broker = _strategy()
    broker.direct_orders["other"] = _order("other-strategy", "other", Order.OrderStatus.FILLED)
    try:
        assert strategy.get_order("other") is None
    finally:
        broker.cleanup_streams()


def test_generic_direct_failure_preserves_none_and_sanitizes_warning():
    strategy, broker = _strategy()
    broker._pull_order = Mock(side_effect=RuntimeError("sensitive provider error"))
    strategy.log_message = Mock()
    try:
        assert strategy.get_order("missing") is None
        broker._pull_order.assert_called_once_with("missing", strategy.name)
        strategy.log_message.assert_called()
        assert "sensitive" not in str(strategy.log_message.call_args_list)
    finally:
        broker.cleanup_streams()


@pytest.mark.parametrize("terminal", ["executed", "canceled"])
def test_unknown_recovers_terminal_lifecycle_once(broker, terminal):
    obj = broker.submit_order(order())
    broker._client.orders[obj.identifier]["status"] = "future_state"
    broker._reconcile_order(obj.identifier)
    if terminal == "executed":
        broker._client.fill(obj, 2)
    else:
        broker._client.orders[obj.identifier].update(status="canceled", remaining_count_fp="0")
    for _ in range(2):
        broker._reconcile_order(obj.identifier)
    assert obj.status == (Order.OrderStatus.FILLED if terminal == "executed" else Order.OrderStatus.CANCELED)
    assert obj._closed_event.is_set()
    assert len(obj.transactions) == (1 if terminal == "executed" else 0)
    assert not broker.get_active_tracked_orders("test")


def test_direct_lookup_rejects_wrong_provider_identifier(broker):
    obj = broker.submit_order(order())
    broker._client.orders[obj.identifier]["order_id"] = "other-id"
    with pytest.raises(KalshiAPIError, match="different order"):
        broker._pull_order(obj.identifier, "test")
    assert broker.get_tracked_order("other-id") is None


def test_tracked_other_strategy_order_never_triggers_direct_reassignment():
    strategy, broker = _strategy()
    owned = _order("another-strategy", "owned", Order.OrderStatus.FILLED)
    broker._filled_orders.append(owned)
    broker._pull_order = Mock(side_effect=AssertionError("must not reassign"))
    try:
        assert strategy.get_order("owned") is None
        broker._pull_order.assert_not_called()
    finally:
        broker.cleanup_streams()


def test_generic_unimplemented_direct_lookup_remains_none():
    strategy, broker = _strategy()
    broker._pull_order = Mock(side_effect=NotImplementedError())
    try:
        assert strategy.get_order("missing") is None
        broker._pull_order.assert_called_once()
    finally:
        broker.cleanup_streams()
