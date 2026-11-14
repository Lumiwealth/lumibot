"""Protective orders held at the broker survive a restart with their links intact.

A stop, a bracket (entry plus target and stop) and an OCO (target plus stop) are
placed through the offline Schwab transport, the strategy saves its order checkpoint,
and a fresh process restores it and runs the startup ``sync_orders``. Every order must
come back with its prices, its parent/child links and the one-cancels-other link
between the two exits, so a fill or cancel after the restart reaches the right order.

The bracket and OTO cases also pin the Schwab fix for stock exit legs: their
``sell_to_close`` side had no stock template, so a stock bracket was never sent and
stayed "unprocessed" with only a log line.
"""

import json

import pytest

from lumibot.entities import Asset, Order
from tests.test_order_restart_without_ownership_id import _TRANSPORT, no_network, restart, strategy_checkpoint  # noqa: F401


def _place(broker, order):
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    assert order.identifier is not None and str(order.status).lower() != "error", order.error_message
    return order


def _restart_with(orders_factory):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    before, _ = _TRANSPORT.data_only_schwab_broker(transport, name="own-bot")
    placed = [_place(before, order) for order in orders_factory()]
    state = json.loads(json.dumps(strategy_checkpoint(before)))
    after, _ = restart(transport, state)
    return transport, placed, after


def _assert_linked_exits(after, parent, target, stop):
    restored = after.get_tracked_order(parent.identifier)
    assert restored is not None
    children = {child.order_type: child for child in restored.child_orders}
    assert set(children) == {Order.OrderType.LIMIT, Order.OrderType.STOP}
    assert float(children[Order.OrderType.LIMIT].limit_price) == pytest.approx(target)
    assert float(children[Order.OrderType.STOP].stop_price) == pytest.approx(stop)
    for child in children.values():
        assert child.parent_identifier == parent.identifier
        assert after.get_tracked_order(child.identifier) is child
    assert children[Order.OrderType.LIMIT].dependent_order is children[Order.OrderType.STOP]
    assert children[Order.OrderType.STOP].dependent_order is children[Order.OrderType.LIMIT]


def test_restart_restores_a_stop_order_with_its_trigger():
    _, (stop,), after = _restart_with(lambda: [Order("own-bot", Asset("SYN"), 1, "sell", stop_price=9)])
    restored = after.get_tracked_order(stop.identifier)
    assert restored is not None and restored.order_type == Order.OrderType.STOP
    assert float(restored.stop_price) == pytest.approx(9)
    assert restored.is_active()


def test_restart_restores_a_bracket_with_linked_target_and_stop():
    transport, (bracket,), after = _restart_with(
        lambda: [Order("own-bot", Asset("SYN"), 1, "buy", limit_price=10, order_class="bracket",
                       secondary_limit_price=12, secondary_stop_price=8)]
    )
    sent = transport.attempted_orders[-1]["order_spec"]
    assert sent["orderStrategyType"] == "TRIGGER" and sent["childOrderStrategies"], sent
    _assert_linked_exits(after, bracket, 12, 8)


def test_restart_restores_an_oco_with_linked_target_and_stop():
    _, (oco,), after = _restart_with(
        lambda: [Order("own-bot", Asset("SYN"), 1, "sell", limit_price=12, stop_price=8, order_class="oco",
                       position_filled=True)]
    )
    _assert_linked_exits(after, oco, 12, 8)


def test_restart_restores_an_oto_stop_leg():
    _, (oto,), after = _restart_with(
        lambda: [Order("own-bot", Asset("SYN"), 1, "buy", limit_price=10, order_class="oto",
                       secondary_stop_price=8)]
    )
    restored = after.get_tracked_order(oto.identifier)
    assert [child.order_type for child in restored.child_orders] == [Order.OrderType.STOP]
    child = restored.child_orders[0]
    assert float(child.stop_price) == pytest.approx(8)
    assert child.parent_identifier == oto.identifier
