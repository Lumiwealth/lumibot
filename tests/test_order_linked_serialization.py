"""Linked orders (bracket and OCO) serialize without infinite recursion.

Regression (2026-11-13): the stop and target child orders reference each
other through ``dependent_order``, and ``Order.to_dict`` called ``to_dict`` on
every attribute that has one, so serializing a bracket or OCO order (directly,
through ``Position.to_dict`` or through the live restart checkpoint
``export_order_state``) raised RecursionError. Linked orders are written as
identifiers and the restore path rebuilds the links.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from lumibot.entities import Asset, Order, Position

_SPEC = importlib.util.spec_from_file_location(
    "linked_order_transport", Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py"
)
_TRANSPORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_TRANSPORT)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError("Serialization tests cannot send HTTP")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def _bracket(strategy="linked"):
    return Order(strategy, Asset("SYN"), 10, "buy", order_type="market", order_class=Order.OrderClass.BRACKET,
                 secondary_limit_price=12.0, secondary_stop_price=9.0)


def _oco(strategy="linked"):
    return Order(strategy, Asset("SYN"), 10, "sell", order_class=Order.OrderClass.OCO,
                 limit_price=12.0, stop_price=9.0)


@pytest.mark.parametrize("make", [_bracket, _oco])
def test_linked_orders_serialize_to_identifiers(make):
    parent = make()
    first, second = parent.child_orders
    assert first.dependent_order is second and second.dependent_order is first

    data = parent.to_dict()

    json.dumps(data)  # finite and JSON-ready
    children = data["child_orders"]
    assert children[0]["dependent_order"] == str(second.identifier)
    assert children[1]["dependent_order"] == str(first.identifier)


def test_position_holding_a_bracket_child_serializes():
    parent = _bracket()
    position = Position("linked", Asset("SYN"), 10, orders=[parent, *parent.child_orders])
    json.dumps(position.to_dict())


def test_restart_checkpoint_keeps_stop_and_target_linked():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="linked")
    entry = Order("linked", Asset("SYN"), 1, "buy", order_type="limit", limit_price=10)
    broker.submit_order(entry)
    _TRANSPORT.drain_stream_events(broker)
    target = Order("linked", Asset("SYN"), 1, "sell", order_type="limit", limit_price=12, identifier="target-1")
    stop = Order("linked", Asset("SYN"), 1, "sell", order_type="stop", stop_price=9, identifier="stop-1")
    target.dependent_order, stop.dependent_order = stop, target
    entry.child_orders = [target, stop]

    state = json.loads(json.dumps(broker.export_order_state("linked")))

    recovered, _ = _TRANSPORT.data_only_schwab_broker(transport, name="linked")
    recovered.restore_order_state(state, "linked")
    restored_target = recovered.get_tracked_order("target-1")
    restored_stop = recovered.get_tracked_order("stop-1")
    assert restored_target is not None and restored_stop is not None
    assert restored_target.dependent_order is restored_stop
    assert restored_stop.dependent_order is restored_target
