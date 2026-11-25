"""Schwab reports a partly filled live order as WORKING with execution rows.

Schwab's order status list has no separate partial state in normal responses:
an order that traded some of its quantity stays WORKING and carries
``filledQuantity`` plus EXECUTION activity. LumiBot must surface those fills
while the order is still open (partial-fill status, filled quantity, average
price, one on_partially_filled_order per new fill amount), and still deliver
exactly one on_filled_order at completion.

The inputs are synthetic contract variations; no account data is used.
"""

import importlib.util
from copy import deepcopy
from pathlib import Path
from queue import Empty

import pytest

from lumibot.entities import Asset, Order

_SPEC = importlib.util.spec_from_file_location(
    "working_partial_fill_transport", Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py"
)
_TRANSPORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_TRANSPORT)


@pytest.fixture(autouse=True)
def no_external_requests(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")

    def reject(*_args, **_kwargs):
        raise AssertionError("No external requests in working partial-fill regression")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def fill(quantity, price):
    return {
        "activityType": "EXECUTION",
        "executionType": "FILL",
        "executionLegs": [{"legId": 1, "quantity": quantity, "price": price}],
    }


def snapshot(status, fills):
    filled = sum(q for q, _ in fills)
    average = sum(q * p for q, p in fills) / filled if filled else 0
    return {
        "status": status,
        "filledQuantity": filled,
        "remainingQuantity": 10 - filled,
        "averagePrice": average,
        "orderActivityCollection": [fill(q, p) for q, p in fills],
    }


def drain(recipient):
    events = []
    while True:
        try:
            events.append(recipient.priority_queue.get_nowait())
        except Empty:
            return events


def fill_events(broker, events):
    return [
        (name, data["quantity"], data["price"])
        for name, data in events
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]


def submitted_order(transport):
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport)
    order = Order(
        strategy="fixture", asset=Asset("SYN"), quantity=10, side="buy",
        order_type="limit", limit_price=1.5, time_in_force="day",
    )
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    drain(recipient)
    return broker, recipient, order


@pytest.mark.parametrize("path", ["direct", "reconcile"])
def test_working_order_with_fills_reports_each_new_fill_once(path):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient, order = submitted_order(transport)

    def observe():
        if path == "direct":
            broker._refresh_missing_active_order_from_broker(order, order.strategy)
        else:
            broker._reconcile_active_schwab_orders()
        return fill_events(broker, drain(recipient))

    first = [(4, 1.00)]
    second = first + [(3, 1.20)]
    final = second + [(3, 1.30)]
    transport.queue_order_reads(
        order.identifier,
        snapshot("WORKING", first),
        # Same execution seen again (for example a stream wake, then the REST read).
        snapshot("WORKING", first),
        snapshot("WORKING", second),
        snapshot("FILLED", final),
        snapshot("FILLED", final),
    )

    assert observe() == [(broker.PARTIALLY_FILLED_ORDER, 4, 1.00)]
    assert Order.is_equivalent_status(order.status, Order.OrderStatus.PARTIALLY_FILLED)
    assert order.is_active()
    assert float(order.filled_quantity) == 4
    assert order.get_fill_price() == pytest.approx(1.00)
    # Listed once while open: a second copy would double every exact read.
    assert broker.get_active_tracked_orders().count(order) == 1

    assert observe() == []

    events = observe()
    # The callback carries the price of this new fill, not the running average.
    assert events == [(broker.PARTIALLY_FILLED_ORDER, 3, pytest.approx(1.20))]
    assert float(order.filled_quantity) == 7
    assert order.get_fill_price() == pytest.approx((4 * 1.00 + 3 * 1.20) / 7, abs=0.01)

    events = observe()
    assert events == [(broker.FILLED_ORDER, 3, pytest.approx(1.30))]
    assert order.is_filled()
    assert float(order.filled_quantity) == 10
    assert sum(t.quantity for t in order.transactions) == 10
    assert order.get_fill_price() == pytest.approx((4 * 1.00 + 3 * 1.20 + 3 * 1.30) / 10, abs=0.01)

    assert observe() == []


def test_parsed_working_order_with_fills_reads_as_partially_filled():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport)
    raw = {
        "orderId": "working-partial",
        "orderType": "LIMIT",
        "price": 1.5,
        "duration": "DAY",
        "orderLegCollection": [{
            "legId": 1, "instruction": "BUY", "quantity": 10,
            "orderLegType": "EQUITY", "instrument": {"symbol": "SYN", "assetType": "EQUITY"},
        }],
        **snapshot("WORKING", [(4, 1.00)]),
    }
    parsed = broker._parse_broker_order(deepcopy(raw), "fixture")
    assert Order.is_equivalent_status(parsed.status, Order.OrderStatus.PARTIALLY_FILLED)
    assert float(parsed.filled_quantity) == 4

    untouched = deepcopy(raw)
    untouched.update(snapshot("WORKING", []))
    parsed = broker._parse_broker_order(untouched, "fixture")
    assert Order.is_equivalent_status(parsed.status, Order.OrderStatus.NEW)
