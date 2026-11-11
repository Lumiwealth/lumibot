"""Cancellation-activity regressions through native Schwab order processing.

The neutral inputs below are derived contract variations, not recorded account
responses. Their EXECUTION/CANCELED shape is documented from observed responses:
https://schwaby.readthedocs.io/en/latest/util.html#add-up-what-an-order-filled-leg-by-leg
Live account capture and full strategy execution are separate evidence.
"""

import importlib.util
from copy import deepcopy
from pathlib import Path
from queue import Empty

import pytest

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order

_SPEC = importlib.util.spec_from_file_location(
    "execution_activity_transport", Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py"
)
_TRANSPORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_TRANSPORT)


@pytest.fixture(autouse=True)
def no_external_requests(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")

    def reject(*_args, **_kwargs):
        raise AssertionError("No external requests in execution-activity regression")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def activity(kind, quantity, price, *, leg_id=1):
    row = {
        "activityType": "EXECUTION",
        "executionLegs": [{"legId": leg_id, "quantity": quantity, "price": price}],
    }
    if kind is not None:
        row["executionType"] = kind
    return row


def payload(activities, *, filled=0, quantity=100, status="CANCELED"):
    return {
        "orderId": "execution-activity-order",
        "orderType": "LIMIT",
        "status": status,
        "price": 10,
        "filledQuantity": filled,
        "orderLegCollection": [{
            "legId": 1, "instruction": "BUY", "quantity": quantity,
            "orderLegType": "EQUITY", "instrument": {"symbol": "SYN", "assetType": "EQUITY"},
        }],
        "orderActivityCollection": deepcopy(activities),
    }


@pytest.mark.parametrize("cancel_kind", ["CANCELED", "CANCELLED"])
def test_canceled_execution_legs_are_not_filled(cancel_kind):
    raw = payload([activity(cancel_kind, 1, 0)], quantity=1)
    assert Schwab._schwab_execution_summary(raw, leg_id=1)[0] == 0


def test_mixed_fill_and_cancellation_counts_only_executed_quantity_and_price():
    raw = payload([activity("FILL", 40, 10), activity("CANCELED", 60, 0)], filled=40)
    assert Schwab._schwab_execution_summary(raw, leg_id=1) == (40, 10)


@pytest.mark.parametrize("kind", ["FILL", None])
def test_explicit_and_legacy_fills_preserve_leg_identity_and_vwap(kind):
    raw = payload([
        activity(kind, 1, 10), activity(kind, 1, 11),
        activity(kind, 5, 99, leg_id=2), activity("CANCELED", 1, 0),
    ], filled=2, quantity=3)
    assert Schwab._schwab_execution_summary(raw, leg_id="1") == (2, 10.5)


def test_nonexecution_activity_cannot_contribute_fill_quantity():
    row = activity("FILL", 100, 0)
    row["activityType"] = "ORDER_UPDATE"
    assert Schwab._schwab_execution_summary(payload([row], filled=0))[0] == 0


@pytest.mark.parametrize("path", ["direct", "stream_wakeup"])
@pytest.mark.parametrize("actual_filled", [0, 40, 100])
def test_canceled_activity_uses_native_reconciliation_and_callback_queue(path, actual_filled):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport)
    order = Order(
        strategy="fixture", asset=Asset("SYN"), quantity=100, side="buy",
        order_type="limit", limit_price=10, time_in_force="day",
    )
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    activities = []
    if actual_filled:
        activities.append(activity("FILL", actual_filled, 10))
    activities.append(activity("CANCELED", 100 - actual_filled, 0))
    transport.queue_order_reads(order.identifier, {
        "status": "CANCELED", "filledQuantity": actual_filled,
        "averagePrice": 10 if actual_filled else 0,
        "orderActivityCollection": activities,
    })
    # Exact reads and stream-triggered reconciliation use the real parser and
    # reducer. Repeated terminal observations may not deliver another fill.
    if path == "direct":
        for _ in range(2):
            broker._refresh_missing_active_order_from_broker(order, order.strategy)
    else:
        for _ in range(2):
            broker._reconcile_active_schwab_orders()
    events = []
    while True:
        try:
            events.append(recipient.priority_queue.get_nowait())
        except Empty:
            break
    fills = [(name, data) for name, data in events if name in {
        broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER,
    }]
    assert sum(transaction.quantity for transaction in order.transactions) == actual_filled
    assert sum(data["quantity"] for _name, data in fills) == actual_filled
    assert len(fills) == (1 if actual_filled else 0)
    # A confirmed complete execution wins cancellation; calling it partial and
    # then canceled would prevent filled-order handlers from managing exposure.
    assert sum(name == broker.CANCELED_ORDER for name, _data in events) == (actual_filled < 100)
    if actual_filled == 100:
        assert fills[0][0] == broker.FILLED_ORDER
        assert order.is_filled()


def test_one_contract_executed_before_cancellation_delivers_a_full_fill():
    from datetime import date

    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport)
    option = Asset("SYN", asset_type="option", expiration=date(2026, 11, 20), strike=10, right="CALL")
    order = Order(strategy="fixture", asset=option, quantity=1, side="buy_to_open",
                  order_type="limit", limit_price=1, time_in_force="day")
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    transport.queue_order_reads(order.identifier, {
        "status": "CANCELED", "filledQuantity": 1, "averagePrice": 1,
        "orderActivityCollection": [activity("FILL", 1, 1), activity("CANCELED", 0, 0)],
    })
    broker._reconcile_active_schwab_orders()
    event, data = recipient.priority_queue.get_nowait()
    assert event == broker.FILLED_ORDER
    assert data["quantity"] == 1
    assert recipient.priority_queue.empty()
    assert order.is_filled()

