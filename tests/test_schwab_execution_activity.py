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

from lumibot.brokers.broker import LumibotBrokerAPIError
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


@pytest.mark.parametrize("duration,expected", [("DAY", "day"), ("GOOD_TILL_CANCEL", "gtc")])
def test_parsed_order_keeps_the_duration_reported_by_schwab(duration, expected):
    broker = Schwab.__new__(Schwab)
    raw = payload([], quantity=1, status="NEW")
    raw["duration"] = duration
    order = broker._parse_broker_order(raw, "fixture")
    assert order.time_in_force == expected


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



def test_direct_refresh_keeps_reported_duration_on_local_order():
    broker, _ = _TRANSPORT.data_only_schwab_broker(_TRANSPORT.SchwabLifecycleTransport())
    order = Order("synthetic-scanner", Asset("SYN"), 1, "buy", identifier="duration-order", time_in_force="gtc")
    broker._unprocessed_orders.append(order)
    observed = Order.from_dict(order.to_dict())
    observed.time_in_force = "day"
    observed.raw_broker_payload = {"duration": "DAY"}
    observed.status = Order.OrderStatus.NEW
    broker._apply_direct_order_observation(order, observed)
    assert order.time_in_force == "day"


def test_fill_proof_log_carries_quantity_and_actual_observation_source(caplog):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport)
    order = Order("fixture", Asset("SYN"), 100, "buy", order_type="limit", limit_price=10)
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    transport.queue_order_reads(order.identifier, {
        "status": "CANCELED", "filledQuantity": 40,
        "orderActivityCollection": [activity("FILL", 40, 10), activity("CANCELED", 60, 0)],
    })
    # Capture the call the broker makes on its own logger object. Handler-based
    # capture still missed it in CI depending on test order (quiet-log settings
    # and a lazily loaded strategy logger can drop INFO before any handler).
    records = []
    real_logger = broker.logger

    class _Recorder:
        def info(self, message, *args, **kwargs):
            records.append(message % args if args else message)
            real_logger.info(message, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(real_logger, name)

    broker.logger = _Recorder()
    try:
        broker._reconcile_active_schwab_orders()
    finally:
        broker.logger = real_logger
    proof = [m for m in records if "OrderFillProof" in m]
    assert len(proof) == 1
    for expected in ("event_quantity=40", "cumulative_quantity=40", "broker_filled_quantity=40", "source=rest_direct_order"):
        assert expected in proof[0]
    assert broker._get_order_health().snapshot()["fill_callbacks_enqueued"] == 1


def test_diagnostic_consumer_failure_cannot_interrupt_fill_delivery():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport)
    order = Order("fixture", Asset("SYN"), 1, "buy", order_type="limit", limit_price=10)
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    def broken(**kwargs):
        raise RuntimeError("diagnostic output unavailable")
    broker.on_order_observation = broken
    transport.queue_order_reads(order.identifier, {"status": "FILLED", "filledQuantity": 1, "averagePrice": 10})
    broker._reconcile_active_schwab_orders()
    assert recipient.priority_queue.get_nowait()[0] == broker.FILLED_ORDER


@pytest.mark.parametrize("http_status", [204, 503])
def test_cancel_metrics_failure_cannot_change_native_cancel_result(http_status, monkeypatch):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport)
    order = Order("fixture", Asset("SYN"), 1, "buy", order_type="limit", limit_price=10)
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    transport.queue_cancel_response(order.identifier, http_status, "native broker result")
    def broken(_seconds):
        raise RuntimeError("metric unavailable")
    monkeypatch.setattr(broker._get_order_health(), "cancel_response", broken)
    if http_status == 204:
        assert broker.cancel_order(order) is None
        assert order.status == Order.OrderStatus.CANCELLING
    else:
        with pytest.raises(LumibotBrokerAPIError, match="HTTP 503"):
            broker.cancel_order(order)


def test_broker_counter_disagreement_is_visible_and_does_not_drop_real_execution(caplog):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport)
    order = Order("fixture", Asset("SYN"), 100, "buy", order_type="limit", limit_price=10)
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    transport.queue_order_reads(order.identifier, {
        "status": "CANCELED", "filledQuantity": 0,
        "orderActivityCollection": [activity("FILL", 40, 10), activity("CANCELED", 60, 0)],
    })
    with caplog.at_level("WARNING"):
        broker._reconcile_active_schwab_orders()
    assert sum(t.quantity for t in order.transactions) == 40
    assert recipient.priority_queue.get_nowait()[1]["quantity"] == 40
    assert broker._get_order_health().snapshot()["contradictions"] == {"canceled_zero_broker_filled_with_execution": 1}
