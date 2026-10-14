"""Native Order/Broker boundary proofs; no Strategy, network or trading loop."""

import importlib.util
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
from pathlib import Path
from queue import Empty

import pytest

from lumibot.entities import Asset, Order

SPEC = importlib.util.spec_from_file_location(
    "schwab_lifecycle_transport",
    Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py",
)
TRANSPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRANSPORT)
SchwabLifecycleTransport = TRANSPORT.SchwabLifecycleTransport
data_only_schwab_broker = TRANSPORT.data_only_schwab_broker
drain_stream_events = TRANSPORT.drain_stream_events


@pytest.fixture(autouse=True)
def no_external_requests(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("SCHWAB_CANCEL_DIAGNOSTICS", "false")

    def reject(*_args, **_kwargs):
        raise AssertionError("External network is forbidden in this fixture")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def stock(quantity=100, duration="day"):
    return Order(
        strategy="fixture",
        asset=Asset("LW"),
        quantity=quantity,
        side=Order.OrderSide.SELL_SHORT,
        order_type=Order.OrderType.MARKET,
        time_in_force=duration,
    )


def submitted(quantity=100):
    transport = SchwabLifecycleTransport()
    broker, queues = data_only_schwab_broker(transport)
    current = stock(quantity)
    assert broker.submit_order(current) is current
    drain_stream_events(broker)
    return transport, broker, queues, current


def refresh(broker, current):
    return broker._refresh_missing_active_order_from_broker(current, current.strategy)


def callbacks(queues):
    result = []
    while True:
        try:
            result.append(queues.priority_queue.get_nowait())
        except Empty:
            return result


@pytest.mark.parametrize("requested", [60, 100])
def test_native_serializer_records_actual_quantity_without_forcing_correct_replacement(requested):
    transport, broker, _queues, first = submitted()
    second = stock(requested, "gtc")
    assert broker.submit_order(second) is second
    attempts = transport.attempted_orders
    assert attempts[0]["order_spec"]["orderLegCollection"][0]["quantity"] == 100
    assert attempts[1]["order_spec"]["orderLegCollection"][0]["quantity"] == requested
    assert attempts[0]["order_spec"]["duration"] == "DAY"
    assert attempts[1]["order_spec"]["duration"] == "GOOD_TILL_CANCEL"
    assert first.identifier != second.identifier
    json.dumps(attempts)


def test_native_option_serializer_and_parser_keep_actual_contract_and_underlying():
    transport = SchwabLifecycleTransport()
    broker, queues = data_only_schwab_broker(transport)
    current = Order(
        strategy="fixture",
        asset=Asset("LW", asset_type="option", expiration=date(2026, 5, 29), strike=38, right="CALL"),
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=Decimal("0.04"),
        time_in_force="day",
    )
    assert broker.submit_order(current) is current
    transport.queue_order_reads(current.identifier, {"status": "FILLED", "filledQuantity": 1, "averagePrice": 0.04})
    observed = broker._parse_broker_order(broker._pull_broker_order(current.identifier), current.strategy)
    assert observed.asset.symbol == "LW"
    assert observed.asset.underlying_asset is None
    broker._process_schwab_order_snapshot(observed)
    assert [transaction.quantity for transaction in current.transactions] == [1]
    assert callbacks(queues)[0][1]["quantity"] == 1
    assert (
        transport.attempted_orders[0]["order_spec"]["orderLegCollection"][0]["instrument"]["symbol"]
        == "LW    260529C00038000"
    )


def test_scripted_partial_pending_cancel_canceled_and_late_fill_use_native_delta_queue():
    transport, broker, queues, current = submitted()
    transport.queue_order_reads(
        current.identifier,
        {"status": "PARTIALLY_FILLED", "filledQuantity": 40, "averagePrice": 10},
        {"status": "PENDING_CANCEL", "filledQuantity": 40, "averagePrice": 10},
        {"status": "CANCELED", "filledQuantity": 40, "averagePrice": 10},
        {"status": "CANCELED", "filledQuantity": 50, "averagePrice": 10},
        {"status": "CANCELED", "filledQuantity": 50, "averagePrice": 10},
    )
    refresh(broker, current)
    broker.cancel_order(current)
    assert current.status == Order.OrderStatus.CANCELLING
    refresh(broker, current)
    assert current.status == Order.OrderStatus.CANCELLING
    for _ in range(3):
        refresh(broker, current)
    assert current.is_canceled()
    assert [transaction.quantity for transaction in current.transactions] == [40, 10]
    events = callbacks(queues)
    assert [name for name, _payload in events] == [
        broker.PARTIALLY_FILLED_ORDER,
        broker.CANCELED_ORDER,
        broker.PARTIALLY_FILLED_ORDER,
    ]
    assert [payload["quantity"] for name, payload in events if name == broker.PARTIALLY_FILLED_ORDER] == [40, 10]
    assert queues._queue_wakeup.is_set()


def test_missing_and_stale_reads_do_not_erase_native_confirmed_fills():
    transport, broker, queues, current = submitted()
    transport.queue_order_reads(
        current.identifier,
        {"status": "PARTIALLY_FILLED", "filledQuantity": 50, "averagePrice": 10},
        None,
        {"status": "PARTIALLY_FILLED", "filledQuantity": 40, "averagePrice": 10},
        {"status": "FILLED", "filledQuantity": 100, "averagePrice": 10},
    )
    refresh(broker, current)
    assert refresh(broker, current) is False
    assert [transaction.quantity for transaction in current.transactions] == [50]
    assert current.is_active()
    refresh(broker, current)
    refresh(broker, current)
    refresh(broker, current)  # Exhausted script repeats the last broker snapshot.
    assert current.is_filled()
    assert [transaction.quantity for transaction in current.transactions] == [50, 50]
    assert [payload["quantity"] for _name, payload in callbacks(queues)] == [50, 50]


def test_confirmed_broker_rejection_preserves_description_and_deduplicates_native_error():
    transport, broker, queues, current = submitted()
    reason = "Fixture short sale rejection"
    transport.queue_order_reads(current.identifier, {"status": "REJECTED", "statusDescription": reason})
    refresh(broker, current)
    refresh(broker, current)
    assert current.status == Order.OrderStatus.ERROR
    assert reason in current.error_message
    events = callbacks(queues)
    assert len(events) == 1 and events[0][0] == broker.ERROR_ORDER
    assert reason in str(events[0][1]["error"])
    assert current.transactions == []


def test_http_submission_rejection_records_attempt_and_uses_native_stream_error_queue():
    transport = SchwabLifecycleTransport()
    broker, queues = data_only_schwab_broker(transport)
    transport.queue_submission_response(400, "Fixture bad request")
    current = stock()
    assert broker.submit_order(current) is None
    drain_stream_events(broker)
    assert len(transport.attempted_orders) == 1
    assert transport.attempted_orders[0]["order_spec"]["orderLegCollection"][0]["quantity"] == 100
    assert current.status == Order.OrderStatus.ERROR
    events = callbacks(queues)
    assert len(events) == 1 and events[0][0] == broker.ERROR_ORDER
    assert "Fixture bad request" in str(events[0][1]["error"])


def test_cancel_http_conflict_reconciles_actual_filled_snapshot_instead_of_assuming_cancel():
    transport, broker, queues, current = submitted()
    transport.queue_cancel_response(current.identifier, 409, "Already filled")
    transport.queue_order_reads(current.identifier, {"status": "FILLED", "filledQuantity": 100, "averagePrice": 10})
    broker.cancel_order(current)
    assert current.is_filled()
    assert [transaction.quantity for transaction in current.transactions] == [100]
    events = callbacks(queues)
    assert len(events) == 1 and events[0][0] == broker.FILLED_ORDER


def test_paused_cancel_transport_does_not_block_native_fill_reconciliation():
    transport, broker, queues, current = submitted()
    pause = transport.pause_next("cancel_order", current.identifier)
    transport.queue_order_reads(
        current.identifier, {"status": "PARTIALLY_FILLED", "filledQuantity": 40, "averagePrice": 10}
    )
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(broker.cancel_order, current)
        try:
            assert pause.entered.wait(1)
            refresh(broker, current)
            events = callbacks(queues)
            assert len(events) == 1 and events[0][0] == broker.PARTIALLY_FILLED_ORDER
            assert events[0][1]["quantity"] == 40
            assert not pending.done()
        finally:
            pause.release.set()
        pending.result(timeout=1)
    assert current.status == Order.OrderStatus.CANCELLING


def test_transport_owns_copies_and_rejects_unowned_script_identity():
    transport, _broker, _queues, current = submitted()
    patch = {"status": "PARTIALLY_FILLED", "filledQuantity": 40, "averagePrice": 10}
    transport.queue_order_reads(current.identifier, patch)
    patch["filledQuantity"] = 100
    assert transport.get_order(current.identifier, "fixture-account").json()["filledQuantity"] == 40
    attempts = transport.attempted_orders
    attempts[0]["order_spec"]["orderLegCollection"][0]["quantity"] = 0
    assert transport.attempted_orders[0]["order_spec"]["orderLegCollection"][0]["quantity"] == 100
    with pytest.raises(ValueError):
        transport.queue_order_reads("foreign-account-order", patch)
    assert transport.get_order("foreign-account-order", "fixture-account").status_code == 404


def test_transport_bounds_request_recording_instead_of_hiding_overflow():
    transport = SchwabLifecycleTransport(max_requests=2)
    for _ in range(2):
        transport.get_order("missing", "fixture-account")
    with pytest.raises(RuntimeError, match="limit"):
        transport.get_order("missing", "fixture-account")
    assert len(transport.calls) == 2


def test_rejected_over_budget_read_does_not_consume_or_publish_another_snapshot():
    transport = SchwabLifecycleTransport(max_requests=2)
    broker, _queues = data_only_schwab_broker(transport)
    current = stock()
    broker.submit_order(current)
    transport.queue_order_reads(
        current.identifier,
        {"status": "PARTIALLY_FILLED", "filledQuantity": 40},
        {"status": "FILLED", "filledQuantity": 100},
    )
    transport.get_order(current.identifier, "fixture-account")
    with pytest.raises(RuntimeError, match="limit"):
        transport.get_order(current.identifier, "fixture-account")
    assert transport._latest[current.identifier]["filledQuantity"] == 40
    assert len(transport._reads[current.identifier]) == 1


def test_broad_history_foreign_and_duplicate_inputs_do_not_create_owned_fill_callbacks():
    transport, broker, queues, current = submitted()
    owned = transport.get_order(current.identifier, "fixture-account").json()
    owned.update(status="PARTIALLY_FILLED", filledQuantity=40, averagePrice=10)
    foreign = json.loads(json.dumps(owned))
    foreign.update(orderId="unrelated-order", status="FILLED", filledQuantity=100)
    transport.queue_history_response([foreign, owned, owned])
    for payload in broker._pull_broker_all_orders():
        broker._process_schwab_order_snapshot(broker._parse_broker_order(payload, current.strategy))
    assert broker.get_tracked_order("unrelated-order") is None
    assert [transaction.quantity for transaction in current.transactions] == [40]
    events = callbacks(queues)
    assert len(events) == 1 and events[0][1]["quantity"] == 40
    assert len(transport.attempted_orders) == 1


def test_unknown_account_argument_cannot_silently_read_the_fixture_account():
    transport, _broker, _queues, current = submitted()
    with pytest.raises(ValueError, match="fixture account"):
        transport.get_order(current.identifier, "different-account")
    assert len(transport.calls) == 1


def test_paused_reply_timeout_is_recorded_as_undelivered_not_an_observed_success():
    transport, _broker, _queues, current = submitted()
    pause = transport.pause_next("get_order", current.identifier, timeout=0.01)
    with pytest.raises(TimeoutError):
        transport.get_order(current.identifier, "fixture-account")
    assert pause.entered.is_set()
    call = transport.calls[-1]
    assert call["status_code"] == 200  # The broker reply was prepared, not received.
    assert call["delivery"] == "transport_exception"
    assert call["error_type"] == "TimeoutError"
    assert transport.calls[0]["delivery"] == "delivered"


def strategy_order_refresh(broker, identifier):
    # Exercise the public accessor and real refresh/reducer path without a
    # market scheduler, authentication, or external network.
    from lumibot.strategies.strategy import Strategy
    strategy = Strategy.__new__(Strategy)
    strategy._name = "fixture"
    strategy.broker = broker
    return strategy.get_order(identifier)


def history_snapshot(transport, current, **patch):
    snapshot = dict(transport._base[current.identifier])
    snapshot.update(patch)
    transport.queue_history_response([snapshot])


def test_strategy_get_order_broad_history_delivers_late_fill_after_cancellation():
    transport, broker, queues, current = submitted()
    for cumulative in (40, 60, 60):
        history_snapshot(transport, current, status="CANCELED", filledQuantity=cumulative, averagePrice=10)
        assert strategy_order_refresh(broker, current.identifier) is current
    assert current.is_canceled()
    assert [item.quantity for item in current.transactions] == [40, 20]
    assert current.raw_broker_payload["filledQuantity"] == 60
    events = callbacks(queues)
    assert [payload["quantity"] for name, payload in events if name == broker.PARTIALLY_FILLED_ORDER] == [40, 20]
    assert [name for name, payload in events].count(broker.CANCELED_ORDER) == 1


def test_strategy_get_order_broad_history_preserves_broker_rejection():
    transport, broker, queues, current = submitted()
    history_snapshot(transport, current, status="REJECTED", statusDescription="Fixture rejection detail")
    assert strategy_order_refresh(broker, current.identifier) is current
    assert current.status == Order.OrderStatus.ERROR
    assert "Fixture rejection detail" in current.error_message
    assert callbacks(queues)[0][0] == broker.ERROR_ORDER


def test_strategy_get_order_broad_history_filled_does_not_skip_transactions():
    transport, broker, queues, current = submitted()
    history_snapshot(transport, current, status="FILLED", filledQuantity=100, averagePrice=10)
    assert strategy_order_refresh(broker, current.identifier) is current
    assert current.is_filled()
    assert sum(item.quantity for item in current.transactions) == 100
    assert callbacks(queues)[0][0] == broker.FILLED_ORDER


def test_strategy_get_order_stale_history_cannot_reopen_canceled_order():
    transport, broker, queues, current = submitted()
    history_snapshot(transport, current, status="CANCELED", filledQuantity=40, averagePrice=10)
    strategy_order_refresh(broker, current.identifier)
    history_snapshot(transport, current, status="WORKING", filledQuantity=0)
    strategy_order_refresh(broker, current.identifier)
    assert current.is_canceled()
    assert sum(item.quantity for item in current.transactions) == 40


def test_strategy_get_order_pending_cancel_then_fill_retains_native_cancel_request():
    transport, broker, queues, current = submitted()
    broker.cancel_order(current)
    assert current.status == Order.OrderStatus.CANCELLING
    history_snapshot(transport, current, status="PENDING_CANCEL", filledQuantity=0)
    strategy_order_refresh(broker, current.identifier)
    assert current.status == Order.OrderStatus.CANCELLING
    history_snapshot(transport, current, status="FILLED", filledQuantity=100, averagePrice=10)
    strategy_order_refresh(broker, current.identifier)
    assert current.is_filled()
    assert sum(item.quantity for item in current.transactions) == 100
    assert len([call for call in transport.calls if call["method"] == "cancel_order"]) == 1
    assert [name for name, payload in callbacks(queues)] == [broker.FILLED_ORDER]
