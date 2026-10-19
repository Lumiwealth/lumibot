"""Shared-account isolation and order duration through native broker methods."""
import importlib.util
from pathlib import Path
from queue import Empty
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor

spec = importlib.util.spec_from_file_location(
    "shared_runtime_transport", Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py")
transport_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport_module)


def queue_rows(queue):
    rows = []
    while True:
        try:
            rows.append(queue.get_nowait())
        except Empty:
            return rows


def native_strategy(broker, name):
    strategy = Strategy.__new__(Strategy)
    strategy._name = name
    strategy.broker = broker
    strategy._quote_asset = Asset("USD", asset_type="forex")
    strategy.broker.data_source.get_datetime = lambda **_: __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
    return strategy


@pytest.mark.parametrize("first_iteration", [True, False])
def test_fill_from_current_scan_is_delivered_before_live_iteration_finishes(first_iteration):
    """A newly submitted order is not startup history, even on the first scan."""
    from queue import Queue
    from threading import Event

    delivered = Event()
    executor = StrategyExecutor.__new__(StrategyExecutor)
    executor.strategy = SimpleNamespace(
        name="OwnedScanner", _name="OwnedScanner", logger=Mock(),
        _first_iteration=first_iteration, _update_cash=Mock(),
    )
    executor.broker = SimpleNamespace(_first_iteration=first_iteration)
    executor.priority_queue = Queue()
    executor.queue = Queue()
    executor._process_smart_limit_orders = Mock()
    executor._on_filled_order = Mock(side_effect=lambda **_: delivered.set())
    order = Order(strategy="OwnedScanner", asset=Asset("F"), quantity=1,
                  side="buy", order_type="limit", limit_price=10, time_in_force="day")

    def current_scan():
        executor.priority_queue.put((executor.FILLED_ORDER, {
            "position": None, "order": order, "price": 10,
            "quantity": 1, "multiplier": 1,
        }))
        delivered.wait(timeout=0.3)

    executor._run_live_trading_iteration_with_priority_drains(current_scan)
    assert delivered.is_set(), "The current iteration's fill was discarded as startup history"
    executor._on_filled_order.assert_called_once()


@pytest.mark.parametrize("first_iteration", [True, False])
@pytest.mark.parametrize("other_name", ["SharedClass", "DifferentClass"])
def test_account_sync_cannot_adopt_another_instances_order(first_iteration, other_name):
    transport = transport_module.SchwabLifecycleTransport()
    first, first_queues = transport_module.data_only_schwab_broker(transport, name="SharedClass")
    second, _ = transport_module.data_only_schwab_broker(transport, name=other_name)
    order = Order(strategy=other_name, asset=Asset("F"), quantity=1,
                  side="buy", order_type="limit", limit_price=10, time_in_force="day")
    second.submit_order(order)
    transport_module.drain_stream_events(second)
    first._first_iteration = first_iteration
    first._get_balances_at_broker = lambda *_: (1000, 0, 1000)
    first.sync_positions = lambda *_: None
    strategy = SimpleNamespace(name="SharedClass", logger=Mock(), quote_asset=Asset("USD", asset_type="forex"),
                               _set_cash_position=lambda *_: None)
    executor = SimpleNamespace(strategy=strategy, broker=first, name="SharedClass",
        _index_orders_by_identifier=StrategyExecutor._index_orders_by_identifier,
        _get_all_order_identifiers=StrategyExecutor._get_all_order_identifiers)
    # Exercise the production broad-sync owner, not only the guarded reducer.
    StrategyExecutor.sync_broker(executor)
    assert first.get_tracked_order(order.identifier) is None
    assert not queue_rows(first_queues.priority_queue)


def test_account_visibility_does_not_assign_a_manual_order_to_the_strategy():
    transport = transport_module.SchwabLifecycleTransport()
    broker, queues = transport_module.data_only_schwab_broker(transport, name="SharedClass")
    transport.place_order(transport.account_hash, {"orderType": "LIMIT", "session": "NORMAL",
        "duration": "DAY", "quantity": 1, "price": 10, "orderStrategyType": "SINGLE",
        "orderLegCollection": [{"instruction": "BUY", "quantity": 1,
            "instrument": {"symbol": "F", "assetType": "EQUITY"}}]})
    broker._first_iteration = True
    broker._get_balances_at_broker = lambda *_: (1000, 0, 1000)
    broker.sync_positions = lambda *_: None
    strategy = SimpleNamespace(name="SharedClass", logger=Mock(), quote_asset=Asset("USD", asset_type="forex"),
                               _set_cash_position=lambda *_: None)
    executor = SimpleNamespace(strategy=strategy, broker=broker, name="SharedClass",
        _index_orders_by_identifier=StrategyExecutor._index_orders_by_identifier,
        _get_all_order_identifiers=StrategyExecutor._get_all_order_identifiers)
    StrategyExecutor.sync_broker(executor)
    assert broker.get_tracked_order("fixture-1") is None
    assert not queue_rows(queues.priority_queue)


def test_locally_submitted_order_still_reconciles_when_account_history_is_visible():
    transport = transport_module.SchwabLifecycleTransport()
    broker, _ = transport_module.data_only_schwab_broker(transport, name="SharedClass")
    order = Order(strategy="SharedClass", asset=Asset("F"), quantity=1, side="buy",
        order_type="limit", limit_price=10, time_in_force="day")
    broker.submit_order(order)
    transport_module.drain_stream_events(broker)
    broker._first_iteration = True
    broker._get_balances_at_broker = lambda *_: (1000, 0, 1000)
    broker.sync_positions = lambda *_: None
    strategy = SimpleNamespace(name="SharedClass", logger=Mock(), quote_asset=Asset("USD", asset_type="forex"),
                               _set_cash_position=lambda *_: None)
    executor = SimpleNamespace(strategy=strategy, broker=broker, name="SharedClass",
        _index_orders_by_identifier=StrategyExecutor._index_orders_by_identifier,
        _get_all_order_identifiers=StrategyExecutor._get_all_order_identifiers)
    StrategyExecutor.sync_broker(executor)
    assert broker.get_tracked_order(order.identifier) is order
    assert order.is_active()


@pytest.mark.parametrize("duration,wire", [("day", "DAY"), ("gtc", "GOOD_TILL_CANCEL")])
def test_strategy_order_api_preserves_explicit_intraday_and_gtc_duration(duration, wire):
    transport = transport_module.SchwabLifecycleTransport()
    broker, _ = transport_module.data_only_schwab_broker(transport)
    strategy = native_strategy(broker, "fixture")
    order = strategy.create_order(Asset("F"), 1, "sell_short", time_in_force=duration)
    broker.submit_order(order)
    assert transport.attempted_orders[0]["order_spec"]["duration"] == wire


def test_canceled_option_without_executions_cannot_generate_a_fill():
    from datetime import date
    transport = transport_module.SchwabLifecycleTransport()
    broker, queues = transport_module.data_only_schwab_broker(transport)
    order = Order(strategy="fixture", asset=Asset("F", asset_type="option", expiration=date(2026, 10, 16),
        strike=10, right="CALL"), quantity=1, side="buy_to_open", order_type="limit", limit_price=.10,
        time_in_force="day")
    broker.submit_order(order)
    transport_module.drain_stream_events(broker)
    queue_rows(queues.priority_queue)
    transport.queue_order_reads(order.identifier, {"status": "CANCELED", "filledQuantity": 0,
                                                 "orderActivityCollection": []})
    broker._refresh_missing_active_order_from_broker(order, "fixture")
    events = queue_rows(queues.priority_queue)
    assert not [event for event, _ in events if event in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}]
    assert not order.transactions


def test_real_fill_reported_with_cancellation_is_counted_once_across_duplicate_reads():
    transport = transport_module.SchwabLifecycleTransport()
    broker, queues = transport_module.data_only_schwab_broker(transport)
    order = Order(strategy="fixture", asset=Asset("F"), quantity=100,
        side="sell_short", order_type="limit", limit_price=10, time_in_force="day")
    broker.submit_order(order)
    transport_module.drain_stream_events(broker)
    queue_rows(queues.priority_queue)
    for _ in range(2):
        transport.queue_order_reads(order.identifier, {"status": "CANCELED",
            "filledQuantity": 40, "remainingQuantity": 60, "price": 10,
            "orderActivityCollection": [{"activityType": "EXECUTION",
                "executionType": "FILL", "quantity": 40,
                "executionLegs": [{"legId": 1, "quantity": 40, "price": 10,
                    "time": "2026-10-15T14:00:00Z"}]}]})
        broker._refresh_missing_active_order_from_broker(order, "fixture")
    events = queue_rows(queues.priority_queue)
    fills = [payload for event, payload in events
        if event in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}]
    assert len(fills) == 1
    assert sum(transaction.quantity for transaction in order.transactions) == 40


def test_late_report_of_pre_cancel_fill_preserves_terminal_status_and_counts_only_delta():
    transport = transport_module.SchwabLifecycleTransport()
    broker, queues = transport_module.data_only_schwab_broker(transport)
    order = Order(strategy="fixture", asset=Asset("F"), quantity=100,
        side="sell_short", order_type="limit", limit_price=10, time_in_force="day")
    broker.submit_order(order)
    transport_module.drain_stream_events(broker)
    queue_rows(queues.priority_queue)
    executions = [{"activityType": "EXECUTION", "executionType": "FILL", "quantity": qty,
        "executionLegs": [{"legId": 1, "quantity": qty, "price": 10,
            "time": time}]} for qty, time in [(40, "2026-10-15T14:00:01Z"),
                                               (20, "2026-10-15T14:00:07Z")]]
    # Cancellation is observed after both execution times. The additional
    # execution is reported late; it did not happen after cancellation.
    for quantity, activities in [(40, executions[:1]), (60, executions), (60, executions)]:
        transport.queue_order_reads(order.identifier, {"status": "CANCELED",
            "filledQuantity": quantity, "remainingQuantity": 100 - quantity,
            "price": 10, "orderActivityCollection": activities})
        broker._refresh_missing_active_order_from_broker(order, "fixture")
    events = queue_rows(queues.priority_queue)
    assert sum(transaction.quantity for transaction in order.transactions) == 60
    assert order.status == Order.OrderStatus.CANCELED
    assert len([event for event, _ in events if event == broker.CANCELED_ORDER]) == 1
    assert len([event for event, _ in events if event == broker.PARTIALLY_FILLED_ORDER]) == 2


@pytest.mark.parametrize('first_iteration', [True, False])
def test_account_sync_of_zero_fill_cancellation_does_not_emit_a_fill(first_iteration):
    from datetime import date
    transport = transport_module.SchwabLifecycleTransport()
    broker, queues = transport_module.data_only_schwab_broker(transport, name='GenericScanner')
    order = Order(strategy='GenericScanner', asset=Asset('F', asset_type='option',
        expiration=date(2026, 10, 16), strike=10, right='CALL'), quantity=1,
        side='buy_to_open', order_type='limit', limit_price=.10, time_in_force='day')
    broker.submit_order(order)
    transport_module.drain_stream_events(broker)
    queue_rows(queues.priority_queue)
    snapshot = transport.get_order(order.identifier, transport.account_hash).json()
    snapshot.update(status='CANCELED', filledQuantity=0, orderActivityCollection=[])
    transport.queue_history_response([snapshot])
    broker._first_iteration = first_iteration
    broker._get_balances_at_broker = lambda *_: (1000, 0, 1000)
    broker.sync_positions = lambda *_: None
    strategy = SimpleNamespace(name='GenericScanner', logger=Mock(),
        quote_asset=Asset('USD', asset_type='forex'), _set_cash_position=lambda *_: None)
    executor = SimpleNamespace(strategy=strategy, broker=broker, name='GenericScanner',
        _index_orders_by_identifier=StrategyExecutor._index_orders_by_identifier,
        _get_all_order_identifiers=StrategyExecutor._get_all_order_identifiers)
    StrategyExecutor.sync_broker(executor)
    events = queue_rows(queues.priority_queue)
    assert not [event for event, _ in events
        if event in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}]
    assert not order.transactions


def test_known_canceled_hedge_can_report_a_later_positive_fill_delta_once():
    transport = transport_module.SchwabLifecycleTransport()
    broker, queues = transport_module.data_only_schwab_broker(transport)
    order = Order(strategy='fixture', asset=Asset('F'), quantity=100,
        side='sell_short', order_type='limit', limit_price=10, time_in_force='day')
    broker.submit_order(order)
    transport_module.drain_stream_events(broker)
    queue_rows(queues.priority_queue)
    for cumulative in (40, 60, 60):
        executions = [{'activityType': 'EXECUTION', 'executionType': 'FILL', 'quantity':40,
            'executionLegs':[{'legId':1, 'quantity':40, 'price':10,
                'time':'2026-10-15T14:00:00Z'}]}]
        if cumulative == 60:
            executions.append({'activityType':'EXECUTION', 'executionType':'FILL', 'quantity':20,
                'executionLegs':[{'legId':1, 'quantity':20, 'price':10,
                    'time':'2026-10-15T14:00:01Z'}]})
        transport.queue_order_reads(order.identifier, {'status':'CANCELED',
            'filledQuantity':cumulative, 'remainingQuantity':100-cumulative,
            'orderActivityCollection':executions})
        broker._refresh_missing_active_order_from_broker(order, 'fixture')
    events = queue_rows(queues.priority_queue)
    fills = [payload for event, payload in events if event in {
        broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}]
    assert [float(payload['quantity']) for payload in fills] == [40, 20]
    assert sum(transaction.quantity for transaction in order.transactions) == 60
    assert order.status == 'canceled'
    assert sum(event == broker.CANCELED_ORDER for event, _ in events) == 1


@pytest.mark.parametrize("filled_field", ["omitted", None, 0, "0", "0.0"])
def test_canceled_one_contract_without_execution_never_invents_a_fill(filled_field):
    from datetime import date
    transport = transport_module.SchwabLifecycleTransport()
    broker, queues = transport_module.data_only_schwab_broker(transport)
    order = Order(strategy="fixture", asset=Asset("F", asset_type="option",
        expiration=date(2026, 10, 16), strike=10, right="CALL"), quantity=1,
        side="buy_to_open", order_type="limit", limit_price=.10, time_in_force="day")
    broker.submit_order(order)
    transport_module.drain_stream_events(broker)
    queue_rows(queues.priority_queue)
    raw=transport.get_order(order.identifier,transport.account_hash).json()
    raw.update(status="CANCELED",remainingQuantity=1,orderActivityCollection=[])
    if filled_field=="omitted":raw.pop("filledQuantity",None)
    else:raw["filledQuantity"]=filled_field
    observed=broker._parse_broker_order(raw,"fixture")
    broker._process_schwab_order_snapshot(observed)
    events=queue_rows(queues.priority_queue)
    assert not [event for event,_ in events if event in {broker.FILLED_ORDER,broker.PARTIALLY_FILLED_ORDER}]
    assert not order.transactions


def test_full_contract_execution_with_terminal_cancel_reaches_filled_callback_once():
    from datetime import date
    transport=transport_module.SchwabLifecycleTransport()
    broker,queues=transport_module.data_only_schwab_broker(transport)
    order=Order(strategy="fixture",asset=Asset("F",asset_type="option",
        expiration=date(2026,10,16),strike=10,right="CALL"),quantity=1,
        side="buy_to_open",order_type="limit",limit_price=.10,time_in_force="day")
    broker.submit_order(order)
    transport_module.drain_stream_events(broker)
    queue_rows(queues.priority_queue)
    raw=transport.get_order(order.identifier,transport.account_hash).json()
    raw.update(status="CANCELED",filledQuantity=1,remainingQuantity=0,
        orderActivityCollection=[{"activityType":"EXECUTION","executionType":"FILL",
            "quantity":1,"executionLegs":[{"legId":1,"quantity":1,"price":.10,
            "time":"2026-10-15T14:00:00Z"}]}])
    for _ in range(2):
        broker._process_schwab_order_snapshot(broker._parse_broker_order(raw,"fixture"))
    events=queue_rows(queues.priority_queue)
    assert sum(transaction.quantity for transaction in order.transactions)==1
    fills=[(event,payload) for event,payload in events
           if event in {broker.FILLED_ORDER,broker.PARTIALLY_FILLED_ORDER}]
    assert [(event,float(payload["quantity"])) for event,payload in fills]==[(broker.FILLED_ORDER,1.0)]
    assert order.is_filled()
