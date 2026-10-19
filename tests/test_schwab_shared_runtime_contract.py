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
