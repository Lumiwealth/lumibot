"""Second-review safety rules for skipped live reads. No network, no broker writes.

* A strategy that asks for positions or cash gets a fresh broker read, even while the
  per-iteration sync is being skipped; an external change (manual sell, assignment,
  dividend) that arrives while the account stream is down is seen before the next order.
* Before submit or modify after a skipped account read, positions and cash are read again.
* Only brokers that stamp their reads watch unresolved submits (no misleading warning).
* A Schwab submit that got no order id but reached Schwab is re-attached by asset, side,
  quantity and entry time; two candidates are never guessed and nothing is resent.
* Backtests never call the wall clock for order status times.
"""
import logging

import pytest

from lumibot.entities import Asset, Order, Position
from tests.test_schwab_lifecycle_transport import drain_stream_events, stock, submitted
from tests.test_strategy_live_order_accessors import _LiveBroker, _order, _strategy


def _skip_reads(broker, strategy):
    """The state after a per-iteration sync was skipped: good reads stamped, then served idle."""
    epoch = getattr(broker, "_order_activity_epoch", 0)
    broker._note_orders_list_ok(epoch)
    broker._note_account_read_ok(epoch)
    assert broker.idle_read_skip_age(strategy.name) is not None


# ------------------------------------------------------------------ fresh positions and cash

def test_an_external_sell_while_idle_is_seen_by_the_next_position_read():
    strategy, broker = _strategy()
    asset = Asset("LWX")
    broker.broker_positions = [Position(strategy.name, asset, 100)]
    strategy.get_position(asset)
    _skip_reads(broker, strategy)
    broker.broker_positions = [Position(strategy.name, asset, 40)]  # sold by hand, stream down
    pulls = broker.position_pull_count
    assert strategy.get_position(asset).quantity == 40
    assert broker.position_pull_count == pulls + 1


def test_a_dividend_while_idle_is_seen_by_get_cash():
    strategy, broker = _strategy()
    _skip_reads(broker, strategy)
    broker.broker_balances = (100_250.0, {}, 100_250.0)
    assert strategy.get_cash() == pytest.approx(100_250.0)


def test_positions_and_cash_are_read_again_before_an_order_after_a_skipped_account_read():
    strategy, broker = _strategy()
    _skip_reads(broker, strategy)
    positions, balances = broker.position_pull_count, broker.balance_pull_count
    order = strategy.create_order(Asset("LWX"), 1, Order.OrderSide.BUY, limit_price=1.0)
    strategy.submit_order(order)
    assert broker.position_pull_count == positions + 1
    assert broker.balance_pull_count == balances + 1
    # Nothing was skipped since: the next order does not pay the extra reads again.
    strategy.submit_order(strategy.create_order(Asset("LWX"), 1, Order.OrderSide.BUY, limit_price=1.0))
    assert broker.position_pull_count == positions + 1


def test_max_age_zero_forces_a_position_read_inside_a_ttl():
    strategy, broker = _strategy()
    asset = Asset("LWX")
    strategy.get_positions(broker_refresh_ttl_seconds=60)
    pulls = broker.position_pull_count
    strategy.get_positions(broker_refresh_ttl_seconds=60)
    assert broker.position_pull_count == pulls  # inside the ttl
    strategy.get_positions(broker_refresh_ttl_seconds=60, max_age=0)
    strategy.get_position(asset, broker_refresh_ttl_seconds=60, max_age=0)
    assert broker.position_pull_count == pulls + 2


# ------------------------------------------------------------------ unresolved submit watch

def test_a_broker_that_does_not_stamp_reads_never_logs_the_unresolved_submit_warning(caplog):
    strategy, broker = _strategy()
    broker._submit_order = lambda order: None
    with caplog.at_level(logging.WARNING):
        broker.submit_order(_order(strategy.name, "x-1", Order.OrderStatus.UNPROCESSED))
    assert "without a broker order id" not in caplog.text
    assert getattr(broker, "_unresolved_submit_until", None) is None


def _submit_that_times_out_after_reaching_schwab(transport, broker, quantity=10):
    order = stock(quantity)
    transport.pause_next("place_order", timeout=0.05)  # Schwab took it; the answer never came
    assert broker.submit_order(order) is None
    drain_stream_events(broker)
    return order


def test_a_timed_out_submit_that_reached_schwab_is_re_attached_and_tracked():
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    lost = _submit_that_times_out_after_reaching_schwab(transport, broker)
    live_id = transport.attempted_orders[-1]["order_id"]
    assert live_id and lost.identifier != live_id
    broker.sync_orders("fixture")
    drain_stream_events(broker)
    assert lost.identifier == live_id
    assert broker.get_tracked_order(live_id) is lost
    assert lost.is_active()
    assert not broker.__dict__.get("_unresolved_submits")
    # From here it is an ordinary tracked order: it can be canceled.
    broker.cancel_order(lost)
    assert [c["method"] for c in transport.calls].count("cancel_order") == 1


def test_no_matching_order_keeps_reading_and_re_attaches_nothing():
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    transport.queue_submission_response(503, "upstream failure")  # never reached Schwab
    lost = stock(10)
    assert broker.submit_order(lost) is None
    drain_stream_events(broker)
    broker.sync_orders("fixture")
    broker._note_account_read_ok(broker._order_activity_epoch)
    assert broker.get_tracked_order(lost.identifier) is lost and not lost.is_active()
    assert broker.idle_read_skip_age("fixture") is None  # still watching


def test_two_candidates_are_never_guessed_and_nothing_is_resent(caplog):
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    a = _submit_that_times_out_after_reaching_schwab(transport, broker)
    b = _submit_that_times_out_after_reaching_schwab(transport, broker)
    sends = [c["method"] for c in transport.calls].count("place_order")
    with caplog.at_level(logging.ERROR):
        broker.sync_orders("fixture")
        broker.sync_orders("fixture")
    drain_stream_events(broker)
    assert not a.is_active() and not b.is_active()  # neither guessed
    # loud, once per lost order (not once per read)
    assert caplog.text.count("could not be matched to one Schwab order") == 2
    assert [c["method"] for c in transport.calls].count("place_order") == sends  # never resent
    broker._note_account_read_ok(broker._order_activity_epoch)
    assert broker.idle_read_skip_age("fixture") is None  # keeps reading


def test_a_different_side_or_quantity_is_not_a_match():
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    lost = _submit_that_times_out_after_reaching_schwab(transport, broker, quantity=10)
    lost.quantity = 11  # the order Schwab holds is for 10
    broker.sync_orders("fixture")
    assert not lost.is_active()


# ------------------------------------------------------------------ backtests and the clock

def test_backtest_orders_never_record_status_times(monkeypatch):
    monkeypatch.setattr(Order, "_record_status_times", False)
    order = _order("s", "b-1", Order.OrderStatus.OPEN)
    order.status = Order.OrderStatus.FILLED
    assert order.status_updated_at is None


def test_a_live_broker_turns_status_times_on(monkeypatch):
    monkeypatch.setattr(Order, "_record_status_times", False)
    _LiveBroker()
    assert Order._record_status_times is True


def test_a_timed_out_submit_that_already_filled_is_re_attached_with_its_fill():
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    lost = _submit_that_times_out_after_reaching_schwab(transport, broker)
    live_id = transport.attempted_orders[-1]["order_id"]
    snapshot = dict(transport._latest[live_id], status="FILLED", filledQuantity=10)
    snapshot["orderActivityCollection"] = [{
        "activityType": "EXECUTION", "executionType": "FILL", "quantity": 10,
        "executionLegs": [{"legId": 1, "quantity": 10, "price": 10.0, "time": "2026-11-24T15:00:00+0000"}],
    }]
    others = [value for key, value in transport._latest.items() if key != live_id and value is not None]
    transport.queue_history_response(others + [snapshot])
    broker.sync_orders("fixture")
    drain_stream_events(broker)
    assert lost.identifier == live_id and lost.is_filled()
