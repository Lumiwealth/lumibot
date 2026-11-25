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
import datetime
import logging
from copy import deepcopy

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


def _schwab_time(offset_seconds=0.0):
    moment = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=offset_seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%S+0000")


def _submit_that_times_out_after_reaching_schwab(transport, broker, quantity=10, order=None):
    order = order or stock(quantity)
    transport.pause_next("place_order", timeout=0.05)  # Schwab took it; the answer never came
    assert broker.submit_order(order) is None
    live_id = transport.attempted_orders[-1]["order_id"]
    transport._latest[live_id]["enteredTime"] = _schwab_time()  # Schwab stamps its entry time
    drain_stream_events(broker)
    return order


def _another_identical_order_at_schwab(transport, entered_offset_seconds, status="WORKING", **changes):
    """An order nobody in this process sent: a person's or another bot's."""
    template = deepcopy(next(value for value in transport._latest.values() if value is not None))
    identifier = f"other-{len(transport._latest) + 1}"
    template.update(orderId=identifier, status=status, filledQuantity=0, **changes)
    template["enteredTime"] = _schwab_time(entered_offset_seconds)
    template.pop("orderActivityCollection", None)
    transport._latest[identifier] = template
    transport._base[identifier] = deepcopy(template)
    return identifier


def _lost_with_no_schwab_copy(transport, broker, order=None):
    """A lost submit whose order never reached Schwab (only other orders are there)."""
    transport.queue_submission_response(503, "upstream failure")
    order = order or stock(10)
    assert broker.submit_order(order) is None
    drain_stream_events(broker)
    return order


@pytest.mark.parametrize(
    "offset,status,changes",
    [
        (60, "WORKING", {}),               # a person's identical order a minute later
        (-30, "WORKING", {}),              # another bot's identical order before the submit
        (0, "CANCELED", {}),               # an identical order that is already canceled
        (0, "REJECTED", {}),
    ],
)
def test_an_identical_order_outside_the_submit_window_or_ended_is_never_bound(offset, status, changes):
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    lost = _lost_with_no_schwab_copy(transport, broker)
    other = _another_identical_order_at_schwab(transport, offset, status, quantity=10, **changes)
    for leg in transport._latest[other].get("orderLegCollection", []):
        leg["quantity"] = 10
    broker.sync_orders("fixture")
    drain_stream_events(broker)
    assert lost.identifier != other and not lost.is_active()
    assert broker._unresolved_submits  # still watching, still reading
    assert broker.get_tracked_order(other) is None  # someone else's order stays theirs


def test_same_asset_side_and_quantity_but_another_limit_price_is_never_bound():
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    mine = Order(strategy="fixture", asset=Asset("LW"), quantity=10, side=Order.OrderSide.SELL_SHORT,
                 order_type=Order.OrderType.LIMIT, limit_price=10.00, time_in_force="day")
    lost = _lost_with_no_schwab_copy(transport, broker, mine)
    other = _another_identical_order_at_schwab(transport, 0, "WORKING", quantity=10, orderType="LIMIT", price=10.05)
    for leg in transport._latest[other].get("orderLegCollection", []):
        leg["quantity"] = 10
    broker.sync_orders("fixture")
    assert lost.identifier != other and not lost.is_active()


def test_an_order_with_no_entry_time_is_never_bound():
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    lost = _submit_that_times_out_after_reaching_schwab(transport, broker)
    live_id = transport.attempted_orders[-1]["order_id"]
    transport._latest[live_id].pop("enteredTime")
    broker.sync_orders("fixture")
    assert lost.identifier != live_id and not lost.is_active()


def test_positions_and_cash_changed_outside_the_bot_while_idle_are_in_place_before_the_next_order():
    """A trade by hand or an assignment while the bot is idle and the stream is down: the
    strategy's own state is up to date when its next order goes out, without it asking."""
    strategy, broker = _strategy()
    asset = Asset("LWX")
    broker.broker_positions = [Position(strategy.name, asset, 100)]
    strategy.get_position(asset)
    _skip_reads(broker, strategy)
    broker.broker_positions = [Position(strategy.name, asset, 300)]  # assigned 2 contracts
    broker.broker_balances = (80_000.0, {}, 110_000.0)                 # paid for the assignment
    strategy.submit_order(strategy.create_order(asset, 1, Order.OrderSide.SELL, limit_price=1.0))
    assert strategy.get_position(asset, broker_refresh=False).quantity == 300
    assert strategy.cash == pytest.approx(80_000.0)


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


def test_status_times_switch_reaches_an_order_module_loaded_later():
    """A live broker built before Order is imported still turns status times on."""
    import subprocess
    import sys
    import textwrap

    code = textwrap.dedent(
        """
        import sys
        from lumibot.brokers.broker import Broker
        Broker._enable_order_status_times()
        assert "lumibot.entities.order" not in sys.modules
        from lumibot.entities.order import Order
        print(Order._record_status_times)
        """
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip().splitlines()[-1] == "True"


def test_one_schwab_order_matching_two_lost_submits_is_never_guessed(caplog):
    """Two identical lost submits, only one reached Schwab: neither is bound (CodeRabbit PR #1340)."""
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    a = _submit_that_times_out_after_reaching_schwab(transport, broker)  # reached Schwab
    b = _lost_with_no_schwab_copy(transport, broker, order=stock(10))  # never reached Schwab
    sends = [c["method"] for c in transport.calls].count("place_order")
    with caplog.at_level(logging.ERROR):
        broker.sync_orders("fixture")
    drain_stream_events(broker)
    assert not a.is_active() and not b.is_active()
    assert "will not guess which one it belongs to" in caplog.text
    assert [c["method"] for c in transport.calls].count("place_order") == sends
