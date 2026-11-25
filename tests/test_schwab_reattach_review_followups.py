"""Follow-ups to the 4.6.98 review of lost-submit re-attach and order read ages.

* A Schwab 201 with no Location header and no body reached Schwab but gave no
  order id. It must be re-attachable like a timed-out submit.
* A single-leg smart limit order is sent to Schwab as a plain LIMIT; the local
  order keeps its SMART_LIMIT type, and re-attach must still match it.
* ``get_order``/``get_orders`` with a positive ``max_age`` accept a broker read at
  most that old, as ``get_positions`` already does, even when
  ``broker_refresh_ttl_seconds`` is larger.

No network and no strategy loop; the synthetic transport only scripts replies.
"""
import datetime

from lumibot.entities import Asset, Order
from tests.test_schwab_lifecycle_transport import drain_stream_events, stock, submitted
from tests.test_strategy_live_order_accessors import _order, _strategy


def _schwab_time(offset_seconds=0.0):
    moment = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=offset_seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%S+0000")


def _strip_location(transport):
    real_place = transport.place_order

    def place_without_location(account_hash, order_spec):
        response = real_place(account_hash, order_spec)
        del response.headers["Location"]
        return response

    transport.place_order = place_without_location


def test_a_201_without_location_or_body_is_re_attached():
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    _strip_location(transport)
    lost = stock(10)
    assert broker.submit_order(lost) is None
    live_id = transport.attempted_orders[-1]["order_id"]
    transport._latest[live_id]["enteredTime"] = _schwab_time()
    drain_stream_events(broker)

    broker.sync_orders("fixture")
    drain_stream_events(broker)

    assert lost.identifier == live_id
    assert broker.get_tracked_order(live_id) is lost
    assert lost.is_active()


def test_a_smart_limit_sent_as_limit_is_re_attached():
    transport, broker, _queues, first = submitted()
    first.status = Order.OrderStatus.FILLED
    lost = Order(
        strategy="fixture", asset=Asset("LW"), quantity=10, side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT, limit_price=12.34, time_in_force="day",
    )
    transport.pause_next("place_order", timeout=0.05)  # Schwab took it; the answer never came
    assert broker.submit_order(lost) is None
    # Strategy.submit_order sends a smart limit as LIMIT, then restores its type.
    lost.order_type = Order.OrderType.SMART_LIMIT
    live_id = transport.attempted_orders[-1]["order_id"]
    transport._latest[live_id]["enteredTime"] = _schwab_time()
    drain_stream_events(broker)

    broker.sync_orders("fixture")
    drain_stream_events(broker)

    assert lost.identifier == live_id
    assert broker.get_tracked_order(live_id) is lost


def _recorded_order_ttls(strategy):
    seen = []
    strategy._refresh_live_orders = lambda broker_refresh=True, broker_refresh_ttl_seconds=0.0: seen.append(
        broker_refresh_ttl_seconds
    )
    return seen


def test_positive_max_age_bounds_the_order_read_age_like_positions():
    strategy, broker = _strategy()
    working = _order(strategy.name, "open-1", Order.OrderStatus.OPEN)
    broker._new_orders.append(working)
    seen = _recorded_order_ttls(strategy)

    strategy.get_orders(broker_refresh_ttl_seconds=60, max_age=5)
    strategy.get_order("open-1", broker_refresh_ttl_seconds=60, max_age=5)
    strategy.get_orders(identifiers=["open-1"], broker_refresh_ttl_seconds=60, max_age=5)

    assert seen == [5.0, 5.0, 5.0]
