"""A trailing stop can protect a bracket or OTO entry on its own.

Before, a bracket or OTO entry with only ``secondary_trail_percent`` or
``secondary_trail_price`` (no fixed ``secondary_stop_price``) raised
"Order class is OTO but no child order ... has been provided" and the trailing
exit was impossible to attach.
"""

from decimal import Decimal

import pytest

from lumibot.entities import Asset, Order
from tests.test_crypto_backtesting_order_matrix import BASE, QUOTE, _advance_one_bar, _crypto_strategy
from tests.backtest.test_backtesting_broker_processing import position_quantity, submit_and_fill


@pytest.mark.parametrize("trail", [{"secondary_trail_percent": 2.0}, {"secondary_trail_price": 1.5}])
def test_oto_with_only_a_trailing_stop_builds_a_trailing_child(trail):
    order = Order("s", Asset("SPY"), 10, side="buy", order_class=Order.OrderClass.OTO, **trail)
    assert len(order.child_orders) == 1
    child = order.child_orders[0]
    assert child.order_type == Order.OrderType.TRAIL
    assert child.stop_price is None
    assert child.trail_percent == trail.get("secondary_trail_percent")
    assert child.trail_price == trail.get("secondary_trail_price")
    assert child.is_sell_order()


def test_bracket_with_target_and_trailing_stop_links_both_children():
    order = Order(
        "s",
        Asset("SPY"),
        10,
        side="buy",
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=520.0,
        secondary_trail_percent=3.0,
    )
    types = sorted(child.order_type for child in order.child_orders)
    assert types == sorted([Order.OrderType.LIMIT, Order.OrderType.TRAIL])
    assert order.child_orders[0].dependent_order is order.child_orders[1]
    assert order.child_orders[1].dependent_order is order.child_orders[0]


def test_oto_still_rejects_target_and_trailing_stop_together():
    with pytest.raises(ValueError, match="OTO only allows"):
        Order(
            "s",
            Asset("SPY"),
            10,
            side="buy",
            order_class=Order.OrderClass.OTO,
            secondary_limit_price=520.0,
            secondary_trail_percent=3.0,
        )


def test_backtest_oto_trailing_child_follows_price_then_exits():
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20050.0),
            (20100.0, 21000.0, 20050.0, 20900.0),  # trail moves up to 21000 - 200
            (20900.0, 20950.0, 20700.0, 20750.0),  # touches 20800
        ]
    )
    order = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.OTO,
        secondary_trail_price=200.0,
        quote=QUOTE,
    )
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.5)

    _advance_one_bar(strategy, broker)
    child = order.child_orders[0]
    assert child.is_filled()
    assert child.get_fill_price() == pytest.approx(20800.0)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)


@pytest.mark.parametrize("order_class", [Order.OrderClass.BRACKET, Order.OrderClass.OTO])
def test_protective_child_logs_a_new_event_when_it_starts_resting(order_class):
    """A bracket/OTO stop that becomes active after the entry fills is a working
    order at the broker, so the trade event log must show it as "new" then.
    Before, it went straight to the order book with no event, so anyone reading
    the trade log saw an unprotected entry until the stop filled."""
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20050.0),
            (20050.0, 20100.0, 20000.0, 20060.0),
        ]
    )
    order = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=order_class,
        secondary_stop_price=19000.0,
        quote=QUOTE,
    )
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)
    log = broker._trade_event_log_df
    stops = log[(log["type"] == "stop") & (log["status"] == "new")]
    assert len(stops) == 1
    assert stops.iloc[0]["side"] == "sell_to_close"
