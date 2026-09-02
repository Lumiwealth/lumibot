from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.brokers.interactive_brokers import IBApp, IBWrapper
from lumibot.entities import Asset, Order


def _legacy_ib_app():
    app = IBApp.__new__(IBApp)
    identifiers = iter(range(1000, 1100))
    app.nextOrderId = lambda: next(identifiers)
    return app


def _legacy_order(order_class):
    return SimpleNamespace(
        order_class=order_class,
        identifier=999,
        side="buy",
        quantity=1,
        order_type="limit",
        limit_price=100.0,
        stop_price=95.0,
        trail_price=None,
        trail_percent=None,
        time_in_force="gtd",
        good_till_date=datetime(2026, 9, 1, 15, 30),
    )


@pytest.mark.parametrize(
    ("order_class", "expected_order_count"),
    [("", 1), ("bracket", 3), ("oto", 2), ("oco", 2)],
)
def test_legacy_ib_orders_apply_gtd_to_every_native_leg(order_class, expected_order_count):
    native_orders = _legacy_ib_app().create_order(_legacy_order(order_class))

    assert len(native_orders) == expected_order_count
    assert [native_order.tif for native_order in native_orders] == ["GTD"] * expected_order_count
    assert [native_order.goodTillDate for native_order in native_orders] == ["20260901 15:30:00"] * expected_order_count


def test_legacy_ib_last_close_populates_price_only_without_last_trade():
    wrapper = IBWrapper.__new__(IBWrapper)
    wrapper.should_use_last_close = True

    wrapper.tickPrice(reqId=1, tickType=9, price=123.45, attrib=None)

    assert wrapper.price == 123.45
    assert wrapper.tick_type_used == 9

    wrapper.tickPrice(reqId=1, tickType=4, price=125.00, attrib=None)
    wrapper.tickPrice(reqId=1, tickType=9, price=123.45, attrib=None)

    assert wrapper.price == 125.00
    assert wrapper.tick_type_used == 4


@pytest.mark.parametrize(
    "order_class, prices",
    [
        (Order.OrderClass.OCO, {"limit_price": 110.0, "stop_price": 95.0}),
        (Order.OrderClass.BRACKET, {"secondary_limit_price": 110.0, "secondary_stop_price": 95.0}),
        (Order.OrderClass.OTO, {"secondary_limit_price": 110.0}),
    ],
)
def test_automatic_advanced_children_inherit_parent_gtd(order_class, prices):
    good_till_date = datetime(2026, 9, 1, 15, 30, tzinfo=timezone.utc)
    order_kwargs = {
        "strategy": "legacy_gateway_test",
        "asset": Asset("SPY"),
        "quantity": 1,
        "side": Order.OrderSide.BUY,
        "order_type": Order.OrderType.LIMIT,
        "order_class": order_class,
        "time_in_force": "gtd",
        "good_till_date": good_till_date,
    }
    if order_class is not Order.OrderClass.OCO:
        order_kwargs["limit_price"] = 100.0
    order_kwargs.update(prices)
    parent = Order(**order_kwargs)

    assert parent.child_orders
    assert all(child.time_in_force == "gtd" for child in parent.child_orders)
    assert all(child.good_till_date == good_till_date for child in parent.child_orders)
