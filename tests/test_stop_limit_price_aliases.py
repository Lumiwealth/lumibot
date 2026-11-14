"""A stop-limit order's limit can be given as ``limit_price`` or ``stop_limit_price``.

Both spellings are public (``create_order(stop_price=..., stop_limit_price=...)`` and
``create_order(stop_price=..., limit_price=..., order_type="stop_limit")``), but each
broker read only one of them. A stop-limit written the other way reached the backtest
with no limit and never filled, and reached live brokers without its limit price.
"""

from decimal import Decimal

import pytest

from lumibot.entities import Asset, Order
from tests.test_crypto_backtesting_order_matrix import BASE, QUOTE, _advance_one_bar, _crypto_strategy, _entry_position


@pytest.mark.parametrize(
    "kwargs",
    [
        {"stop_price": 10.0, "stop_limit_price": 9.5},
        {"stop_price": 10.0, "limit_price": 9.5, "order_type": "stop_limit"},
    ],
)
def test_both_spellings_carry_the_limit_in_both_fields(kwargs):
    order = Order("s", Asset("SPY"), 1, side="sell", **kwargs)
    assert order.order_type == Order.OrderType.STOP_LIMIT
    assert order.limit_price == pytest.approx(9.5)
    assert order.stop_limit_price == pytest.approx(9.5)


def test_bracket_stop_limit_child_carries_its_limit():
    order = Order(
        "s",
        Asset("SPY"),
        1,
        side="buy",
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=12.0,
        secondary_stop_price=10.0,
        secondary_stop_limit_price=9.5,
    )
    child = next(c for c in order.child_orders if c.order_type == Order.OrderType.STOP_LIMIT)
    assert child.limit_price == pytest.approx(9.5)


def test_backtest_stop_limit_written_with_stop_limit_price_fills():
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20025.0),
            (20000.0, 20100.0, 19900.0, 20025.0),
            (19800.0, 19900.0, 19400.0, 19500.0),
        ]
    )
    _entry_position(strategy, broker)
    _advance_one_bar(strategy, broker)
    stop_limit = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.SELL,
        stop_price=19700.0,
        stop_limit_price=19600.0,
        quote=QUOTE,
    )
    strategy.submit_order(stop_limit)
    _advance_one_bar(strategy, broker)

    # Triggered at the 19700 stop; the 19600 sell limit is met there, so it fills at 19700.
    assert stop_limit.is_filled()
    assert stop_limit.get_fill_price() == pytest.approx(19700.0)
