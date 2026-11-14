"""Which exit fills when one bar touches both the stop and the target.

A bar's high and low say both prices traded, not which came first. Letting the
target win flatters results, so the backtester takes the conservative path: the
stop fills first, unless the bar opened beyond the target (then the target fills
at the open, because the open is the first price of the bar). See
docsrc/backtesting.how_orders_fill.rst.
"""

from decimal import Decimal

import pytest

from lumibot.entities import Order
from tests.test_crypto_backtesting_order_matrix import (
    BASE,
    QUOTE,
    _advance_one_bar,
    _crypto_strategy,
    _entry_position,
)
from tests.backtest.test_backtesting_broker_processing import position_quantity, submit_and_fill


def _bracket(strategy, side, target, stop, **extra):
    return strategy.create_order(
        BASE,
        Decimal("0.5"),
        side,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=target,
        secondary_stop_price=stop,
        quote=QUOTE,
        **extra,
    )


def _exit_fill(order):
    filled = [child for child in order.child_orders if child.is_filled()]
    assert len(filled) == 1
    return filled[0]


ENTRY_BAR = (20000.0, 20100.0, 19900.0, 20025.0)


def test_long_bracket_bar_touching_both_fills_the_stop():
    strategy, broker, _ = _crypto_strategy([ENTRY_BAR, (20000.0, 20600.0, 19400.0, 20000.0)])
    order = _bracket(strategy, Order.OrderSide.BUY, 20500.0, 19500.0)
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)

    exit_order = _exit_fill(order)
    assert exit_order.is_stop_order()
    assert exit_order.get_fill_price() == pytest.approx(19500.0)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)


def test_short_bracket_bar_touching_both_fills_the_stop():
    strategy, broker, _ = _crypto_strategy([ENTRY_BAR, (20000.0, 20600.0, 19400.0, 20000.0)])
    order = _bracket(strategy, Order.OrderSide.SELL, 19500.0, 20500.0)
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)

    exit_order = _exit_fill(order)
    assert exit_order.is_stop_order()
    assert exit_order.get_fill_price() == pytest.approx(20500.0)


def test_bar_opening_beyond_the_target_fills_the_target_at_the_open():
    strategy, broker, _ = _crypto_strategy([ENTRY_BAR, (20700.0, 20800.0, 19400.0, 20000.0)])
    order = _bracket(strategy, Order.OrderSide.BUY, 20500.0, 19500.0)
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)

    exit_order = _exit_fill(order)
    assert exit_order.order_type == Order.OrderType.LIMIT
    assert exit_order.get_fill_price() == pytest.approx(20700.0)


def test_bar_gapping_through_the_stop_fills_at_the_open():
    strategy, broker, _ = _crypto_strategy([ENTRY_BAR, (19300.0, 20600.0, 19200.0, 20000.0)])
    order = _bracket(strategy, Order.OrderSide.BUY, 20500.0, 19500.0)
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)

    exit_order = _exit_fill(order)
    assert exit_order.is_stop_order()
    assert exit_order.get_fill_price() == pytest.approx(19300.0)


def test_oco_bar_touching_both_fills_the_stop():
    strategy, broker, _ = _crypto_strategy([ENTRY_BAR, (20000.0, 20600.0, 19400.0, 20000.0)])
    _entry_position(strategy, broker)
    _advance_one_bar(strategy, broker)
    oco = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.SELL,
        order_class=Order.OrderClass.OCO,
        limit_price=20500.0,
        stop_price=19500.0,
        quote=QUOTE,
    )
    strategy.submit_order(oco)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    exit_order = _exit_fill(oco)
    assert exit_order.is_stop_order()
    assert exit_order.get_fill_price() == pytest.approx(19500.0)


def test_entry_bar_cannot_stop_out():
    # The entry fills at this bar's open; its low is under the stop, but the exits
    # only start working on the next bar.
    strategy, broker, _ = _crypto_strategy([(20000.0, 20100.0, 18000.0, 20025.0), ENTRY_BAR])
    order = _bracket(strategy, Order.OrderSide.BUY, 25000.0, 19000.0)
    submit_and_fill(strategy, broker, order)

    assert not any(child.is_filled() for child in order.child_orders)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.5)
