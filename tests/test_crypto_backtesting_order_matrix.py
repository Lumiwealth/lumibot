from decimal import Decimal
from datetime import timedelta

import pytest

from lumibot.entities import Asset, Order
from tests.backtest.test_backtesting_broker_processing import (
    position_quantity,
    setup_strategy_with_prices,
    submit_and_fill,
)


BASE = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
QUOTE = Asset("USDT", asset_type=Asset.AssetType.CRYPTO)


def _crypto_strategy(bars):
    return setup_strategy_with_prices(BASE, QUOTE, bars, budget=100000.0)


def _advance_one_bar(strategy, broker):
    broker._update_datetime(broker.datetime + timedelta(minutes=1))
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()


def _entry_position(strategy, broker, quantity=Decimal("0.5")):
    order = strategy.create_order(
        BASE,
        quantity,
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=QUOTE,
    )
    submit_and_fill(strategy, broker, order)
    assert order.is_filled()
    return order


@pytest.mark.parametrize(
    "order_type,order_kwargs,expected_price",
    [
        (Order.OrderType.MARKET, {}, 20000.0),
        (Order.OrderType.LIMIT, {"limit_price": 19950.0}, 19950.0),
        (Order.OrderType.STOP, {"stop_price": 20050.0}, 20050.0),
        (Order.OrderType.STOP_LIMIT, {"stop_price": 20050.0, "limit_price": 20050.0}, 20050.0),
    ],
)
def test_crypto_buy_order_types_fill_at_expected_price(order_type, order_kwargs, expected_price):
    strategy, broker, _ = _crypto_strategy([(20000.0, 20100.0, 19900.0, 20025.0)])

    order = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=order_type,
        quote=QUOTE,
        **order_kwargs,
    )
    submit_and_fill(strategy, broker, order)

    assert order.is_filled()
    assert order.get_fill_price() == pytest.approx(expected_price)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.5)


@pytest.mark.parametrize(
    "order_type,order_kwargs,expected_price",
    [
        (Order.OrderType.MARKET, {}, 20100.0),
        (Order.OrderType.LIMIT, {"limit_price": 20150.0}, 20150.0),
        (Order.OrderType.STOP, {"stop_price": 19950.0}, 19950.0),
        (Order.OrderType.STOP_LIMIT, {"stop_price": 19950.0, "limit_price": 19950.0}, 19950.0),
    ],
)
def test_crypto_sell_order_types_fill_at_expected_price(order_type, order_kwargs, expected_price):
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20025.0),
            (20100.0, 20200.0, 19850.0, 20050.0),
        ]
    )
    _entry_position(strategy, broker)
    _advance_one_bar(strategy, broker)

    order = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.SELL,
        order_type=order_type,
        quote=QUOTE,
        **order_kwargs,
    )
    submit_and_fill(strategy, broker, order)

    assert order.is_filled()
    assert order.get_fill_price() == pytest.approx(expected_price)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)


def test_crypto_bracket_take_profit_closes_position_and_cancels_stop():
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20025.0),
            (20500.0, 20600.0, 20400.0, 20550.0),
        ]
    )

    order = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=20500.0,
        secondary_stop_price=19000.0,
        quote=QUOTE,
    )
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)

    filled = [child for child in order.child_orders if child.is_filled()]
    canceled = [child for child in order.child_orders if child.is_canceled()]
    assert len(filled) == 1
    assert filled[0].get_fill_price() == pytest.approx(20500.0)
    assert len(canceled) == 1
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)


def test_crypto_bracket_stop_closes_position_and_cancels_limit():
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20025.0),
            (19500.0, 19600.0, 19400.0, 19450.0),
        ]
    )

    order = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=25000.0,
        secondary_stop_price=19500.0,
        quote=QUOTE,
    )
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)

    filled = [child for child in order.child_orders if child.is_filled()]
    canceled = [child for child in order.child_orders if child.is_canceled()]
    assert len(filled) == 1
    assert filled[0].get_fill_price() == pytest.approx(19500.0)
    assert len(canceled) == 1
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)


def test_crypto_bracket_trailing_stop_closes_position_after_trail_update():
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20500.0, 19900.0, 20400.0),
            (20400.0, 21000.0, 20300.0, 20900.0),
            (20900.0, 21050.0, 20850.0, 20950.0),
        ]
    )

    order = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=25000.0,
        secondary_stop_price=20200.0,
        secondary_trail_price=100.0,
        quote=QUOTE,
    )
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.5)

    _advance_one_bar(strategy, broker)

    filled = [child for child in order.child_orders if child.is_filled()]
    assert len(filled) == 1
    assert filled[0].get_fill_price() == pytest.approx(20900.0)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)


def test_crypto_oco_limit_exit_cancels_stop_sibling():
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20025.0),
            (20500.0, 20600.0, 20400.0, 20550.0),
        ]
    )
    _entry_position(strategy, broker)
    _advance_one_bar(strategy, broker)

    oco = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.SELL,
        order_class=Order.OrderClass.OCO,
        limit_price=20500.0,
        stop_price=19000.0,
        quote=QUOTE,
    )
    strategy.submit_order(oco)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    filled = [child for child in oco.child_orders if child.is_filled()]
    canceled = [child for child in oco.child_orders if child.is_canceled()]
    assert len(filled) == 1
    assert filled[0].get_fill_price() == pytest.approx(20500.0)
    assert len(canceled) == 1
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)


def test_crypto_oco_stop_exit_cancels_limit_sibling():
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20025.0),
            (19500.0, 19600.0, 19400.0, 19450.0),
        ]
    )
    _entry_position(strategy, broker)
    _advance_one_bar(strategy, broker)

    oco = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.SELL,
        order_class=Order.OrderClass.OCO,
        limit_price=25000.0,
        stop_price=19500.0,
        quote=QUOTE,
    )
    strategy.submit_order(oco)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    filled = [child for child in oco.child_orders if child.is_filled()]
    canceled = [child for child in oco.child_orders if child.is_canceled()]
    assert len(filled) == 1
    assert filled[0].get_fill_price() == pytest.approx(19500.0)
    assert len(canceled) == 1
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)


@pytest.mark.parametrize(
    "child_kwargs,expected_price",
    [
        ({"secondary_limit_price": 20500.0}, 20500.0),
        ({"secondary_stop_price": 19500.0}, 19500.0),
    ],
    ids=["limit_child", "stop_child"],
)
def test_crypto_oto_child_order_closes_position(child_kwargs, expected_price):
    second_bar = (20500.0, 20600.0, 20400.0, 20550.0)
    if "secondary_stop_price" in child_kwargs:
        second_bar = (19500.0, 19600.0, 19400.0, 19450.0)
    strategy, broker, _ = _crypto_strategy(
        [
            (20000.0, 20100.0, 19900.0, 20025.0),
            second_bar,
        ]
    )

    order = strategy.create_order(
        BASE,
        Decimal("0.5"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.OTO,
        quote=QUOTE,
        **child_kwargs,
    )
    submit_and_fill(strategy, broker, order)
    _advance_one_bar(strategy, broker)

    assert order.is_filled()
    assert len(order.child_orders) == 1
    assert order.child_orders[0].is_filled()
    assert order.child_orders[0].get_fill_price() == pytest.approx(expected_price)
    assert position_quantity(broker, strategy, BASE) == pytest.approx(0.0)
