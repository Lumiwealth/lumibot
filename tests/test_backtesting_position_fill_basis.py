"""Unit-level fill accounting; no backtest, data source, or broker connection."""

from threading import RLock
from unittest.mock import Mock

import pytest

from lumibot.backtesting.backtesting_broker import BacktestingBroker
from lumibot.entities import Asset, Order
from lumibot.trading_builtins import SafeList


@pytest.fixture
def broker():
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker._lock = RLock()
    for name in ("new_orders", "unprocessed_orders", "partially_filled_orders", "filled_orders", "filled_positions"):
        setattr(broker, "_" + name, SafeList(broker._lock))
    broker._tracked_position_cache = {}
    broker._cancel_open_orders_for_asset = Mock()
    return broker


def order(side="buy", quantity=10):
    return Order("fill_basis", Asset("SPY", asset_type="stock"), quantity, side)


def test_add_then_reduce_preserves_weighted_entry_basis(broker):
    position = broker._process_filled_order(order(), 100, 10)
    position = broker._process_filled_order(order(), 120, 10)
    assert position.quantity == 20
    assert position.avg_fill_price == pytest.approx(110)
    position = broker._process_filled_order(order("sell", 5), 150, 5)
    assert position.quantity == 15
    assert position.avg_fill_price == pytest.approx(110)


def test_short_add_reduce_and_flip_basis(broker):
    broker._process_filled_order(order("sell_short"), 100, 10)
    position = broker._process_filled_order(order("sell_short"), 120, 10)
    assert position.quantity == -20
    assert position.avg_fill_price == pytest.approx(110)
    position = broker._process_filled_order(order("buy_to_cover", 5), 90, 5)
    assert position.avg_fill_price == pytest.approx(110)
    position = broker._process_filled_order(order("buy", 20), 95, 20)
    assert position.quantity == 5
    assert position.avg_fill_price == pytest.approx(95)


def test_partial_fill_is_tracked_and_final_fill_adds_only_remainder(broker):
    entry = order(quantity=10)
    _, position = broker._process_partially_filled_order(entry, 100, 4)
    assert broker.get_tracked_position(entry.strategy, entry.asset) is position
    assert position.quantity == 4
    assert position.avg_fill_price == pytest.approx(100)
    _, position = broker._process_partially_filled_order(entry, 110, 2)
    assert position.quantity == 6
    assert position.avg_fill_price == pytest.approx(620 / 6)
    position = broker._process_filled_order(entry, 120, 4)
    assert position.quantity == 10
    assert position.avg_fill_price == pytest.approx(110)
    assert len(broker._filled_positions) == 1


def test_flat_then_reopen_does_not_reuse_previous_basis(broker):
    broker._process_filled_order(order(), 100, 10)
    closed = broker._process_filled_order(order("sell"), 120, 10)
    assert closed.quantity == 0
    assert closed.avg_fill_price is None
    assert not broker._filled_positions
    reopened = broker._process_filled_order(order(), 80, 10)
    assert reopened.avg_fill_price == pytest.approx(80)


def test_unknown_existing_basis_stays_unknown(broker):
    position = broker._process_filled_order(order(), 100, 10)
    position.avg_fill_price = None
    position = broker._process_filled_order(order(), 120, 10)
    assert position.avg_fill_price is None


def test_partial_close_to_flat_then_remaining_fill_opens_new_basis(broker):
    broker._process_filled_order(order(quantity=4), 100, 4)
    exit_order = order("sell_short", 6)
    _, position = broker._process_partially_filled_order(exit_order, 110, 4)
    assert position.quantity == 0
    assert position.avg_fill_price is None
    assert not broker._filled_positions
    position = broker._process_filled_order(exit_order, 120, 2)
    assert position.quantity == -2
    assert position.avg_fill_price == pytest.approx(120)


def test_fractional_add_and_zero_price_fill_are_weighted(broker):
    broker._process_filled_order(order(quantity=0.5), 100, 0.5)
    position = broker._process_filled_order(order(quantity=0.25), 0, 0.25)
    assert position.quantity == 0.75
    assert position.avg_fill_price == pytest.approx(50 / 0.75)
