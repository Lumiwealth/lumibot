from collections import defaultdict
from datetime import date

import pytest

from lumibot.backtesting.backtesting_broker import BacktestingBroker, get_futures_margin_requirement
from lumibot.entities import Asset, Order


class DummyStrategy:
    def __init__(self, cash=100_000):
        self._name = "TestStrategy"
        self.cash = float(cash)

    @property
    def name(self):
        return self._name

    def _set_cash_position(self, value):
        self.cash = float(value)


def make_broker():
    broker = BacktestingBroker.__new__(BacktestingBroker)
    broker._futures_lot_ledgers = defaultdict(list)
    return broker


def make_order(strategy, asset, side, quantity):
    return Order(
        strategy=strategy,
        asset=asset,
        quantity=quantity,
        side=side,
        order_type=Order.OrderType.MARKET,
    )


def test_futures_calendar_spread_does_not_net_margin_between_expiries():
    broker = make_broker()
    strategy = DummyStrategy(cash=100_000)

    # Calendar spreads often trade multiple expiries of the same root symbol.
    # The futures ledger must key by contract (including expiration), otherwise
    # the legs incorrectly net as if they were the same instrument.
    front = Asset("CL", asset_type=Asset.AssetType.FUTURE, expiration=date(2026, 2, 1), multiplier=1000)
    next_ = Asset("CL", asset_type=Asset.AssetType.FUTURE, expiration=date(2026, 3, 1), multiplier=1000)

    margin = float(get_futures_margin_requirement(front))
    assert margin > 0

    broker._process_futures_fill(
        strategy, make_order(strategy, front, Order.OrderSide.BUY, 1), price=60.0, filled_quantity=1
    )
    broker._process_futures_fill(
        strategy, make_order(strategy, next_, Order.OrderSide.SELL, 1), price=59.8, filled_quantity=1
    )

    # Both legs are opening positions, so both margins should be reserved.
    assert strategy.cash == pytest.approx(100_000 - (2 * margin))

    key_front = broker._get_futures_ledger_key(strategy, front)
    key_next = broker._get_futures_ledger_key(strategy, next_)
    assert key_front != key_next

    assert key_front in broker._futures_lot_ledgers
    assert key_next in broker._futures_lot_ledgers
    assert broker._futures_lot_ledgers[key_front][0]["qty"] == pytest.approx(1)
    assert broker._futures_lot_ledgers[key_front][0]["price"] == pytest.approx(60.0)
    assert broker._futures_lot_ledgers[key_next][0]["qty"] == pytest.approx(-1)
    assert broker._futures_lot_ledgers[key_next][0]["price"] == pytest.approx(59.8)
