from collections import defaultdict

import pytest

from lumibot.backtesting.backtesting_broker import BacktestingBroker
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


def make_asset():
    asset = Asset("GC", asset_type=Asset.AssetType.CONT_FUTURE)
    asset.multiplier = 100
    return asset


def make_order(strategy, asset, side, quantity):
    return Order(
        strategy=strategy,
        asset=asset,
        quantity=quantity,
        side=side,
        order_type=Order.OrderType.MARKET,
    )


def test_futures_flip_releases_margin_and_creates_new_entry():
    broker = make_broker()
    strategy = DummyStrategy()
    asset = make_asset()

    buy_order = make_order(strategy, asset, Order.OrderSide.BUY, 1)
    broker._process_futures_fill(strategy, buy_order, price=2000, filled_quantity=1)

    assert strategy.cash == pytest.approx(90_000)

    key = broker._get_futures_ledger_key(strategy, asset)
    assert key in broker._futures_lot_ledgers
    assert broker._futures_lot_ledgers[key][0]["qty"] == pytest.approx(1)

    reverse_order = make_order(strategy, asset, Order.OrderSide.SELL, 2)
    broker._process_futures_fill(strategy, reverse_order, price=2005, filled_quantity=2)

    assert strategy.cash == pytest.approx(90_500)
    ledger = broker._futures_lot_ledgers[key]
    assert len(ledger) == 1
    assert ledger[0]["qty"] == pytest.approx(-1)
    assert ledger[0]["price"] == pytest.approx(2005)

    cover_order = make_order(strategy, asset, Order.OrderSide.BUY, 1)
    broker._process_futures_fill(strategy, cover_order, price=2003, filled_quantity=1)

    assert strategy.cash == pytest.approx(100_700)
    assert key not in broker._futures_lot_ledgers
