import pytest

from lumibot.entities import Asset, Order


class TestOrderBasics:
    def test_side_must_be_one_of(self):
        assert Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc').side == 'buy'
        assert Order(asset=Asset("SPY"), quantity=10, side="sell", strategy='abc').side == 'sell'

        with pytest.raises(ValueError):
            Order(asset=Asset("SPY"), quantity=10, side="unknown", strategy='abc')

    def test_is_option(self):
        # Standard stock order
        asset = Asset("SPY")
        order = Order(asset=asset, quantity=10, side="buy", strategy='abc')
        assert not order.is_option()

        # Option order
        asset = Asset("SPY", asset_type="option")
        order = Order(asset=asset, quantity=10, side="buy", strategy='abc')
        assert order.is_option()

    def test_get_filled_price(self):
        asset = Asset("SPY")
        buy_order = Order(strategy='abc', asset=asset, side="buy", quantity=100)

        # No transactions
        assert buy_order.get_fill_price() == 0

        buy_order.transactions = [
            Order.Transaction(quantity=50, price=20.0),
            Order.Transaction(quantity=50, price=30.0)
        ]

        assert buy_order.get_fill_price() == 25.0

        # Error case where quantity is not set
        buy_order._quantity = 0
        assert buy_order.get_fill_price() == 0

        # Ensure Weighted Average used
        sell_order = Order(strategy='abc', asset=asset, side="sell", quantity=100)
        sell_order.transactions = [
            Order.Transaction(quantity=80, price=30.0),
            Order.Transaction(quantity=20, price=40.0)
        ]
        sell_order.position_filled = True
        assert sell_order.get_fill_price() == 32.0

    def test_filled(self):
        asset = Asset("SPY")
        order = Order(strategy='abc', asset=asset, side="buy", quantity=100)
        assert not order.is_filled()
        order.position_filled = True
        assert order.is_filled()
        order.position_filled = False
        order.status = 'filled'
        assert order.is_filled()

    def test_cancelled(self):
        asset = Asset("SPY")
        order = Order(strategy='abc', asset=asset, side="buy", quantity=100)
        assert not order.is_canceled()
        order.status = 'cancelled'
        assert order.is_canceled()
        order.status = 'canceled'
        assert order.is_canceled()
        order.status = 'cancel'
        assert order.is_canceled()

    def test_active(self):
        asset = Asset("SPY")
        order = Order(strategy='abc', asset=asset, side="buy", quantity=100)
        assert order.is_active()
        order.status = 'filled'
        assert not order.is_active()
        order.status = 'cancelled'
        assert not order.is_active()
        order.status = 'submitted'
        assert order.is_active()
