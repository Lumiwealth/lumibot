import pytest

from lumibot.entities import Asset, Order


class TestOrderBasics:
    def test_side_must_be_one_of(self):
        assert Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc').side == 'buy'
        assert Order(asset=Asset("SPY"), quantity=10, side="sell", strategy='abc').side == 'sell'

    def test_blank_oco(self):
        # Create an OCO order without any child 
        order = Order(
            strategy="",
            type=Order.OrderType.OCO,
        )

        assert order.avg_fill_price == None
        assert order.quantity == None
        assert order.status == "unprocessed"

        # Add a child order
        child_order = Order(
            strategy="",
            type=Order.OrderType.LIMIT,
            side="buy",
            asset=Asset("SPY"),
            quantity=10,
            limit_price=100,
            identifier="child_order_1"
        )

        # Assert that the child order does not have any child orders of its own
        assert child_order.child_orders == []

        # Add a child order to the OCO order
        order.child_orders.append(child_order)

        # Assert that the child order still does not have any child orders of its own
        assert child_order.child_orders == []

        # Check that the original order did not change as a result of adding a child order
        assert order.avg_fill_price == None
        assert order.quantity == None
        assert order.status == "unprocessed"
        assert order.side == None
        assert order.asset == None
        assert order.child_orders == [child_order]

        # Add another child order to the OCO order
        child_order_2 = Order(
            strategy="",
            type=Order.OrderType.MARKET,
            side="sell",
            asset=Asset("SPY"),
            quantity=55,
            limit_price=200,
        )

        order.add_child_order(child_order_2)

        # Print the order and child order 
        order_text = str(order).lower()
        first_child_order = order.child_orders[0]
        first_child_order_text = str(first_child_order).lower()

        # Assert the order text contains the order type
        assert Order.OrderType.OCO in order_text

        # Check that both child orders are present in the parent order text
        assert "buy" in order_text
        assert "sell" in order_text
        assert "10" in order_text
        assert "55" in order_text
        assert Order.OrderType.LIMIT in order_text
        assert Order.OrderType.MARKET in order_text

        # Assert the first child order text contains the order type
        assert Order.OrderType.LIMIT in first_child_order_text

        # Assert the first child order does not contain information about the second child order
        assert "sell" not in first_child_order_text
        assert "55" not in first_child_order_text

    def test_price_doesnt_exist(self):
        # Test that the price does not exist for any orders, we should be more specific such as limit_price, stop_price, fill_price, etc.
        with pytest.raises(TypeError):
            Order(
                strategy="",
                asset=Asset("SPY"),
                quantity=10,
                side="buy",
                price=100,
            )

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
        assert buy_order.get_fill_price() == None

        buy_order.transactions = [
            Order.Transaction(quantity=50, price=20.0),
            Order.Transaction(quantity=50, price=30.0)
        ]
        assert buy_order.get_fill_price() == 25.0

        # Error case where quantity is not set
        buy_order._quantity = 0

        # Fill price should be None because quantity is 0, this is an error case
        assert buy_order.get_fill_price() is None

        # Ensure Weighted Average used
        sell_order = Order(strategy='abc', asset=asset, side="sell", quantity=100)
        sell_order.transactions = [
            Order.Transaction(quantity=80, price=30.0),
            Order.Transaction(quantity=20, price=40.0)
        ]
        sell_order.position_filled = True
        assert sell_order.get_fill_price() == 32.0

        # If Average Price is set, it should be used
        buy_order.avg_fill_price = 50.0
        assert buy_order.get_fill_price() == 50.0

    def test_filled(self):
        asset = Asset("SPY")
        order = Order(strategy='abc', asset=asset, side="buy", quantity=100)
        assert not order.is_filled()
        order.position_filled = True
        assert order.is_filled()
        order.position_filled = False
        order.status = 'filled'
        assert order.is_filled()

    def test_is_buy_order(self):
        asset = Asset("SPY")
        order = Order(strategy='abc', asset=asset, side=Order.OrderSide.BUY, quantity=100)
        assert order.is_buy_order()
        order.side = Order.OrderSide.SELL
        assert not order.is_buy_order()

        # Test unique buy order types
        order.side = Order.OrderSide.BUY_TO_COVER
        assert order.is_buy_order()
        order.side = Order.OrderSide.BUY_TO_OPEN
        assert order.is_buy_order()
        order.side = Order.OrderSide.BUY_TO_CLOSE
        assert order.is_buy_order()

    def test_is_sell_order(self):
        asset = Asset("SPY")
        order = Order(strategy='abc', asset=asset, side=Order.OrderSide.SELL, quantity=100)
        assert order.is_sell_order()
        order.side = Order.OrderSide.BUY
        assert not order.is_sell_order()

        # Test unique sell order types
        order.side = Order.OrderSide.SELL_SHORT
        assert order.is_sell_order()
        order.side = Order.OrderSide.SELL_TO_OPEN
        assert order.is_sell_order()
        order.side = Order.OrderSide.SELL_TO_CLOSE
        assert order.is_sell_order()

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

    def test_status(self):
        asset = Asset("SPY")
        order = Order(strategy='abc', asset=asset, side="buy", quantity=100)
        assert order.status == "unprocessed"
        order.status = "filled"
        assert order.status == "fill"
        order.status = "canceled"
        assert order.status == "canceled"
        order.status = "submit"
        assert order.status == "submitted"
        order.status = "cancel"
        assert order.status == "canceled"

    def test_equivalent_status(self):
        asset = Asset("SPY")
        order1 = Order(strategy='abc', asset=asset, side="buy", quantity=100)
        order2 = Order(strategy='abc', asset=asset, side="buy", quantity=100)
        assert order1.equivalent_status(order2)
        order2.status = "filled"
        assert not order1.equivalent_status(order2)
        order1.status = "filled"
        assert order1.equivalent_status(order2)

        order1.status = "canceled"
        order2.status = "cancelled"
        assert order1.equivalent_status(order2)

        order1.status = "submit"
        order2.status = "submitted"
        assert order1.equivalent_status(order2)

        order1.status = "open"
        order2.status = "new"
        assert order1.equivalent_status(order2)

        order1.status = "open"
        order2.status = ""
        assert not order1.equivalent_status("")
