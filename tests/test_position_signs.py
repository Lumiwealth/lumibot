import unittest
from decimal import Decimal

from lumibot.entities import Asset, Order, Position


class TestPositionSigns(unittest.TestCase):
    def setUp(self):
        self.asset = Asset("TEST", "stock")

    def _create_position(self, quantity=0):
        return Position(strategy="test_strategy", asset=self.asset, quantity=quantity)

    def _create_order(self, side):
        return Order(strategy="test_strategy", asset=self.asset, quantity=1, side=side)

    def test_buy_and_sell_update_quantity(self):
        position = self._create_position()
        buy_order = self._create_order(Order.OrderSide.BUY)
        position.add_order(buy_order, Decimal("2"))
        self.assertEqual(position.quantity, 2)

        sell_order = self._create_order(Order.OrderSide.SELL)
        position.add_order(sell_order, Decimal("1"))
        self.assertEqual(position.quantity, 1)

    def test_buy_to_close_reduces_short_quantity(self):
        position = self._create_position(quantity=-3)
        order = self._create_order(Order.OrderSide.BUY_TO_CLOSE)
        position.add_order(order, Decimal("2"))
        self.assertEqual(position.quantity, -1)

        position.add_order(order, Decimal("1"))
        self.assertEqual(position.quantity, 0)

    def test_sell_to_close_reduces_long_quantity(self):
        position = self._create_position(quantity=4)
        order = self._create_order(Order.OrderSide.SELL_TO_CLOSE)
        position.add_order(order, Decimal("3"))
        self.assertEqual(position.quantity, 1)

        position.add_order(order, Decimal("1"))
        self.assertEqual(position.quantity, 0)

    def test_sell_short_and_buy_to_cover_invert_signs(self):
        position = self._create_position()
        sell_short_order = self._create_order(Order.OrderSide.SELL_SHORT)
        position.add_order(sell_short_order, Decimal("5"))
        self.assertEqual(position.quantity, -5)

        buy_to_cover_order = self._create_order(Order.OrderSide.BUY_TO_COVER)
        position.add_order(buy_to_cover_order, Decimal("2"))
        self.assertEqual(position.quantity, -3)

        position.add_order(buy_to_cover_order, Decimal("3"))
        self.assertEqual(position.quantity, 0)


if __name__ == "__main__":
    unittest.main()
