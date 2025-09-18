"""
Test that Position.to_dict() properly excludes problematic fields
that can cause DynamoDB 400KB limit errors.
"""
import unittest
from decimal import Decimal
from lumibot.entities import Position, Asset, Order


class TestPositionSerialization(unittest.TestCase):
    """Test Position.to_dict() excludes problematic fields"""

    def setUp(self):
        """Set up test position with asset"""
        self.asset = Asset("AAPL", "stock")
        self.position = Position(
            strategy="test_strategy",
            asset=self.asset,
            quantity=100,
            avg_fill_price=150.00
        )

    def test_to_dict_excludes_internal_fields(self):
        """Test that to_dict() doesn't include internal Python fields"""
        # Add some problematic fields that should NOT be serialized
        self.position._bars = {"test": "This is 1.8MB of bar data" * 100000}  # Simulate large data
        self.position._raw = {"broker_response": "22KB of raw data" * 1000}
        self.position._asset = {"duplicate": "asset data"}
        self.position._broker = "internal_broker_ref"
        self.position._transmitted = True
        self.position._error = "some error"

        # Get the serialized dict
        result = self.position.to_dict()

        # Verify problematic fields are NOT included
        self.assertNotIn('_bars', result, "_bars should not be in to_dict() output")
        self.assertNotIn('_raw', result, "_raw should not be in to_dict() output")
        self.assertNotIn('_asset', result, "_asset should not be in to_dict() output")
        self.assertNotIn('_broker', result, "_broker should not be in to_dict() output")
        self.assertNotIn('_transmitted', result, "_transmitted should not be in to_dict() output")
        self.assertNotIn('_error', result, "_error should not be in to_dict() output")

        # Verify no field starting with underscore is included
        for key in result.keys():
            self.assertFalse(key.startswith('_'), f"Field {key} starts with underscore and should not be included")

    def test_to_dict_includes_essential_fields(self):
        """Test that to_dict() includes all essential fields"""
        result = self.position.to_dict()

        # Verify essential fields ARE included
        self.assertIn('strategy', result)
        self.assertIn('asset', result)
        self.assertIn('quantity', result)
        self.assertIn('orders', result)
        self.assertIn('hold', result)
        self.assertIn('available', result)
        self.assertIn('avg_fill_price', result)

        # Verify values are correct
        self.assertEqual(result['strategy'], "test_strategy")
        self.assertEqual(result['quantity'], 100.0)
        self.assertEqual(result['avg_fill_price'], 150.0)
        self.assertIsInstance(result['asset'], dict)
        self.assertEqual(result['asset']['symbol'], "AAPL")

    def test_to_dict_size_reduction(self):
        """Test that to_dict() significantly reduces data size"""
        import json

        # Add large internal fields
        self.position._bars = {"data": "X" * 1000000}  # 1MB of data
        self.position._raw = {"raw": "Y" * 20000}  # 20KB of data

        # Get serialized dict
        result = self.position.to_dict()

        # Check that serialized size is reasonable (not megabytes)
        json_str = json.dumps(result)
        size_kb = len(json_str.encode('utf-8')) / 1024

        self.assertLess(size_kb, 10, f"Serialized position should be < 10KB, got {size_kb:.2f}KB")

        # Verify the large fields were excluded
        self.assertNotIn("X" * 100, json_str, "Large _bars data should not be in output")
        self.assertNotIn("Y" * 100, json_str, "Large _raw data should not be in output")

    def test_orders_list_included(self):
        """Test that orders list is properly included"""
        # Add some orders
        order1 = Order(self.position.strategy, self.asset, 50, "buy")
        order2 = Order(self.position.strategy, self.asset, 50, "sell")
        self.position.orders = [order1, order2]

        result = self.position.to_dict()

        self.assertIn('orders', result)
        self.assertIsInstance(result['orders'], list)
        self.assertEqual(len(result['orders']), 2)


if __name__ == '__main__':
    unittest.main()