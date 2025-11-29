"""
Test that Order.to_dict() properly excludes problematic fields
that can cause DynamoDB 400KB limit errors.
"""
import unittest
from decimal import Decimal
from lumibot.entities import Order, Asset


class TestOrderSerialization(unittest.TestCase):
    """Test Order.to_dict() excludes problematic fields"""

    def setUp(self):
        """Set up test order with asset"""
        self.asset = Asset("AAPL", "stock")
        self.order = Order(
            strategy="test_strategy",
            asset=self.asset,
            quantity=100,
            side="buy",
            limit_price=150.00
        )

    def test_to_dict_excludes_internal_fields(self):
        """Test that to_dict() doesn't include internal Python fields"""
        # Add problematic fields that should NOT be serialized
        self.order._bars = {"test": "This is 1.8MB of bar data" * 100000}
        self.order._raw = {"broker_response": "22KB of raw data" * 1000}
        self.order._transmitted = True
        self.order._error = "some error"
        self.order._broker = "internal_broker_ref"
        self.order.transactions = ["tx1", "tx2", "tx3"] * 100

        # Get the serialized dict
        result = self.order.to_dict()

        # Verify problematic fields are NOT included
        self.assertNotIn('_bars', result, "_bars should not be in to_dict() output")
        self.assertNotIn('_raw', result, "_raw should not be in to_dict() output")
        self.assertNotIn('_transmitted', result, "_transmitted should not be in to_dict() output")
        self.assertNotIn('_error', result, "_error should not be in to_dict() output")
        self.assertNotIn('_broker', result, "_broker should not be in to_dict() output")
        self.assertNotIn('transactions', result, "transactions should not be in to_dict() output")

        # Verify no field starting with underscore is included
        for key in result.keys():
            self.assertFalse(key.startswith('_'), f"Field {key} starts with underscore and should not be included")

    def test_to_dict_includes_essential_fields(self):
        """Test that to_dict() includes all essential fields"""
        result = self.order.to_dict()

        # Verify essential fields ARE included
        self.assertIn('strategy', result)
        self.assertIn('asset', result)
        self.assertIn('quantity', result)
        self.assertIn('side', result)
        self.assertIn('status', result)

        # Verify values are correct
        self.assertEqual(result['strategy'], "test_strategy")
        self.assertEqual(result['quantity'], 100.0)
        self.assertEqual(result['side'], "buy")

    def test_to_dict_size_reduction(self):
        """Test that to_dict() significantly reduces data size"""
        import json

        # Add large internal fields
        self.order._bars = {"data": "X" * 1000000}  # 1MB of data
        self.order._raw = {"raw": "Y" * 20000}  # 20KB of data
        self.order.transactions = ["transaction"] * 1000  # Large transaction list

        # Get serialized dict
        result = self.order.to_dict()

        # Check that serialized size is reasonable (not megabytes)
        json_str = json.dumps(result, default=str)
        size_kb = len(json_str.encode('utf-8')) / 1024

        self.assertLess(size_kb, 10, f"Serialized order should be < 10KB, got {size_kb:.2f}KB")

        # Verify the large fields were excluded
        self.assertNotIn("X" * 100, json_str, "Large _bars data should not be in output")
        self.assertNotIn("Y" * 100, json_str, "Large _raw data should not be in output")
        self.assertNotIn("transaction", json_str, "Transactions should not be in output")


if __name__ == '__main__':
    unittest.main()