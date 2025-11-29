import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from decimal import Decimal

from lumibot.brokers.broker import Broker
from lumibot.entities import Asset, Order, Position
from lumibot.trading_builtins import SafeList
from threading import RLock


class MockDataSource:
    """Mock data source for testing."""
    def __init__(self, current_time=None):
        self.current_time = current_time or datetime.now()

    def get_datetime(self):
        return self.current_time

    def set_datetime(self, dt):
        self.current_time = dt


class MockBroker(Broker):
    """Mock broker implementation for testing cleanup functionality."""
    
    def __init__(self, *args, **kwargs):
        # Mock the abstract methods to create a concrete implementation
        super().__init__(*args, **kwargs)
        
    def cancel_order(self, order):
        pass
    
    def _modify_order(self, order, limit_price=None, stop_price=None):
        pass
    
    def _submit_order(self, order):
        return order
    
    def _get_balances_at_broker(self, quote_asset, strategy):
        return (1000.0, 0.0, 1000.0)
    
    def get_historical_account_value(self):
        return {}
    
    def _get_stream_object(self):
        return None
    
    def _register_stream_events(self):
        pass
    
    def _run_stream(self):
        pass
    
    def _pull_positions(self, strategy):
        return []
    
    def _pull_position(self, strategy, asset):
        return None
    
    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        order = Order("test", Asset("TEST"), 100, Order.OrderSide.BUY)
        order.strategy = strategy_name
        return order
    
    def _pull_broker_order(self, identifier):
        return None
    
    def _pull_broker_all_orders(self):
        return []


class TestBrokerCleanup(unittest.TestCase):
    """Test suite for broker memory cleanup functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_data_source = MockDataSource()
        self.broker = MockBroker(
            name="test_broker",
            connect_stream=False,
            data_source=self.mock_data_source,
            cleanup_config=None  # Use default config
        )

    def create_test_order(self, identifier, strategy="test_strategy", created_days_ago=0):
        """Create a test order with a specific creation date."""
        asset = Asset("TEST")
        order = Order(identifier, asset, 100, Order.OrderSide.BUY)
        order.strategy = strategy
        
        # Set creation date
        creation_date = self.mock_data_source.current_time - timedelta(days=created_days_ago)
        order._date_created = creation_date
        # Note: broker_create_date and broker_update_date are set to None by default
        # We'll use _date_created for timestamp in our cleanup logic
        
        return order

    def create_test_position(self, asset_symbol, strategy="test_strategy", created_days_ago=0):
        """Create a test position with a specific creation date."""
        asset = Asset(asset_symbol)
        position = Position(strategy, asset, Decimal("100"))
        
        # Simulate position creation through an order
        order = self.create_test_order(f"order_{asset_symbol}", strategy, created_days_ago)
        position.orders = [order]
        
        return position

    def test_default_cleanup_config_initialization(self):
        """Test that default cleanup configuration is properly initialized."""
        self.assertTrue(self.broker._cleanup_config["enabled"])
        self.assertEqual(self.broker._cleanup_config["cleanup_interval_iterations"], 100)
        self.assertIn("filled_orders", self.broker._cleanup_config["retention_policies"])
        self.assertIn("canceled_orders", self.broker._cleanup_config["retention_policies"])
        self.assertIn("error_orders", self.broker._cleanup_config["retention_policies"])
        self.assertIn("filled_positions", self.broker._cleanup_config["retention_policies"])

    def test_custom_cleanup_config_initialization(self):
        """Test that custom cleanup configuration overrides defaults properly."""
        custom_config = {
            "enabled": False,
            "cleanup_interval_iterations": 50,
            "retention_policies": {
                "filled_orders": {
                    "max_age_days": 15,
                    "max_count": 5000
                }
            }
        }
        
        # Create a fresh broker for this test to avoid interference from other tests
        fresh_mock_data_source = MockDataSource()
        broker = MockBroker(
            name="test_broker",
            connect_stream=False,
            data_source=fresh_mock_data_source,
            cleanup_config=custom_config
        )
        
        self.assertFalse(broker._cleanup_config["enabled"])
        self.assertEqual(broker._cleanup_config["cleanup_interval_iterations"], 50)
        self.assertEqual(broker._cleanup_config["retention_policies"]["filled_orders"]["max_age_days"], 15)
        self.assertEqual(broker._cleanup_config["retention_policies"]["filled_orders"]["max_count"], 5000)
        # Ensure other defaults are preserved
        self.assertEqual(broker._cleanup_config["retention_policies"]["filled_orders"]["min_keep"], 100)

    def test_cleanup_respects_min_keep(self):
        """Ensure cleanup never removes below min_keep threshold."""
        # Add orders that should be cleaned up by age, but respect min_keep
        policy = {"max_age_days": 1, "max_count": 1000, "min_keep": 5}
        
        # Add 3 old orders (should normally be cleaned up)
        for i in range(3):
            old_order = self.create_test_order(f"old_{i}", created_days_ago=5)
            self.broker._filled_orders.append(old_order)
        
        initial_count = len(self.broker._filled_orders)
        self.assertEqual(initial_count, 3)
        
        # Run cleanup
        current_time = self.mock_data_source.current_time
        removed_count = self.broker._cleanup_tracking_list(
            self.broker._filled_orders, policy, current_time
        )
        
        # Should not remove any orders because we're below min_keep threshold
        self.assertEqual(removed_count, 0)
        self.assertEqual(len(self.broker._filled_orders), 3)

    def test_cleanup_age_based_retention(self):
        """Test that old items are removed based on max_age_days."""
        policy = {"max_age_days": 5, "max_count": 1000, "min_keep": 2}
        
        # Add 5 orders: 2 recent, 3 old
        for i in range(2):
            recent_order = self.create_test_order(f"recent_{i}", created_days_ago=1)
            self.broker._filled_orders.append(recent_order)
        
        for i in range(3):
            old_order = self.create_test_order(f"old_{i}", created_days_ago=10)
            self.broker._filled_orders.append(old_order)
        
        initial_count = len(self.broker._filled_orders)
        self.assertEqual(initial_count, 5)
        
        # Run cleanup
        current_time = self.mock_data_source.current_time
        removed_count = self.broker._cleanup_tracking_list(
            self.broker._filled_orders, policy, current_time
        )
        
        # Should remove 3 old orders
        self.assertEqual(removed_count, 3)
        self.assertEqual(len(self.broker._filled_orders), 2)
        
        # Verify remaining orders are the recent ones
        remaining_orders = self.broker._filled_orders.get_list()
        for order in remaining_orders:
            age = current_time - order._date_created
            self.assertLess(age.days, 5)

    def test_cleanup_count_based_retention(self):
        """Test that excess items are removed based on max_count."""
        policy = {"max_age_days": 365, "max_count": 3, "min_keep": 1}
        
        # Add 6 orders with different ages
        for i in range(6):
            order = self.create_test_order(f"order_{i}", created_days_ago=i)
            self.broker._filled_orders.append(order)
        
        initial_count = len(self.broker._filled_orders)
        self.assertEqual(initial_count, 6)
        
        # Run cleanup
        current_time = self.mock_data_source.current_time
        removed_count = self.broker._cleanup_tracking_list(
            self.broker._filled_orders, policy, current_time
        )
        
        # Should remove 3 oldest orders (keeping 3 most recent)
        self.assertEqual(removed_count, 3)
        self.assertEqual(len(self.broker._filled_orders), 3)
        
        # Verify remaining orders are the most recent ones
        remaining_orders = self.broker._filled_orders.get_list()
        remaining_ages = [order._date_created for order in remaining_orders]
        remaining_ages.sort(reverse=True)  # Most recent first
        
        # The remaining orders should be the 3 most recent (days 0, 1, 2)
        expected_dates = [
            current_time - timedelta(days=i) for i in range(3)
        ]
        for i, remaining_date in enumerate(remaining_ages):
            expected_date = expected_dates[i]
            # Allow small time differences due to test execution timing
            self.assertLess(abs((remaining_date - expected_date).total_seconds()), 60)

    def test_cleanup_preserves_recent_items(self):
        """Ensure most recent items are always preserved."""
        policy = {"max_age_days": 1, "max_count": 2, "min_keep": 3}
        
        # Add 5 orders, all old enough to be cleaned by age
        for i in range(5):
            order = self.create_test_order(f"order_{i}", created_days_ago=5 + i)
            self.broker._filled_orders.append(order)
        
        initial_count = len(self.broker._filled_orders)
        self.assertEqual(initial_count, 5)
        
        # Run cleanup
        current_time = self.mock_data_source.current_time
        removed_count = self.broker._cleanup_tracking_list(
            self.broker._filled_orders, policy, current_time
        )
        
        # Should keep min_keep=3 orders despite age and count limits
        self.assertEqual(removed_count, 2)
        self.assertEqual(len(self.broker._filled_orders), 3)

    def test_full_cleanup_process(self):
        """Test the complete cleanup process across all tracking lists."""
        # Override the min_keep to allow cleanup with fewer orders
        self.broker._cleanup_config["retention_policies"]["filled_orders"]["min_keep"] = 2
        self.broker._cleanup_config["retention_policies"]["canceled_orders"]["min_keep"] = 2  
        self.broker._cleanup_config["retention_policies"]["error_orders"]["min_keep"] = 2
        
        # Add test data to various tracking lists
        
        # Filled orders: 5 recent, 5 old
        for i in range(5):
            recent = self.create_test_order(f"filled_recent_{i}", created_days_ago=1)
            old = self.create_test_order(f"filled_old_{i}", created_days_ago=40)
            self.broker._filled_orders.append(recent)
            self.broker._filled_orders.append(old)
        
        # Canceled orders: 3 recent, 7 old
        for i in range(3):
            recent = self.create_test_order(f"canceled_recent_{i}", created_days_ago=1)
            self.broker._canceled_orders.append(recent)
        for i in range(7):
            old = self.create_test_order(f"canceled_old_{i}", created_days_ago=10)
            self.broker._canceled_orders.append(old)
        
        # Error orders: 2 recent, 3 old
        for i in range(2):
            recent = self.create_test_order(f"error_recent_{i}", created_days_ago=1)
            self.broker._error_orders.append(recent)
        for i in range(3):
            old = self.create_test_order(f"error_old_{i}", created_days_ago=40)
            self.broker._error_orders.append(old)
        
        # Record initial counts
        initial_filled = len(self.broker._filled_orders)
        initial_canceled = len(self.broker._canceled_orders)
        initial_error = len(self.broker._error_orders)
        
        self.assertEqual(initial_filled, 10)
        self.assertEqual(initial_canceled, 10)
        self.assertEqual(initial_error, 5)
        
        # Run full cleanup
        self.broker._cleanup_old_tracking_data()
        
        # Check results based on modified policies
        # filled_orders: max_age_days=30, should remove 5 old orders (40 days old)
        final_filled = len(self.broker._filled_orders)
        self.assertEqual(final_filled, 5)  # Only recent orders remain
        
        # canceled_orders: max_age_days=7, should remove 7 old orders (10 days old)
        final_canceled = len(self.broker._canceled_orders)
        self.assertEqual(final_canceled, 3)  # Only recent orders remain
        
        # error_orders: max_age_days=30, should remove 3 old orders (40 days old)  
        final_error = len(self.broker._error_orders)
        self.assertEqual(final_error, 2)  # Only recent orders remain

    def test_cleanup_disabled(self):
        """Test that cleanup can be completely disabled."""
        # Disable cleanup
        self.broker._cleanup_config["enabled"] = False
        
        # Add old orders that would normally be cleaned up
        for i in range(5):
            old_order = self.create_test_order(f"old_{i}", created_days_ago=100)
            self.broker._filled_orders.append(old_order)
        
        initial_count = len(self.broker._filled_orders)
        
        # Run cleanup
        self.broker._cleanup_old_tracking_data()
        
        # Nothing should be removed
        final_count = len(self.broker._filled_orders)
        self.assertEqual(final_count, initial_count)

    def test_periodic_cleanup_trigger(self):
        """Test that cleanup is triggered based on iteration counter."""
        # Set low cleanup interval for testing and low min_keep
        self.broker._cleanup_config["cleanup_interval_iterations"] = 3
        self.broker._cleanup_config["retention_policies"]["filled_orders"]["min_keep"] = 1
        
        # Add old orders
        for i in range(5):
            old_order = self.create_test_order(f"old_{i}", created_days_ago=40)
            self.broker._filled_orders.append(old_order)
        
        initial_count = len(self.broker._filled_orders)
        
        # Trigger cleanup before interval
        self.broker._trigger_periodic_cleanup()  # iteration 1
        self.broker._trigger_periodic_cleanup()  # iteration 2
        
        # No cleanup should have occurred yet
        self.assertEqual(len(self.broker._filled_orders), initial_count)
        
        # Trigger cleanup at interval
        self.broker._trigger_periodic_cleanup()  # iteration 3
        
        # Cleanup should have occurred
        self.assertLess(len(self.broker._filled_orders), initial_count)

    def test_manual_force_cleanup(self):
        """Test manual cleanup functionality."""
        # Set a lower min_keep to allow cleanup
        self.broker._cleanup_config["retention_policies"]["filled_orders"]["min_keep"] = 1
        
        # Add old orders
        for i in range(5):
            old_order = self.create_test_order(f"old_{i}", created_days_ago=40)
            self.broker._filled_orders.append(old_order)
        
        initial_count = len(self.broker._filled_orders)
        
        # Force manual cleanup
        self.broker.force_cleanup()
        
        # Cleanup should have occurred
        final_count = len(self.broker._filled_orders)
        self.assertLess(final_count, initial_count)

    def test_cleanup_with_missing_timestamps(self):
        """Test cleanup behavior when orders have missing timestamp fields."""
        # Create order with no timestamp fields
        order = Order("test_id", Asset("TEST"), 100, Order.OrderSide.BUY)
        order.strategy = "test"
        # Don't set any timestamp fields
        
        self.broker._filled_orders.append(order)
        
        # Should not crash and should not remove the order (fallback behavior)
        policy = {"max_age_days": 1, "max_count": 1000, "min_keep": 0}
        current_time = self.mock_data_source.current_time
        
        removed_count = self.broker._cleanup_tracking_list(
            self.broker._filled_orders, policy, current_time
        )
        
        # Order without timestamp should not be removed
        self.assertEqual(removed_count, 0)
        self.assertEqual(len(self.broker._filled_orders), 1)

    def test_cleanup_thread_safety(self):
        """Test that cleanup works correctly with concurrent operations."""
        import threading
        import time
        
        # Create a fresh broker for this test to avoid modifying shared state
        fresh_mock_data_source = MockDataSource()
        broker = MockBroker(
            name="test_thread_safety",
            connect_stream=False,
            data_source=fresh_mock_data_source,
            cleanup_config={
                "enabled": True,
                "retention_policies": {
                    "filled_orders": {
                        "max_age_days": 30,
                        "max_count": 50,
                        "min_keep": 10  # Lower min_keep to allow cleanup
                    }
                }
            }
        )
        
        # Add initial orders
        for i in range(100):
            order = self.create_test_order(f"order_{i}", created_days_ago=40)
            broker._filled_orders.append(order)
        
        # Function to add orders concurrently
        def add_orders():
            for i in range(50):
                order = self.create_test_order(f"concurrent_{i}", created_days_ago=1)
                broker._filled_orders.append(order)
                time.sleep(0.001)  # Small delay to simulate real operations
        
        # Start concurrent thread
        thread = threading.Thread(target=add_orders)
        thread.start()
        
        # Run cleanup while orders are being added
        broker._cleanup_old_tracking_data()
        
        # Wait for concurrent thread to finish
        thread.join()
        
        # Verify that cleanup worked without crashing
        # Should have some orders remaining (recent ones and concurrent ones)
        final_count = len(broker._filled_orders)
        self.assertGreater(final_count, 0)
        self.assertLess(final_count, 140)  # Some old orders should be removed


if __name__ == '__main__':
    unittest.main()
