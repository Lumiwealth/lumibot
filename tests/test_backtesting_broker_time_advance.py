import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime, time, timedelta
import pytz

# Try importing BacktestingBroker, and if it fails, add the project root to sys.path and retry
try:
    from lumibot.backtesting.backtesting_broker import BacktestingBroker
    from lumibot.entities import Asset
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
    from lumibot.backtesting.backtesting_broker import BacktestingBroker
    from lumibot.entities import Asset


class TestBacktestingBrokerTimeAdvance(unittest.TestCase):

    def setUp(self):
        """Set up a mock BacktestingBroker instance for testing."""
        # Use patch to mock the data_source during instantiation or assign afterwards
        with patch('lumibot.backtesting.backtesting_broker.DataSourceBacktesting') as MockDataSource:
            # Prevent __init__ from running fully if it causes issues
            self.broker = BacktestingBroker.__new__(BacktestingBroker)
            self.broker.data_source = MockDataSource() # Assign mock data_source

        self.broker.logger = MagicMock()
        # Mock the get_datetime method on the data_source
        self.mock_datetime = pd.Timestamp('2023-01-01 10:00:00', tz='America/New_York')
        self.broker.data_source.get_datetime = MagicMock(return_value=self.mock_datetime)

        # Setup trading days directly on the broker instance for testing internal logic
        self.broker._trading_days = pd.DataFrame({
            'market_open': [pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')],
            'market_close': [pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')]
        }, index=[pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')]) # Index is market_close_time

        # Mock other methods used by the tested logic
        self.broker.get_time_to_close = MagicMock()
        self.broker._update_datetime = MagicMock()
        self.broker.process_pending_orders = MagicMock()
        self.mock_strategy = MagicMock() # Mock strategy object

    def _set_current_time(self, timestamp_str):
        """Helper to set the mock time."""
        self.mock_datetime = pd.Timestamp(timestamp_str, tz='America/New_York')
        self.broker.data_source.get_datetime.return_value = self.mock_datetime

    def test_await_close_during_market_hours_no_buffer(self):
        """Test _await_market_to_close during market hours without buffer."""
        self._set_current_time('2023-01-01 15:30:00')
        market_close_time = pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')
        expected_time_to_close_seconds = (market_close_time - self.mock_datetime).total_seconds() # 1800

        # Mock get_time_to_close to return the calculated value
        self.broker.get_time_to_close.return_value = expected_time_to_close_seconds

        # Call the method under test
        self.broker._await_market_to_close(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_close.assert_called_once()
        self.broker._update_datetime.assert_called_once_with(expected_time_to_close_seconds)

    def test_await_close_get_time_to_close_returns_none(self):
        """Test _await_market_to_close when get_time_to_close returns None (e.g., market closed)."""
        # Simulate a time when market might be considered closed or get_time_to_close fails
        self._set_current_time('2023-01-01 17:00:00')

        # Mock get_time_to_close returning None
        self.broker.get_time_to_close.return_value = None # Simulate market closed or error

        # Call the method under test
        self.broker._await_market_to_close(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_close.assert_called_once()
        # _update_datetime should NOT be called if time_to_close is None or <= 0
        self.broker._update_datetime.assert_not_called()

    def test_await_close_with_buffer(self):
        """Test _await_market_to_close with a timedelta buffer."""
        self._set_current_time('2023-01-01 15:00:00')
        market_close_time = pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')
        base_time_to_close = (market_close_time - self.mock_datetime).total_seconds() # 3600 seconds
        buffer_minutes = 5
        expected_update_time = base_time_to_close - (buffer_minutes * 60) # 3300 seconds

        # Mock get_time_to_close returning the base value
        self.broker.get_time_to_close.return_value = base_time_to_close

        # Call the method under test with the buffer
        self.broker._await_market_to_close(timedelta=buffer_minutes, strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_close.assert_called_once()
        self.broker._update_datetime.assert_called_once_with(expected_update_time)

    def test_await_close_when_already_past_close_no_buffer(self):
        """Test _await_market_to_close when current time is past market close (no buffer)."""
        self._set_current_time('2023-01-01 16:01:00')

        # Mock get_time_to_close returning 0 or negative
        self.broker.get_time_to_close.return_value = -60

        # Call the method under test
        self.broker._await_market_to_close(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_close.assert_called_once()
        # _update_datetime should NOT be called
        self.broker._update_datetime.assert_not_called()

    # Remove the old simulation tests as they are replaced by direct method calls
    # def test_advance_time_before_market_open(self): ...
    # def test_advance_time_during_market_hours_no_buffer(self): ...
    # def test_advance_time_get_time_to_close_returns_none(self): ...
    # def test_advance_time_with_buffer(self): ...
    # def test_advance_time_with_buffer_making_time_negative(self): ...


if __name__ == '__main__':
    unittest.main()
