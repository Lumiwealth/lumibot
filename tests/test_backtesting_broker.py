import datetime
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from datetime import datetime as dt, time, timedelta # Renamed datetime to dt to avoid conflict
import pytz


# Assuming the BacktestingBroker class is importable like this
# Adjust the import path if necessary based on your project structure
try:
    from lumibot.backtesting.backtesting_broker import BacktestingBroker
    from lumibot.data_sources import PandasData
    from lumibot.entities import Asset, Order # Import Asset if needed by mocked methods
except ImportError:
    # Add path modification if running tests directly and lumibot is not installed
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    from lumibot.backtesting.backtesting_broker import BacktestingBroker
    from lumibot.data_sources import PandasData
    from lumibot.entities import Asset, Order


class TestBacktestingBroker:
    def test_limit_fills(self):
        start = dt(2023, 8, 1) # Use dt alias
        end = dt(2023, 8, 2) # Use dt alias
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # Limit triggered by candle body
        limit_price = 105
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == limit_price

        # Limit triggered by candle wick
        limit_price = 109
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == limit_price

        # Limit Sell Triggered by a gap up candle
        limit_price = 85
        assert broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90) == 100

        # Limit Buy Triggered by a gap down candle
        limit_price = 115
        assert broker.limit_order(limit_price, 'buy', open_=100, high=110, low=90) == 100

        # Limit not triggered
        limit_price = 120
        assert not broker.limit_order(limit_price, 'sell', open_=100, high=110, low=90)

    def test_stop_fills(self):
        start = dt(2023, 8, 1) # Use dt alias
        end = dt(2023, 8, 2) # Use dt alias
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # Stop triggered by candle body
        stop_price = 95
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == stop_price

        # Stop triggered by candle wick
        stop_price = 91
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == stop_price

        # Stop Sell Triggered by a gap down candle
        stop_price = 115
        assert broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90) == 100

        # Stop Buy Triggered by a gap up candle
        stop_price = 85
        assert broker.stop_order(stop_price, 'buy', open_=100, high=110, low=90) == 100

        # Stop not triggered
        stop_price = 80
        assert not broker.stop_order(stop_price, 'sell', open_=100, high=110, low=90)

    def test_submit_order_calls_conform_order(self):
        start = dt(2023, 8, 1) # Use dt alias
        end = dt(2023, 8, 2) # Use dt alias
        data_source = PandasData(datetime_start=start, datetime_end=end, pandas_data={})
        broker = BacktestingBroker(data_source=data_source)

        # mock _conform_order method
        broker._conform_order = MagicMock()
        Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc')
        broker.submit_order(Order(asset=Asset("SPY"), quantity=10, side="buy", strategy='abc'))
        broker._conform_order.assert_called_once()


# New Test Class for Time Advancement Logic
class TestBacktestingBrokerTimeAdvance(unittest.TestCase):

    def setUp(self):
        """Set up a mock BacktestingBroker instance for testing."""
        # Use patch to mock the data_source during instantiation or assign afterwards
        with patch('lumibot.backtesting.backtesting_broker.DataSourceBacktesting') as MockDataSource:
            # Prevent __init__ from running fully if it causes issues
            self.broker = BacktestingBroker.__new__(BacktestingBroker)
            # Mock necessary attributes that would normally be set in __init__
            self.broker._trading_days = pd.DataFrame() # Initialize attribute
            self.broker.data_source = MockDataSource() # Assign mock data_source

        self.broker.logger = MagicMock()
        # Mock the get_datetime method on the data_source
        self.mock_datetime = pd.Timestamp('2023-01-01 10:00:00', tz='America/New_York')
        self.broker.data_source.get_datetime = MagicMock(return_value=self.mock_datetime)

        # Setup trading days directly on the broker instance for testing internal logic
        # Ensure _trading_days is set correctly after __new__
        self.broker._trading_days = pd.DataFrame({
            'market_open': [pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')],
            'market_close': [pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')]
        }, index=[pd.Timestamp('2023-01-01 16:00:00', tz='America/New_York')]) # Index is market_close_time

        # Mock other methods used by the tested logic
        self.broker.get_time_to_close = MagicMock()
        self.broker.get_time_to_open = MagicMock() # Mock get_time_to_open
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

    # ===== Tests for _await_market_to_open =====

    def test_await_open_before_market_opens_no_buffer(self):
        """Test _await_market_to_open before market opens without buffer."""
        self._set_current_time('2023-01-01 09:00:00')
        market_open_time = pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')
        expected_time_to_open_seconds = (market_open_time - self.mock_datetime).total_seconds() # 1800

        # Mock get_time_to_open to return the calculated value
        self.broker.get_time_to_open.return_value = expected_time_to_open_seconds

        # Call the method under test
        self.broker._await_market_to_open(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_open.assert_called_once()
        self.broker._update_datetime.assert_called_once_with(expected_time_to_open_seconds)

    def test_await_open_with_buffer(self):
        """Test _await_market_to_open with a timedelta buffer."""
        self._set_current_time('2023-01-01 08:00:00')
        market_open_time = pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')
        base_time_to_open = (market_open_time - self.mock_datetime).total_seconds() # 5400 seconds
        buffer_minutes = 5
        expected_update_time = base_time_to_open - (buffer_minutes * 60) # 5100 seconds

        # Mock get_time_to_open returning the base value
        self.broker.get_time_to_open.return_value = base_time_to_open

        # Call the method under test with the buffer
        self.broker._await_market_to_open(timedelta=buffer_minutes, strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_open.assert_called_once()
        self.broker._update_datetime.assert_called_once_with(expected_update_time)

    def test_await_open_when_market_already_open(self):
        """Test _await_market_to_open when the market is already open (time_to_open is 0)."""
        self._set_current_time('2023-01-01 10:00:00') # Time is during market hours

        # Mock get_time_to_open returning 0
        self.broker.get_time_to_open.return_value = 0

        # Call the method under test
        self.broker._await_market_to_open(strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_open.assert_called_once()
        # _update_datetime should NOT be called because time_to_open is 0
        self.broker._update_datetime.assert_not_called()

    def test_await_open_with_buffer_making_time_negative(self):
        """Test _await_market_to_open when buffer makes time_to_open non-positive."""
        self._set_current_time('2023-01-01 09:28:00') # 2 minutes before open
        market_open_time = pd.Timestamp('2023-01-01 09:30:00', tz='America/New_York')
        base_time_to_open = (market_open_time - self.mock_datetime).total_seconds() # 120 seconds
        buffer_minutes = 3 # 180 seconds buffer

        # Mock get_time_to_open returning the base value
        self.broker.get_time_to_open.return_value = base_time_to_open

        # Call the method under test with the buffer
        self.broker._await_market_to_open(timedelta=buffer_minutes, strategy=self.mock_strategy)

        # Assertions
        self.broker.process_pending_orders.assert_called_once_with(strategy=self.mock_strategy)
        self.broker.get_time_to_open.assert_called_once()
        # _update_datetime should NOT be called because calculated time_to_open is <= 0
        self.broker._update_datetime.assert_not_called()


if __name__ == '__main__':
    unittest.main()
