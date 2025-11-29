import unittest
from unittest.mock import MagicMock
import pandas as pd
from collections import OrderedDict
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
        class _StubDataSource:
            def __init__(self):
                self.get_datetime = MagicMock()
                self._update_datetime = MagicMock()

        # Prevent __init__ from running fully if it causes issues
        self.broker = BacktestingBroker.__new__(BacktestingBroker)
        self.broker.data_source = _StubDataSource()  # Assign lightweight data_source stub
        self.broker.market = "NYSE"
        self.broker._market_session_cache = OrderedDict()
        self.broker._cache_max_size = 500
        self.broker._daily_sessions = {}
        self.broker._sessions_built = False

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

    def _set_current_time(self, timestamp):
        """Helper to set the mock time."""
        if isinstance(timestamp, pd.Timestamp):
            ts = timestamp
        elif isinstance(timestamp, datetime):
            ts = pd.Timestamp(timestamp)
        else:
            ts = pd.Timestamp(timestamp)

        if ts.tzinfo is None:
            ts = ts.tz_localize('America/New_York')

        self.mock_datetime = ts
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

    def test_get_time_to_close_returns_zero_at_close(self):
        """Regression: ensure get_time_to_close returns 0 when now == market_close."""
        tz = pytz.timezone("America/New_York")
        market_open = tz.localize(datetime(2025, 11, 2, 9, 30))
        market_close = tz.localize(datetime(2025, 11, 2, 16, 0))
        self.broker.get_time_to_close = BacktestingBroker.get_time_to_close.__get__(self.broker, BacktestingBroker)
        self.broker._trading_days = pd.DataFrame(
            {"market_open": [market_open]},
            index=[market_close],
        )
        self.broker._market_open_cache = {market_close: market_open}
        self._set_current_time('2025-11-02 16:00:00')

        seconds = self.broker.get_time_to_close()

        assert seconds == 0.0

    def test_get_time_to_close_handles_contiguous_sessions(self):
        """Futures sessions reopen immediately; broker must advance to the next close."""
        tz = pytz.timezone("America/New_York")
        first_open = tz.localize(datetime(2025, 10, 27, 18, 0))
        first_close = tz.localize(datetime(2025, 10, 28, 18, 0))
        second_open = first_close
        second_close = tz.localize(datetime(2025, 10, 29, 18, 0))

        self.broker.market = "us_futures"
        self.broker.get_time_to_close = BacktestingBroker.get_time_to_close.__get__(self.broker, BacktestingBroker)
        self.broker.is_market_open = MagicMock(return_value=True)
        self.broker._trading_days = pd.DataFrame(
            {"market_open": [first_open, second_open]},
            index=[first_close, second_close],
        )
        self.broker._market_open_cache = {
            first_close: first_open,
            second_close: second_open,
        }
        self._set_current_time(second_open)

        seconds = self.broker.get_time_to_close()

        assert seconds == (second_close - second_open).total_seconds()

    def test_update_datetime_pushes_forward_on_duplicate_timestamp(self):
        """Regression: new_datetime must advance when DST normalization repeats a minute."""
        tz = pytz.timezone("America/New_York")
        stuck_time = tz.localize(datetime(2025, 11, 2, 1, 30), is_dst=True)
        # Restore the real implementation for this regression test.
        self.broker._update_datetime = BacktestingBroker._update_datetime.__get__(self.broker, BacktestingBroker)
        self._set_current_time(pd.Timestamp(stuck_time))
        self.broker.option_source = None
        update_spy = MagicMock()
        self.broker.data_source._update_datetime = update_spy

        self.broker._update_datetime(stuck_time)

        update_spy.assert_called_once()
        forwarded = update_spy.call_args[0][0]
        self.assertGreater(forwarded, stuck_time)
        self.assertEqual((forwarded - stuck_time).total_seconds(), 60.0)

    # Remove the old simulation tests as they are replaced by direct method calls
    # def test_advance_time_before_market_open(self): ...
    # def test_advance_time_during_market_hours_no_buffer(self): ...
    # def test_advance_time_get_time_to_close_returns_none(self): ...
    # def test_advance_time_with_buffer(self): ...
    # def test_advance_time_with_buffer_making_time_negative(self): ...


if __name__ == '__main__':
    unittest.main()
