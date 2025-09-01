"""
Test the new session-based architecture to ensure it solves the infinite restart bug.

This test validates that the session manager approach provides guaranteed time
progression and clean separation of concerns between backtesting and live trading.
"""

import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import signal
import sys
import pytest

from lumibot.strategies.session_manager import SessionManager, BacktestingSession, LiveTradingSession


class TimeoutError(Exception):
    """Custom timeout exception for test timeouts."""
    pass


def timeout_handler(signum, frame):
    """Signal handler for timeout."""
    raise TimeoutError("Test timed out - possible infinite loop detected")


class TestSessionManager(unittest.TestCase):
    """Test the session manager architecture."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.timeout_seconds = 10  # Much shorter timeout for unit tests
        
        # Create mock strategy executor
        self.mock_executor = Mock()
        self.mock_executor.datetime = datetime(2024, 1, 1, 9, 30)
        self.mock_executor.is_backtesting_finished.return_value = False
        self.mock_executor.is_alive.return_value = True

    @pytest.mark.skipif(sys.platform == "win32", reason="SIGALRM not available on Windows")
    def test_backtesting_session_guarantees_time_progression(self):
        """Test that BacktestingSession guarantees forward time progression."""
        print("\n=== Testing BacktestingSession Time Progression ===")
        
        # Set up timeout to catch infinite loops
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(self.timeout_seconds)
        
        try:
            session = BacktestingSession(self.mock_executor)
            
            # Configure session times
            start_time = datetime(2024, 1, 1, 9, 30)
            end_time = datetime(2024, 1, 5, 16, 0)  # 4 days later
            time_step = timedelta(hours=1)  # Hourly progression
            
            session.set_time_parameters(start_time, end_time, time_step)
            
            # Track time progression
            times_advanced = []
            max_iterations = 10  # Limit iterations for testing
            
            for i in range(max_iterations):
                if not session.advance_time():
                    print(f"✓ Session correctly ended after {i} iterations")
                    break
                    
                times_advanced.append(session._current_time)
                
                # Verify time is progressing forward
                if len(times_advanced) > 1:
                    self.assertGreater(
                        times_advanced[-1], 
                        times_advanced[-2],
                        "Time must progress forward to prevent infinite loops"
                    )
            
            print(f"✓ Time progressed through {len(times_advanced)} steps")
            print(f"✓ Start: {start_time}, End: {times_advanced[-1] if times_advanced else 'N/A'}")
            
            # Verify we had forward progression
            self.assertGreater(len(times_advanced), 0, "Time must advance at least once")
            
            signal.alarm(0)  # Clear timeout
            print("✓ BacktestingSession guarantees time progression - no infinite loops")
            
        except TimeoutError:
            self.fail("BacktestingSession timed out - time progression not guaranteed")

    @pytest.mark.skipif(sys.platform == "win32", reason="SIGALRM not available on Windows")     
    def test_backtesting_session_prevents_infinite_restart(self):
        """Test that BacktestingSession prevents the infinite restart bug."""
        print("\n=== Testing Infinite Restart Prevention ===")
        
        # Set up timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(self.timeout_seconds)
        
        try:
            session = BacktestingSession(self.mock_executor)
            
            # Simulate the problematic scenario: no data source, brief time period
            start_time = datetime(2024, 1, 2, 9, 30)
            end_time = datetime(2024, 1, 2, 16, 0)  # Same day
            session.set_time_parameters(start_time, end_time, timedelta(minutes=30))
            
            # Mock the executor to simulate minimal strategy
            self.mock_executor.on_trading_iteration = Mock()
            
            # Track session execution
            iterations = 0
            max_safe_iterations = 20  # Safety limit
            
            while session.should_continue_session() and iterations < max_safe_iterations:
                session.execute_trading_cycle()
                
                # CRITICAL: This must advance time to prevent infinite restart
                if not session.advance_time():
                    print(f"✓ Session ended naturally after {iterations} iterations")
                    break
                    
                session.wait_for_next_cycle()
                iterations += 1
                
            signal.alarm(0)  # Clear timeout
            
            # Verify session completed without hanging
            self.assertLess(iterations, max_safe_iterations, 
                          "Session should complete within reasonable iterations")
            self.assertGreater(iterations, 0, 
                             "Session should execute at least one iteration")
            
            print(f"✓ Session completed {iterations} iterations without infinite restart")
            print("✓ Infinite restart bug prevented by guaranteed time progression")
            
        except TimeoutError:
            self.fail("BacktestingSession still has infinite restart bug")

    @pytest.mark.skipif(sys.platform == "win32", reason="SIGALRM not available on Windows")    
    def test_live_trading_session_time_management(self):
        """Test that LiveTradingSession handles time correctly."""
        print("\n=== Testing LiveTradingSession Time Management ===")
        
        session = LiveTradingSession(self.mock_executor)
        
        # Test time advancement (should always succeed in live trading)
        start_time = session._last_execution_time
        result = session.advance_time()
        
        self.assertTrue(result, "Live trading time advancement should always succeed")
        
        # Verify time was updated
        if start_time is not None:
            self.assertGreater(session._last_execution_time, start_time,
                             "Live trading should update execution time")
        
        print("✓ LiveTradingSession time management working correctly")
        
    def test_session_architecture_separation_of_concerns(self):
        """Test that session architecture properly separates concerns."""
        print("\n=== Testing Separation of Concerns ===")
        
        # Test that both session types can be created without interference
        backtesting_session = BacktestingSession(self.mock_executor)
        live_session = LiveTradingSession(self.mock_executor)
        
        # Verify they have different class types (better test for separation)
        self.assertNotEqual(
            type(backtesting_session), 
            type(live_session),
            "Sessions should be different classes for different concerns"
        )
        
        # Verify they are both SessionManager subclasses
        from lumibot.strategies.session_manager import SessionManager
        self.assertIsInstance(backtesting_session, SessionManager)
        self.assertIsInstance(live_session, SessionManager)
        
        # Test that backtesting requires time parameters
        start_time = datetime(2024, 1, 1)
        end_time = datetime(2024, 1, 2)
        backtesting_session.set_time_parameters(start_time, end_time)
        
        self.assertEqual(backtesting_session._current_time, start_time)
        self.assertEqual(backtesting_session._end_time, end_time)
        
        print("✓ Session architecture maintains proper separation of concerns")
        
    def test_session_replaces_problematic_run_trading_session(self):
        """Test that session approach can replace the problematic _run_trading_session."""
        print("\n=== Testing Session Replacement for _run_trading_session ===")
        
        # Set up timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(self.timeout_seconds)
        
        try:
            session = BacktestingSession(self.mock_executor)
            
            # Configure for a minimal run
            start_time = datetime(2024, 1, 1, 9, 30)
            end_time = datetime(2024, 1, 1, 10, 30)  # 1 hour window
            session.set_time_parameters(start_time, end_time, timedelta(minutes=15))
            
            # Mock the trading iteration
            call_count = 0
            def mock_trading_iteration():
                nonlocal call_count
                call_count += 1
                print(f"  Trading iteration {call_count} at {session._current_time}")
                
            self.mock_executor.on_trading_iteration = mock_trading_iteration
            
            # Run the session (replacement for _run_trading_session)
            session.run_session()
            
            signal.alarm(0)  # Clear timeout
            
            # Verify session executed successfully
            self.assertGreater(call_count, 0, "Session should execute trading iterations")
            print(f"✓ Session executed {call_count} trading iterations successfully")
            print("✓ Session architecture successfully replaces problematic _run_trading_session")
            
        except TimeoutError:
            self.fail("Session-based approach still has timing issues")
            
    def test_data_source_agnostic_design(self):
        """Test that session design is data source agnostic."""
        print("\n=== Testing Data Source Agnostic Design ===")
        
        # Test with mock executor that has no data source (like ES futures issue)
        no_data_executor = Mock()
        no_data_executor.datetime = datetime(2024, 1, 1)
        no_data_executor.is_backtesting_finished.return_value = False
        no_data_executor.on_trading_iteration = Mock()
        
        # Session should work regardless of data source presence
        session = BacktestingSession(no_data_executor)
        session.set_time_parameters(
            datetime(2024, 1, 1), 
            datetime(2024, 1, 1, 2, 0),  # 2 hours (longer window)
            timedelta(minutes=30)
        )
        
        # Should be able to advance time regardless of data source
        result1 = session.advance_time()
        result2 = session.advance_time()
        result3 = session.advance_time()  # This should still work
        
        self.assertTrue(result1, "Time advancement should work without data source")
        self.assertTrue(result2, "Multiple time advancements should work")
        self.assertTrue(result3, "Time advancement should continue working")
        
        # Verify time progressed
        self.assertGreater(
            session._current_time, 
            datetime(2024, 1, 1),
            "Time should progress independently of data source"
        )
        
        print("✓ Session design is properly data source agnostic")
        print("✓ Solves ES futures issue where no data source caused infinite restart")


if __name__ == '__main__':
    unittest.main(verbosity=2)
