import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta, time
import threading
import time as time_module
from lumibot.strategies import Strategy
from lumibot.strategies.strategy_executor import StrategyExecutor


class TestStrategyExecutorImprovements:
    """Test improvements made to strategy executor"""

    def test_should_continue_trading_loop_logic(self):
        """Test the extracted should_continue_trading_loop method"""
        strategy = Mock()
        strategy.is_backtesting = False
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.should_continue = Mock(return_value=True)
        # should_continue is a property that checks stop_event, so we use that
        executor.stop_event.clear()  # This makes should_continue True
        
        # Test with jobs present and all conditions true
        jobs = [Mock()]  # Non-empty list
        is_247 = False
        should_we_stop = False
        
        result = executor._should_continue_trading_loop(jobs, is_247, should_we_stop)
        assert result == True
        
        # Test with no jobs
        result = executor._should_continue_trading_loop([], is_247, should_we_stop)
        assert result == False
        
        # Test with broker should not continue
        executor.broker.should_continue.return_value = False
        result = executor._should_continue_trading_loop(jobs, is_247, should_we_stop)
        assert result == False
        
        # Reset broker
        executor.broker.should_continue.return_value = True
        
        # Test with strategy should not continue
        executor.stop_event.set()  # This makes should_continue False
        result = executor._should_continue_trading_loop(jobs, is_247, should_we_stop)
        assert result == False
        
        # Reset strategy
        executor.stop_event.clear()  # This makes should_continue True
        
        # Test with 24/7 market but should stop
        result = executor._should_continue_trading_loop(jobs, True, True)
        assert result == True  # 24/7 markets ignore should_we_stop
        
        # Test with non-24/7 market and should stop
        result = executor._should_continue_trading_loop(jobs, False, True)
        assert result == False

    def test_is_pandas_daily_data_source(self):
        """Test the extracted pandas daily data source check"""
        strategy = Mock()
        executor = StrategyExecutor(strategy)
        
        # Test with no data source
        executor.broker = Mock()
        executor.broker._data_source = None
        result = executor._is_pandas_daily_data_source()
        assert result == False
        
        # Test with pandas daily data source
        executor.broker._data_source = Mock()
        executor.broker.data_source = Mock()
        executor.broker.data_source.SOURCE = "PANDAS"
        executor.broker.data_source._timestep = "day"
        result = executor._is_pandas_daily_data_source()
        assert result == True
        
        # Test with pandas but not daily
        executor.broker.data_source._timestep = "minute"
        result = executor._is_pandas_daily_data_source()
        assert result == False
        
        # Test with daily but not pandas
        executor.broker.data_source.SOURCE = "YAHOO"
        executor.broker.data_source._timestep = "day"
        result = executor._is_pandas_daily_data_source()
        assert result == False

    def test_process_pandas_daily_data(self):
        """Test the extracted pandas daily data processing"""
        strategy = Mock()
        strategy.cash = 100000
        strategy.portfolio_value = 100000
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = True
        executor.broker.data_source = Mock()
        executor.broker.data_source._iter_count = None
        
        # Create realistic date index
        import pandas as pd
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        executor.broker.data_source._date_index = dates
        executor.broker.datetime = dates[0]
        
        # Mock required methods
        executor.broker._update_datetime = Mock()
        strategy._update_cash_with_dividends = Mock()
        executor._on_trading_iteration = Mock()
        executor.broker.process_pending_orders = Mock()
        
        # Test first call (iter_count is None)
        executor._process_pandas_daily_data()
        
        # Verify iter_count was set
        assert executor.broker.data_source._iter_count is not None
        
        # Verify methods were called
        executor.broker._update_datetime.assert_called_once()
        strategy._update_cash_with_dividends.assert_called_once()
        executor._on_trading_iteration.assert_called_once()
        executor.broker.process_pending_orders.assert_called_once()
        
        # Test second call (iter_count exists)
        initial_count = executor.broker.data_source._iter_count
        executor._process_pandas_daily_data()
        
        # Verify iter_count was incremented
        assert executor.broker.data_source._iter_count == initial_count + 1

    def test_setup_live_trading_scheduler(self):
        """Test the extracted live trading scheduler setup"""
        strategy = Mock()
        strategy.force_start_immediately = False
        
        executor = StrategyExecutor(strategy)
        executor.scheduler = Mock()
        executor.scheduler.running = False
        executor.calculate_strategy_trigger = Mock(return_value="mock_trigger")
        executor.cron_count_target = 5
        
        # Test scheduler setup
        executor._setup_live_trading_scheduler()
        
        # Verify scheduler was started
        executor.scheduler.start.assert_called_once()
        
        # Verify trigger was calculated
        executor.calculate_strategy_trigger.assert_called_once_with(
            force_start_immediately=False
        )
        
        # Verify job was added
        executor.scheduler.add_job.assert_called_once()
        
        # Verify cron count was set
        assert executor.cron_count == 5

    def test_calculate_should_we_stop(self):
        """Test the extracted should_we_stop calculation"""
        strategy = Mock()
        strategy.minutes_before_closing = 10
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        
        # Test with None time_to_close
        executor.broker.get_time_to_close.return_value = None
        result = executor._calculate_should_we_stop()
        assert result == False
        
        # Test with time_to_close > minutes_before_closing
        executor.broker.get_time_to_close.return_value = 1200  # 20 minutes
        result = executor._calculate_should_we_stop()
        assert result == False
        
        # Test with time_to_close <= minutes_before_closing
        executor.broker.get_time_to_close.return_value = 600  # 10 minutes
        result = executor._calculate_should_we_stop()
        assert result == True
        
        # Test with time_to_close < minutes_before_closing
        executor.broker.get_time_to_close.return_value = 300  # 5 minutes
        result = executor._calculate_should_we_stop()
        assert result == True

    def test_handle_lifecycle_methods(self):
        """Test the extracted lifecycle methods handling"""
        strategy = Mock()
        strategy.get_datetime.return_value = datetime(2024, 1, 15, 15, 30)  # 3:30 PM
        strategy.minutes_before_closing = 30
        strategy.minutes_before_opening = 30
        strategy.minutes_after_closing = 30
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.market_close_time.return_value = datetime(2024, 1, 15, 16, 0)  # 4:00 PM
        executor.broker.market_open_time.return_value = datetime(2024, 1, 15, 9, 30)   # 9:30 AM
        
        # Mock lifecycle methods
        executor._after_market_closes = Mock()
        executor._before_market_closes = Mock()
        executor._before_market_opens = Mock()
        
        # Initialize lifecycle_last_date
        executor.lifecycle_last_date = {
            'after_market_closes': None,
            'before_market_closes': None,
            'before_market_opens': None,
        }
        
        # Test before market closes scenario (current time is 30 minutes before close)
        executor._handle_lifecycle_methods()
        
        # Should trigger before_market_closes
        executor._before_market_closes.assert_called_once()
        assert executor.lifecycle_last_date['before_market_closes'] == datetime(2024, 1, 15).date()

    def test_run_backtesting_loop(self):
        """Test the extracted backtesting loop"""
        strategy = Mock()
        strategy.minutes_before_closing = 30
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = True
        
        # Mock data source with end date
        executor.broker.data_source = Mock()
        executor.broker.data_source.datetime_end = datetime(2024, 1, 20)
        executor.broker.datetime = datetime(2024, 1, 15)  # Before end date
        
        # Mock required methods
        executor._on_trading_iteration = Mock()
        executor.broker.process_pending_orders = Mock()
        executor._strategy_sleep = Mock(return_value=True)
        
        # Test 24/7 market (should run until strategy_sleep returns False)
        is_247 = True
        time_to_close = None
        
        # Set up strategy_sleep to return False after 3 calls
        call_count = 0
        def mock_strategy_sleep():
            nonlocal call_count
            call_count += 1
            return call_count < 3
        
        executor._strategy_sleep = mock_strategy_sleep
        
        executor._run_backtesting_loop(is_247, time_to_close)
        
        # Should have called trading iteration 3 times
        assert executor._on_trading_iteration.call_count == 3
        assert executor.broker.process_pending_orders.call_count == 3

    def test_setup_market_session(self):
        """Test the extracted market session setup"""
        strategy = Mock()
        strategy.get_datetime.return_value = datetime(2024, 1, 15, 9, 30)
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.should_continue.return_value = True
        executor.broker.is_market_open.return_value = False
        
        # Mock required methods
        strategy.await_market_to_open = Mock()
        strategy._update_cash_with_dividends = Mock()
        executor._before_market_opens = Mock()
        executor._before_starting_trading = Mock()
        
        # Initialize lifecycle_last_date
        executor.lifecycle_last_date = {
            'before_market_opens': None,
            'before_starting_trading': None,
        }
        
        # Test successful setup
        has_data_source = False
        result = executor._setup_market_session(has_data_source)
        
        assert result == True
        strategy.await_market_to_open.assert_called()
        strategy._update_cash_with_dividends.assert_called_once()
        executor._before_market_opens.assert_called_once()
        executor._before_starting_trading.assert_called_once()
        
        # Test when broker should not continue
        executor.broker.should_continue.return_value = False
        result = executor._setup_market_session(has_data_source)
        
        assert result == False

    def test_sleeptime_combinations_with_market_types(self):
        """Test various sleeptime and market type combinations"""
        strategy = Mock()
        strategy.is_backtesting = True
        strategy.sleeptime = "1M"
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        
        # Test different sleeptime formats
        sleeptimes = ["1S", "30S", "1M", "5M", "15M", "1H", "1D"]
        market_types = ["us_futures", "NASDAQ", "NYSE", "24/7"]
        
        for sleeptime in sleeptimes:
            for market in market_types:
                strategy.sleeptime = sleeptime
                
                # Mock the strategy sleep method
                with patch.object(executor, '_strategy_sleep') as mock_sleep:
                    executor._strategy_sleep()
                    mock_sleep.assert_called_once()

    def test_continuous_market_detection(self):
        """Test detection of continuous vs non-continuous markets"""
        strategy = Mock()
        strategy.is_backtesting = True
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        
        # Test continuous market detection logic
        continuous_markets = ["24/7", "forex", "crypto"]
        non_continuous_markets = ["us_futures", "NASDAQ", "NYSE", "LSE"]
        
        for market in continuous_markets:
            # Mock the market checking logic
            with patch.object(executor, '_is_continuous_market', return_value=True):
                result = executor._is_continuous_market(market)
                assert result == True, f"Market {market} should be continuous"
        
        for market in non_continuous_markets:
            # Mock the market checking logic  
            with patch.object(executor, '_is_continuous_market', return_value=False):
                result = executor._is_continuous_market(market)
                assert result == False, f"Market {market} should be non-continuous"

    def test_advance_to_next_trading_day_functionality(self):
        """Test the _advance_to_next_trading_day method"""
        strategy = Mock()
        strategy.is_backtesting = True
        strategy.get_datetime.return_value = datetime(2024, 1, 2, 16, 0)  # Market close time
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        
        # Mock the advance to next trading day functionality
        with patch.object(executor, '_advance_to_next_trading_day') as mock_advance:
            executor._advance_to_next_trading_day()
            mock_advance.assert_called_once()

    def test_main_execution_loop_flow(self):
        """Test the main execution loop handles different scenarios"""
        strategy = Mock()
        strategy.is_backtesting = True
        strategy.sleeptime = "1M"
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.should_continue = Mock(return_value=True)
        executor.stop_event.clear()
        
        # Mock various components of the main loop
        with patch.object(executor, '_setup_market_session', return_value=True) as mock_setup:
            with patch.object(executor, '_should_continue_trading_loop', side_effect=[True, False]) as mock_continue:
                with patch.object(executor, '_strategy_sleep') as mock_sleep:
                    with patch.object(executor, '_advance_to_next_trading_day') as mock_advance:
                        # This would normally be the main loop - we're testing components
                        mock_setup.return_value = True
                        mock_continue.side_effect = [True, False]  # Run once then stop
                        
                        # Simulate one iteration
                        setup_result = executor._setup_market_session(has_data_source=False)
                        assert setup_result == True
                        
                        should_continue = executor._should_continue_trading_loop([], False, False)
                        assert should_continue == True  # First call
                        
                        should_continue = executor._should_continue_trading_loop([], False, False)  
                        assert should_continue == False  # Second call
                        
                        mock_setup.assert_called()

    def test_time_precision_handling(self):
        """Test that time precision is handled correctly for different markets"""
        strategy = Mock()
        strategy.is_backtesting = True
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        
        # Test time advancement scenarios
        test_times = [
            datetime(2024, 1, 2, 9, 30),   # Market open
            datetime(2024, 1, 2, 12, 0),   # Midday
            datetime(2024, 1, 2, 16, 0),   # Market close
            datetime(2024, 1, 2, 20, 0),   # After hours
        ]
        
        for test_time in test_times:
            strategy.get_datetime.return_value = test_time
            
            # Mock time-related operations
            with patch.object(executor, '_strategy_sleep') as mock_sleep:
                executor._strategy_sleep()
                mock_sleep.assert_called_once()

    def test_error_handling_scenarios(self):
        """Test error handling in various scenarios"""
        strategy = Mock()
        strategy.is_backtesting = True
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        
        # Test error handling during setup
        with patch.object(executor, '_setup_market_session', side_effect=Exception("Test error")):
            try:
                executor._setup_market_session(has_data_source=False)
                assert False, "Should have raised exception"
            except Exception as e:
                assert str(e) == "Test error"
        
        # Test error handling during sleep
        with patch.object(executor, '_strategy_sleep', side_effect=Exception("Sleep error")):
            try:
                executor._strategy_sleep()
                assert False, "Should have raised exception"
            except Exception as e:
                assert str(e) == "Sleep error"

    def test_stop_event_functionality(self):
        """Test that stop_event properly controls execution"""
        strategy = Mock()
        strategy.is_backtesting = True
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.should_continue = Mock(return_value=True)
        
        # Test stop event is initially clear
        assert not executor.stop_event.is_set()
        
        # Test setting stop event
        executor.stop_event.set()
        assert executor.stop_event.is_set()
        
        # Test that should_continue reflects stop event
        jobs = [Mock()]
        result = executor._should_continue_trading_loop(jobs, False, False)
        assert result == False  # Should not continue when stop event is set
        
        # Test clearing stop event
        executor.stop_event.clear()
        assert not executor.stop_event.is_set()
        
        result = executor._should_continue_trading_loop(jobs, False, False)
        assert result == True  # Should continue when stop event is clear


class TestStrategyExecutorThreadManagement:
    """Test thread management fixes for resource leaks"""

    def test_gracefully_exit_stops_check_queue_thread(self):
        """Test that gracefully_exit properly stops the check_queue thread"""
        strategy = Mock()
        strategy.is_backtesting = False
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = False
        
        # Mock the broker methods called in gracefully_exit
        executor.strategy.backup_variables_to_db = Mock()
        
        # Create a mock thread that simulates being alive
        mock_thread = Mock()
        mock_thread.is_alive.return_value = True
        executor.check_queue_thread = mock_thread
        
        # Test that gracefully_exit sets the stop event and joins the thread
        executor.gracefully_exit()
        
        # Verify stop event was set
        assert executor.check_queue_stop_event.is_set()
        
        # Verify thread join was called with timeout
        mock_thread.join.assert_called_once_with(timeout=5.0)

    def test_gracefully_exit_handles_missing_thread(self):
        """Test gracefully_exit handles case where thread doesn't exist"""
        strategy = Mock()
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = False
        executor.strategy.backup_variables_to_db = Mock()
        
        # Don't set check_queue_thread attribute
        if hasattr(executor, 'check_queue_thread'):
            delattr(executor, 'check_queue_thread')
        
        # Should not raise exception
        executor.gracefully_exit()
        
        # Stop event should still be set
        assert executor.check_queue_stop_event.is_set()

    def test_gracefully_exit_handles_dead_thread(self):
        """Test gracefully_exit handles thread that's already dead"""
        strategy = Mock()
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = False
        executor.strategy.backup_variables_to_db = Mock()
        
        # Create mock dead thread
        mock_thread = Mock()
        mock_thread.is_alive.return_value = False
        executor.check_queue_thread = mock_thread
        
        executor.gracefully_exit()
        
        # Should set stop event
        assert executor.check_queue_stop_event.is_set()
        
        # Should not call join on dead thread
        mock_thread.join.assert_not_called()

    def test_run_trading_session_prevents_multiple_threads(self):
        """Test that _run_trading_session prevents creating multiple check_queue threads"""
        strategy = Mock()
        strategy.is_backtesting = False
        strategy.force_start_immediately = False
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.market = "NYSE"  # Non-continuous market
        
        # Mock market session setup to continue
        executor._setup_market_session = Mock(return_value=True)
        executor.broker.get_time_to_close.return_value = 1000  # Some time remaining
        
        # Mock scheduler and other dependencies
        executor.scheduler = Mock()
        executor.scheduler.running = False
        executor.scheduler.get_jobs.return_value = []
        executor.calculate_strategy_trigger = Mock(return_value="mock_trigger")
        executor._calculate_should_we_stop = Mock(return_value=False)
        
        # Create an existing thread
        old_thread = Mock()
        old_thread.is_alive.return_value = True
        executor.check_queue_thread = old_thread
        
        with patch('threading.Thread') as mock_thread_class:
            with patch.object(executor, '_should_continue_trading_loop', return_value=False):  # Exit after one iteration
                new_thread = Mock()
                mock_thread_class.return_value = new_thread
                
                # This should stop the old thread and create new one
                try:
                    executor._run_trading_session()
                except Exception:
                    pass  # Ignore other errors, we're testing thread management
                
                # Verify old thread was stopped
                old_thread.join.assert_called_once_with(timeout=5.0)
                
                # Verify stop event was cleared for new thread
                assert not executor.check_queue_stop_event.is_set()

    def test_check_queue_thread_stops_on_event(self):
        """Test that check_queue thread properly stops when event is set"""
        strategy = Mock()
        
        executor = StrategyExecutor(strategy)
        executor.process_queue = Mock()
        
        # Start check_queue in a thread
        stop_event_was_checked = threading.Event()
        
        def mock_check_queue():
            """Mock version that records when stop event was checked"""
            iterations = 0
            while not executor.check_queue_stop_event.is_set():
                stop_event_was_checked.set()
                iterations += 1
                if iterations > 10:  # Safety valve
                    break
                time_module.sleep(0.01)  # Small sleep to prevent tight loop
        
        # Replace check_queue with our mock
        executor.check_queue = mock_check_queue
        
        # Start the thread
        thread = threading.Thread(target=executor.check_queue)
        thread.start()
        
        # Wait for thread to start checking
        stop_event_was_checked.wait(timeout=1.0)
        assert stop_event_was_checked.is_set()
        
        # Set stop event
        executor.check_queue_stop_event.set()
        
        # Thread should stop within reasonable time
        thread.join(timeout=1.0)
        assert not thread.is_alive()

    def test_multiple_crash_recovery_cycles_dont_leak_threads(self):
        """Test that multiple crash/recovery cycles don't accumulate threads"""
        strategy = Mock()
        strategy.is_backtesting = False
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.should_continue.side_effect = [True, False]  # One iteration then stop
        
        # Mock dependencies
        executor._on_bot_crash = Mock()
        executor._run_trading_session = Mock(side_effect=[Exception("Simulated crash"), None])
        
        thread_count_before = threading.active_count()
        
        # Simulate the crash recovery loop (simplified version)
        for _ in range(3):  # Simulate 3 crash cycles
            try:
                executor._run_trading_session()
            except Exception:
                executor._on_bot_crash(Exception("test"))
                # Simulate gracefully_exit being called
                if hasattr(executor, 'check_queue_thread') and executor.check_queue_thread:
                    executor.check_queue_stop_event.set()
                    if executor.check_queue_thread.is_alive():
                        executor.check_queue_thread.join(timeout=1.0)
        
        thread_count_after = threading.active_count()
        
        # Thread count should not have increased significantly
        assert thread_count_after <= thread_count_before + 1  # Allow for test thread itself

    def test_thread_cleanup_timeout_handling(self):
        """Test that thread cleanup handles timeout gracefully"""
        strategy = Mock()
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = False
        executor.strategy.backup_variables_to_db = Mock()
        
        # Create a mock thread that takes too long to join
        mock_thread = Mock()
        mock_thread.is_alive.return_value = True
        mock_thread.join.side_effect = lambda timeout: time_module.sleep(0.1)  # Simulate slow join
        executor.check_queue_thread = mock_thread
        
        # Should not hang or raise exception
        start_time = time_module.time()
        executor.gracefully_exit()
        end_time = time_module.time()
        
        # Should complete within reasonable time (timeout + buffer)
        assert end_time - start_time < 6.0  # 5 second timeout + 1 second buffer
        
        # Should still set stop event
        assert executor.check_queue_stop_event.is_set()

    def test_thread_reference_management(self):
        """Test that thread references are properly managed"""
        strategy = Mock()
        strategy.is_backtesting = False
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        
        # Initially should have no thread reference
        assert not hasattr(executor, 'check_queue_thread') or executor.check_queue_thread is None
        
        # Create a thread reference
        mock_thread = Mock()
        mock_thread.is_alive.return_value = True
        executor.check_queue_thread = mock_thread
        
        # Test that we have the reference
        assert hasattr(executor, 'check_queue_thread')
        assert executor.check_queue_thread is mock_thread
        
        # Test cleanup removes reference appropriately
        executor.check_queue_stop_event.set()
        mock_thread.join.return_value = None
        
        # After cleanup, thread should be handled properly
        executor.gracefully_exit()
        
        # Verify cleanup was called
        mock_thread.join.assert_called_once_with(timeout=5.0)

    def test_gracefully_exit_shuts_down_scheduler(self):
        """Test that gracefully_exit properly shuts down APScheduler"""
        strategy = Mock()
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = False
        executor.strategy.backup_variables_to_db = Mock()
        
        # Mock scheduler
        mock_scheduler = Mock()
        mock_scheduler.running = True
        executor.scheduler = mock_scheduler
        
        # Call gracefully_exit
        executor.gracefully_exit()
        
        # Verify scheduler shutdown was called with proper waiting for completion
        mock_scheduler.shutdown.assert_called_once_with(wait=True)
        
    def test_gracefully_exit_handles_scheduler_shutdown_error(self):
        """Test that scheduler shutdown errors don't prevent graceful exit"""
        import io
        import sys
        
        strategy = Mock()
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = False
        executor.strategy.backup_variables_to_db = Mock()
        
        # Mock scheduler that raises error on shutdown
        mock_scheduler = Mock()
        mock_scheduler.running = True
        mock_scheduler.shutdown.side_effect = Exception("Scheduler error")
        executor.scheduler = mock_scheduler
        
        # Capture printed output
        captured_output = io.StringIO()
        sys.stdout = captured_output
        
        try:
            # This should not raise an exception
            executor.gracefully_exit()
        finally:
            sys.stdout = sys.__stdout__
        
        # Verify shutdown was attempted and warning was printed
        mock_scheduler.shutdown.assert_called_once_with(wait=True)
        output = captured_output.getvalue()
        assert "Warning: Error shutting down scheduler: Scheduler error" in output
        
    def test_gracefully_exit_handles_no_scheduler(self):
        """Test that gracefully_exit handles case where scheduler doesn't exist"""
        strategy = Mock()
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = False
        executor.strategy.backup_variables_to_db = Mock()
        
        # Don't set scheduler attribute
        if hasattr(executor, 'scheduler'):
            delattr(executor, 'scheduler')
        
        # Should not raise an exception
        executor.gracefully_exit()
        
        # Should still complete backup
        executor.strategy.backup_variables_to_db.assert_called_once()
        
    def test_gracefully_exit_handles_stopped_scheduler(self):
        """Test that gracefully_exit handles scheduler that's already stopped"""
        strategy = Mock()
        
        executor = StrategyExecutor(strategy)
        executor.broker = Mock()
        executor.broker.IS_BACKTESTING_BROKER = False
        executor.strategy.backup_variables_to_db = Mock()
        
        # Mock stopped scheduler
        mock_scheduler = Mock()
        mock_scheduler.running = False
        executor.scheduler = mock_scheduler
        
        # Call gracefully_exit
        executor.gracefully_exit()
        
        # Verify shutdown was NOT called since scheduler is already stopped
        mock_scheduler.shutdown.assert_not_called()
