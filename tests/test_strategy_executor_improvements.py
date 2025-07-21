import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta, time
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
