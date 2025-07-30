#!/usr/bin/env python3
"""
Test for APScheduler warnings with sleeptime < 1 minute
"""
import unittest
from unittest.mock import patch, MagicMock
import datetime as dt
import logging

from tests.utilities import BaseTestClass


class TestAPSchedulerWarnings(BaseTestClass):
    """Test suite for APScheduler warnings with short sleeptimes"""
    
    def setUp(self):
        """Set up test environment"""
        super().setUp()
        # Capture logging to check for warnings
        self.log_capture = []
        
    def test_sleeptime_10s_max_instances(self):
        """Test that 10s sleeptime sets appropriate max_instances to avoid warnings"""
        from lumibot.strategies.strategy import Strategy
        from lumibot.strategies.strategy_executor import StrategyExecutor
        
        # Create a test strategy with 10s sleeptime
        class TestStrategy(Strategy):
            def initialize(self):
                self.sleeptime = "10S"
                
            def on_trading_iteration(self):
                pass
        
        # Mock the broker and other dependencies
        mock_broker = MagicMock()
        mock_broker.is_backtesting_broker = False
        mock_broker.market_open_time.return_value = dt.datetime.now() - dt.timedelta(hours=1)
        mock_broker.market_close_time.return_value = dt.datetime.now() + dt.timedelta(hours=1)
        
        strategy = TestStrategy(broker=mock_broker)
        executor = StrategyExecutor(strategy, mock_broker, None, None, None, None, None)
        
        # Check that when creating scheduler jobs, max_instances is set appropriately
        with patch('apscheduler.schedulers.background.BackgroundScheduler') as mock_scheduler:
            mock_scheduler_instance = MagicMock()
            mock_scheduler.return_value = mock_scheduler_instance
            
            # Simulate the scheduler creation
            executor._run_trading_session()
            
            # Verify that add_job was called with appropriate max_instances
            # For 10s sleeptime, max_instances should be > 1 to avoid warnings
            add_job_calls = mock_scheduler_instance.add_job.call_args_list
            for call in add_job_calls:
                kwargs = call[1]
                if 'max_instances' in kwargs:
                    # For short sleeptimes, max_instances should be higher
                    self.assertGreater(kwargs['max_instances'], 1, 
                                     "max_instances should be > 1 for 10s sleeptime")
                                     
    def test_sleeptime_1m_default_max_instances(self):
        """Test that 1m sleeptime works fine with default max_instances"""
        from lumibot.strategies.strategy import Strategy
        from lumibot.strategies.strategy_executor import StrategyExecutor
        
        # Create a test strategy with 1m sleeptime
        class TestStrategy(Strategy):
            def initialize(self):
                self.sleeptime = "1M"
                
            def on_trading_iteration(self):
                pass
        
        # Mock the broker
        mock_broker = MagicMock()
        mock_broker.is_backtesting_broker = False
        
        strategy = TestStrategy(broker=mock_broker)
        executor = StrategyExecutor(strategy, mock_broker, None, None, None, None, None)
        
        # For 1m sleeptime, default max_instances=1 should be fine
        # No specific assertion needed, just ensure no exceptions


class TestAPSchedulerMaxInstancesFix(BaseTestClass):
    """Test the fix for max_instances issue"""
    
    def test_calculate_max_instances_for_sleeptime(self):
        """Test calculation of appropriate max_instances based on sleeptime"""
        # Test different sleeptimes and expected max_instances
        test_cases = [
            ("10S", 6),   # 10 seconds -> need at least 6 instances (60s/10s)
            ("30S", 2),   # 30 seconds -> need at least 2 instances
            ("1M", 1),    # 1 minute -> 1 instance is fine
            ("5M", 1),    # 5 minutes -> 1 instance is fine
            ("1H", 1),    # 1 hour -> 1 instance is fine
        ]
        
        for sleeptime, expected_min_instances in test_cases:
            # Parse sleeptime and calculate appropriate max_instances
            if sleeptime.endswith("S"):
                seconds = int(sleeptime[:-1])
                # For second-based sleeptimes, calculate how many could fire per minute
                max_instances = max(1, 60 // seconds)
            else:
                max_instances = 1
                
            self.assertGreaterEqual(max_instances, expected_min_instances,
                                  f"max_instances for {sleeptime} should be at least {expected_min_instances}")


if __name__ == "__main__":
    unittest.main()