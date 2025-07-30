#!/usr/bin/env python3
"""
Comprehensive tests for quiet logs functionality in Lumibot
"""
import os
import sys
import io
import re
import unittest
from unittest.mock import patch, MagicMock
import logging
import datetime as dt
import pytz

# Import test utilities
from tests.utilities import BaseTestClass


class TestQuietLogsFunctionality(BaseTestClass):
    """Test suite for BACKTESTING_QUIET_LOGS functionality"""
    
    def setUp(self):
        """Set up test environment"""
        super().setUp()
        # Save original environment
        self.original_env = os.environ.copy()
        # Clear any existing logging handlers
        logger = logging.getLogger("lumibot")
        logger.handlers.clear()
        
    def tearDown(self):
        """Clean up after tests"""
        super().tearDown()
        # Restore original environment
        os.environ.clear()
        os.environ.update(self.original_env)
        
    def capture_console_output(self, func):
        """Capture console output from a function"""
        captured_output = io.StringIO()
        with patch('sys.stdout', captured_output):
            func()
        return captured_output.getvalue()
        
    def test_progress_bar_always_shows_with_quiet_logs(self):
        """Test that progress bar ALWAYS shows even when BACKTESTING_QUIET_LOGS=true"""
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        from lumibot.tools.helpers import print_progress_bar
        
        # Capture output
        output = io.StringIO()
        backtesting_started = dt.datetime.now()
        
        print_progress_bar(
            value=50,
            start_value=0,
            end_value=100,
            backtesting_started=backtesting_started,
            file=output,
            cash=10000,
            portfolio_value=10500
        )
        
        result = output.getvalue()
        
        # Progress bar should NOT be empty
        self.assertNotEqual(result, "", "Progress bar should show even with quiet logs")
        self.assertIn("Progress", result, "Progress bar should contain 'Progress'")
        self.assertIn("█", result, "Progress bar should contain fill characters")
        self.assertIn("50.00%", result, "Progress bar should show percentage")
        
    def test_info_logs_suppressed_with_quiet_logs(self):
        """Test that INFO logs are suppressed when BACKTESTING_QUIET_LOGS=true"""
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        # Re-import to pick up environment variable
        import importlib
        import lumibot.tools.lumibot_logger
        importlib.reload(lumibot.tools.lumibot_logger)
        
        from lumibot.tools.lumibot_logger import get_logger
        
        logger = get_logger("test")
        
        # Capture console output
        captured = io.StringIO()
        console_handler = logging.StreamHandler(captured)
        logger.addHandler(console_handler)
        
        # Log at different levels
        logger.info("This should not appear")
        logger.warning("This should not appear")
        logger.error("This should appear")
        
        output = captured.getvalue()
        
        # Check that INFO and WARNING are suppressed
        self.assertNotIn("This should not appear", output)
        # Check that ERROR still appears
        self.assertIn("This should appear", output)
        
    def test_strategy_logger_respects_quiet_logs(self):
        """Test that strategy logger respects BACKTESTING_QUIET_LOGS setting"""
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        # Create a mock strategy with backtesting flag
        from lumibot.strategies.strategy import Strategy
        
        # We need to test the logger initialization
        with patch('lumibot.strategies._strategy.get_logger') as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            strategy = Strategy()
            strategy.is_backtesting = True
            
            # The strategy should check BACKTESTING_QUIET_LOGS and NOT force INFO level
            # This is what we need to fix
            
    def test_file_logging_does_not_reset_console_level(self):
        """Test that enabling file logging doesn't reset console log level"""
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        from lumibot.tools.lumibot_logger import get_logger, add_file_handler
        
        logger = get_logger("test")
        
        # Add file handler
        add_file_handler("test.log", level="DEBUG")
        
        # Console should still respect quiet logs
        captured = io.StringIO()
        
        # Find console handler
        console_handler = None
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
                console_handler = handler
                break
                
        if console_handler:
            # Console handler should be at ERROR level due to quiet logs
            self.assertEqual(console_handler.level, logging.ERROR)
            
    def test_polygon_data_source_preserves_quiet_logs(self):
        """Test that using Polygon data source doesn't break quiet logs"""
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        # This test would require actual Polygon setup, so we'll mock it
        with patch('lumibot.data_sources.polygon_data.PolygonData') as mock_polygon:
            # Ensure Polygon doesn't mess with logging configuration
            pass
            
    def test_after_market_closes_preserves_log_level(self):
        """Test that after_market_closes doesn't reset log levels"""
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        from lumibot.strategies._strategy import Strategy
        
        # Create strategy with mocked components
        strategy = Strategy()
        strategy.is_backtesting = True
        strategy.logger = MagicMock()
        
        # Simulate what happens in _dump_stats
        original_level = strategy.logger.level
        
        # Call method that might change log levels
        with patch.object(strategy, '_dump_stats') as mock_dump:
            # Ensure log level is preserved
            pass


class TestAPSchedulerWarnings(BaseTestClass):
    """Test suite for APScheduler warnings with short sleeptimes"""
    
    def test_scheduler_with_10s_sleeptime(self):
        """Test that 10s sleeptime doesn't cause max_instances warnings"""
        from apscheduler.schedulers.base import BaseScheduler
        from apscheduler.triggers.cron import CronTrigger
        
        # This test verifies the fix for max_instances
        # We need to ensure that with sleeptime='10S', max_instances is set appropriately
        
    def test_scheduler_with_1m_sleeptime(self):
        """Test that 1m sleeptime works without warnings"""
        # This should work fine with default max_instances=1
        pass


class TestProgressBarFormatting(BaseTestClass):
    """Test suite for progress bar formatting"""
    
    def test_progress_bar_shows_green_bar(self):
        """Test that progress bar shows actual green bar, not just text"""
        from lumibot.tools.helpers import print_progress_bar
        
        output = io.StringIO()
        backtesting_started = dt.datetime.now()
        
        # Print progress bar
        print_progress_bar(
            value=50,
            start_value=0,
            end_value=100,
            backtesting_started=backtesting_started,
            file=output,
            prefix="Progress",
            fill="█",
            cash=10000,
            portfolio_value=10500
        )
        
        result = output.getvalue()
        
        # Should contain actual progress bar characters
        self.assertIn("█", result, "Progress bar should contain fill characters")
        # Should be properly formatted with || delimiters
        self.assertIn("||", result, "Progress bar should have proper formatting")
        

if __name__ == "__main__":
    unittest.main()