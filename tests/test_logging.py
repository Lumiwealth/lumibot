import datetime
import logging
import pytest
import os
import sys

from lumibot.example_strategies.lifecycle_logger import LifecycleLogger
from lumibot.backtesting import YahooDataBacktesting


class TestLogging:

    def test_logging(self, caplog):
        caplog.set_level(logging.INFO)
        logger = logging.getLogger()
        logger.info("This is an info message")
        assert "This is an info message" in caplog.text

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_backtest_produces_no_logs_when_quiet_logs_is_true(self, caplog):
        caplog.set_level(logging.INFO)
        backtesting_start = datetime.datetime(2023, 1, 2)
        backtesting_end = datetime.datetime(2023, 1, 4)

        LifecycleLogger.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            parameters={"sleeptime": "1D", "market": "NYSE"},
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=True
        )
        # count that this contains 3 new lines. Its an easy proxy for the number of log messages and avoids
        # the issue where the datetime is always gonna be different.
        assert caplog.text.count("\n") == 4
        assert "Starting backtest...\n" in caplog.text
        assert "Backtesting starting...\n" in caplog.text
        assert "Backtesting finished\n" in caplog.text
        assert "Backtest took " in caplog.text

    def test_backtesting_quiet_logs_environment_variable(self):
        """Test that BACKTESTING_QUIET_LOGS environment variable sets ERROR level."""
        # Store original environment
        original_quiet = os.environ.get("BACKTESTING_QUIET_LOGS")
        original_log_level = os.environ.get("LUMIBOT_LOG_LEVEL")
        
        try:
            # Test with BACKTESTING_QUIET_LOGS=true
            os.environ["BACKTESTING_QUIET_LOGS"] = "true"
            if "LUMIBOT_LOG_LEVEL" in os.environ:
                del os.environ["LUMIBOT_LOG_LEVEL"]
            
            # Clear lumibot logger module cache to force re-initialization
            modules_to_clear = [mod for mod in list(sys.modules.keys()) if 'lumibot.tools.lumibot_logger' in mod]
            for mod in modules_to_clear:
                del sys.modules[mod]
            
            # Import fresh logger instance
            from lumibot.tools.lumibot_logger import get_logger
            
            logger = get_logger("test_quiet_logs")
            
            # Check that the root lumibot logger is set to ERROR level
            root_logger = logging.getLogger("lumibot")
            assert root_logger.level == logging.ERROR, f"Expected ERROR level ({logging.ERROR}), got {root_logger.level}"
            
            # Check that console handlers are also set to ERROR level
            stream_handlers = [h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)]
            for handler in stream_handlers:
                assert handler.level == logging.ERROR, f"Console handler should be ERROR level, got {handler.level}"
                
        finally:
            # Restore original environment
            if original_quiet is not None:
                os.environ["BACKTESTING_QUIET_LOGS"] = original_quiet
            elif "BACKTESTING_QUIET_LOGS" in os.environ:
                del os.environ["BACKTESTING_QUIET_LOGS"]
                
            if original_log_level is not None:
                os.environ["LUMIBOT_LOG_LEVEL"] = original_log_level
            elif "LUMIBOT_LOG_LEVEL" in os.environ:
                del os.environ["LUMIBOT_LOG_LEVEL"]
    
    def test_backtesting_quiet_logs_false(self):
        """Test that BACKTESTING_QUIET_LOGS=false uses normal log level."""
        # Store original environment
        original_quiet = os.environ.get("BACKTESTING_QUIET_LOGS")
        original_log_level = os.environ.get("LUMIBOT_LOG_LEVEL")
        
        try:
            # Test with BACKTESTING_QUIET_LOGS=false
            os.environ["BACKTESTING_QUIET_LOGS"] = "false"
            if "LUMIBOT_LOG_LEVEL" in os.environ:
                del os.environ["LUMIBOT_LOG_LEVEL"]
            
            # Clear lumibot logger module cache to force re-initialization
            modules_to_clear = [mod for mod in list(sys.modules.keys()) if 'lumibot.tools.lumibot_logger' in mod]
            for mod in modules_to_clear:
                del sys.modules[mod]
            
            # Import fresh logger instance
            from lumibot.tools.lumibot_logger import get_logger
            
            logger = get_logger("test_normal_logs")
            
            # Check that the root lumibot logger is at default INFO level
            root_logger = logging.getLogger("lumibot")
            assert root_logger.level == logging.INFO, f"Expected INFO level ({logging.INFO}), got {root_logger.level}"
                
        finally:
            # Restore original environment
            if original_quiet is not None:
                os.environ["BACKTESTING_QUIET_LOGS"] = original_quiet
            elif "BACKTESTING_QUIET_LOGS" in os.environ:
                del os.environ["BACKTESTING_QUIET_LOGS"]
                
            if original_log_level is not None:
                os.environ["LUMIBOT_LOG_LEVEL"] = original_log_level
            elif "LUMIBOT_LOG_LEVEL" in os.environ:
                del os.environ["LUMIBOT_LOG_LEVEL"]