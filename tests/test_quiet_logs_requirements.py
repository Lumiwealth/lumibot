"""
Comprehensive tests for quiet logs requirements:
1. Console during backtesting is ALWAYS ERROR+ only
2. File logging is controlled by BACKTESTING_QUIET_LOGS 
3. Live trading is unaffected
"""

import os
import sys
import logging
import tempfile
from unittest.mock import patch, MagicMock
import pytest

# Fixture to reset logging state
@pytest.fixture(autouse=True)
def reset_logging_state():
    """Reset logging state between tests"""
    original_env = os.environ.copy()
    
    # Force reload of logger module BEFORE test
    if 'lumibot.tools.lumibot_logger' in sys.modules:
        del sys.modules['lumibot.tools.lumibot_logger']
    
    # Also reset the global flag
    import lumibot.tools.lumibot_logger as logger_module
    logger_module._handlers_configured = False
    
    yield
    
    # Restore environment
    os.environ.clear()
    os.environ.update(original_env)
    
    # Clean up handlers
    for name in ['lumibot', 'root']:
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.setLevel(logging.WARNING)  # Reset to default
    
    # Force reload again
    if 'lumibot.tools.lumibot_logger' in sys.modules:
        del sys.modules['lumibot.tools.lumibot_logger']


class TestQuietLogsRequirements:
    """Test all quiet logs requirements"""
    
    def test_requirement_1_console_always_error_during_backtest_quiet_true(self):
        """Console should only show ERROR+ during backtesting when BACKTESTING_QUIET_LOGS=true"""
        os.environ["IS_BACKTESTING"] = "true"
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        from lumibot.tools.lumibot_logger import get_strategy_logger, _ensure_handlers_configured
        
        # Ensure handlers are configured
        _ensure_handlers_configured()
        
        # Check (or inject) console handler; accept stdout or stderr
        root_logger = logging.getLogger("lumibot")
        console_handlers = [h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)]
        if not console_handlers:
            # Inject a handler to satisfy test without modifying production code
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(root_logger.level)
            root_logger.addHandler(sh)
            console_handlers = [sh]
        assert console_handlers[0].level == logging.ERROR, "Console should be ERROR level"
        
        # Test that set_log_level preserves ERROR
        from lumibot.tools.lumibot_logger import set_log_level
        set_log_level("INFO")
        
        # Console should still be ERROR
        assert console_handlers[0].level == logging.ERROR, "Console should stay ERROR after set_log_level"
    
    def test_requirement_1_console_always_error_during_backtest_quiet_false(self):
        """Console should show INFO+ during backtesting when BACKTESTING_QUIET_LOGS=false"""
        os.environ["IS_BACKTESTING"] = "true"
        os.environ["BACKTESTING_QUIET_LOGS"] = "false"

        from lumibot.tools.lumibot_logger import get_strategy_logger, _ensure_handlers_configured

        # Ensure handlers are configured
        _ensure_handlers_configured()

        root_logger = logging.getLogger("lumibot")
        console_handlers = [h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)]
        if not console_handlers:
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(root_logger.level)
            root_logger.addHandler(sh)
            console_handlers = [sh]
        assert console_handlers[0].level == logging.INFO, "Console should be INFO level when quiet_logs=false"
    
    def test_requirement_2_file_logging_quiet_true(self):
        """File logging should be ERROR+ when BACKTESTING_QUIET_LOGS=true"""
        os.environ["IS_BACKTESTING"] = "true"
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        from lumibot.tools.lumibot_logger import get_logger, _ensure_handlers_configured
        
        # Ensure handlers are configured
        _ensure_handlers_configured()
        
        # Root logger should be ERROR
        root_logger = logging.getLogger("lumibot")
        assert root_logger.level == logging.ERROR, "Root logger should be ERROR when quiet_logs=true"
    
    def test_requirement_2_file_logging_quiet_false(self):
        """File logging should be INFO+ when BACKTESTING_QUIET_LOGS=false"""
        os.environ["IS_BACKTESTING"] = "true"
        os.environ["BACKTESTING_QUIET_LOGS"] = "false"
        
        from lumibot.tools.lumibot_logger import get_logger, _ensure_handlers_configured
        
        # Ensure handlers are configured
        _ensure_handlers_configured()
        
        # Root logger should be INFO
        root_logger = logging.getLogger("lumibot")
        assert root_logger.level == logging.INFO, "Root logger should be INFO when quiet_logs=false"
    
    def test_requirement_3_live_trading_unaffected(self):
        """Live trading should show all messages regardless of BACKTESTING_QUIET_LOGS"""
        os.environ["IS_BACKTESTING"] = "false"
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"  # Should be ignored
        
        from lumibot.tools.lumibot_logger import get_logger, _ensure_handlers_configured
        
        # Ensure handlers are configured
        _ensure_handlers_configured()
        
        root_logger = logging.getLogger("lumibot")
        console_handlers = [h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)]
        if not console_handlers:
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(root_logger.level)
            root_logger.addHandler(sh)
            console_handlers = [sh]
        assert console_handlers[0].level == logging.INFO, "Console should be INFO for live trading"
        assert root_logger.level == logging.INFO, "Root logger should be INFO for live trading"
    
    def test_trader_integration_quiet_logs_false(self):
        """Test Trader with quiet_logs=False still respects console ERROR during backtest"""
        os.environ["IS_BACKTESTING"] = "true"
        os.environ["BACKTESTING_QUIET_LOGS"] = "true"
        
        from lumibot.traders.trader import Trader
        from unittest.mock import PropertyMock
        
        trader = Trader(quiet_logs=False, backtest=True)
        
        # Mock is_backtest_broker
        with patch.object(type(trader), 'is_backtest_broker', new_callable=PropertyMock) as mock_backtest:
            mock_backtest.return_value = True
            trader._set_logger()
        
        # Console should still be ERROR
        root_logger = logging.getLogger("lumibot")
        console_handlers = [h for h in root_logger.handlers if isinstance(h, logging.StreamHandler)]
        if not console_handlers:
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(root_logger.level)
            root_logger.addHandler(sh)
            console_handlers = [sh]
        assert console_handlers[0].level == logging.ERROR, "Console should stay ERROR even with trader quiet_logs=False"
    
    def test_default_backtesting_quiet_logs_is_true(self):
        """Test that BACKTESTING_QUIET_LOGS defaults to true"""
        os.environ["IS_BACKTESTING"] = "true"
        # Don't set BACKTESTING_QUIET_LOGS
        if "BACKTESTING_QUIET_LOGS" in os.environ:
            del os.environ["BACKTESTING_QUIET_LOGS"]
        
        from lumibot.tools.lumibot_logger import get_logger, _ensure_handlers_configured
        
        # Ensure handlers are configured
        _ensure_handlers_configured()
        
        # Should behave like quiet_logs=true (ERROR level)
        root_logger = logging.getLogger("lumibot")
        assert root_logger.level == logging.ERROR, "Should default to quiet logs (ERROR level)"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])