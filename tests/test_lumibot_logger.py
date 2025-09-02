import re
import logging
import tempfile
import os
from unittest.mock import patch
import pytest
from lumibot.tools import lumibot_logger

@pytest.fixture(autouse=True)
def reset_logging_state():
    """Fixture to save and restore logging state between tests."""
    # Save original state
    original_logger_level = logging.getLogger("lumibot").level
    original_handler_levels = {}
    lumibot_logger_root = logging.getLogger("lumibot")
    
    for handler in lumibot_logger_root.handlers:
        original_handler_levels[handler] = handler.level
    
    # Save environment variables
    original_env = {
        'BACKTESTING_QUIET_LOGS': os.environ.get('BACKTESTING_QUIET_LOGS'),
        'IS_BACKTESTING': os.environ.get('IS_BACKTESTING'),
        'LUMIBOT_LOG_LEVEL': os.environ.get('LUMIBOT_LOG_LEVEL')
    }
    
    yield
    
    # Restore logging state
    logging.getLogger("lumibot").setLevel(original_logger_level)
    for handler, level in original_handler_levels.items():
        if handler in lumibot_logger_root.handlers:  # Check if handler still exists
            handler.setLevel(level)
    
    # Restore environment variables
    for key, value in original_env.items():
        if value is None:
            if key in os.environ:
                del os.environ[key]
        else:
            os.environ[key] = value
    
    # Reset the handlers configured flag to pick up changes
    lumibot_logger._handlers_configured = False
    lumibot_logger._ensure_handlers_configured()

def test_unified_logger_includes_source_context(caplog):
    logger = lumibot_logger.get_logger(__name__)
    with caplog.at_level(logging.WARNING, logger="lumibot"):
        logger.warning("Test warning message")
    # Should include filename and line number
    found = False
    for record in caplog.records:
        if record.levelno == logging.WARNING and "Test warning message" in record.getMessage():
            # Check for source context in the log output - format is "filename:line"
            assert re.search(r"\w+\.py:\d+", caplog.text), caplog.text
            found = True
    assert found, "Warning log not found in caplog"

def test_unified_logger_info_includes_module_context(caplog):
    logger = lumibot_logger.get_logger(__name__)
    with caplog.at_level(logging.INFO):
        logger.info("Test info message")
    found = False
    for record in caplog.records:
        if record.levelno == logging.INFO and "Test info message" in record.getMessage():
            # Should include the module name in the log output
            assert "lumibot.tests.test_lumibot_logger" in caplog.text, caplog.text
            found = True
    assert found, "Info log not found in caplog"

def test_file_handler_uses_lumibot_formatter():
    import os
    logger = lumibot_logger.get_logger(__name__)
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        tmp_name = tmp.name
    
    try:
        lumibot_logger.add_file_handler(tmp_name, level="INFO")
        logger.info("File handler info test")
        
        # Force flush all handlers
        for handler in logging.getLogger("lumibot").handlers:
            handler.flush()

        # Remove and close all handlers that use the temp file before reading/deleting
        logger_obj = logging.getLogger("lumibot")
        for handler in logger_obj.handlers[:]:
            if hasattr(handler, "baseFilename") and handler.baseFilename == tmp_name:
                handler.close()
                logger_obj.removeHandler(handler)
        
        # Read the file contents
        with open(tmp_name, 'r') as f:
            contents = f.read()
        
        assert "File handler info test" in contents
        assert "| INFO |" in contents
    finally:
        # Clean up
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)

def test_quiet_logs_functionality():
    """Test that quiet logs work correctly through BACKTESTING_QUIET_LOGS env var and Trader"""
    # Test via environment variable during backtesting
    with patch.dict(os.environ, {'BACKTESTING_QUIET_LOGS': 'true', 'IS_BACKTESTING': 'true'}):
        # Reset logger state to pick up environment variable
        lumibot_logger._handlers_configured = False
        lumibot_logger._ensure_handlers_configured()
        
        # Check that the global log level is set to ERROR during backtesting
        root_logger = logging.getLogger("lumibot")
        assert root_logger.level == logging.ERROR
        
        console_handlers = [h for h in root_logger.handlers 
                           if isinstance(h, logging.StreamHandler)]
        assert len(console_handlers) > 0
        assert console_handlers[0].level == logging.ERROR

def test_trader_quiet_logs_integration():
    """Test that Trader's quiet_logs parameter integrates with unified logger"""
    from lumibot.traders.trader import Trader
    from unittest.mock import PropertyMock
    
    # Set environment to simulate backtesting
    with patch.dict(os.environ, {'IS_BACKTESTING': 'true'}):
        # Create trader with quiet_logs=True
        trader = Trader(quiet_logs=True, backtest=True)
        
        # Mock the is_backtest_broker property to return True
        with patch.object(type(trader), 'is_backtest_broker', new_callable=PropertyMock) as mock_is_backtest:
            mock_is_backtest.return_value = True
            
            # Call _set_logger which should now treat this as a backtesting trader
            trader._set_logger()
            
            # With quiet_logs=True and backtesting broker, log level should be ERROR
            root_logger = logging.getLogger("lumibot")
            assert root_logger.level == logging.ERROR
