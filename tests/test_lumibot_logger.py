import re
import logging
import tempfile
import os
from unittest.mock import patch
from lumibot.tools import lumibot_logger

def test_unified_logger_includes_source_context(caplog):
    logger = lumibot_logger.get_logger(__name__)
    with caplog.at_level(logging.WARNING):
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
        
        # Read the file contents
        with open(tmp_name, 'r') as f:
            contents = f.read()
        
        assert "File handler info test" in contents
        assert "| INFO |" in contents
        assert "lumibot.tests.test_lumibot_logger" in contents
    finally:
        # Clean up
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)

def test_quiet_logs_functionality():
    """Test that quiet logs work correctly through BACKTESTING_QUIET_LOGS env var and Trader"""
    # Test via environment variable
    with patch.dict(os.environ, {'BACKTESTING_QUIET_LOGS': 'true'}):
        # Reset logger state to pick up environment variable
        lumibot_logger._handlers_configured = False
        lumibot_logger._ensure_handlers_configured()
        
        # Check that the global log level is set to ERROR
        root_logger = logging.getLogger("lumibot")
        assert root_logger.level == logging.ERROR
        
        console_handlers = [h for h in root_logger.handlers 
                           if isinstance(h, logging.StreamHandler)]
        assert len(console_handlers) > 0
        assert console_handlers[0].level == logging.ERROR

def test_trader_quiet_logs_integration():
    """Test that Trader's quiet_logs parameter integrates with unified logger"""
    from lumibot.traders.trader import Trader
    
    # Create trader with quiet_logs=True
    trader = Trader(quiet_logs=True, backtest=True)
    
    # The trader's _set_logger should have called set_log_level("ERROR")
    root_logger = logging.getLogger("lumibot")
    assert root_logger.level == logging.ERROR
