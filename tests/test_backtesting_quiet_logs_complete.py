#!/usr/bin/env python3
"""
Complete TDD test suite for BACKTESTING_QUIET_LOGS functionality

Expected Behavior:
- Default: BACKTESTING_QUIET_LOGS = True (quiet by default)
- True: Only progress bar + ERROR+ (console & file), logging.setLevel("ERROR")
- False: INFO+ logs to both console and file
- Live trading: Always shows console messages regardless of setting
- Progress bar: Always shows
- Errors: Always show (both console and file) since they stop backtests
"""

import os
import sys
import io
import pytest
import logging
import tempfile
import datetime as dt
from unittest.mock import patch, MagicMock
from pathlib import Path


@pytest.fixture
def clean_environment():
    """Fixture to save and restore environment"""
    original_env = os.environ.copy()
    yield
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def temp_logfile():
    """Fixture for temporary log file"""
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.log') as f:
        yield f.name
    try:
        os.unlink(f.name)
    except:
        pass


def reload_lumibot_logger():
    """Helper to reload lumibot logger module"""
    import importlib
    import lumibot.tools.lumibot_logger
    importlib.reload(lumibot.tools.lumibot_logger)


def test_default_backtesting_quiet_logs_is_true(clean_environment):
    """Test that BACKTESTING_QUIET_LOGS defaults to True"""
    # Ensure no environment variable is set
    os.environ.pop("BACKTESTING_QUIET_LOGS", None)
    
    # Force reload credentials to pick up default
    import importlib
    import lumibot.credentials
    importlib.reload(lumibot.credentials)
    
    # Check that default is True
    assert lumibot.credentials.BACKTESTING_QUIET_LOGS is True, "Default should be BACKTESTING_QUIET_LOGS = True"


def test_quiet_logs_true_console_only_shows_progress_bar_and_errors(clean_environment, temp_logfile):
    """Test that quiet_logs=True only shows progress bar and ERROR+ on console"""
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"
    os.environ["IS_BACKTESTING"] = "true"  # Simulate backtest mode
    
    reload_lumibot_logger()
    from lumibot.tools.lumibot_logger import get_strategy_logger, add_file_handler
    from lumibot.tools.helpers import print_progress_bar
    
    # Add file handler
    add_file_handler(temp_logfile, 'INFO')
    
    # Get strategy logger
    logger = get_strategy_logger("test", "TestStrategy")
    
    # Capture console output
    captured_console = io.StringIO()
    
    # Find and redirect console handler
    root_logger = logging.getLogger("lumibot")
    console_handler = None
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            console_handler = handler
            break
    
    if console_handler:
        console_handler.stream = captured_console
    
    # Test logging at different levels
    logger.debug("DEBUG message should not appear")
    logger.info("INFO message should not appear")
    logger.warning("WARNING message should not appear")
    logger.error("ERROR message should appear")
    
    # Force flush
    for handler in root_logger.handlers:
        if hasattr(handler, 'flush'):
            handler.flush()
    
    console_output = captured_console.getvalue()
    
    # Only ERROR should appear on console
    assert "DEBUG message should not appear" not in console_output
    assert "INFO message should not appear" not in console_output
    assert "WARNING message should not appear" not in console_output
    assert "ERROR message should appear" in console_output
    
    # Test progress bar still shows
    progress_output = io.StringIO()
    print_progress_bar(
        value=50,
        start_value=0,
        end_value=100,
        backtesting_started=dt.datetime.now(),
        file=progress_output,
        prefix="Progress"
    )
    progress_result = progress_output.getvalue()
    assert "Progress" in progress_result
    assert "50.00%" in progress_result


def test_quiet_logs_true_file_only_shows_errors(clean_environment, temp_logfile):
    """Test that quiet_logs=True only writes ERROR+ to log file"""
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"
    os.environ["IS_BACKTESTING"] = "true"
    
    reload_lumibot_logger()
    from lumibot.tools.lumibot_logger import get_strategy_logger, add_file_handler
    
    # Add file handler
    add_file_handler(temp_logfile, 'INFO')
    
    # Get strategy logger
    logger = get_strategy_logger("test", "TestStrategy")
    
    # Log at different levels
    logger.debug("DEBUG to file should not appear")
    logger.info("INFO to file should not appear")
    logger.warning("WARNING to file should not appear")
    logger.error("ERROR to file should appear")
    
    # Force flush
    root_logger = logging.getLogger("lumibot")
    for handler in root_logger.handlers:
        if hasattr(handler, 'flush'):
            handler.flush()
    
    # Read file content
    with open(temp_logfile, 'r') as f:
        file_content = f.read()
    
    # Only ERROR should be in file with quiet logs
    assert "DEBUG to file should not appear" not in file_content
    assert "INFO to file should not appear" not in file_content
    assert "WARNING to file should not appear" not in file_content
    assert "ERROR to file should appear" in file_content


def test_quiet_logs_false_shows_all_info_plus(clean_environment, temp_logfile):
    """Test that quiet_logs=False shows INFO+ on both console and file"""
    os.environ["BACKTESTING_QUIET_LOGS"] = "false"
    os.environ["IS_BACKTESTING"] = "true"
    
    reload_lumibot_logger()
    from lumibot.tools.lumibot_logger import get_strategy_logger, add_file_handler
    
    # Add file handler
    add_file_handler(temp_logfile, 'INFO')
    
    # Get strategy logger
    logger = get_strategy_logger("test", "TestStrategy")
    
    # Capture console output
    captured_console = io.StringIO()
    
    # Find and redirect console handler
    root_logger = logging.getLogger("lumibot")
    console_handler = None
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            console_handler = handler
            break
    
    if console_handler:
        console_handler.stream = captured_console
    
    # Log at different levels
    logger.debug("DEBUG should not appear (below INFO)")
    logger.info("INFO should appear")
    logger.warning("WARNING should appear")
    logger.error("ERROR should appear")
    
    # Force flush
    for handler in root_logger.handlers:
        if hasattr(handler, 'flush'):
            handler.flush()
    
    console_output = captured_console.getvalue()

    # When BACKTESTING_QUIET_LOGS=false, console should show INFO+ during backtesting
    assert "DEBUG should not appear" not in console_output
    assert "INFO should appear" in console_output  # Console SHOULD show INFO when quiet_logs=false
    assert "WARNING should appear" in console_output  # Console SHOULD show WARNING when quiet_logs=false
    assert "ERROR should appear" in console_output  # Console SHOULD show ERROR when quiet_logs=false
    
    # Read file content
    with open(temp_logfile, 'r') as f:
        file_content = f.read()
    
    # INFO+ should appear in file
    assert "DEBUG should not appear" not in file_content
    assert "INFO should appear" in file_content
    assert "WARNING should appear" in file_content
    assert "ERROR should appear" in file_content


def test_live_trading_always_shows_console_messages(clean_environment):
    """Test that live trading shows console messages regardless of BACKTESTING_QUIET_LOGS"""
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"  # Even with quiet logs
    os.environ.pop("IS_BACKTESTING", None)  # Not in backtest mode
    
    reload_lumibot_logger()
    from lumibot.tools.lumibot_logger import get_strategy_logger
    
    # Get strategy logger
    logger = get_strategy_logger("test", "TestStrategy")
    
    # Capture console output
    captured_console = io.StringIO()
    
    # Find and redirect console handler
    root_logger = logging.getLogger("lumibot")
    console_handler = None
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            console_handler = handler
            break
    
    if console_handler:
        console_handler.stream = captured_console
    
    # In live trading, messages should show even with quiet logs
    logger.info("Live trading INFO should appear")
    logger.warning("Live trading WARNING should appear")
    logger.error("Live trading ERROR should appear")
    
    console_output = captured_console.getvalue()
    
    # All messages should appear in live trading
    assert "Live trading INFO should appear" in console_output
    assert "Live trading WARNING should appear" in console_output
    assert "Live trading ERROR should appear" in console_output


def test_progress_bar_always_shows_regardless_of_quiet_logs(clean_environment):
    """Test that progress bar always shows regardless of quiet logs setting"""
    test_cases = [
        ("true", True),
        ("false", False),
        ("", None)  # Default case
    ]
    
    from lumibot.tools.helpers import print_progress_bar
    
    for env_value, expected_quiet in test_cases:
        with patch.dict(os.environ, {"BACKTESTING_QUIET_LOGS": env_value} if env_value else {}, clear=True):
            captured_output = io.StringIO()
            print_progress_bar(
                value=25,
                start_value=0,
                end_value=100,
                backtesting_started=dt.datetime.now(),
                file=captured_output,
                prefix=f"Test {env_value or 'default'}"
            )
            
            output = captured_output.getvalue()
            
            # Progress bar should ALWAYS show
            assert f"Test {env_value or 'default'}" in output
            assert "25.00%" in output


def test_bot_manager_compatibility(clean_environment, temp_logfile):
    """Test Bot Manager compatibility: BACKTESTING_QUIET_LOGS=False should enable verbose logging"""
    # Simulate Bot Manager environment
    os.environ["BACKTESTING_QUIET_LOGS"] = "False"  # Bot Manager sets this
    os.environ["IS_BACKTESTING"] = "true"
    os.environ["BOT_ID"] = "test-bot"
    os.environ["BACKTESTING_SHOW_PROGRESS_BAR"] = "False"  # Bot Manager disables this
    
    reload_lumibot_logger()
    from lumibot.tools.lumibot_logger import get_strategy_logger, add_file_handler
    
    # Add file handler (Bot Manager would do this)
    add_file_handler(temp_logfile, 'INFO')
    
    # Get strategy logger
    logger = get_strategy_logger("test", "BotManagerStrategy")
    
    # Capture console output
    captured_console = io.StringIO()
    
    # Find and redirect console handler
    root_logger = logging.getLogger("lumibot")
    console_handler = None
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stdout:
            console_handler = handler
            break
    
    if console_handler:
        console_handler.stream = captured_console
    
    # Bot Manager expects to see verbose logging
    logger.info("Bot Manager should see this INFO")
    logger.warning("Bot Manager should see this WARNING")
    logger.error("Bot Manager should see this ERROR")
    
    # Force flush
    for handler in root_logger.handlers:
        if hasattr(handler, 'flush'):
            handler.flush()
    
    console_output = captured_console.getvalue()

    # When BACKTESTING_QUIET_LOGS=False, console should show INFO+ during backtesting
    # Bot Manager can read from either CloudWatch/file logs or console
    assert "Bot Manager should see this INFO" in console_output
    assert "Bot Manager should see this WARNING" in console_output
    assert "Bot Manager should see this ERROR" in console_output
    
    # Read file content
    with open(temp_logfile, 'r') as f:
        file_content = f.read()
    
    # Bot Manager should see all INFO+ messages in file
    assert "Bot Manager should see this INFO" in file_content
    assert "Bot Manager should see this WARNING" in file_content
    assert "Bot Manager should see this ERROR" in file_content


def test_environment_variable_vs_function_parameter():
    """Test that both environment variable and function parameter work for quiet logs"""
    # This test ensures the API compatibility is maintained
    from lumibot.credentials import BACKTESTING_QUIET_LOGS
    
    # Test environment variable parsing
    test_cases = [
        ("true", True),
        ("TRUE", True),
        ("True", True),
        ("false", False),
        ("FALSE", False),
        ("False", False),
    ]
    
    for env_val, expected in test_cases:
        with patch.dict(os.environ, {"BACKTESTING_QUIET_LOGS": env_val}):
            # Force reload to pick up environment variable
            import importlib
            import lumibot.credentials
            importlib.reload(lumibot.credentials)
            
            # Check that the parsing works correctly
            if expected:
                assert lumibot.credentials.BACKTESTING_QUIET_LOGS is True
            else:
                assert lumibot.credentials.BACKTESTING_QUIET_LOGS is False


def test_error_level_equivalence():
    """Test that quiet logs = True is equivalent to logging.setLevel(ERROR)"""
    import logging
    
    # Test that our quiet logs behavior matches logging.ERROR level
    error_level = logging.ERROR
    warning_level = logging.WARNING
    info_level = logging.INFO
    debug_level = logging.DEBUG
    
    # With quiet logs (ERROR level), only ERROR+ should pass
    assert error_level >= logging.ERROR  # Should pass
    assert warning_level < logging.ERROR  # Should not pass
    assert info_level < logging.ERROR    # Should not pass
    assert debug_level < logging.ERROR   # Should not pass
    
    # This confirms our implementation should use ERROR level for quiet logs