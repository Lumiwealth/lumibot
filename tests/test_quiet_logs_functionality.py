#!/usr/bin/env python3
"""
Comprehensive tests for quiet logs functionality in Lumibot
"""
import os
import sys
import io
import pytest
from unittest.mock import patch, MagicMock
import logging
import datetime as dt


@pytest.fixture
def clean_environment():
    """Fixture to save and restore environment"""
    original_env = os.environ.copy()
    yield
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


def test_progress_bar_always_shows_with_quiet_logs(clean_environment):
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
    assert result != "", "Progress bar should show even with quiet logs"
    assert "Progress" in result, "Progress bar should contain 'Progress'"
    assert "50.00%" in result, "Progress bar should show percentage"


def test_strategy_logger_respects_quiet_logs(clean_environment):
    """Test that strategy logger respects BACKTESTING_QUIET_LOGS setting"""
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"
    os.environ["IS_BACKTESTING"] = "true"  # Set backtesting mode
    
    # Force reload to pick up environment variable
    import importlib
    import lumibot.tools.lumibot_logger
    importlib.reload(lumibot.tools.lumibot_logger)
    
    from lumibot.tools.lumibot_logger import get_strategy_logger
    
    logger = get_strategy_logger("test", "TestStrategy")
    
    # Test isEnabledFor
    assert not logger.isEnabledFor(logging.INFO), "INFO should not be enabled with quiet logs"
    assert logger.isEnabledFor(logging.ERROR), "ERROR should be enabled with quiet logs"


def test_quiet_logs_environment_variable(clean_environment):
    """Test that BACKTESTING_QUIET_LOGS environment variable is respected"""
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"
    
    # Force reload
    import importlib
    import lumibot.tools.lumibot_logger
    importlib.reload(lumibot.tools.lumibot_logger)
    
    from lumibot.tools.lumibot_logger import get_logger
    
    logger = get_logger("test")
    
    # Capture console output
    captured = io.StringIO()
    handler = logging.StreamHandler(captured)
    logger.addHandler(handler)
    
    # Log at different levels
    logger.info("This should not appear")
    logger.error("This should appear")
    
    output = captured.getvalue()
    
    # Check that ERROR still appears (since we're at ERROR level)
    assert "This should appear" in output, "ERROR messages should appear even with quiet logs"


def test_progress_bar_shows_green_bar(clean_environment):
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
    
    # Should be properly formatted with || delimiters  
    assert "||" in result, "Progress bar should have proper formatting"
    # Should contain percentage
    assert "50.00%" in result, "Progress bar should show percentage"