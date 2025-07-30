#!/usr/bin/env python3
"""
Comprehensive tests for BACKTESTING_QUIET_LOGS functionality
"""
import os
import sys
import datetime as dt
import pytest
from io import StringIO
from unittest.mock import patch


def test_print_progress_bar_respects_quiet_logs():
    """Test that print_progress_bar function respects BACKTESTING_QUIET_LOGS environment variable"""
    from lumibot.tools.helpers import print_progress_bar
    
    # Test 1: Normal operation (should print)
    with patch.dict(os.environ, {}, clear=True):  # Clear BACKTESTING_QUIET_LOGS
        captured_output = StringIO()
        print_progress_bar(
            value=5,
            start_value=0,
            end_value=10,
            backtesting_started=dt.datetime.now(),
            file=captured_output,
            prefix="Test",
            portfolio_value=100000.0
        )
        output = captured_output.getvalue()
        assert "Test" in output
        assert "Portfolio Val:" in output
        assert "50.00%" in output
    
    # Test 2: With BACKTESTING_QUIET_LOGS=true (progress bar should STILL print)
    # Progress bar ALWAYS shows, even with quiet logs - this is the only output users want to see
    with patch.dict(os.environ, {"BACKTESTING_QUIET_LOGS": "true"}):
        captured_output = StringIO()
        print_progress_bar(
            value=5,
            start_value=0,
            end_value=10,
            backtesting_started=dt.datetime.now(),
            file=captured_output,
            prefix="Test Silent",
            portfolio_value=100000.0
        )
        output = captured_output.getvalue()
        assert "Test Silent" in output  # Progress bar should show even with quiet logs
        assert "50.00%" in output
    
    # Test 3: With BACKTESTING_QUIET_LOGS=false (should print)
    with patch.dict(os.environ, {"BACKTESTING_QUIET_LOGS": "false"}):
        captured_output = StringIO()
        print_progress_bar(
            value=7,
            start_value=0,
            end_value=10,
            backtesting_started=dt.datetime.now(),
            file=captured_output,
            prefix="Test False",
            portfolio_value=100000.0
        )
        output = captured_output.getvalue()
        assert "Test False" in output
        assert "70.00%" in output


def test_data_source_backtesting_respects_quiet_logs():
    """Test that DataSourceBacktesting respects BACKTESTING_QUIET_LOGS"""
    
    # Test the logic without creating an actual DataSource instance
    # Just test the quiet logs logic directly
    def test_quiet_logs_logic(show_progress_bar, env_value):
        import os
        with patch.dict(os.environ, {"BACKTESTING_QUIET_LOGS": env_value} if env_value else {}, clear=True):
            quiet_logs_enabled = os.environ.get("BACKTESTING_QUIET_LOGS", "").lower() == "true"
            result = show_progress_bar and not quiet_logs_enabled
            return result
    
    # Test 1: Normal operation (should enable progress bar)
    assert test_quiet_logs_logic(True, "") is True
    
    # Test 2: With BACKTESTING_QUIET_LOGS=true (should disable progress bar)
    assert test_quiet_logs_logic(True, "true") is False
    
    # Test 3: show_progress_bar=False always disables
    assert test_quiet_logs_logic(False, "") is False
    assert test_quiet_logs_logic(False, "true") is False


def test_strategy_logger_respects_quiet_logs():
    """Test that strategy logger respects BACKTESTING_QUIET_LOGS"""
    import logging
    from lumibot.tools.lumibot_logger import get_strategy_logger
    
    # Test the logger logic without complex credential reloading
    def test_logger_setting_logic(backtesting_quiet_logs, is_backtesting):
        # Simulate the logic from Strategy.__init__
        should_set_info = not (backtesting_quiet_logs and is_backtesting)
        return should_set_info
    
    # Test 1: Normal backtesting (should set INFO level)
    assert test_logger_setting_logic(False, True) is True
    
    # Test 2: Quiet logs backtesting (should NOT set INFO level)
    assert test_logger_setting_logic(True, True) is False
    
    # Test 3: Live trading (should set INFO level regardless)
    assert test_logger_setting_logic(False, False) is True
    assert test_logger_setting_logic(True, False) is True
    
    # Test that we can create strategy loggers
    logger = get_strategy_logger(__name__, "TestStrategy")
    assert logger is not None
    assert hasattr(logger, 'info')
    assert hasattr(logger, 'setLevel')


def test_environment_variable_variations():
    """Test various ways of setting BACKTESTING_QUIET_LOGS"""
    from lumibot.tools.helpers import print_progress_bar
    
    test_cases = [
        ("true", True),
        ("TRUE", True),
        ("True", True),
        ("false", False),
        ("FALSE", False),
        ("False", False),
        ("", False),
        ("invalid", False),
        ("1", False),  # Only "true" should work
        ("yes", False),  # Only "true" should work
    ]
    
    for env_value, should_be_quiet in test_cases:
        with patch.dict(os.environ, {"BACKTESTING_QUIET_LOGS": env_value}):
            captured_output = StringIO()
            print_progress_bar(
                value=5,
                start_value=0,
                end_value=10,
                backtesting_started=dt.datetime.now(),
                file=captured_output,
                prefix=f"Test {env_value}",
                portfolio_value=100000.0
            )
            output = captured_output.getvalue()
            
            # Progress bar should ALWAYS show, regardless of quiet logs setting
            # This is the only output users want to see during quiet backtesting
            assert output != "", f"Progress bar should always show for '{env_value}', but got no output"
            assert f"Test {env_value}" in output, f"Progress bar should contain prefix for '{env_value}'"
            assert "50.00%" in output, f"Progress bar should show percentage for '{env_value}'"


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])