"""
Unit tests for the Botspot error handler integration with unified logger.

Tests the Botspot API error reporting functionality including:
- Handler initialization based on environment variables
- Error mapping and formatting
- API call behavior
- Integration with strategy loggers
- Rate limiting and deduplication
"""

import os
import logging
import tempfile
import time
from unittest.mock import patch, MagicMock, call
import pytest

from lumibot.tools.lumibot_logger import (
    get_logger,
    get_strategy_logger,
    BotspotErrorHandler,
    BotspotSeverity
)


class TestBotspotErrorHandler:
    """Test Botspot error handler functionality."""
    
    def test_handler_initialization_without_api_key(self):
        """Test handler initialization when API key is not set."""
        with patch.dict(os.environ, {}, clear=True):
            handler = BotspotErrorHandler()
            assert handler.api_key is None
            assert handler.bot_id is None
            assert handler.requests is None
    
    def test_handler_initialization_with_api_key(self):
        """Test handler initialization when API key is set."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id'
        }):
            handler = BotspotErrorHandler()
            assert handler.api_key == 'test-api-key'
            assert handler.bot_id == 'test-bot-id'
            assert handler.base_url == "https://api.botspot.trade/bots/report-bot-error"
    
    def test_log_level_to_severity_mapping(self):
        """Test mapping of log levels to Botspot severity."""
        handler = BotspotErrorHandler()
        
        assert handler._map_log_level_to_severity(logging.DEBUG) == BotspotSeverity.DEBUG
        assert handler._map_log_level_to_severity(logging.INFO) == BotspotSeverity.INFO
        assert handler._map_log_level_to_severity(logging.WARNING) == BotspotSeverity.WARNING
        assert handler._map_log_level_to_severity(logging.ERROR) == BotspotSeverity.CRITICAL
        assert handler._map_log_level_to_severity(logging.CRITICAL) == BotspotSeverity.CRITICAL
    
    def test_extract_error_info_basic(self):
        """Test basic error info extraction from log record."""
        handler = BotspotErrorHandler()
        
        record = logging.LogRecord(
            name="test.module", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="Test error message", args=(), exc_info=None,
            func="test_func"
        )
        
        error_code, message, details = handler._extract_error_info(record)
        
        assert error_code == "MODULE_ERROR"
        assert message == "Test error message"
        assert "File: test.py:10" in details
        assert "Function: test_func" in details
    
    def test_extract_error_info_with_strategy(self):
        """Test error info extraction with strategy name prefix."""
        handler = BotspotErrorHandler()
        
        record = logging.LogRecord(
            name="test.module", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="[MyStrategy] Strategy error occurred", args=(), exc_info=None,
            func="test_func"
        )
        
        error_code, message, details = handler._extract_error_info(record)
        
        assert error_code == "_MYSTRATEGY_ERROR"
        assert message == "[MyStrategy] Strategy error occurred"
        assert "File: test.py:10" in details
    
    def test_extract_error_info_structured_format(self):
        """Test error info extraction with structured format."""
        handler = BotspotErrorHandler()
        
        record = logging.LogRecord(
            name="test.module", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="CUSTOM_ERROR: Something went wrong | Additional context here",
            args=(), exc_info=None, func="test_func"
        )
        
        error_code, message, details = handler._extract_error_info(record)
        
        assert error_code == "CUSTOM_ERROR"
        assert message == "Something went wrong"
        assert "Additional context here" in details
        assert "File: test.py:10" in details
    
    @patch('requests.post')
    def test_report_to_botspot_success(self, mock_post):
        """Test successful API call to Botspot."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id'
        }):
            handler = BotspotErrorHandler()
            handler.requests = MagicMock()
            handler.requests.post = mock_post
            
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response
            
            result = handler._report_to_botspot(
                BotspotSeverity.CRITICAL,
                "TEST_ERROR",
                "Test message",
                "Test details",
                1
            )
            
            assert result is True
            
            # Verify API call
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            
            assert args[0] == handler.base_url
            assert kwargs['headers']['x-api-key'] == 'test-api-key'
            assert kwargs['json']['bot_id'] == 'test-bot-id'
            assert kwargs['json']['severity'] == 'CRITICAL'
            assert kwargs['json']['error_code'] == 'TEST_ERROR'
            assert kwargs['json']['message'] == 'Test message'
            assert kwargs['json']['details'] == 'Test details'
            assert 'timestamp' in kwargs['json']
    
    def test_emit_without_api_key(self):
        """Test that emit does nothing without API key."""
        with patch.dict(os.environ, {}, clear=True):
            handler = BotspotErrorHandler()
            
            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname="test.py",
                lineno=10, msg="Test error", args=(), exc_info=None,
                func="test_func"
            )
            
            # Should not raise any exception
            handler.emit(record)
    
    def test_emit_below_warning_level(self):
        """Test that emit ignores messages below WARNING level."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id'
        }):
            handler = BotspotErrorHandler()
            handler._report_to_botspot = MagicMock()
            
            record = logging.LogRecord(
                name="test", level=logging.INFO, pathname="test.py",
                lineno=10, msg="Info message", args=(), exc_info=None,
                func="test_func"
            )
            
            handler.emit(record)
            
            # Should not call report method
            handler._report_to_botspot.assert_not_called()
    
    def test_error_counting(self):
        """Test that duplicate errors are counted but rate limited."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id'
        }):
            handler = BotspotErrorHandler()
            handler._report_to_botspot = MagicMock(return_value=True)
            
            # Create identical log records
            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname="test.py",
                lineno=10, msg="Duplicate error", args=(), exc_info=None,
                func="test_func"
            )
            
            # Emit the same error three times
            handler.emit(record)
            handler.emit(record)
            handler.emit(record)
            
            # Only one call should be made due to rate limiting
            calls = handler._report_to_botspot.call_args_list
            assert len(calls) == 1
            assert calls[0][0][4] == 1  # First call has count=1
            
            # Verify that the error count was tracked internally
            error_key = ('TEST_ERROR', 'Duplicate error', 'File: test.py:10, Function: test_func')
            assert handler._error_counts[error_key] == 3
    
    def test_rate_limit_window(self):
        """Test that same errors are rate limited within the time window."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id',
            'BOTSPOT_RATE_LIMIT_WINDOW': '2'  # 2 second window
        }):
            handler = BotspotErrorHandler()
            handler._report_to_botspot = MagicMock(return_value=True)
            
            # Create identical log records
            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname="test.py",
                lineno=10, msg="Rate limited error", args=(), exc_info=None,
                func="test_func"
            )
            
            # First emit should go through
            handler.emit(record)
            assert handler._report_to_botspot.call_count == 1
            
            # Second emit within window should be rate limited
            handler.emit(record)
            assert handler._report_to_botspot.call_count == 1  # No new call
            
            # Wait for window to expire
            time.sleep(2.1)
            
            # Third emit after window should go through
            handler.emit(record)
            assert handler._report_to_botspot.call_count == 2
    
    def test_max_errors_per_minute(self):
        """Test that total errors per minute are limited."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id',
            'BOTSPOT_MAX_ERRORS_PER_MINUTE': '3'  # Only 3 errors per minute
        }):
            handler = BotspotErrorHandler()
            handler._report_to_botspot = MagicMock(return_value=True)
            
            # Send different errors to avoid per-error rate limiting
            for i in range(5):
                record = logging.LogRecord(
                    name="test", level=logging.ERROR, pathname="test.py",
                    lineno=10, msg=f"Error {i}", args=(), exc_info=None,
                    func="test_func"
                )
                handler.emit(record)
            
            # Only first 3 should be sent
            assert handler._report_to_botspot.call_count == 3
    
    def test_rate_limit_counter_reset(self):
        """Test that the per-minute counter resets after 60 seconds."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id',
            'BOTSPOT_MAX_ERRORS_PER_MINUTE': '2'
        }):
            handler = BotspotErrorHandler()
            handler._report_to_botspot = MagicMock(return_value=True)
            
            # Send 2 errors (hitting the limit)
            for i in range(2):
                record = logging.LogRecord(
                    name="test", level=logging.ERROR, pathname="test.py",
                    lineno=10, msg=f"Error {i}", args=(), exc_info=None,
                    func="test_func"
                )
                handler.emit(record)
            
            assert handler._report_to_botspot.call_count == 2
            
            # Manually advance the minute start time to simulate time passing
            handler._minute_start_time = time.time() - 61
            
            # Send another error - should go through after counter reset
            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname="test.py",
                lineno=10, msg="Error after reset", args=(), exc_info=None,
                func="test_func"
            )
            handler.emit(record)
            
            assert handler._report_to_botspot.call_count == 3


class TestBotspotIntegration:
    """Test integration of Botspot handler with the logger system."""
    
    def setup_method(self):
        """Reset logger configuration before each test."""
        import lumibot.tools.lumibot_logger as logger_module
        logger_module._handlers_configured = False
        logger_module._logger_registry.clear()
        logger_module._strategy_logger_registry.clear()
    
    def test_handler_added_when_api_key_set(self):
        """Test that Botspot handler is added when API key is set."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id'
        }):
            logger = get_logger(__name__)
            
            # Check that a BotspotErrorHandler was added to the root lumibot logger
            root_logger = logging.getLogger("lumibot")
            botspot_handlers = [h for h in root_logger.handlers 
                              if isinstance(h, BotspotErrorHandler)]
            assert len(botspot_handlers) > 0
    
    def test_handler_not_added_without_api_key(self):
        """Test that Botspot handler is not added without API key."""
        with patch.dict(os.environ, {}, clear=True):
            logger = get_logger(__name__)
            
            # Check that no BotspotErrorHandler was added
            botspot_handlers = [h for h in logger.handlers 
                              if isinstance(h, BotspotErrorHandler)]
            assert len(botspot_handlers) == 0
    
    @patch('requests.post')
    def test_strategy_logger_integration(self, mock_post):
        """Test that strategy logger errors are properly reported to Botspot."""
        with patch.dict(os.environ, {
            'BOTSPOT_ERROR_API_KEY': 'test-api-key',
            'BOT_ID': 'test-bot-id'
        }):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response
            
            strategy_logger = get_strategy_logger(__name__, "TestStrategy")
            
            # Log an error
            strategy_logger.error("Strategy execution failed")
            
            # Verify API was called
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            
            # Check that strategy name is in the message
            assert "[TestStrategy]" in kwargs['json']['message']
            assert kwargs['json']['error_code'] == "_TESTSTRATEGY_ERROR"


if __name__ == "__main__":
    pytest.main([__file__])