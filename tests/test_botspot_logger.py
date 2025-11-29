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
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
                handler = BotspotErrorHandler()
                assert handler.api_key is None
                assert handler.requests is None
    
    def test_handler_initialization_with_api_key(self):
        """Test handler initialization when API key is set."""
        with patch.dict(os.environ, {
            'LUMIWEALTH_API_KEY': 'test-api-key'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
                handler = BotspotErrorHandler()
                assert handler.api_key == 'test-api-key'
                assert handler.base_url == "https://api.botspot.trade/bots/report-bot-error"
    
    def test_log_level_to_severity_mapping(self):
        """Test mapping of log levels to Botspot severity."""
        handler = BotspotErrorHandler()
        
        # Test each mapping explicitly using value comparison
        result = handler._map_log_level_to_severity(logging.DEBUG)
        assert result.value == "DEBUG"
        
        result = handler._map_log_level_to_severity(logging.INFO)
        assert result.value == "INFO"
        
        result = handler._map_log_level_to_severity(logging.WARNING)
        assert result.value == "WARNING"
        
        result = handler._map_log_level_to_severity(logging.ERROR)
        assert result.value == "CRITICAL"
        
        result = handler._map_log_level_to_severity(logging.CRITICAL)
        assert result.value == "CRITICAL"
    
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
        
        assert error_code == "MYSTRATEGY_ERROR"
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
    
    def test_extract_error_info_strategy_with_structured_format(self):
        """Test error info extraction with both strategy prefix and structured format."""
        handler = BotspotErrorHandler()
        
        record = logging.LogRecord(
            name="test.module", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="[MyStrategy] CUSTOM_ERROR: Something went wrong | Additional context",
            args=(), exc_info=None, func="test_func"
        )
        
        error_code, message, details = handler._extract_error_info(record)
        
        # Should extract CUSTOM_ERROR, not include the strategy prefix in error code
        assert error_code == "CUSTOM_ERROR"
        assert message == "[MyStrategy] Something went wrong"
        assert "Additional context" in details
        assert "File: test.py:10" in details
    
    def test_extract_error_info_strategy_name_in_message(self):
        """Test that strategy name in message content doesn't prevent prefix re-addition."""
        handler = BotspotErrorHandler()
        
        record = logging.LogRecord(
            name="test.module", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="[MyStrategy] Failed to process MyStrategy configuration",
            args=(), exc_info=None, func="test_func"
        )
        
        error_code, message, details = handler._extract_error_info(record)
        
        assert error_code == "MYSTRATEGY_ERROR"
        # Strategy prefix should be present exactly once
        assert message == "[MyStrategy] Failed to process MyStrategy configuration"
        assert message.count("[MyStrategy]") == 1
    
    def test_extract_error_info_no_double_spacing(self):
        """Test that removing and re-adding strategy prefix doesn't create double spaces."""
        handler = BotspotErrorHandler()
        
        record = logging.LogRecord(
            name="test.module", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="[MyStrategy]  Double space after prefix", args=(), exc_info=None,
            func="test_func"
        )
        
        error_code, message, details = handler._extract_error_info(record)
        
        # Should not have double spaces
        assert message == "[MyStrategy] Double space after prefix"
        assert "  " not in message
    
    @patch('requests.post')
    def test_report_to_botspot_success(self, mock_post):
        """Test successful API call to Botspot."""
        with patch.dict(os.environ, {
            'LUMIWEALTH_API_KEY': 'test-api-key'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
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
            assert 'bot_id' not in kwargs['json']  # bot_id should not be sent
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
            'LUMIWEALTH_API_KEY': 'test-api-key'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
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
    
    def test_duplicate_aggregation(self):
        """Duplicates within window suppressed; aggregated count sent after window."""
        with patch.dict(os.environ, {
            'LUMIWEALTH_API_KEY': 'test-api-key',
            'BOTSPOT_RATE_LIMIT_WINDOW': '1'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
                handler = BotspotErrorHandler()
                handler._report_to_botspot = MagicMock(return_value=True)

            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname="test.py",
                lineno=10, msg="Duplicate error", args=(), exc_info=None,
                func="test_func"
            )

            # First occurrence -> send immediately (count=1)
            handler.emit(record)
            # Two more inside window -> suppressed
            handler.emit(record)
            handler.emit(record)
            assert handler._report_to_botspot.call_count == 1
            first_args = handler._report_to_botspot.call_args_list[0][0]
            assert first_args[4] == 1

            # After window expires, next emit should send aggregated count (suppressed 2 + current 1 = 3)
            time.sleep(1.1)
            handler.emit(record)
            assert handler._report_to_botspot.call_count == 2
            second_args = handler._report_to_botspot.call_args_list[1][0]
            assert second_args[4] == 3
    
    def test_rate_limit_window(self):
        """Second window send includes suppressed count."""
        with patch.dict(os.environ, {
            'LUMIWEALTH_API_KEY': 'test-api-key',
            'BOTSPOT_RATE_LIMIT_WINDOW': '2'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
                handler = BotspotErrorHandler()
                handler._report_to_botspot = MagicMock(return_value=True)

            record = logging.LogRecord(
                name="test", level=logging.ERROR, pathname="test.py",
                lineno=10, msg="Rate limited error", args=(), exc_info=None,
                func="test_func"
            )

            handler.emit(record)  # immediate send
            handler.emit(record)  # suppressed
            assert handler._report_to_botspot.call_count == 1
            time.sleep(2.1)
            handler.emit(record)  # aggregated send (count=2)
            assert handler._report_to_botspot.call_count == 2
            second_args = handler._report_to_botspot.call_args_list[1][0]
            assert second_args[4] == 2
    
    def test_max_errors_per_minute(self):
        """Test that total errors per minute are limited."""
        with patch.dict(os.environ, {
            'LUMIWEALTH_API_KEY': 'test-api-key',
            'BOTSPOT_MAX_ERRORS_PER_MINUTE': '3'  # Only 3 errors per minute
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
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
            'LUMIWEALTH_API_KEY': 'test-api-key',
            'BOTSPOT_MAX_ERRORS_PER_MINUTE': '2'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
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
    
    @patch('logging.getLogger')
    @patch('requests.post')
    def test_api_error_uses_logging_not_print(self, mock_post, mock_get_logger):
        """Test that API errors are logged using logger, not print()."""
        with patch.dict(os.environ, {
            'LUMIWEALTH_API_KEY': 'test-api-key'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
                handler = BotspotErrorHandler()
                handler.requests = MagicMock()
                handler.requests.post = mock_post
            
            # Mock logger
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            # Simulate API error
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_response.text = "Internal Server Error"
            mock_post.return_value = mock_response
            
            result = handler._report_to_botspot(
                BotspotSeverity.CRITICAL,
                "TEST_ERROR",
                "Test message",
                "Test details",
                1
            )
            
            assert result is False
            
            # Verify logger.debug was called, not print()
            mock_logger.debug.assert_called_once()
            assert "Botspot API error: 500" in mock_logger.debug.call_args[0][0]
    
    @patch('logging.getLogger')
    @patch('requests.post')
    def test_api_exception_uses_logging_not_print(self, mock_post, mock_get_logger):
        """Test that API exceptions are logged using logger, not print()."""
        with patch.dict(os.environ, {
            'LUMIWEALTH_API_KEY': 'test-api-key'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
                handler = BotspotErrorHandler()
                handler.requests = MagicMock()
                handler.requests.post = mock_post
            
            # Mock logger
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            # Simulate exception
            mock_post.side_effect = Exception("Connection timeout")
            
            result = handler._report_to_botspot(
                BotspotSeverity.CRITICAL,
                "TEST_ERROR",
                "Test message",
                "Test details",
                1
            )
            
            assert result is False
            
            # Verify logger.debug was called for the exception
            mock_logger.debug.assert_called_once()
            assert "Botspot API exception: Connection timeout" in mock_logger.debug.call_args[0][0]


class TestBotspotIntegration:
    """Test integration of Botspot handler with the logger system."""
    
    def setup_method(self):
        """Reset logger configuration before each test."""
        import lumibot.tools.lumibot_logger as logger_module
        logger_module._handlers_configured = False
        logger_module._logger_registry.clear()
        logger_module._strategy_logger_registry.clear()
        
        # Also clear any existing handlers from the root logger
        root_logger = logging.getLogger("lumibot")
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    def test_handler_not_added_without_api_key(self):
        """Test that Botspot handler is not added without API key."""
        with patch.dict(os.environ, {}, clear=True):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', None):
                # Force reconfiguration after patching
                import lumibot.tools.lumibot_logger as logger_module
                logger_module._handlers_configured = False
                
                logger = get_logger(__name__)
                
                # Check that no BotspotErrorHandler was added to the root lumibot logger
                root_logger = logging.getLogger("lumibot")
                botspot_handlers = [h for h in root_logger.handlers 
                                  if isinstance(h, BotspotErrorHandler)]
                assert len(botspot_handlers) == 0
    
    def test_strategy_logger_integration(self):
        """Test that strategy logger errors are properly reported to Botspot."""
        # We'll verify that the BotspotErrorHandler processes strategy errors correctly
        # without actually making the API call
        handler = BotspotErrorHandler()
        
        # Set up the handler with a mock API key
        handler.api_key = 'test-api-key'
        
        # Create a test log record with strategy prefix
        record = logging.LogRecord(
            name="test.module", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="[TestStrategy] Strategy execution failed", args=(), 
            exc_info=None, func="test_func"
        )
        
        # Extract error info to verify strategy name handling
        error_code, message, details = handler._extract_error_info(record)
        
        # Verify that strategy name is handled correctly
        assert error_code == "TESTSTRATEGY_ERROR"
        assert "[TestStrategy]" in message
        assert "Strategy execution failed" in message
        
        # Now test with a real logger setup
        with patch.dict(os.environ, {
            'LUMIWEALTH_API_KEY': 'test-api-key'
        }):
            with patch('lumibot.tools.lumibot_logger.LUMIWEALTH_API_KEY', 'test-api-key'):
                strategy_logger = get_strategy_logger(__name__, "TestStrategy")
                
                # Verify that strategy logger adds the prefix
                with patch.object(logging.Logger, '_log') as mock_log:
                    strategy_logger.error("Test error message")
                    mock_log.assert_called_once()
                    args = mock_log.call_args[0]
                    # args[1] is the message
                    assert "[TestStrategy]" in str(args[1])


if __name__ == "__main__":
    pytest.main([__file__])