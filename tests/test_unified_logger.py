"""
Unit tests for the unified lumibot logger.

Tests the core functionality of the logging system including:
- Basic logging functionality
- CSV error handling
- Emergency shutdown behavior
- Strategy logger adapter
- Thread safety
"""

import csv
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest

from lumibot.tools.lumibot_logger import (
    get_logger,
    get_strategy_logger,
    CSVErrorHandler,
    LumibotFormatter,
    set_log_level
)

class TestBasicLogging:
    """Test basic logging functionality."""
    
    def test_logger_hierarchy(self):
        """Test that loggers are properly hierarchical under lumibot."""
        logger = get_logger("test.module")
        assert logger.name == "lumibot.test.module"
    
    def test_logger_already_starts_with_lumibot(self):
        """Test that logger names starting with lumibot are not double-prefixed."""
        logger = get_logger("lumibot.existing.module")
        assert logger.name == "lumibot.existing.module"
    
    def test_strategy_logger_prefix(self):
        """Test that strategy logger adds strategy name prefix."""
        strategy_logger = get_strategy_logger(__name__, "TestStrategy")
        
        with patch('sys.stdout') as mock_stdout:
            strategy_logger.info("Test message")
            # The actual output check would depend on the handler setup
            # This test ensures the adapter is created properly
        
        assert strategy_logger.strategy_name == "TestStrategy"
    
    def test_strategy_logger_update_name(self):
        """Test updating strategy name on strategy logger."""
        strategy_logger = get_strategy_logger(__name__, "OldStrategy")
        strategy_logger.update_strategy_name("NewStrategy")
        assert strategy_logger.strategy_name == "NewStrategy"


class TestCSVErrorHandler:
    """Test CSV error logging functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.temp_dir, "test_errors.csv")
    
    def teardown_method(self):
        """Clean up test environment."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_csv_handler_initialization(self):
        """Test CSV handler initializes properly."""
        handler = CSVErrorHandler(self.csv_path)
        assert handler.csv_path == os.path.abspath(self.csv_path)
        assert handler.level == logging.WARNING
    
    def test_csv_file_creation(self):
        """Test that CSV file is created with proper headers."""
        handler = CSVErrorHandler(self.csv_path)
        
        # Create a test log record
        logger = logging.getLogger("test")
        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="Test error", args=(), exc_info=None,
            func="test_func"
        )
        
        handler.emit(record)
        
        # Check file was created
        assert os.path.exists(self.csv_path)
        
        # Check headers
        with open(self.csv_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            assert headers == ['severity', 'error_code', 'timestamp', 'message', 'details', 'count']
    
    def test_error_deduplication(self):
        """Test that duplicate errors are counted rather than duplicated."""
        handler = CSVErrorHandler(self.csv_path)
        
        # Create identical log records
        logger = logging.getLogger("test")
        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="Duplicate error", args=(), exc_info=None,
            func="test_func"
        )
        
        # Emit the same error twice
        handler.emit(record)
        handler.emit(record)
        
        # Check that only one row exists but count is 2
        with open(self.csv_path, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert int(rows[0]['count']) == 2
    
    def test_error_details_normalization(self):
        """Test that dynamic values in error details are normalized."""
        handler = CSVErrorHandler(self.csv_path)
        
        # Test request ID normalization
        normalized = handler._normalize_error_details(
            'Error with "request_id":"abc123" in response',
            "TEST_ERROR"
        )
        assert '"request_id":"<REDACTED>"' in normalized
        assert 'abc123' not in normalized
        
        # Test timestamp normalization
        normalized = handler._normalize_error_details(
            'Error at 2023-01-01T10:30:00',
            "TEST_ERROR"
        )
        assert '<TIMESTAMP>' in normalized
        assert '2023-01-01T10:30:00' not in normalized

class TestLoggerConfiguration:
    """Test logger configuration and setup."""
    
    def test_set_log_level(self):
        """Test setting global log level."""
        import os
        from unittest.mock import patch
        
        original_level = logging.getLogger("lumibot").level
        
        # Clear any environment variables that might interfere with log level setting
        with patch.dict(os.environ, {'BACKTESTING_QUIET_LOGS': '', 'LUMIBOT_LOG_LEVEL': ''}, clear=False):
            try:
                set_log_level('DEBUG')
                assert logging.getLogger("lumibot").level == logging.DEBUG
                
                set_log_level('ERROR')
                assert logging.getLogger("lumibot").level == logging.ERROR
            finally:
                # Restore original level
                logging.getLogger("lumibot").setLevel(original_level)
    
    def test_invalid_log_level(self):
        """Test that invalid log level raises ValueError."""
        with pytest.raises(ValueError):
            set_log_level('INVALID')

class TestLoggerFormatter:
    """Test custom formatter functionality."""
    
    def test_formatter_creates_different_formats(self):
        """Test that formatter uses different formats for different levels."""
        formatter = LumibotFormatter()
        
        # Create records for different levels
        info_record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="test.py",
            lineno=10, msg="Info message", args=(), exc_info=None,
            func="test_func"
        )
        
        warning_record = logging.LogRecord(
            name="test", level=logging.WARNING, pathname="test.py",
            lineno=10, msg="Warning message", args=(), exc_info=None,
            func="test_func"
        )
        
        error_record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="test.py",
            lineno=10, msg="Error message", args=(), exc_info=None,
            func="test_func"
        )
        
        info_formatted = formatter.format(info_record)
        warning_formatted = formatter.format(warning_record)
        error_formatted = formatter.format(error_record)
        
        # Info should not include file info
        assert "test.py" not in info_formatted
        assert "test_func" not in info_formatted
        
        # Warning and error should include file info
        assert "test.py" in warning_formatted
        assert "test_func" in warning_formatted
        assert "test.py" in error_formatted
        assert "test_func" in error_formatted
    
    def test_message_cleaning(self):
        """Test that messages are cleaned of whitespace."""
        formatter = LumibotFormatter()
        
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="test.py",
            lineno=10, msg="  Message with whitespace  \n", args=(), exc_info=None,
            func="test_func"
        )
        
        formatted = formatter.format(record)
        assert "Message with whitespace" in formatted
        assert "  Message with whitespace  \n" not in formatted


class TestThreadSafety:
    """Test thread safety of logging operations."""
    
    def test_concurrent_csv_logging(self):
        """Test that concurrent CSV logging is thread-safe."""
        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "concurrent_test.csv")
        
        try:
            handler = CSVErrorHandler(csv_path)
            
            def log_errors(thread_id):
                for i in range(10):
                    record = logging.LogRecord(
                        name=f"test.thread{thread_id}", level=logging.ERROR,
                        pathname="test.py", lineno=i, msg=f"Error {i} from thread {thread_id}",
                        args=(), exc_info=None, func="test_func"
                    )
                    handler.emit(record)
                    time.sleep(0.01)  # Small delay to increase chance of race conditions
            
            # Start multiple threads
            threads = []
            for i in range(5):
                thread = threading.Thread(target=log_errors, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join()
            
            # Verify file integrity
            assert os.path.exists(csv_path)
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                # Should have some rows (exact count depends on deduplication)
                assert len(rows) > 0
                
        finally:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestEnvironmentVariables:
    """Test environment variable handling."""
    
    def test_backtesting_quiet_logs(self):
        """Test BACKTESTING_QUIET_LOGS environment variable during backtesting."""
        with patch.dict(os.environ, {'BACKTESTING_QUIET_LOGS': 'true', 'IS_BACKTESTING': 'true'}):
            # Reset handlers to pick up environment change  
            import lumibot.tools.lumibot_logger as logger_module
            logger_module._handlers_configured = False
            
            logger_module._ensure_handlers_configured()
            
            # Should be at ERROR level during backtesting
            root_logger = logging.getLogger("lumibot")
            console_handlers = [h for h in root_logger.handlers 
                             if isinstance(h, logging.StreamHandler)]
            assert len(console_handlers) > 0
            assert console_handlers[0].level == logging.ERROR

if __name__ == "__main__":
    pytest.main([__file__])
