import csv
import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch
from lumibot.tools.error_logger import ErrorLogger


class TestErrorLogger:
    """Test the ErrorLogger class to ensure it works correctly."""

    def test_error_logger_no_csv_when_no_errors(self):
        """Test that no CSV file is created when log_errors_to_csv=True but no errors are logged."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # No errors logged, so no CSV file should be created
            assert not csv_path.exists()

    def test_error_logger_creates_csv_on_first_error(self):
        """Test that CSV file is created only when the first error is logged."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # No CSV file should exist yet
            assert not csv_path.exists()
            
            # Log first error
            logger.log_error("ERROR", "TEST_ERROR", "Test message", "Test details")
            
            # Now CSV file should exist with headers and one error row
            assert csv_path.exists()
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 1 data row
                assert len(rows) == 2
                
                # Check header
                assert rows[0] == ['severity', 'error_code', 'timestamp', 'message', 'details']
                
                # Check data row
                assert rows[1][0] == 'ERROR'  # severity
                assert rows[1][1] == 'TEST_TEST_ERROR'  # error_code with data source prefix
                assert rows[1][3] == 'Test message'  # message
                assert rows[1][4] == 'Test details'  # details

    def test_error_logger_appends_multiple_errors(self):
        """Test that multiple errors are correctly appended to the CSV file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # Log multiple errors
            logger.log_error("ERROR", "ERROR_1", "First error", "First details")
            logger.log_error("WARNING", "WARNING_1", "First warning", "Warning details")
            logger.log_error("ERROR", "ERROR_2", "Second error", "Second details")
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 3 data rows
                assert len(rows) == 4
                
                # Check that all errors are logged
                assert rows[1][0] == 'ERROR'
                assert rows[1][1] == 'TEST_ERROR_1'
                assert rows[2][0] == 'WARNING'
                assert rows[2][1] == 'TEST_WARNING_1'
                assert rows[3][0] == 'ERROR'
                assert rows[3][1] == 'TEST_ERROR_2'

    def test_error_logger_disabled_csv_logging(self):
        """Test that no CSV file is created when log_errors_to_csv=False."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging disabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=False
            )
            
            # Log error - should not create CSV file
            with patch('logging.log') as mock_log:
                logger.log_error("ERROR", "TEST_ERROR", "Test message", "Test details")
                
                # Should log to console instead
                mock_log.assert_called_once()
                
            # No CSV file should be created
            assert not csv_path.exists()

    def test_error_logger_convenience_methods(self):
        """Test that convenience methods work correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="POLYGON",
                log_errors_to_csv=True
            )
            
            # Test rate limit logging
            logger.log_rate_limit(60, "https://api.polygon.io", "Rate limit exceeded")
            
            # Test API error logging
            test_exception = ValueError("Test API error")
            logger.log_api_error(test_exception, "https://api.polygon.io", "GET request")
            
            # Test data error logging
            logger.log_data_error("SPY", "Failed to retrieve data")
            
            # Test validation error logging
            logger.log_validation_error("SYMBOL", "Invalid symbol format")
            
            # Test cache error logging
            logger.log_cache_error("READ", "/path/to/cache.feather", "File not found")
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 5 data rows
                assert len(rows) == 6
                
                # Check rate limit error
                assert rows[1][0] == 'WARNING'
                assert rows[1][1] == 'POLYGON_RATE_LIMIT_EXCEEDED'
                
                # Check API error
                assert rows[2][0] == 'ERROR'
                assert rows[2][1] == 'POLYGON_API_VALUEERROR'
                
                # Check data error
                assert rows[3][0] == 'ERROR'
                assert rows[3][1] == 'POLYGON_DATA_RETRIEVAL_FAILED'
                
                # Check validation error
                assert rows[4][0] == 'ERROR'
                assert rows[4][1] == 'POLYGON_VALIDATION_SYMBOL_FAILED'
                
                # Check cache error
                assert rows[5][0] == 'WARNING'
                assert rows[5][1] == 'POLYGON_CACHE_READ_FAILED'

    def test_error_logger_authorization_error(self):
        """Test that authorization errors are logged with the correct error code."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="POLYGON",
                log_errors_to_csv=True
            )
            
            # Test authorization error logging
            error_msg = '{"status":"NOT_AUTHORIZED","request_id":"8d40e43d65b20250455760ccfa4e3e0b","message":"You are not entitled to this data. Please upgrade your plan at https://polygon.io/pricing"}'
            logger.log_authorization_error(
                url="/v2/aggs/ticker/I:SPX/range/1/minute/2024-12-02/2024-12-31",
                operation="HTTP GET request",
                error_details=error_msg
            )
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 1 data row
                assert len(rows) == 2
                
                # Check authorization error
                assert rows[1][0] == 'ERROR'
                assert rows[1][1] == 'POLYGON_NOT_AUTHORIZED'
                assert "authorization error - insufficient permissions" in rows[1][3]
                assert "/v2/aggs/ticker/I:SPX/range/1/minute/2024-12-02/2024-12-31" in rows[1][4]
                assert "NOT_AUTHORIZED" in rows[1][4]

    def test_error_logger_creates_directory(self):
        """Test that the ErrorLogger creates the directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a nested path that doesn't exist
            csv_path = Path(temp_dir) / "nested" / "directory" / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # Log error - should create directory and file
            logger.log_error("ERROR", "TEST_ERROR", "Test message", "Test details")
            
            # Directory and file should be created
            assert csv_path.parent.exists()
            assert csv_path.exists()

    def test_error_logger_thread_safety(self):
        """Test that the ErrorLogger is thread-safe by checking the lock is used."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # Check that the lock exists
            assert hasattr(logger, '_csv_lock')
            assert hasattr(logger, '_csv_initialized')
            
            # Log error to initialize CSV
            logger.log_error("ERROR", "TEST_ERROR", "Test message", "Test details")
            
            # Check that CSV was initialized
            assert logger._csv_initialized == True
            assert csv_path.exists()

    def test_error_logger_fallback_on_csv_failure(self):
        """Test that ErrorLogger falls back to console logging if CSV writing fails."""
        # Create ErrorLogger with an invalid path (read-only directory)
        logger = ErrorLogger(
            errors_csv="/root/invalid_path/test_errors.csv",  # This should fail
            data_source_name="TEST",
            log_errors_to_csv=True
        )
        
        # Mock logging.error to capture fallback
        with patch('logging.error') as mock_error:
            logger.log_error("ERROR", "TEST_ERROR", "Test message", "Test details")
            
            # Should have logged the fallback error
            mock_error.assert_called_once()
            assert "Failed to write to errors CSV" in str(mock_error.call_args)
