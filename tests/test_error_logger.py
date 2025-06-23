import csv
import os
import re
import tempfile
import pytest
import threading
import time
from pathlib import Path
from unittest.mock import patch, MagicMock
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
                assert rows[0] == ['severity', 'error_code', 'timestamp', 'message', 'details', 'count']
                
                # Check data row
                assert rows[1][0] == 'ERROR'  # severity
                assert rows[1][1] == 'TEST_TEST_ERROR'  # error_code with data source prefix
                assert rows[1][3] == 'Test message'  # message
                assert rows[1][4] == 'Test details'  # details
                assert rows[1][5] == '1'  # count

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
                
                # Check header includes count column
                assert rows[0] == ['severity', 'error_code', 'timestamp', 'message', 'details', 'count']
                
                # Check that all errors are logged
                assert rows[1][0] == 'ERROR'
                assert rows[1][1] == 'TEST_ERROR_1'
                assert rows[1][5] == '1'  # count
                assert rows[2][0] == 'WARNING'
                assert rows[2][1] == 'TEST_WARNING_1'
                assert rows[2][5] == '1'  # count
                assert rows[3][0] == 'ERROR'
                assert rows[3][1] == 'TEST_ERROR_2'
                assert rows[3][5] == '1'  # count

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
                
                # Check header includes count column
                assert rows[0] == ['severity', 'error_code', 'timestamp', 'message', 'details', 'count']
                
                # Check rate limit error
                assert rows[1][0] == 'WARNING'
                assert rows[1][1] == 'POLYGON_RATE_LIMIT_EXCEEDED'
                assert rows[1][5] == '1'  # count
                
                # Check API error
                assert rows[2][0] == 'ERROR'
                assert rows[2][1] == 'POLYGON_API_VALUEERROR'
                assert rows[2][5] == '1'  # count
                
                # Check data error
                assert rows[3][0] == 'ERROR'
                assert rows[3][1] == 'POLYGON_DATA_RETRIEVAL_FAILED'
                assert rows[3][5] == '1'  # count
                
                # Check validation error
                assert rows[4][0] == 'ERROR'
                assert rows[4][1] == 'POLYGON_VALIDATION_SYMBOL_FAILED'
                assert rows[4][5] == '1'  # count
                
                # Check cache error
                assert rows[5][0] == 'WARNING'
                assert rows[5][1] == 'POLYGON_CACHE_READ_FAILED'
                assert rows[5][5] == '1'  # count

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
                
                # Check header includes count column
                assert rows[0] == ['severity', 'error_code', 'timestamp', 'message', 'details', 'count']
                
                # Check authorization error
                assert rows[1][0] == 'CRITICAL'  # POLYGON uses CRITICAL for authorization errors
                assert rows[1][1] == 'POLYGON_NOT_AUTHORIZED'
                assert "authorization error - insufficient permissions" in rows[1][3]
                assert "/v2/aggs/ticker/I:SPX/range/1/minute/<DATE_RANGE>" in rows[1][4]
                assert "NOT_AUTHORIZED" in rows[1][4]
                assert '"request_id":"<REDACTED>"' in rows[1][4]
                assert rows[1][5] == '1'  # count

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

    def test_error_logger_count_column_in_header(self):
        """Test that the CSV file includes a count column in the header."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # Log first error
            logger.log_error("ERROR", "TEST_ERROR", "Test message", "Test details")
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 1 data row
                assert len(rows) == 2
                
                # Check header includes count column
                assert rows[0] == ['severity', 'error_code', 'timestamp', 'message', 'details', 'count']
                
                # Check data row includes count
                assert len(rows[1]) == 6
                assert rows[1][5] == '1'  # First occurrence should have count of 1

    def test_error_logger_deduplication(self):
        """Test that duplicate errors increment count instead of adding new rows."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # Log the same error multiple times
            logger.log_error("ERROR", "DUPLICATE_ERROR", "Same message", "Same details")
            logger.log_error("ERROR", "DUPLICATE_ERROR", "Same message", "Same details")
            logger.log_error("ERROR", "DUPLICATE_ERROR", "Same message", "Same details")
            
            # Log a different error for comparison
            logger.log_error("WARNING", "DIFFERENT_ERROR", "Different message", "Different details")
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 2 unique error rows (not 4)
                assert len(rows) == 3
                
                # Check header
                assert rows[0] == ['severity', 'error_code', 'timestamp', 'message', 'details', 'count']
                
                # Check first error has count of 3
                assert rows[1][0] == 'ERROR'
                assert rows[1][1] == 'TEST_DUPLICATE_ERROR'
                assert rows[1][3] == 'Same message'
                assert rows[1][4] == 'Same details'
                assert rows[1][5] == '3'
                
                # Check second error has count of 1
                assert rows[2][0] == 'WARNING'
                assert rows[2][1] == 'TEST_DIFFERENT_ERROR'
                assert rows[2][3] == 'Different message'
                assert rows[2][4] == 'Different details'
                assert rows[2][5] == '1'

    def test_error_logger_deduplication_different_severity(self):
        """Test that errors with different severity are treated as different."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # Log same message/details but different severity
            logger.log_error("ERROR", "SAME_CODE", "Same message", "Same details")
            logger.log_error("WARNING", "SAME_CODE", "Same message", "Same details")
            logger.log_error("ERROR", "SAME_CODE", "Same message", "Same details")
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 2 unique error rows (different severity = different error)
                assert len(rows) == 3
                
                # Check that we have both ERROR and WARNING entries
                severities = [row[0] for row in rows[1:]]
                assert 'ERROR' in severities
                assert 'WARNING' in severities
                
                # Check ERROR has count of 2
                error_row = next(row for row in rows[1:] if row[0] == 'ERROR')
                assert error_row[5] == '2'
                
                # Check WARNING has count of 1
                warning_row = next(row for row in rows[1:] if row[0] == 'WARNING')
                assert warning_row[5] == '1'

    def test_error_logger_load_existing_counts(self):
        """Test that ErrorLogger loads existing error counts from CSV file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # First, create a CSV file with some errors
            logger1 = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # Log some errors
            logger1.log_error("ERROR", "EXISTING_ERROR", "Existing message", "Existing details")
            logger1.log_error("ERROR", "EXISTING_ERROR", "Existing message", "Existing details")
            
            # Create a new logger instance (simulating restart)
            logger2 = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="TEST",
                log_errors_to_csv=True
            )
            
            # Log the same error again
            logger2.log_error("ERROR", "EXISTING_ERROR", "Existing message", "Existing details")
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 1 unique error row
                assert len(rows) == 2
                
                # Check that count is 3 (2 from first logger + 1 from second logger)
                assert rows[1][5] == '3'

    def test_error_logger_convenience_methods_deduplication(self):
        """Test that convenience methods also support deduplication."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="POLYGON",
                log_errors_to_csv=True
            )
            
            # Log same rate limit error multiple times
            logger.log_rate_limit(60, "https://api.polygon.io", "Rate limit exceeded")
            logger.log_rate_limit(60, "https://api.polygon.io", "Rate limit exceeded")
            logger.log_rate_limit(60, "https://api.polygon.io", "Rate limit exceeded")
            
            # Log same authorization error multiple times
            error_msg = '{"status":"NOT_AUTHORIZED","message":"You are not entitled to this data"}'
            logger.log_authorization_error(
                url="/v2/aggs/ticker/SPY/range/1/minute/2024-12-02/2024-12-31",
                operation="HTTP GET request",
                error_details=error_msg
            )
            logger.log_authorization_error(
                url="/v2/aggs/ticker/SPY/range/1/minute/2024-12-02/2024-12-31",
                operation="HTTP GET request",
                error_details=error_msg
            )
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 2 unique error rows
                assert len(rows) == 3
                
                # Check rate limit error has count of 3
                rate_limit_row = next(row for row in rows[1:] if 'RATE_LIMIT_EXCEEDED' in row[1])
                assert rate_limit_row[5] == '3'
                
                # Check authorization error has count of 2
                auth_row = next(row for row in rows[1:] if 'NOT_AUTHORIZED' in row[1])
                assert auth_row[5] == '2'

    def test_critical_error_shutdown_callback_called(self):
        """Test that shutdown callback is called when critical error is logged."""
        shutdown_called = threading.Event()
        
        def mock_shutdown():
            shutdown_called.set()
        
        logger = ErrorLogger(
            data_source_name="TEST",
            log_errors_to_csv=False,
            shutdown_callback=mock_shutdown,
            auto_shutdown_on_critical=True
        )
        
        # Log a critical error
        logger.log_critical_error("SYSTEM_FAILURE", "Critical system failure", "System is unstable")
        
        # Wait a short time for the callback to be called
        assert shutdown_called.wait(timeout=1.0), "Shutdown callback was not called for critical error"

    def test_fatal_error_shutdown_callback_called(self):
        """Test that shutdown callback is called when fatal error is logged."""
        shutdown_called = threading.Event()
        
        def mock_shutdown():
            shutdown_called.set()
        
        logger = ErrorLogger(
            data_source_name="TEST",
            log_errors_to_csv=False,
            shutdown_callback=mock_shutdown,
            auto_shutdown_on_critical=True
        )
        
        # Log a fatal error
        logger.log_fatal_error("DATA_CORRUPTION", "Fatal data corruption detected", "Database integrity compromised")
        
        # Wait a short time for the callback to be called
        assert shutdown_called.wait(timeout=1.0), "Shutdown callback was not called for fatal error"

    def test_no_shutdown_when_auto_shutdown_disabled(self):
        """Test that shutdown callback is NOT called when auto_shutdown_on_critical is False."""
        shutdown_called = threading.Event()
        
        def mock_shutdown():
            shutdown_called.set()
        
        logger = ErrorLogger(
            data_source_name="TEST",
            log_errors_to_csv=False,
            shutdown_callback=mock_shutdown,
            auto_shutdown_on_critical=False  # Disabled
        )
        
        # Log a critical error
        logger.log_critical_error("SYSTEM_FAILURE", "Critical system failure", "System is unstable")
        
        # Wait a short time and verify callback was NOT called
        assert not shutdown_called.wait(timeout=0.5), "Shutdown callback should not be called when auto_shutdown_on_critical is False"

    def test_no_shutdown_when_no_callback_provided(self):
        """Test that no shutdown occurs when no callback is provided."""
        logger = ErrorLogger(
            data_source_name="TEST",
            log_errors_to_csv=False,
            shutdown_callback=None,  # No callback
            auto_shutdown_on_critical=True
        )
        
        # This should not crash even with critical error and no callback
        logger.log_critical_error("SYSTEM_FAILURE", "Critical system failure", "System is unstable")

    def test_regular_errors_dont_trigger_shutdown(self):
        """Test that regular ERROR and WARNING level messages don't trigger shutdown."""
        shutdown_called = threading.Event()
        
        def mock_shutdown():
            shutdown_called.set()
        
        logger = ErrorLogger(
            data_source_name="TEST",
            log_errors_to_csv=False,
            shutdown_callback=mock_shutdown,
            auto_shutdown_on_critical=True
        )
        
        # Log regular errors and warnings
        logger.log_error("ERROR", "NORMAL_ERROR", "Normal error", "Details")
        logger.log_error("WARNING", "NORMAL_WARNING", "Normal warning", "Details")
        logger.log_error("INFO", "NORMAL_INFO", "Normal info", "Details")
        
        # Wait a short time and verify callback was NOT called
        assert not shutdown_called.wait(timeout=0.5), "Shutdown callback should not be called for non-critical errors"

    def test_shutdown_only_called_once(self):
        """Test that shutdown callback is only called once even with multiple critical errors."""
        shutdown_call_count = [0]  # Use list to allow modification in nested function
        
        def mock_shutdown():
            shutdown_call_count[0] += 1
        
        logger = ErrorLogger(
            data_source_name="TEST",
            log_errors_to_csv=False,
            shutdown_callback=mock_shutdown,
            auto_shutdown_on_critical=True
        )
        
        # Log multiple critical errors
        logger.log_critical_error("SYSTEM_FAILURE_1", "First critical error", "Details 1")
        logger.log_critical_error("SYSTEM_FAILURE_2", "Second critical error", "Details 2")
        logger.log_fatal_error("FATAL_ERROR", "Fatal error", "Details 3")
        
        # Wait for any callbacks to complete
        time.sleep(0.1)
        
        # Verify shutdown was only called once
        assert shutdown_call_count[0] == 1, f"Shutdown callback should be called exactly once, but was called {shutdown_call_count[0]} times"

    def test_emergency_shutdown_method(self):
        """Test the manual emergency shutdown method."""
        shutdown_called = threading.Event()
        
        def mock_shutdown():
            shutdown_called.set()
        
        logger = ErrorLogger(
            data_source_name="TEST",
            log_errors_to_csv=False,
            shutdown_callback=mock_shutdown,
            auto_shutdown_on_critical=True
        )
        
        # Trigger emergency shutdown
        logger.trigger_emergency_shutdown("Testing emergency shutdown")
        
        # Wait for callback to be called
        assert shutdown_called.wait(timeout=1.0), "Emergency shutdown should trigger shutdown callback"

    @patch('os.kill')
    def test_fallback_sigint_when_callback_fails(self, mock_kill):
        """Test that SIGINT is sent as fallback when shutdown callback fails."""
        def failing_shutdown():
            raise Exception("Shutdown callback failed")
        
        logger = ErrorLogger(
            data_source_name="TEST",
            log_errors_to_csv=False,
            shutdown_callback=failing_shutdown,
            auto_shutdown_on_critical=True
        )
        
        # Log critical error with failing callback
        logger.log_critical_error("SYSTEM_FAILURE", "Critical error with failing callback", "Details")
        
        # Wait for fallback to execute
        time.sleep(0.1)
        
        # Verify SIGINT was sent as fallback
        mock_kill.assert_called_once()
        call_args = mock_kill.call_args[0]
        assert call_args[0] == os.getpid(), "SIGINT should be sent to current process"
        assert call_args[1] == 2, "Signal should be SIGINT (2)"  # signal.SIGINT = 2

    def test_critical_severities_constant(self):
        """Test that the CRITICAL_SEVERITIES constant contains expected values."""
        assert "CRITICAL" in ErrorLogger.CRITICAL_SEVERITIES
        assert "FATAL" in ErrorLogger.CRITICAL_SEVERITIES
        assert "ERROR" not in ErrorLogger.CRITICAL_SEVERITIES
        assert "WARNING" not in ErrorLogger.CRITICAL_SEVERITIES

    def test_url_normalization_in_authorization_errors(self):
        """Test that URLs with date ranges are normalized in authorization errors."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="POLYGON",
                log_errors_to_csv=True
            )
            
            # Test different URL patterns that should be normalized
            test_cases = [
                {
                    'url': '/v2/aggs/ticker/SPY/range/1/minute/2024-01-01/2024-01-31',
                    'expected_pattern': '/v2/aggs/ticker/SPY/range/1/minute/<DATE_RANGE>'
                },
                {
                    'url': '/v2/aggs/ticker/AAPL/range/1/day/2023-12-01/2023-12-31', 
                    'expected_pattern': '/v2/aggs/ticker/AAPL/range/1/day/<DATE_RANGE>'
                }
            ]
            
            for i, test_case in enumerate(test_cases):
                error_msg = f'{{"status":"NOT_AUTHORIZED","request_id":"test-{i}","message":"Test error {i}"}}'
                logger.log_authorization_error(
                    url=test_case['url'],
                    operation="HTTP GET request",
                    error_details=error_msg
                )
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + test case rows
                assert len(rows) == len(test_cases) + 1
                
                # Check that URLs are normalized in details
                for i, test_case in enumerate(test_cases):
                    row = rows[i + 1]
                    assert test_case['expected_pattern'] in row[4], f"Expected normalized URL pattern '{test_case['expected_pattern']}' not found in details: {row[4]}"
                    assert test_case['url'] not in row[4], f"Original URL should be normalized, but found: {row[4]}"

    def test_request_id_redaction_in_authorization_errors(self):
        """Test that request IDs are redacted in authorization error details."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="POLYGON",
                log_errors_to_csv=True
            )
            
            # Test different request ID patterns
            test_cases = [
                '{"status":"NOT_AUTHORIZED","request_id":"8d40e43d65b20250455760ccfa4e3e0b","message":"Test"}',
                '{"status":"NOT_AUTHORIZED","request_id":"abc123def456","message":"Test"}',
                'Error occurred with request_id=xyz789abc123 in the system'
            ]
            
            for i, error_msg in enumerate(test_cases):
                logger.log_authorization_error(
                    url=f"/test/url/{i}",
                    operation="HTTP GET request",
                    error_details=error_msg
                )
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Check that request IDs are redacted
                for i, original_error in enumerate(test_cases):
                    row = rows[i + 1]
                    details = row[4]
                    
                    # Should contain redacted placeholder
                    assert '"request_id":"<REDACTED>"' in details or 'request_id=<REDACTED>' in details
                    
                    # Should not contain original request IDs
                    if 'request_id":"' in original_error:
                        # Extract original request ID
                        import re as regex_module
                        match = regex_module.search(r'"request_id":"([^"]*)"', original_error)
                        if match and match.group(1) != '<REDACTED>':
                            original_id = match.group(1)
                            assert original_id not in details, f"Original request ID {original_id} should be redacted"
                    elif 'request_id=' in original_error:
                        import re as regex_module
                        match = regex_module.search(r'request_id=([^\s,]*)', original_error)
                        if match and match.group(1) != '<REDACTED>':
                            original_id = match.group(1)
                            assert original_id not in details, f"Original request ID {original_id} should be redacted"

    def test_normalization_different_error_types(self):
        """Test that normalization is applied correctly for different error types."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "test_errors.csv"
            
            # Create ErrorLogger with CSV logging enabled
            logger = ErrorLogger(
                errors_csv=str(csv_path),
                data_source_name="POLYGON",
                log_errors_to_csv=True
            )
            
            # Test rate limit error normalization
            logger.log_rate_limit(
                wait_time=60,
                url="/v2/aggs/ticker/SPY/range/1/minute/2024-01-01/2024-01-31",
                error_details="Rate limit exceeded for date range query"
            )
            
            # Test authorization error normalization  
            logger.log_authorization_error(
                url="/v2/aggs/ticker/AAPL/range/1/day/2024-02-01/2024-02-28",
                operation="HTTP GET request",
                error_details='{"status":"NOT_AUTHORIZED","request_id":"test123","message":"Not authorized"}'
            )
            
            # Test regular API error (should not be normalized)
            logger.log_api_error(
                exception=ValueError("Test error"),
                url="/v2/aggs/ticker/MSFT/range/1/minute/2024-03-01/2024-03-31",
                operation="HTTP GET request"
            )
            
            # Check file contents
            with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = list(reader)
                
                # Should have header + 3 error rows
                assert len(rows) == 4
                
                # Rate limit error should have normalized URL
                rate_limit_row = next(row for row in rows[1:] if 'RATE_LIMIT_EXCEEDED' in row[1])
                assert '<DATE_RANGE>' in rate_limit_row[4]
                
                # Authorization error should have normalized URL and redacted request ID
                auth_row = next(row for row in rows[1:] if 'NOT_AUTHORIZED' in row[1])
                assert '<DATE_RANGE>' in auth_row[4]
                assert '<REDACTED>' in auth_row[4]
                
                # Regular API error should NOT have normalization
                api_error_row = next(row for row in rows[1:] if 'API_VALUEERROR' in row[1])
                assert '2024-03-01/2024-03-31' in api_error_row[4]  # Original dates preserved

    def test_error_logger_initialization_parameters(self):
        """Test ErrorLogger initialization with various parameter combinations."""
        # Test default initialization
        logger1 = ErrorLogger()
        assert logger1.errors_csv == "errors.csv"
        assert logger1.data_source_name == "UNKNOWN"
        assert logger1.log_errors_to_csv == False
        assert logger1.shutdown_callback is None
        assert logger1.auto_shutdown_on_critical == False
        
        # Test custom initialization
        def mock_callback():
            pass
            
        logger2 = ErrorLogger(
            errors_csv="/custom/path/errors.csv",
            data_source_name="CUSTOM_SOURCE",
            log_errors_to_csv=True,
            shutdown_callback=mock_callback,
            auto_shutdown_on_critical=True
        )
        assert logger2.errors_csv == "/custom/path/errors.csv"
        assert logger2.data_source_name == "CUSTOM_SOURCE"
        assert logger2.log_errors_to_csv == True
        assert logger2.shutdown_callback == mock_callback
        assert logger2.auto_shutdown_on_critical == True
        
        # Test data source name normalization
        logger3 = ErrorLogger(data_source_name="lowercase_source")
        assert logger3.data_source_name == "LOWERCASE_SOURCE"
