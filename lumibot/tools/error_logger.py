# Generic error logging utility for backtesting data sources
import csv
import logging
import os
import re
import signal
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple, Callable


class ErrorLogger:
    """
    A reusable error logging utility for backtesting data sources.
    
    This class provides structured CSV error logging with severity levels and 
    descriptive error codes that can be used across different data sources 
    like Polygon, Yahoo, ThetaData, etc.
    
    The logger supports automatic shutdown when critical errors occur, making it
    suitable for mission-critical trading systems that need to fail fast.
    """
    
    # Severity levels that trigger automatic shutdown
    CRITICAL_SEVERITIES = {"CRITICAL", "FATAL"}
    
    def __init__(
        self, 
        errors_csv: Optional[str] = None, 
        data_source_name: str = "UNKNOWN", 
        log_errors_to_csv: bool = False,
        shutdown_callback: Optional[Callable] = None,
        auto_shutdown_on_critical: bool = False
    ):
        """
        Initialize the error logger.
        
        Parameters
        ----------
        errors_csv : str, optional
            Path to the CSV error log file. Defaults to "errors.csv".
        data_source_name : str
            Name of the data source (e.g., "POLYGON", "YAHOO", "THETADATA").
        log_errors_to_csv : bool
            Whether to log errors to CSV file.
        shutdown_callback : callable, optional
            Function to call when critical errors occur. Should accept no parameters.
            Examples: trader._stop_pool, sys.exit, or custom shutdown handlers.
        auto_shutdown_on_critical : bool
            Whether to automatically shutdown when CRITICAL or FATAL errors are logged.
            Requires shutdown_callback to be set.
        """
        self.errors_csv = errors_csv if errors_csv is not None else "errors.csv"
        self.data_source_name = data_source_name.upper()
        self.log_errors_to_csv = log_errors_to_csv
        self.shutdown_callback = shutdown_callback
        self.auto_shutdown_on_critical = auto_shutdown_on_critical
        self._csv_lock = threading.Lock()  # Thread safety for CSV writes
        self._csv_initialized = False  # Track if CSV has been initialized
        self._error_counts: Dict[Tuple[str, str, str, str], int] = {}  # Track error counts for deduplication
        self._shutdown_triggered = False  # Prevent multiple shutdown calls
    
    def _ensure_csv_initialized(self):
        """Initialize the errors CSV file with headers if it doesn't exist and hasn't been initialized."""
        if self.log_errors_to_csv and not self._csv_initialized:
            # Create directory if it doesn't exist
            Path(self.errors_csv).parent.mkdir(parents=True, exist_ok=True)
            
            if not os.path.exists(self.errors_csv):
                with open(self.errors_csv, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['severity', 'error_code', 'timestamp', 'message', 'details', 'count'])
            else:
                # Load existing error counts from CSV
                self._load_existing_error_counts()
            
            self._csv_initialized = True
    
    def _load_existing_error_counts(self):
        """Load existing error counts from the CSV file for deduplication."""
        try:
            with open(self.errors_csv, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Create key for deduplication using normalized details
                    normalized_details = self._normalize_error_details(row['details'], row['error_code'])
                    key = (row['severity'], row['error_code'], row['message'], normalized_details)
                    count = int(row.get('count', 1))  # Default to 1 for backwards compatibility
                    self._error_counts[key] = count
        except (FileNotFoundError, KeyError, ValueError):
            # If file doesn't exist or has format issues, start fresh
            self._error_counts = {}
    
    def _rewrite_csv_with_updated_counts(self):
        """Rewrite the entire CSV file with updated counts."""
        # Create a temporary file first to ensure atomicity
        temp_file = self.errors_csv + '.tmp'
        try:
            with open(temp_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['severity', 'error_code', 'timestamp', 'message', 'details', 'count'])
                
                # Write all known errors with their counts
                for (severity, error_code, message, details), count in self._error_counts.items():
                    timestamp = datetime.now().isoformat()  # Use current timestamp for updates
                    writer.writerow([severity, error_code, timestamp, message, details, count])
            
            # Atomically replace the original file
            os.replace(temp_file, self.errors_csv)
        except Exception as e:
            # Clean up temp file if something went wrong
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise e
        
    def _normalize_error_details(self, details: str, error_code: str) -> str:
        """
        Normalize error details for deduplication purposes.
        
        This removes unique identifiers like request_ids and normalizes date ranges
        to allow proper grouping of similar errors.
        
        Parameters
        ----------
        details : str
            The original error details.
        error_code : str
            The error code (with data source prefix).
            
        Returns
        -------
        str
            Normalized error details for deduplication.
        """
        
        normalized = details
        
        # For authorization errors, remove request_id and normalize date ranges
        if "NOT_AUTHORIZED" in error_code:
            # Remove request_id patterns like "request_id":"6ae96aa7620285a5404c5f7fd8832456"
            normalized = re.sub(r'"request_id":"[^"]*"', '"request_id":"<REDACTED>"', normalized)
            normalized = re.sub(r'request_id=[^,\s]*', 'request_id=<REDACTED>', normalized)
            
            # Normalize date ranges in URLs to group similar authorization errors
            # Pattern: /YYYY-MM-DD/YYYY-MM-DD -> /<DATE_RANGE>
            normalized = re.sub(r'/\d{4}-\d{2}-\d{2}/\d{4}-\d{2}-\d{2}', '/<DATE_RANGE>', normalized)
            
            # Also normalize other date patterns that might appear
            normalized = re.sub(r'\d{4}-\d{2}-\d{2}', '<DATE>', normalized)
        
        # For rate limit errors, normalize URLs and wait times for better grouping
        elif "RATE_LIMIT" in error_code:
            # Normalize date ranges in URLs
            normalized = re.sub(r'/\d{4}-\d{2}-\d{2}/\d{4}-\d{2}-\d{2}', '/<DATE_RANGE>', normalized)
            normalized = re.sub(r'\d{4}-\d{2}-\d{2}', '<DATE>', normalized)
        
        return normalized

    def log_error(self, severity: str, error_code: str, message: str, details: str = ""):
        """
        Log an error to the CSV file with deduplication.
        
        If the same error (based on severity, error_code, message, normalized details) has been 
        logged before, increment its count instead of adding a new row.
        
        For CRITICAL or FATAL severity levels, this method can automatically trigger
        system shutdown if auto_shutdown_on_critical is enabled.
        
        Parameters
        ----------
        severity : str
            The severity level ("ERROR", "WARNING", "INFO", "CRITICAL", "FATAL").
            CRITICAL and FATAL levels can trigger automatic shutdown.
        error_code : str
            A descriptive error code (e.g., "RATE_LIMIT_EXCEEDED", "API_CONNECTION_FAILED").
        message : str
            A human-readable error message.
        details : str, optional
            Additional details about the error (e.g., URL, parameters, stack trace).
        """
        timestamp = datetime.now().isoformat()
        severity = severity.upper()  # Normalize severity
        
        # Prefix error code with data source name for clarity
        full_error_code = f"{self.data_source_name}_{error_code}"
        
        if self.log_errors_to_csv:
            try:
                with self._csv_lock:  # Thread-safe CSV writing
                    self._ensure_csv_initialized()  # Only create CSV when needed
                    
                    # Normalize details for deduplication
                    normalized_details = self._normalize_error_details(details, full_error_code)
                    
                    # Create key for deduplication using normalized details
                    error_key = (severity, full_error_code, message, normalized_details)
                    
                    if error_key in self._error_counts:
                        # Increment count for existing error
                        self._error_counts[error_key] += 1
                    else:
                        # New error, initialize count to 1
                        self._error_counts[error_key] = 1
                    
                    # Rewrite the entire CSV with updated counts
                    # This ensures consistency and handles the deduplication properly
                    self._rewrite_csv_with_updated_counts()
                    
            except Exception as e:
                # Fallback logging if CSV writing fails
                logging.error(f"Failed to write to errors CSV: {e}")
        else:
            # Log to console when CSV logging is disabled
            logging.log(
                getattr(logging, severity, logging.INFO),
                f"{self.data_source_name} - {full_error_code}: {message} | {details}"
            )
        
        # Check if this is a critical error that should trigger shutdown
        self._handle_critical_error(severity, full_error_code, message, details)
        
    def _handle_critical_error(self, severity: str, error_code: str, message: str, details: str):
        """
        Handle critical errors that may require immediate shutdown.
        
        Parameters
        ----------
        severity : str
            The severity level of the error.
        error_code : str
            The full error code (with data source prefix).
        message : str
            The error message.
        details : str
            Additional error details.
        """
        if (severity in self.CRITICAL_SEVERITIES and 
            self.auto_shutdown_on_critical and 
            self.shutdown_callback is not None and 
            not self._shutdown_triggered):
            
            self._shutdown_triggered = True
            
            # Log the shutdown trigger
            critical_msg = (
                f"CRITICAL ERROR DETECTED - INITIATING EMERGENCY SHUTDOWN: "
                f"{error_code}: {message}"
            )
            logging.critical(critical_msg)
            
            try:
                # Call the shutdown callback
                logging.critical(f"Calling shutdown callback for critical error: {error_code}")
                self.shutdown_callback()
            except Exception as e:
                logging.error(f"Failed to execute shutdown callback: {e}")
                # As a last resort, try to send SIGINT to self
                try:
                    os.kill(os.getpid(), signal.SIGINT)
                except Exception as e2:
                    logging.error(f"Failed to send SIGINT: {e2}")
    
    def trigger_emergency_shutdown(self, reason: str = "Manual shutdown requested"):
        """
        Manually trigger an emergency shutdown.
        
        Parameters
        ----------
        reason : str
            The reason for the emergency shutdown.
        """
        self.log_error(
            severity="CRITICAL",
            error_code="EMERGENCY_SHUTDOWN",
            message=reason,
            details="Manual emergency shutdown triggered"
        )
    
    def log_rate_limit(self, wait_time: int, url: str = "", error_details: str = ""):
        """
        Convenience method for logging rate limit errors.
        
        Parameters
        ----------
        wait_time : int
            The wait time in seconds before retrying.
        url : str, optional
            The URL that was rate limited.
        error_details : str, optional
            Additional error details.
        """
        self.log_error(
            severity="WARNING",
            error_code="RATE_LIMIT_EXCEEDED",
            message=f"{self.data_source_name} rate limit reached",
            details=f"URL: {url}, Wait time: {wait_time}s, Error: {error_details}"
        )
    
    def log_api_error(self, exception: Exception, url: str = "", operation: str = ""):
        """
        Convenience method for logging API errors.
        
        Parameters
        ----------
        exception : Exception
            The exception that occurred.
        url : str, optional
            The URL that caused the error.
        operation : str, optional
            The operation being performed when the error occurred.
        """
        error_type = type(exception).__name__
        self.log_error(
            severity="ERROR",
            error_code=f"API_{error_type.upper()}",
            message=f"{self.data_source_name} API error: {error_type}",
            details=f"URL: {url}, Operation: {operation}, Error: {str(exception)}"
        )
    
    def log_data_error(self, symbol: str, error_details: str = ""):
        """
        Convenience method for logging data retrieval errors.
        
        Parameters
        ----------
        symbol : str
            The symbol that had data issues.
        error_details : str, optional
            Additional error details.
        """
        self.log_error(
            severity="ERROR",
            error_code="DATA_RETRIEVAL_FAILED",
            message=f"Failed to retrieve data for {symbol}",
            details=error_details
        )
    
    def log_validation_error(self, validation_type: str, error_details: str = ""):
        """
        Convenience method for logging validation errors.
        
        Parameters
        ----------
        validation_type : str
            The type of validation that failed (e.g., "SYMBOL", "DATE_RANGE").
        error_details : str, optional
            Additional error details.
        """
        self.log_error(
            severity="ERROR",
            error_code=f"VALIDATION_{validation_type.upper()}_FAILED",
            message=f"{validation_type} validation failed",
            details=error_details
        )
    
    def log_cache_error(self, operation: str, file_path: str = "", error_details: str = ""):
        """
        Convenience method for logging cache-related errors.
        
        Parameters
        ----------
        operation : str
            The cache operation that failed (e.g., "READ", "WRITE", "DELETE").
        file_path : str, optional
            The file path involved in the operation.
        error_details : str, optional
            Additional error details.
        """
        self.log_error(
            severity="WARNING",
            error_code=f"CACHE_{operation.upper()}_FAILED",
            message=f"Cache {operation.lower()} operation failed",
            details=f"File: {file_path}, Error: {error_details}"
        )
    
    def log_authorization_error(self, url: str = "", operation: str = "", error_details: str = ""):
        """
        Convenience method for logging authorization/entitlement errors.
        
        For Polygon data source, authorization errors are treated as CRITICAL since they 
        indicate insufficient subscription level which cannot be resolved without manual 
        intervention. For other data sources, they are treated as ERROR level.
        
        Parameters
        ----------
        url : str, optional
            The URL that caused the authorization error.
        operation : str, optional
            The operation being performed when the error occurred.
        error_details : str, optional
            Additional error details from the API response.
        """
        # Polygon authorization errors are critical as they require subscription upgrade
        severity = "CRITICAL" if self.data_source_name == "POLYGON" else "ERROR"
        
        self.log_error(
            severity=severity,
            error_code="NOT_AUTHORIZED",
            message=f"{self.data_source_name} authorization error - insufficient permissions or subscription level",
            details=f"URL: {url}, Operation: {operation}, Error: {error_details}"
        )
    
    def log_critical_error(self, error_code: str, message: str, details: str = ""):
        """
        Convenience method for logging critical errors that trigger shutdown.
        
        Parameters
        ----------
        error_code : str
            A descriptive error code.
        message : str
            A human-readable error message.
        details : str, optional
            Additional error details.
        """
        self.log_error(
            severity="CRITICAL",
            error_code=error_code,
            message=message,
            details=details
        )
    
    def log_fatal_error(self, error_code: str, message: str, details: str = ""):
        """
        Convenience method for logging fatal errors that trigger shutdown.
        
        Parameters
        ----------
        error_code : str
            A descriptive error code.
        message : str
            A human-readable error message.
        details : str, optional
            Additional error details.
        """
        self.log_error(
            severity="FATAL",
            error_code=error_code,
            message=message,
            details=details
        )
