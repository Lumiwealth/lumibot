# Generic error logging utility for backtesting data sources
import csv
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional


class ErrorLogger:
    """
    A reusable error logging utility for backtesting data sources.
    
    This class provides structured CSV error logging with severity levels and 
    descriptive error codes that can be used across different data sources 
    like Polygon, Yahoo, ThetaData, etc.
    """
    
    def __init__(self, errors_csv: Optional[str] = None, data_source_name: str = "UNKNOWN", log_errors_to_csv = False):
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
        """
        self.errors_csv = errors_csv if errors_csv is not None else "errors.csv"
        self.data_source_name = data_source_name.upper()
        self.log_errors_to_csv = log_errors_to_csv
        self._csv_lock = threading.Lock()  # Thread safety for CSV writes
        self._csv_initialized = False  # Track if CSV has been initialized
    
    def _ensure_csv_initialized(self):
        """Initialize the errors CSV file with headers if it doesn't exist and hasn't been initialized."""
        if self.log_errors_to_csv and not self._csv_initialized:
            # Create directory if it doesn't exist
            Path(self.errors_csv).parent.mkdir(parents=True, exist_ok=True)
            
            if not os.path.exists(self.errors_csv):
                with open(self.errors_csv, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['severity', 'error_code', 'timestamp', 'message', 'details'])
            
            self._csv_initialized = True
        
    def log_error(self, severity: str, error_code: str, message: str, details: str = ""):
        """
        Log an error to the CSV file.
        
        Parameters
        ----------
        severity : str
            The severity level ("ERROR", "WARNING", "INFO").
        error_code : str
            A descriptive error code (e.g., "RATE_LIMIT_EXCEEDED", "API_CONNECTION_FAILED").
        message : str
            A human-readable error message.
        details : str, optional
            Additional details about the error (e.g., URL, parameters, stack trace).
        """
        timestamp = datetime.now().isoformat()
        
        # Prefix error code with data source name for clarity
        full_error_code = f"{self.data_source_name}_{error_code}"
        
        if self.log_errors_to_csv:
            try:
                with self._csv_lock:  # Thread-safe CSV writing
                    self._ensure_csv_initialized()  # Only create CSV when needed
                    with open(self.errors_csv, 'a', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow([severity, full_error_code, timestamp, message, details])
            except Exception as e:
                # Fallback logging if CSV writing fails
                logging.error(f"Failed to write to errors CSV: {e}")
        else:
            # Log to console when CSV logging is disabled
            logging.log(
                getattr(logging, severity, logging.INFO),
                f"{self.data_source_name} - {full_error_code}: {message} | {details}"
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
        
        Parameters
        ----------
        url : str, optional
            The URL that caused the authorization error.
        operation : str, optional
            The operation being performed when the error occurred.
        error_details : str, optional
            Additional error details from the API response.
        """
        self.log_error(
            severity="ERROR",
            error_code="NOT_AUTHORIZED",
            message=f"{self.data_source_name} authorization error - insufficient permissions or subscription level",
            details=f"URL: {url}, Operation: {operation}, Error: {error_details}"
        )
