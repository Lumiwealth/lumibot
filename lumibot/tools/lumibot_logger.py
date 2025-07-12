"""
Unified logger for Lumibot.

This module provides a centralized logging solution for the entire Lumibot codebase,
ensuring consistent formatting, behavior, and ease of use across all modules.

Features:
- Standard console/file logging with enhanced formatting
- Optional CSV error logging with deduplication via CSVErrorHandler
- Strategy-specific logging with context
- Thread-safe operations
- External library noise reduction
- Backtesting quiet logs support
- Centralized environment variable handling (avoids circular imports with credentials.py)

Environment Variables (all handled centrally in this module):
- LUMIBOT_LOG_LEVEL: Set global log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- LOG_ERRORS_TO_CSV: Enable CSV error logging (true/false)
- LUMIBOT_ERROR_CSV_PATH: Path for CSV error log file (default: logs/errors.csv)
- BACKTESTING_QUIET_LOGS: Enable quiet logs for backtesting (true/false) - only shows ERROR+ messages

Note: Some logging-related environment variables are also available in credentials.py 
for backwards compatibility, but this module is the authoritative source for configuration.

Usage:
    from lumibot.tools.lumibot_logger import get_logger
    
    logger = get_logger(__name__)
    logger.info("This is an info message")
    logger.warning("This is a warning")
    logger.error("This is an error")  # Auto-saved to CSV if LOG_ERRORS_TO_CSV=true

For strategy-specific logging (with strategy name prefix):
    from lumibot.tools.lumibot_logger import get_strategy_logger
    
    logger = get_strategy_logger(__name__, strategy_name="MyStrategy")
    logger.info("This message will include [MyStrategy] prefix")
"""

import csv
import logging
import os
import re
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple
from functools import lru_cache

class CSVErrorHandler(logging.Handler):
    """
    Handler that writes ERROR and WARNING messages to CSV with deduplication.
    """
    
    def __init__(self, csv_path: str):
        super().__init__(level=logging.WARNING)
        self.csv_path = os.path.abspath(csv_path)
        self._error_counts: Dict[Tuple[str, str, str, str], int] = {}
        self._csv_lock = threading.Lock()
        self._csv_initialized = False
        self._auto_shutdown_on_critical = True
    
    def _ensure_csv_initialized(self):
        """Initialize the errors CSV file with headers if needed."""
        if not self._csv_initialized:
            Path(self.csv_path).parent.mkdir(parents=True, exist_ok=True)
            
            if not os.path.exists(self.csv_path):
                with open(self.csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(['severity', 'error_code', 'timestamp', 'message', 'details', 'count'])
            else:
                self._load_existing_error_counts()
            
            self._csv_initialized = True
    
    def _load_existing_error_counts(self):
        """Load existing error counts from CSV for deduplication."""
        try:
            with open(self.csv_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    normalized_details = self._normalize_error_details(row['details'], row.get('error_code', ''))
                    key = (row['severity'], row.get('error_code', ''), row['message'], normalized_details)
                    count = int(row.get('count', 1))
                    self._error_counts[key] = count
        except (FileNotFoundError, KeyError, ValueError):
            self._error_counts = {}
    
    def _rewrite_csv_with_updated_counts(self):
        """Rewrite CSV file with updated error counts."""
        temp_file = self.csv_path + '.tmp'
        try:
            with open(temp_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['severity', 'error_code', 'timestamp', 'message', 'details', 'count'])
                
                for (severity, error_code, message, details), count in self._error_counts.items():
                    timestamp = datetime.now().isoformat()
                    writer.writerow([severity, error_code, timestamp, message, details, count])
            
            os.replace(temp_file, self.csv_path)
        except Exception as e:
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise e
    
    def _normalize_error_details(self, details: str, error_code: str) -> str:
        """Normalize error details for deduplication by removing dynamic values."""
        normalized = details
        
        # Remove request IDs and session IDs
        normalized = re.sub(r'"request_id":"[^"]*"', '"request_id":"<REDACTED>"', normalized)
        normalized = re.sub(r'request_id=[^,\s]*', 'request_id=<REDACTED>', normalized)
        
        # Normalize date ranges and timestamps
        normalized = re.sub(r'/\d{4}-\d{2}-\d{2}/\d{4}-\d{2}-\d{2}', '/<DATE_RANGE>', normalized)
        normalized = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', '<TIMESTAMP>', normalized)
        normalized = re.sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', '<TIMESTAMP>', normalized)
        normalized = re.sub(r'\d{4}-\d{2}-\d{2}', '<DATE>', normalized)
        
        return normalized
    
    def _extract_error_details(self, record: logging.LogRecord) -> Tuple[str, str, str]:
        """Extract error code and details from log record."""
        message = record.getMessage()
        
        # Try to extract structured error code if message has format "ERROR_CODE: message | details"
        error_code = ""
        details = ""
        
        if ":" in message and "|" in message:
            parts = message.split(":", 1)
            if len(parts) == 2:
                potential_error_code = parts[0].strip()
                rest = parts[1].strip()
                
                if "|" in rest:
                    msg_part, details_part = rest.split("|", 1)
                    error_code = potential_error_code
                    message = msg_part.strip()
                    details = details_part.strip()
        
        # Fallback: create error code from logger name
        if not error_code:
            logger_name = record.name.split('.')[-1].upper()
            error_code = f"{logger_name}_{record.levelname}"
            details = f"File: {record.pathname}:{record.lineno}, Function: {record.funcName}"
        
        return error_code, message, details
    
    def _trigger_emergency_shutdown(self, record: logging.LogRecord, error_code: str, message: str):
        """Trigger emergency shutdown when a CRITICAL error is logged."""
        import sys
        import time
        
        # Print emergency message to stderr
        emergency_msg = f"""
{'='*60}
🚨 CRITICAL ERROR DETECTED - EMERGENCY SHUTDOWN 🚨
{'='*60}
Error Code: {error_code}
Message: {message}
File: {record.pathname}:{record.lineno}
Function: {record.funcName}
Time: {datetime.now().isoformat()}

The application is shutting down immediately to prevent
potential data corruption or unsafe trading operations.
{'='*60}
"""
        print(emergency_msg, file=sys.stderr, flush=True)
        
        # Try to flush all logging handlers
        try:
            logging.shutdown()
        except:
            pass
        
        # Small delay to ensure message is visible
        time.sleep(0.1)
        
        # Emergency exit
        sys.exit(1)
    
    def emit(self, record):
        """Handle a log record by writing to CSV."""
        if record.levelno < logging.WARNING:
            return
            
        try:
            with self._csv_lock:
                self._ensure_csv_initialized()
                
                severity = record.levelname
                error_code, message, details = self._extract_error_details(record)
                
                normalized_details = self._normalize_error_details(details, error_code)
                error_key = (severity, error_code, message, normalized_details)
                
                if error_key in self._error_counts:
                    self._error_counts[error_key] += 1
                else:
                    self._error_counts[error_key] = 1
                
                self._rewrite_csv_with_updated_counts()
                
                # Emergency shutdown on CRITICAL messages
                if record.levelno >= logging.CRITICAL and self._auto_shutdown_on_critical:
                    self._trigger_emergency_shutdown(record, error_code, message)
                
        except Exception:
            # Don't let CSV logging errors break the main application
            pass


class LumibotLogger(logging.Logger):
    """
    Enhanced Logger class for Lumibot with consistent formatting.
    """
    
    def __init__(self, name: str, level=logging.NOTSET):
        super().__init__(name, level)


class LumibotFormatter(logging.Formatter):
    """
    Custom formatter for Lumibot that provides consistent formatting
    and includes source information for warnings and errors.
    """
    
    def __init__(self):
        # Define format strings for different log levels
        self.info_format = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        self.warning_format = "%(asctime)s | %(levelname)s | %(pathname)s:%(funcName)s:%(lineno)d | %(message)s"
        self.error_format = "%(asctime)s | %(levelname)s | %(pathname)s:%(funcName)s:%(lineno)d | %(message)s"
        
        # Create formatters for each level
        self.info_formatter = logging.Formatter(
            self.info_format,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.warning_formatter = logging.Formatter(
            self.warning_format,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.error_formatter = logging.Formatter(
            self.error_format,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def format(self, record):
        # Shorten the pathname to just the filename for cleaner output
        if hasattr(record, 'pathname'):
            record.pathname = os.path.basename(record.pathname)
        
        # Clean up the message by stripping leading/trailing whitespace and newlines
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            record.msg = record.msg.strip()
        
        # Also clean up the formatted message if it exists
        original_message = record.getMessage()
        cleaned_message = original_message.strip()
        
        # Temporarily replace the getMessage method to return cleaned message
        def clean_getMessage():
            return cleaned_message
        
        original_getMessage = record.getMessage
        record.getMessage = clean_getMessage
        
        try:
            # Choose formatter based on log level
            if record.levelno >= logging.ERROR:
                formatted = self.error_formatter.format(record)
            elif record.levelno >= logging.WARNING:
                formatted = self.warning_formatter.format(record)
            else:
                formatted = self.info_formatter.format(record)
        finally:
            # Restore original getMessage method
            record.getMessage = original_getMessage
        
        return formatted


# Global registry to track created loggers and their handlers
_logger_registry: Dict[str, logging.Logger] = {}
_strategy_logger_registry: Dict[str, 'StrategyLoggerAdapter'] = {}
_handlers_configured = False
_config_lock = threading.Lock()


class StrategyLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds strategy name prefix to log messages.
    This provides context for which strategy is generating the log message.
    """
    
    def __init__(self, logger, strategy_name: str):
        super().__init__(logger, {'strategy_name': strategy_name})
        self.strategy_name = strategy_name
    
    def process(self, msg, kwargs):
        # Clean up the message by stripping leading/trailing whitespace and newlines
        if isinstance(msg, str):
            msg = msg.strip()
        
        # Add strategy name prefix to all messages
        return f"[{self.strategy_name}] {msg}", kwargs
    
    def update_strategy_name(self, new_strategy_name: str):
        """Update the strategy name for this logger adapter."""
        self.strategy_name = new_strategy_name
        # Update the extra dict with new strategy name
        if self.extra is not None:
            extra_dict = dict(self.extra)  # Create a mutable copy
            extra_dict['strategy_name'] = new_strategy_name
            self.extra = extra_dict


def _ensure_handlers_configured():
    """
    Ensure that the root logger has the appropriate handlers configured.
    This is called once globally to set up consistent formatting.
    Thread-safe implementation using double-checked locking pattern.
    
    Environment Variables Used:
    - LUMIBOT_LOG_LEVEL: Set global log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - LOG_ERRORS_TO_CSV: Enable CSV error logging (true/false)
    - LUMIBOT_ERROR_CSV_PATH: Path for CSV error log file (default: logs/errors.csv)
    - BACKTESTING_QUIET_LOGS: Enable quiet logs for backtesting (true/false)
    """
    global _handlers_configured
    
    if _handlers_configured:
        return

    with _config_lock:
        # Double-check pattern to avoid race conditions
        if _handlers_configured:
            return
        
        # Set the logger class to our custom LumibotLogger
        logging.setLoggerClass(LumibotLogger)
            
        # Get the root logger directly to avoid circular calls
        root_logger = logging.getLogger("lumibot")
        
        # Remove any existing handlers to avoid duplicates
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create console handler with our custom formatter
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(LumibotFormatter())
        
        # Set default level (can be overridden by environment variable)
        default_level = os.environ.get('LUMIBOT_LOG_LEVEL', 'INFO').upper()
        try:
            log_level = getattr(logging, default_level)
        except AttributeError:
            log_level = logging.INFO

        # Handle BACKTESTING_QUIET_LOGS environment variable
        backtesting_quiet = os.environ.get("BACKTESTING_QUIET_LOGS")
        if backtesting_quiet and backtesting_quiet.lower() == "true":
            # When quiet logs are enabled, only show ERROR and CRITICAL messages
            log_level = logging.ERROR
            console_handler.setLevel(logging.ERROR)
        else:
            # Set console handler to same level as logger
            console_handler.setLevel(log_level)
        
        root_logger.setLevel(log_level)
        root_logger.addHandler(console_handler)
        
        # Add CSV error handler if enabled
        log_errors_to_csv = os.environ.get("LOG_ERRORS_TO_CSV")
        if log_errors_to_csv and log_errors_to_csv.lower() in ("true", "1", "yes", "on"):
            csv_path = os.environ.get("LUMIBOT_ERROR_CSV_PATH", "logs/errors.csv")
            csv_handler = CSVErrorHandler(csv_path)
            root_logger.addHandler(csv_handler)
        
        # Keep propagation enabled for proper logging behavior
        root_logger.propagate = True
        
        _handlers_configured = True


@lru_cache(maxsize=128)
def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the given name with consistent Lumibot formatting.
    
    This function is cached to ensure that the same logger instance is returned
    for the same name, avoiding duplicate handler configuration.
    
    When LOG_ERRORS_TO_CSV is enabled, any logger.warning(), logger.error() or logger.critical() 
    calls will automatically be written to errors.csv with deduplication.
    
    CRITICAL messages will trigger an emergency shutdown of the application to prevent
    unsafe trading operations.
    
    Parameters
    ----------
    name : str
        The name for the logger, typically __name__ of the calling module.
        
    Returns
    -------
    logging.Logger
        A configured logger instance.
        
    Examples
    --------
    >>> from lumibot.tools.lumibot_logger import get_logger
    >>> logger = get_logger()
    >>> logger.info("Application started")
    >>> logger.warning("This is a warning with source info")
    >>> logger.error("This is an error with full source info")  # Auto-saved to CSV if enabled
    """
    # Ensure global handler configuration is done
    _ensure_handlers_configured()
    
    # Ensure the logger name is under the lumibot hierarchy
    if not name.startswith("lumibot"):
        # If the name doesn't start with lumibot, make it a child of lumibot
        logger_name = f"lumibot.{name}"
    else:
        logger_name = name
    
    # Get or create the logger
    logger = logging.getLogger(logger_name)
    
    # Store in registry for potential cleanup later
    _logger_registry[name] = logger
    
    return logger


def get_strategy_logger(name: str, strategy_name: str) -> StrategyLoggerAdapter:
    """
    Get a strategy-specific logger that includes the strategy name in all messages.
    
    This is particularly useful for broker and strategy classes where you want
    to clearly identify which strategy is generating the log messages.
    
    Parameters
    ----------
    name : str
        The name for the underlying logger, typically __name__ of the calling module.
    strategy_name : str
        The name of the strategy to include in log message prefixes.
        
    Returns
    -------
    StrategyLoggerAdapter
        A logger adapter that includes strategy name in all messages.
        
    Examples
    --------
    >>> from lumibot.tools.lumibot_logger import get_strategy_logger
    >>> logger = get_strategy_logger(__name__, "MyTradingStrategy")
    >>> logger.info("Portfolio rebalanced")  # Output: [MyTradingStrategy] Portfolio rebalanced
    >>> logger.error("Order failed")  # Output: [MyTradingStrategy] Order failed
    """
    # Create cache key combining logger name and strategy name
    cache_key = f"{name}::{strategy_name}"
    
    # Check if strategy logger already exists
    if cache_key in _strategy_logger_registry:
        return _strategy_logger_registry[cache_key]
    
    # Create new strategy logger
    base_logger = get_logger(name)
    strategy_logger = StrategyLoggerAdapter(base_logger, strategy_name)
    
    # Cache the strategy logger
    _strategy_logger_registry[cache_key] = strategy_logger
    
    return strategy_logger


def set_log_level(level: str):
    """
    Set the global log level for all Lumibot loggers.
    
    Parameters
    ----------
    level : str
        The log level to set ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
        
    Examples
    --------
    >>> from lumibot.tools.lumibot_logger import set_log_level
    >>> set_log_level('DEBUG')  # Enable debug logging
    >>> set_log_level('WARNING')  # Only show warnings and errors
    """
    try:
        log_level = getattr(logging, level.upper())
        root_logger = get_logger("root")
        root_logger.setLevel(log_level)
        
        # Also update all handlers to the new level
        for handler in root_logger.handlers:
            handler.setLevel(log_level)
        
        # Also update all existing loggers in our registry
        for logger in _logger_registry.values():
            logger.setLevel(log_level)
            
    except AttributeError:
        raise ValueError(f"Invalid log level: {level}. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL")


def add_file_handler(file_path: str, level: str = 'INFO'):
    """
    Add a file handler to the root logger to also log to a file.
    
    Parameters
    ----------
    file_path : str
        Path to the log file.
    level : str, optional
        Log level for the file handler. Defaults to 'INFO'.
        
    Examples
    --------
    >>> from lumibot.tools.lumibot_logger import add_file_handler
    >>> add_file_handler('/path/to/lumibot.log', 'DEBUG')
    """
    _ensure_handlers_configured()
    
    try:
        file_level = getattr(logging, level.upper())
    except AttributeError:
        raise ValueError(f"Invalid log level: {level}")
    
    # Create file handler
    file_handler = logging.FileHandler(file_path)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(LumibotFormatter())
    
    # Add to the actual root logger, not a logger named "root"
    root_logger = logging.getLogger("lumibot")
    root_logger.addHandler(file_handler)




# Initialize the logging system when the module is imported
_ensure_handlers_configured()
