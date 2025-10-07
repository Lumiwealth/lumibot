"""
Unified logger for Lumibot.

This module provides a centralized logging solution for the entire Lumibot codebase,
ensuring consistent formatting, behavior, and ease of use across all modules.

Features:
- Standard console/file logging with enhanced formatting
- Optional CSV error logging with deduplication via CSVErrorHandler
- Optional Botspot API error reporting via BotspotErrorHandler
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
- LUMIWEALTH_API_KEY: API key for Lumiwealth/Botspot error reporting (when set, enables automatic error reporting)
- BOTSPOT_RATE_LIMIT_WINDOW: Rate limit window in seconds (default: 60) - same errors are only sent once per window
- BOTSPOT_MAX_ERRORS_PER_MINUTE: Maximum total errors sent per minute (default: 100)

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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple, Optional
from functools import lru_cache
from enum import Enum

# Import LUMIWEALTH_API_KEY from credentials
try:
    from ..credentials import LUMIWEALTH_API_KEY
except ImportError:
    LUMIWEALTH_API_KEY = None

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


class BotspotSeverity(Enum):
    """Severity levels for Botspot error reporting."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class BotspotErrorHandler(logging.Handler):
    """
    Handler that reports errors to Botspot API endpoint.
    Only active when LUMIWEALTH_API_KEY is available.
    
    Includes rate limiting to prevent excessive API calls:
    - Same errors are only sent once per time window (default: 60 seconds)
    - Maximum total errors per minute (default: 100)
    """
    
    def __init__(self):
        # Only handle ERROR and CRITICAL messages for external reporting
        super().__init__(level=logging.ERROR)
        self.base_url = "https://api.botspot.trade/bots/report-bot-error"
        # Use LUMIWEALTH_API_KEY from credentials or environment
        self.api_key = LUMIWEALTH_API_KEY or os.environ.get("LUMIWEALTH_API_KEY")
        # Fingerprint state keyed by simplified fingerprint (error_code, filename, function, message_signature)
        # to reduce API spam while preserving differentiation between distinct error messages sharing same location.
        # fingerprint -> dict(last_sent: float|None, suppressed_count: int, total_count: int, last_details: str, last_message: str)
        self._fingerprints: Dict[Tuple[str, str, str, str], Dict[str, object]] = {}

        self._total_errors_sent = 0
        self._minute_start_time = time.time()
        self._lock = threading.Lock()

        # Rate limiting configuration
        self.rate_limit_window = int(os.environ.get("BOTSPOT_RATE_LIMIT_WINDOW", "60"))
        self.max_errors_per_minute = int(os.environ.get("BOTSPOT_MAX_ERRORS_PER_MINUTE", "100"))

        # Only import requests if we need it
        if self.api_key:
            try:
                import requests
                self.requests = requests
            except ImportError:
                logging.getLogger(__name__).warning(
                    "requests library not available - Botspot error reporting disabled"
                )
                self.requests = None
        else:
            self.requests = None
    
    def _map_log_level_to_severity(self, level: int) -> BotspotSeverity:
        """Map logging level to Botspot severity."""
        if level >= logging.CRITICAL:
            return BotspotSeverity.CRITICAL
        elif level >= logging.ERROR:
            return BotspotSeverity.CRITICAL  # Treat ERROR as CRITICAL for Botspot
        elif level >= logging.WARNING:
            return BotspotSeverity.WARNING
        elif level >= logging.INFO:
            return BotspotSeverity.INFO
        else:
            return BotspotSeverity.DEBUG
    
    def _extract_error_info(self, record: logging.LogRecord) -> Tuple[str, str, str]:
        """Extract error code, message, and details from log record."""
        original_message = record.getMessage()
        
        # First, extract strategy name if present
        strategy_name = ""
        message = original_message
        if message.startswith("[") and "]" in message:
            end_bracket = message.index("]")
            strategy_name = message[1:end_bracket]
            # Remove strategy prefix temporarily for parsing
            message = message[end_bracket + 1:].strip()
        
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
        
        # Fallback: create error code from logger name and strategy
        if not error_code:
            logger_name = record.name.split('.')[-1].upper()
            if strategy_name:
                error_code = f"{strategy_name.upper()}_ERROR"
            else:
                error_code = f"{logger_name}_{record.levelname}"
        
        # Ensure details includes file location
        if not details or "File:" not in details:
            file_info = f"File: {record.pathname}:{record.lineno}, Function: {record.funcName}"
            if details:
                details = f"{details} | {file_info}"
            else:
                details = file_info
        
        # Always include strategy name in message if it was present originally
        if strategy_name:
            message = f"[{strategy_name}] {message}"
        
        return error_code, message, details
    
    def _report_to_botspot(self, severity: BotspotSeverity, error_code: str, 
                          message: str, details: str, count: int) -> bool:
        """Send error report to Botspot API."""
        if not self.api_key or not self.requests:
            return False
        
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.api_key
        }
        payload = {
            "severity": severity.value,
            "error_code": error_code,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        if details:
            payload["details"] = details
        
        if count > 1:
            payload["count"] = count
        
        try:
            response = self.requests.post(
                self.base_url,
                headers=headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code != 200:
                # Log API errors using the logger module itself
                logger = logging.getLogger(__name__)
                logger.debug(f"Botspot API error: {response.status_code} - {response.text}")
            
            return response.status_code == 200
            
        except Exception as e:
            # Log exceptions using the logger module itself
            logger = logging.getLogger(__name__)
            logger.debug(f"Botspot API exception: {e}")
            return False
    
    def _check_global_rate_limit(self) -> bool:
        """Check global per-minute rate cap only (fingerprint logic handled separately)."""
        current_time = time.time()
        if current_time - self._minute_start_time >= 60:
            self._total_errors_sent = 0
            self._minute_start_time = current_time
        if self._total_errors_sent >= self.max_errors_per_minute:
            return False
        return True

    def _make_fingerprint(self, error_code: str, record: logging.LogRecord) -> Tuple[str, str, str, str]:
        """Create a fingerprint including message signature so distinct messages aren't incorrectly coalesced.

        We intentionally exclude line numbers (they can fluctuate) but include a truncated, normalized message to ensure
        tests expecting multiple distinct errors (different messages) see multiple sends.
        """
        filename = os.path.basename(record.pathname) if hasattr(record, 'pathname') else '<unknown>'
        func = getattr(record, 'funcName', '<unknown>')
        try:
            raw_msg = record.getMessage().strip() if hasattr(record, 'getMessage') else str(getattr(record, 'msg', ''))
        except Exception:
            raw_msg = str(getattr(record, 'msg', ''))
        # Use only portion before first pipe as message signature for dedupe (treat differing details as same base error)
        base_part = raw_msg.split('|', 1)[0].strip()
        if not base_part:
            base_part = raw_msg[:120]
        msg_sig = re.sub(r"\s+", " ", base_part)[:120]
        return (error_code, filename, func, msg_sig)

    def _should_send_now(self, fp_state: Dict[str, object], now: float) -> bool:
        last_sent = fp_state.get('last_sent')  # may be None
        if last_sent is None:
            return True  # first occurrence -> send immediately
        return (now - float(last_sent)) >= self.rate_limit_window
    
    def emit(self, record):
        """Report to Botspot with simplified fingerprint dedupe while preserving full detail payloads."""
        # Only send ERROR and CRITICAL to external service
        if not self.api_key or record.levelno < logging.ERROR:
            return
        try:
            with self._lock:
                now = time.time()
                severity = self._map_log_level_to_severity(record.levelno)
                error_code, message, details = self._extract_error_info(record)
                fp = self._make_fingerprint(error_code, record)

                fp_state = self._fingerprints.get(fp)
                if fp_state is None:
                    fp_state = {
                        'last_sent': None,
                        'suppressed_count': 0,
                        'total_count': 0,
                        'last_details': details,
                        'last_message': message,
                    }
                    self._fingerprints[fp] = fp_state

                # always keep latest details/message so we don't lose most recent stack/uuid
                fp_state['last_details'] = details
                fp_state['last_message'] = message
                fp_state['total_count'] = int(fp_state['total_count']) + 1

                send_now = self._should_send_now(fp_state, now)

                if not send_now:
                    # inside window -> just accumulate
                    fp_state['suppressed_count'] = int(fp_state['suppressed_count']) + 1
                    return

                # Outside window OR first occurrence -> check global cap
                if not self._check_global_rate_limit():
                    # global cap reached; skip sending but still accumulate suppressed
                    fp_state['suppressed_count'] = int(fp_state['suppressed_count']) + 1
                    return

                # Prepare counts
                suppressed = int(fp_state['suppressed_count'])
                count_for_payload = 1 + suppressed  # current event + suppressed during window

                # Send with latest details/message and aggregated count
                success = self._report_to_botspot(
                    severity,
                    error_code,
                    fp_state['last_message'],
                    fp_state['last_details'],
                    count_for_payload,
                )
                if success:
                    self._total_errors_sent += 1
                    fp_state['last_sent'] = now
                    fp_state['suppressed_count'] = 0

                # Opportunistic pruning: drop stale fingerprints occasionally
                if len(self._fingerprints) > 500:  # arbitrary soft cap
                    to_delete = []
                    for k, v in self._fingerprints.items():
                        last_sent = v.get('last_sent') or 0
                        if now - float(last_sent) > 3600 and v.get('suppressed_count', 0) == 0:  # 1h inactivity
                            to_delete.append(k)
                    for k in to_delete:
                        del self._fingerprints[k]
        except Exception:
            # Swallow all exceptions to avoid interfering with main execution
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
        super().__init__()

        # Define format strings for different log levels
        # In the vast majority of cases, log_message() is used by strategies so the %(name)s setting isn't useful
        # and simply clutters the output because it always points to _strategy.py.
        self.info_format = "%(asctime)s | %(levelname)s | %(message)s"
        self.warning_format = "%(asctime)s | %(levelname)s | %(pathname)s:%(funcName)s:%(lineno)d | %(message)s"
        self.error_format = "%(asctime)s | %(levelname)s | %(pathname)s:%(funcName)s:%(lineno)d | %(message)s"
        
        # Create formatters for each level. Use default datefmt for ISO format so that milliseconds are included to
        # assist with performance evaluations when running Live.
        self.info_formatter = logging.Formatter(
            self.info_format,
        )
        self.warning_formatter = logging.Formatter(
            self.warning_format,
        )
        self.error_formatter = logging.Formatter(
            self.error_format,
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
    
    def isEnabledFor(self, level):
        """Override to respect BACKTESTING_QUIET_LOGS for strategy loggers"""
        # BACKTESTING_QUIET_LOGS only applies during backtesting, not live trading
        is_backtesting = os.environ.get("IS_BACKTESTING", "").lower() == "true"
        
        if is_backtesting:
            # During backtesting, check quiet logs setting
            quiet_logs = os.environ.get("BACKTESTING_QUIET_LOGS", "true").lower() == "true"  # Default to True
            if quiet_logs and level < logging.ERROR:
                return False
        
        # For live trading, always show messages
        return self.logger.isEnabledFor(level)
    
    def info(self, msg, *args, **kwargs):
        """Override info to respect quiet logs"""
        if self.isEnabledFor(logging.INFO):
            super().info(msg, *args, **kwargs)
    
    def debug(self, msg, *args, **kwargs):
        """Override debug to respect quiet logs"""
        if self.isEnabledFor(logging.DEBUG):
            super().debug(msg, *args, **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        """Override warning to respect quiet logs"""
        if self.isEnabledFor(logging.WARNING):
            super().warning(msg, *args, **kwargs)
    
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
    This is called once globally to set up consistent formatting, but we also
    re-apply the environment driven log levels when invoked repeatedly.  This is
    important for the unit test-suite which toggles environment variables between
    tests and expects the console handler level to follow suit.

    Environment Variables Used:
    - LUMIBOT_LOG_LEVEL: Set global log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - LOG_ERRORS_TO_CSV: Enable CSV error logging (true/false)
    - LUMIBOT_ERROR_CSV_PATH: Path for CSV error log file (default: logs/errors.csv)
    - BACKTESTING_QUIET_LOGS: Enable quiet logs for backtesting (true/false)
    - LUMIWEALTH_API_KEY: API key for Lumiwealth/Botspot error reporting (when set, enables automatic error reporting)
    """
    global _handlers_configured

    # Resolve baseline log level from the environment (default INFO)
    default_level = os.environ.get('LUMIBOT_LOG_LEVEL', 'INFO').upper()
    try:
        log_level = getattr(logging, default_level)
    except AttributeError:
        log_level = logging.INFO

    is_backtesting = os.environ.get("IS_BACKTESTING", "").lower() == "true"

    # Determine the effective file (root) log level and console level
    if is_backtesting:
        backtesting_quiet = os.environ.get("BACKTESTING_QUIET_LOGS")
        if backtesting_quiet is None:
            backtesting_quiet = "true"

        if backtesting_quiet.lower() == "true":
            # Quiet mode: only ERROR+ messages to console and file
            console_level = logging.ERROR
            effective_log_level = logging.ERROR
        else:
            # Verbose mode: respect LUMIBOT_LOG_LEVEL for both console and file
            console_level = log_level
            effective_log_level = log_level
    else:
        console_level = log_level
        effective_log_level = log_level

    def _apply_levels(root_logger: logging.Logger):
        """Ensure root level and console handler levels reflect the desired state."""
        root_logger.setLevel(effective_log_level)

        console_handlers = [
            handler
            for handler in root_logger.handlers
            if isinstance(handler, logging.StreamHandler)
            and not isinstance(handler, logging.FileHandler)
        ]

        if not console_handlers:
            # Guarantee a console handler exists (needed on some CI environments)
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(LumibotFormatter())
            root_logger.addHandler(console_handler)
            console_handlers = [console_handler]

        for handler in console_handlers:
            handler.setLevel(console_level)
            # Normalise formatter – some tests replace handlers without our formatter
            if handler.formatter is None or not isinstance(handler.formatter, LumibotFormatter):
                handler.setFormatter(LumibotFormatter())

    if _handlers_configured:
        root_logger = logging.getLogger("lumibot")
        _apply_levels(root_logger)
        return

    with _config_lock:
        if _handlers_configured:
            root_logger = logging.getLogger("lumibot")
            _apply_levels(root_logger)
            return

        # Set the logger class to our custom LumibotLogger
        logging.setLoggerClass(LumibotLogger)

        root_logger = logging.getLogger("lumibot")

        # Remove any existing handlers to avoid duplicates
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(LumibotFormatter())
        console_handler.setLevel(console_level)

        root_logger.setLevel(effective_log_level)
        root_logger.addHandler(console_handler)

        # Add CSV error handler if enabled
        log_errors_to_csv = os.environ.get("LOG_ERRORS_TO_CSV")
        if log_errors_to_csv and log_errors_to_csv.lower() in ("true", "1", "yes", "on"):
            csv_path = os.environ.get("LUMIBOT_ERROR_CSV_PATH", "logs/errors.csv")
            csv_handler = CSVErrorHandler(csv_path)
            root_logger.addHandler(csv_handler)

        # Add Botspot error handler if API key is available
        api_key = os.environ.get("LUMIWEALTH_API_KEY")
        if not api_key and LUMIWEALTH_API_KEY:
            api_key = LUMIWEALTH_API_KEY

        if api_key:
            botspot_handler = BotspotErrorHandler()
            root_logger.addHandler(botspot_handler)

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

        # Get the actual lumibot root logger
        root_logger = logging.getLogger("lumibot")
        root_logger.setLevel(log_level)

        # Update handlers with respect to backtesting quiet logs setting
        is_backtesting = os.environ.get("IS_BACKTESTING", "").lower() == "true"

        if is_backtesting:
            # Check if quiet logs are enabled
            backtesting_quiet = os.environ.get("BACKTESTING_QUIET_LOGS")
            if backtesting_quiet is None:
                backtesting_quiet = "true"

            if backtesting_quiet.lower() == "true":
                # Quiet mode: console stays at ERROR, but allow file handlers to use requested level
                root_logger.setLevel(log_level)
                for handler in root_logger.handlers:
                    if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                        handler.setLevel(logging.ERROR)  # Console: quiet
                    else:
                        handler.setLevel(log_level)  # File handlers: verbose
            else:
                # Verbose mode: respect requested level for all handlers
                root_logger.setLevel(log_level)
                for handler in root_logger.handlers:
                    handler.setLevel(log_level)
        else:
            # Live trading: set everything normally
            root_logger.setLevel(log_level)
            for handler in root_logger.handlers:
                handler.setLevel(log_level)

            # Update all existing loggers in our registry
            for logger in _logger_registry.values():
                logger.setLevel(log_level)
            
    except AttributeError:
        raise ValueError(f"Invalid log level: {level}. Must be one of: DEBUG, INFO, WARNING, ERROR, CRITICAL")


def set_console_log_level(level: str):
    """
    Set the log level for the console handler of the root logger.

    This allows you to change the verbosity of console output without affecting
    the global log level.

    Parameters
    ----------
    level : str
        The log level to set for console output ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').

    Examples
    --------
    >>> from lumibot.tools.lumibot_logger import set_console_log_level
    >>> set_console_log_level('DEBUG')  # Enable debug logging in console
    >>> set_console_log_level('ERROR')  # Only show errors and critical messages in console
    """
    _ensure_handlers_configured()

    # Check both "root" and "lumibot" logger to ensure we get the correct root logger.
    # Currently, "lumibot" is used in _ensure_handlers_configured() to set up the console handler, but "root" is
    # used by the set_log_level() function.
    for root_logger_name in ["root", "lumibot"]:
        root_logger = logging.getLogger(root_logger_name)
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and handler.__class__.__name__ == "StreamHandler":
                try:
                    handler.setLevel(level)
                except AttributeError:
                    raise ValueError(f"Invalid log level: {level}. Must be one of: "
                                     f"DEBUG, INFO, WARNING, ERROR, CRITICAL")


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
    file_handler = logging.FileHandler(file_path, mode='w', encoding='utf-8')
    file_handler.setLevel(file_level)
    file_handler.setFormatter(LumibotFormatter())
    
    # Add to the actual root logger, not a logger named "root"
    root_logger = logging.getLogger("lumibot")
    root_logger.addHandler(file_handler)




# Initialize the logging system when the module is imported
_ensure_handlers_configured()
