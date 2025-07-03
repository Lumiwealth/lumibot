"""
Enhanced logging configuration for Lumibot with detailed source information.

This module provides utilities to configure logging so that warnings and errors
show exactly which file, line number, and function they come from.
"""

import logging
import warnings
import sys
import inspect
import os
from typing import Optional


class EnhancedFormatter(logging.Formatter):
    """
    Simple formatter that adds source information for warnings and errors.
    """
    
    def format(self, record):
        # For warnings and errors, try to add source information
        if record.levelno >= logging.WARNING:
            # Get the caller's frame information
            frame = sys._getframe()
            caller_file = None
            caller_func = None
            caller_line = None
            
            try:
                # Walk up the stack to find the actual caller (not logging code)
                while frame:
                    frame = frame.f_back
                    if frame is None:
                        break
                        
                    filename = frame.f_code.co_filename
                    # Skip logging internals
                    if not any(skip in filename for skip in ['logging', 'warnings', 'enhanced_logging']):
                        caller_file = os.path.basename(filename)
                        caller_func = frame.f_code.co_name
                        caller_line = frame.f_lineno
                        break
                
                if caller_file:
                    # Format: timestamp | LEVEL | file:function:line | message
                    timestamp = self.formatTime(record, self.datefmt)
                    return f"{timestamp} | {record.levelname} | {caller_file}:{caller_func}:{caller_line} | {record.getMessage()}"
                    
            except:
                pass  # Fall back to standard formatting
            
            finally:
                del frame  # Avoid reference cycles
        
        # Default formatting for info/debug or if source tracking failed
        timestamp = self.formatTime(record, self.datefmt)
        return f"{timestamp} | {record.levelname} | {record.getMessage()}"


def setup_enhanced_logging(
    level: str = "INFO",
    show_warnings: bool = True,
    capture_warnings: bool = True,
    log_file: Optional[str] = None
) -> None:
    """
    Set up enhanced logging with detailed source information.
    
    Parameters
    ----------
    level : str
        Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    show_warnings : bool
        Whether to show Python warnings through the logging system
    capture_warnings : bool
        Whether to capture warnings.warn() calls
    log_file : str, optional
        If provided, also log to this file
    """
    
    # Get root logger
    root_logger = logging.getLogger()
    
    # Clear any existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Set level
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Create enhanced formatter
    formatter = EnhancedFormatter()
    formatter.datefmt = '%Y-%m-%d %H:%M:%S'
    
    # Console handler with enhanced formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler if requested
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Capture warnings if requested
    if capture_warnings:
        logging.captureWarnings(True)
        warnings_logger = logging.getLogger('py.warnings')
        warnings_logger.setLevel(logging.WARNING)
    
    print(f"Enhanced logging configured - Level: {level}, Warnings: {show_warnings}")


def log_deprecation_with_source(message: str, category=DeprecationWarning, stacklevel=2):
    """
    Log a deprecation warning with enhanced source information.
    
    Parameters
    ----------
    message : str
        The deprecation message
    category : Warning class
        The warning category
    stacklevel : int
        How many stack frames to go up to find the caller
    """
    
    # Get caller information
    frame = sys._getframe(stacklevel)
    filename = frame.f_code.co_filename
    lineno = frame.f_lineno
    function_name = frame.f_code.co_name
    
    filename_short = os.path.basename(filename)
    
    # Log with source info
    logger = logging.getLogger('deprecation')
    logger.warning(f"DEPRECATION in {filename_short}:{function_name}:{lineno} - {message}")
    
    # Also emit as standard warning
    warnings.warn(message, category, stacklevel=stacklevel + 1)


def get_caller_info(skip_frames: int = 1) -> tuple:
    """
    Get information about the calling code.
    
    Parameters
    ----------
    skip_frames : int
        Number of stack frames to skip
        
    Returns
    -------
    tuple
        (filename, line_number, function_name)
    """
    frame = sys._getframe(skip_frames + 1)
    filename = os.path.basename(frame.f_code.co_filename)
    lineno = frame.f_lineno
    function_name = frame.f_code.co_name
    
    return filename, lineno, function_name


# Convenience function to enable enhanced logging system-wide
def enable_detailed_logging():
    """Enable detailed logging for the entire Lumibot system."""
    setup_enhanced_logging(
        level="INFO",
        show_warnings=True,
        capture_warnings=True
    )

# Alias for backwards compatibility
initialize_enhanced_logging = enable_detailed_logging
    
    # Note: Order class patching is available via patch_order_deprecation_warnings()
    # but not called automatically to avoid circular imports


def patch_order_deprecation_warnings():
    """
    Patch the Order class to show more detailed deprecation warnings.
    Call this after the Order class has been imported.
    """
    try:
        from lumibot.entities.order import Order
        
        # Store original __init__
        original_init = Order.__init__
        
        def enhanced_init(self, *args, **kwargs):
            # Check for deprecated parameters before calling original
            deprecated_params = {
                "take_profit_price": "limit_price",
                "stop_loss_price": "stop_price", 
                "stop_loss_limit_price": "stop_limit_price",
                "type": "order_type",
            }
            
            for old_param, new_param in deprecated_params.items():
                if old_param in kwargs and kwargs[old_param] is not None:
                    # Get caller info
                    filename, lineno, function_name = get_caller_info(skip_frames=1)
                    
                    # Enhanced warning with source location
                    logger = logging.getLogger('lumibot.entities.order')
                    logger.warning(
                        f"DEPRECATED PARAMETER in {filename}:{function_name}:{lineno} - "
                        f"Order parameter '{old_param}' is deprecated. Use '{new_param}' instead."
                    )
            
            # Call original
            return original_init(self, *args, **kwargs)
        
        # Monkey patch
        Order.__init__ = enhanced_init
        
        print("Order deprecation warnings enhanced with source tracking")
        
    except ImportError:
        print("Could not patch Order class - module not available")


if __name__ == "__main__":
    # Test the enhanced logging
    enable_detailed_logging()
    
    # Test various log levels
    logger = logging.getLogger('test')
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Test deprecation warning
    warnings.warn("This is a test deprecation warning", DeprecationWarning)
