"""
Test for environment variable handling in lumibot_logger to ensure 
centralized configuration works correctly.
"""
import os
import tempfile
import shutil
import logging
from unittest.mock import patch
import pytest
from lumibot.tools.lumibot_logger import get_logger, _ensure_handlers_configured, CSVErrorHandler


class TestEnvironmentVariableHandling:
    """Test centralized environment variable handling in lumibot_logger."""
    
    def test_backtesting_quiet_logs_overrides_log_level(self):
        """Test that BACKTESTING_QUIET_LOGS=true overrides LUMIBOT_LOG_LEVEL during backtesting."""
        with patch.dict(os.environ, {
            'LUMIBOT_LOG_LEVEL': 'DEBUG',  # This should be overridden
            'BACKTESTING_QUIET_LOGS': 'true',  # This should force ERROR level
            'IS_BACKTESTING': 'true'  # This is required for quiet logs to take effect
        }):
            # Reset handlers to pick up environment changes
            import lumibot.tools.lumibot_logger as logger_module
            logger_module._handlers_configured = False
            
            _ensure_handlers_configured()
            
            # Should be ERROR level despite DEBUG being set
            root_logger = logging.getLogger("lumibot")
            assert root_logger.level == logging.ERROR
            
            console_handlers = [h for h in root_logger.handlers 
                              if isinstance(h, logging.StreamHandler)]
            assert len(console_handlers) > 0
            assert console_handlers[0].level == logging.ERROR

    def test_env_var_precedence_documentation(self):
        """Test that the environment variables work as documented in the logger docstring."""
        # Test default values when no env vars are set
        clean_env = {k: v for k, v in os.environ.items() 
                      if not k.startswith(('LUMIBOT_', 'LOG_', 'BACKTESTING_', 'DISABLE_'))}
        # Also ensure backtesting flag is cleared to avoid ERROR default in console/file split logic
        clean_env.pop('IS_BACKTESTING', None)
        
        with patch.dict(os.environ, clean_env, clear=True):
            # Reset handlers to pick up environment changes
            import lumibot.tools.lumibot_logger as logger_module
            logger_module._handlers_configured = False
            
            _ensure_handlers_configured()
            
            root_logger = logging.getLogger("lumibot")
            
            # Default should be INFO level
            assert root_logger.level == logging.INFO
            
            # No CSV handler by default
            csv_handlers = [h for h in root_logger.handlers 
                          if isinstance(h, CSVErrorHandler)]
            assert len(csv_handlers) == 0
    
    def test_memory_usage_is_minimal(self):
        """Test that environment variable handling doesn't create memory issues."""
        import sys
        
        # Get initial module count
        initial_modules = len(sys.modules)
        
        # Import and use logger multiple times
        for i in range(10):
            logger = get_logger(f"test_module_{i}")
            logger.info(f"Test message {i}")
        
        # Module count should not grow significantly
        final_modules = len(sys.modules)
        module_growth = final_modules - initial_modules
        
        # Should be minimal growth (just a few modules at most)
        assert module_growth < 5, f"Too many modules loaded: {module_growth}"


if __name__ == "__main__":
    pytest.main([__file__])
