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
    
    def test_all_logger_env_vars_handled_centrally(self):
        """Test that all logger-related env vars are handled in lumibot_logger, not credentials."""
        # List of environment variables that should be handled in the logger
        logger_env_vars = [
            'LUMIBOT_LOG_LEVEL',
            'LOG_ERRORS_TO_CSV', 
            'LUMIBOT_ERROR_CSV_PATH',
            'BACKTESTING_QUIET_LOGS',
            'DISABLE_CRITICAL_SHUTDOWN'
        ]
        
        temp_dir = tempfile.mkdtemp()
        csv_path = os.path.join(temp_dir, "test.csv")
        
        try:
            # Test comprehensive environment variable handling
            env_patch = {
                'LUMIBOT_LOG_LEVEL': 'DEBUG',
                'LOG_ERRORS_TO_CSV': 'true',
                'LUMIBOT_ERROR_CSV_PATH': csv_path,
                'BACKTESTING_QUIET_LOGS': 'false',  # Explicitly false to test override
                'DISABLE_CRITICAL_SHUTDOWN': 'true'
            }
            
            with patch.dict(os.environ, env_patch):
                # Reset handlers to pick up environment changes
                import lumibot.tools.lumibot_logger as logger_module
                logger_module._handlers_configured = False
                
                _ensure_handlers_configured()
                
                # Test that logger level is set correctly
                root_logger = logging.getLogger("lumibot")
                assert root_logger.level == logging.DEBUG
                
                # Test that CSV handler is added
                csv_handlers = [h for h in root_logger.handlers 
                              if isinstance(h, CSVErrorHandler)]
                assert len(csv_handlers) == 1
                csv_handler = csv_handlers[0]
                assert csv_handler.csv_path == csv_path
                assert csv_handler._auto_shutdown_on_critical == False  # Should be disabled
                
                # Test that quiet logs override is working (should NOT be ERROR level since explicitly false)
                console_handlers = [h for h in root_logger.handlers 
                                  if isinstance(h, logging.StreamHandler)]
                assert len(console_handlers) > 0
                # Should be DEBUG level, not ERROR (quiet logs disabled)
                assert console_handlers[0].level == logging.DEBUG
                
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_backtesting_quiet_logs_overrides_log_level(self):
        """Test that BACKTESTING_QUIET_LOGS=true overrides LUMIBOT_LOG_LEVEL."""
        with patch.dict(os.environ, {
            'LUMIBOT_LOG_LEVEL': 'DEBUG',  # This should be overridden
            'BACKTESTING_QUIET_LOGS': 'true'  # This should force ERROR level
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
    
    def test_no_circular_import_with_credentials(self):
        """Test that we can import both logger and credentials without circular import."""
        try:
            # This should not raise ImportError or circular import issues
            from lumibot.tools.lumibot_logger import get_logger
            from lumibot import credentials
            
            # Credentials should still work
            logger = get_logger(__name__)
            logger.info("Testing no circular import")
            
            # Should be able to access credentials variables
            assert hasattr(credentials, 'LOG_ERRORS_TO_CSV')
            assert hasattr(credentials, 'BACKTESTING_QUIET_LOGS')
            
        except ImportError as e:
            pytest.fail(f"Circular import detected: {e}")
    
    def test_env_var_precedence_documentation(self):
        """Test that the environment variables work as documented in the logger docstring."""
        # Test default values when no env vars are set
        clean_env = {k: v for k, v in os.environ.items() 
                    if not k.startswith(('LUMIBOT_', 'LOG_', 'BACKTESTING_', 'DISABLE_'))}
        
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
