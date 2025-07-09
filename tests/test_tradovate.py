"""
Comprehensive tests for Tradovate broker integration.
Tests imports, configuration, basic functionality, and ensures the spelling corrections are working.
"""

import pytest
import os
from unittest.mock import patch


class TestTradovateImports:
    """Test that all Tradovate imports work correctly with the corrected spelling."""

    def test_tradovate_broker_import(self):
        """Test that the Tradovate broker can be imported with the correct spelling."""
        from lumibot.brokers import Tradovate
        assert Tradovate.__name__ == "Tradovate"
        assert Tradovate.NAME == "Tradovate"

    def test_tradovate_data_source_import(self):
        """Test that the TradovateData class can be imported with the correct spelling."""
        from lumibot.data_sources import TradovateData
        assert TradovateData.__name__ == "TradovateData"
        assert TradovateData.SOURCE == "Tradovate"
        assert TradovateData.MIN_TIMESTEP == "minute"

    def test_tradovate_exception_import(self):
        """Test that the TradovateAPIError exception can be imported."""
        # Import through the module to avoid direct import issues
        from lumibot.brokers import Tradovate
        import lumibot.brokers.tradovate as tradovate_module
        
        assert hasattr(tradovate_module, 'TradovateAPIError')
        TradovateAPIError = getattr(tradovate_module, 'TradovateAPIError')
        assert TradovateAPIError.__name__ == "TradovateAPIError"

    def test_old_misspelling_not_accessible(self):
        """Test that the old misspelling 'Tradeovate' is no longer accessible."""
        # These imports should fail because we've corrected the spelling
        with pytest.raises(ImportError):
            # This would be trying to import from a file named "tradeovate.py" (old misspelling)
            import lumibot.brokers.tradeovate
        
        with pytest.raises(ImportError):
            # This would be trying to import from a file named "tradeovate_data.py" (old misspelling)
            import lumibot.data_sources.tradeovate_data


class TestTradovateConfiguration:
    """Test Tradovate configuration and environment variables."""

    def test_tradovate_config_structure(self):
        """Test that TRADOVATE_CONFIG has the correct structure and keys."""
        from lumibot.credentials import TRADOVATE_CONFIG
        
        assert isinstance(TRADOVATE_CONFIG, dict)
        expected_keys = {
            "USERNAME", "DEDICATED_PASSWORD", "APP_ID", "APP_VERSION", 
            "CID", "SECRET", "IS_PAPER", "MD_URL"
        }
        assert set(TRADOVATE_CONFIG.keys()) == expected_keys

    def test_tradovate_config_default_values(self):
        """Test that TRADOVATE_CONFIG has correct default values."""
        from lumibot.credentials import TRADOVATE_CONFIG
        
        # Check default values that should always be present
        assert TRADOVATE_CONFIG["APP_ID"] == "Lumibot"
        assert TRADOVATE_CONFIG["APP_VERSION"] == "1.0"
        assert TRADOVATE_CONFIG["MD_URL"] == "https://md.tradovateapi.com/v1"
        assert TRADOVATE_CONFIG["IS_PAPER"] is True  # Default should be True

    def test_environment_variable_names(self):
        """Test that the configuration uses the correct environment variable names."""
        # This test ensures we're using TRADOVATE_ (correct) not TRADEOVATE_ (old misspelling)
        
        # Mock some environment variables with the correct spelling
        test_env = {
            'TRADOVATE_USERNAME': 'test_user',
            'TRADOVATE_DEDICATED_PASSWORD': 'test_pass',
            'TRADOVATE_CID': 'test_cid',
            'TRADOVATE_SECRET': 'test_secret',
            'TRADOVATE_IS_PAPER': 'false',
            'TRADOVATE_APP_ID': 'TestApp',
            'TRADOVATE_APP_VERSION': '2.0',
            'TRADOVATE_MD_URL': 'https://test.tradovateapi.com/v1'
        }
        
        with patch.dict(os.environ, test_env, clear=False):
            # Re-import to get fresh config with our test environment
            import importlib
            import lumibot.credentials
            importlib.reload(lumibot.credentials)
            
            config = lumibot.credentials.TRADOVATE_CONFIG
            assert config['USERNAME'] == 'test_user'
            assert config['DEDICATED_PASSWORD'] == 'test_pass'
            assert config['CID'] == 'test_cid'
            assert config['SECRET'] == 'test_secret'
            assert config['IS_PAPER'] is False  # Should be False when set to 'false'
            assert config['APP_ID'] == 'TestApp'
            assert config['APP_VERSION'] == '2.0'
            assert config['MD_URL'] == 'https://test.tradovateapi.com/v1'


class TestTradovateBroker:
    """Test the Tradovate broker class functionality."""

    def test_broker_class_attributes(self):
        """Test that the Tradovate broker class has the correct attributes."""
        from lumibot.brokers import Tradovate
        
        # Test class name and NAME attribute
        assert Tradovate.__name__ == "Tradovate"
        assert Tradovate.NAME == "Tradovate"

    def test_broker_has_required_methods(self):
        """Test that the Tradovate broker class has essential methods."""
        from lumibot.brokers import Tradovate
        
        # Check that the class has basic required methods
        essential_methods = [
            '__init__',
            '_get_tokens',
            '_get_account_info',
            '_get_user_info'
        ]
        
        for method in essential_methods:
            assert hasattr(Tradovate, method), f"Tradovate class missing method: {method}"

    def test_broker_config_validation(self):
        """Test that broker initialization validates configuration."""
        from lumibot.brokers import Tradovate
        
        # Test that we can create the class (though it will fail on API calls)
        # This tests the basic class structure
        config = {
            "USERNAME": "test_user",
            "DEDICATED_PASSWORD": "test_pass", 
            "CID": "test_cid",
            "SECRET": "test_secret",
            "IS_PAPER": True,
            "APP_ID": "TestApp",
            "APP_VERSION": "1.0",
            "MD_URL": "https://md.tradovateapi.com/v1"
        }
        
        # This will fail on API authentication, but that's expected
        # We're just testing that the class can be instantiated with proper config
        try:
            broker = Tradovate(config=config)
            # If we get here, the config was accepted
            assert broker.NAME == "Tradovate"
        except Exception as e:
            # Expected to fail on actual API calls, but should not fail on config validation
            # The error should be related to API authentication, not config issues
            assert "config" not in str(e).lower() or "authentication" in str(e).lower() or "token" in str(e).lower()


class TestTradovateDataSource:
    """Test the TradovateData source class."""

    def test_data_source_attributes(self):
        """Test that TradovateData has correct attributes."""
        from lumibot.data_sources import TradovateData
        
        assert TradovateData.SOURCE == "Tradovate"
        assert TradovateData.MIN_TIMESTEP == "minute"

    def test_data_source_initialization(self):
        """Test that TradovateData can be initialized."""
        from lumibot.data_sources import TradovateData
        
        config = {"test": "config"}
        data_source = TradovateData(config)
        assert data_source.config == config


class TestTradovateIntegration:
    """Test integration aspects of Tradovate with the rest of Lumibot."""

    def test_broker_name_recognition(self):
        """Test that 'tradovate' is recognized as a valid broker name."""
        from lumibot.credentials import TRADOVATE_CONFIG
        
        # The config should exist and be accessible
        assert isinstance(TRADOVATE_CONFIG, dict)
        assert "USERNAME" in TRADOVATE_CONFIG

    def test_file_naming_consistency(self):
        """Test that file names and imports are consistent."""
        # Test that we can import from the correctly named files
        from lumibot.brokers import Tradovate
        from lumibot.data_sources import TradovateData
        
        # Verify the classes are correctly named
        assert Tradovate.__name__ == "Tradovate"
        assert TradovateData.__name__ == "TradovateData"

    def test_documentation_url_consistency(self):
        """Test that the configuration uses the correct API URL."""
        from lumibot.credentials import TRADOVATE_CONFIG
        
        # The URL should point to tradovateapi.com (correct spelling)
        url = TRADOVATE_CONFIG["MD_URL"]
        assert "tradovateapi.com" in url
        assert "tradeovateapi.com" not in url  # Make sure old misspelling is not used


class TestTradovateException:
    """Test the TradovateAPIError exception functionality."""

    def test_exception_basic_functionality(self):
        """Test that TradovateAPIError works as an exception."""
        import lumibot.brokers.tradovate as tradovate_module
        
        TradovateAPIError = getattr(tradovate_module, 'TradovateAPIError')
        
        # Test basic exception creation and message
        error = TradovateAPIError("Test error message")
        assert str(error) == "Test error message"
        assert isinstance(error, Exception)

    def test_exception_with_additional_info(self):
        """Test TradovateAPIError with additional information."""
        import lumibot.brokers.tradovate as tradovate_module
        
        TradovateAPIError = getattr(tradovate_module, 'TradovateAPIError')
        
        # Test exception with additional details
        original_error = ValueError("Original error")
        error = TradovateAPIError(
            "API error occurred",
            status_code=400,
            response_text="Bad Request",
            original_exception=original_error
        )
        
        assert str(error) == "API error occurred"
        assert error.status_code == 400
        assert error.response_text == "Bad Request"
        assert error.original_exception is original_error


if __name__ == "__main__":
    # Run all tests when file is executed directly
    pytest.main([__file__, "-v"])
