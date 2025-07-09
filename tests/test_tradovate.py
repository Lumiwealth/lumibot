"""
Comprehensive tests for Tradovate broker integration.
Tests imports, configuration, basic functionality, and ensures the spelling corrections are working.

This test suite covers:
- Import verification for all Tradovate classes
- Configuration and environment variable testing
- Basic broker and data source functionality
- Integration with the rest of Lumibot
- Exception handling
- Spelling correction verification (ensures old "Tradeovate" spelling is no longer accessible)

All tests use appropriate mocking to prevent actual API calls during testing,
making them suitable for CI/CD environments like GitHub Actions.
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

    @pytest.mark.skip(reason="Test reloads credentials module which triggers actual API calls in CI")
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
            # Mock the Tradovate broker instantiation to prevent actual API calls
            with patch('lumibot.credentials.Tradovate') as mock_tradovate:
                mock_broker_instance = mock_tradovate.return_value
                mock_broker_instance.NAME = "Tradovate"
                
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

    def test_tradovate_env_var_prefix(self):
        """Test that Tradovate configuration uses the correct environment variable prefix."""
        # Test that the configuration system expects TRADOVATE_ prefix (not TRADEOVATE_)
        
        # Temporarily clear any TRADOVATE env vars to test defaults
        tradovate_env_vars = [key for key in os.environ if key.startswith('TRADOVATE_')]
        original_values = {key: os.environ.get(key) for key in tradovate_env_vars}
        
        try:
            # Clear TRADOVATE environment variables to test defaults
            for key in tradovate_env_vars:
                if key in os.environ:
                    del os.environ[key]
            
            # Re-import to get fresh config with cleared environment
            import importlib
            import lumibot.credentials
            importlib.reload(lumibot.credentials)
            
            config = lumibot.credentials.TRADOVATE_CONFIG
            
            # Verify that all expected config keys exist (they would be populated from env vars with TRADOVATE_ prefix)
            config_keys = set(config.keys())
            expected_keys = {'USERNAME', 'DEDICATED_PASSWORD', 'CID', 'SECRET', 'IS_PAPER', 'APP_ID', 'APP_VERSION', 'MD_URL'}
            assert config_keys == expected_keys
            
            # Test that the configuration has reasonable default values
            assert config['APP_ID'] == "Lumibot"
            assert config['IS_PAPER'] is True
            assert "tradovateapi.com" in config['MD_URL']
            
        finally:
            # Restore original environment variables
            for key, value in original_values.items():
                if value is not None:
                    os.environ[key] = value
            
            # Reload again to restore original state
            importlib.reload(lumibot.credentials)

    def test_credentials_module_import_resilience(self):
        """Test that the credentials module can be imported even with invalid Tradovate credentials."""
        # This test ensures that importing lumibot.credentials doesn't crash 
        # when Tradovate credentials are present but invalid
        
        test_env = {
            'TRADOVATE_USERNAME': 'invalid_user',
            'TRADOVATE_DEDICATED_PASSWORD': 'invalid_pass',
            'TRADOVATE_CID': 'invalid_cid',
            'TRADOVATE_SECRET': 'invalid_secret',
        }
        
        with patch.dict(os.environ, test_env, clear=False):
            try:
                # This should not raise an exception even with invalid credentials
                import importlib
                import lumibot.credentials
                importlib.reload(lumibot.credentials)
                
                # The import should succeed
                assert hasattr(lumibot.credentials, 'TRADOVATE_CONFIG')
                print("✅ Credentials module imported successfully with invalid Tradovate credentials")
                
            except Exception as e:
                assert False, f"Credentials module import failed with invalid credentials: {e}"


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
        
        # Mock all API methods that are called during initialization
        with patch.object(Tradovate, '_get_tokens') as mock_get_tokens, \
             patch.object(Tradovate, '_get_account_info') as mock_get_account_info, \
             patch.object(Tradovate, '_get_user_info') as mock_get_user_info:
            
            # Return the correct token structure that matches what the broker expects
            mock_get_tokens.return_value = {
                'accessToken': 'fake_access_token',
                'marketToken': 'fake_market_token',  # Note: 'marketToken', not 'market_token'
                'hasMarketData': True
            }
            
            # Mock account info response
            mock_get_account_info.return_value = {
                'accountSpec': 'fake_account_spec',
                'accountId': 123456
            }
            
            # Mock user info response
            mock_get_user_info.return_value = 'fake_user_id'
            
            try:
                broker = Tradovate(config=config)
                # If we get here, the config was accepted
                assert broker.NAME == "Tradovate"
                assert broker.account_spec == 'fake_account_spec'
                assert broker.account_id == 123456
                assert broker.user_id == 'fake_user_id'
            except Exception as e:
                # Should not fail on config validation with mocked API calls
                assert False, f"Broker initialization failed with mocked API: {e}"

    def test_broker_handles_missing_credentials(self):
        """Test that the broker handles missing credentials gracefully."""
        from lumibot.brokers import Tradovate
        
        # Test with empty/minimal config
        empty_config = {}
        
        # The broker should handle missing credentials without crashing during import
        # but should fail gracefully when trying to authenticate
        try:
            broker = Tradovate(config=empty_config)
            # Should not reach here with empty config
            assert False, "Broker should have failed with empty config"
        except Exception as e:
            # Should fail due to missing required credentials
            error_msg = str(e).lower()
            # The error should be about missing credentials, not about class structure
            assert any(keyword in error_msg for keyword in ['username', 'password', 'credential', 'authentication', 'config'])


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

    def test_rate_limit_error_message_urls(self):
        """Test that rate limit error messages contain correct web URLs for demo and live accounts."""
        import lumibot.brokers.tradovate as tradovate_module
        from unittest.mock import Mock, patch
        import requests
        
        TradovateAPIError = getattr(tradovate_module, 'TradovateAPIError')
        Tradovate = getattr(tradovate_module, 'Tradovate')
        
        # Test demo account URL
        demo_config = {
            "USERNAME": "test_user",
            "DEDICATED_PASSWORD": "test_pass",
            "IS_PAPER": True  # Demo account
        }
        
        # Mock the response for rate limiting - successful HTTP response with rate limit content
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "p-captcha": True,
            "p-time": 15,  # 15 minutes 
            "p-ticket": "some-ticket"
        }
        mock_response.raise_for_status.return_value = None
        
        # Test demo account rate limit error
        with patch('requests.post', return_value=mock_response):
            try:
                tradovate_broker = Tradovate(demo_config)
                assert False, "Should have raised TradovateAPIError"
            except TradovateAPIError as e:
                error_message = str(e)
                assert "https://demo.tradovate.com/trader/" in error_message
                assert "15 minutes" in error_message
        
        # Test live account URL
        live_config = {
            "USERNAME": "test_user", 
            "DEDICATED_PASSWORD": "test_pass",
            "IS_PAPER": False  # Live account
        }
        
        # Test live account rate limit error
        with patch('requests.post', return_value=mock_response):
            try:
                tradovate_broker = Tradovate(live_config)
                assert False, "Should have raised TradovateAPIError"
            except TradovateAPIError as e:
                error_message = str(e)
                assert "https://tradovate.com/trader/" in error_message
                assert "15 minutes" in error_message

    def test_incorrect_credentials_error_message(self):
        """Test that incorrect credentials error message is user-friendly."""
        import lumibot.brokers.tradovate as tradovate_module
        from unittest.mock import Mock, patch
        
        TradovateAPIError = getattr(tradovate_module, 'TradovateAPIError')
        Tradovate = getattr(tradovate_module, 'Tradovate')
        
        config = {
            "USERNAME": "wrong_user",
            "DEDICATED_PASSWORD": "wrong_pass",
            "IS_PAPER": True
        }
        
        # Mock the response for incorrect credentials - successful HTTP response with error text
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "errorText": "Authorization Failed"
        }
        mock_response.raise_for_status.return_value = None
        
        with patch('requests.post', return_value=mock_response):
            try:
                tradovate_broker = Tradovate(config)
                assert False, "Should have raised TradovateAPIError"
            except TradovateAPIError as e:
                error_message = str(e)
                assert "authorization failed" in error_message.lower() or "authentication failed" in error_message.lower()


if __name__ == "__main__":
    # Run all tests when file is executed directly
    pytest.main([__file__, "-v"])
