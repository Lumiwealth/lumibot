"""
Tests for ProjectX URL mapping functionality
"""

import os
import pytest
from unittest.mock import patch

from lumibot.credentials import get_projectx_config, PROJECTX_BASE_URLS, PROJECTX_STREAMING_URLS


class TestProjectXURLMappings:
    """Test ProjectX URL mapping functionality"""
    
    def test_url_mappings_loaded(self):
        """Test that URL mappings are loaded correctly"""
        assert len(PROJECTX_BASE_URLS) > 0
        assert len(PROJECTX_STREAMING_URLS) > 0
        
        # Test specific firms
        assert "topstepx" in PROJECTX_BASE_URLS
        assert "topone" in PROJECTX_BASE_URLS
        assert "tickticktrader" in PROJECTX_BASE_URLS
        
        assert "topstepx" in PROJECTX_STREAMING_URLS
        assert "topone" in PROJECTX_STREAMING_URLS
        assert "tickticktrader" in PROJECTX_STREAMING_URLS
    
    def test_url_patterns(self):
        """Test that URLs follow expected patterns"""
        # TopStepX uses different pattern
        assert PROJECTX_BASE_URLS["topstepx"] == "https://api.topstepx.com/"
        
        # Other firms use gateway pattern
        assert "gateway-api-" in PROJECTX_BASE_URLS["topone"]
        assert "s2f.projectx.com" in PROJECTX_BASE_URLS["topone"]
        
        # Streaming URLs use gateway-rtc pattern
        assert "gateway-rtc-" in PROJECTX_STREAMING_URLS["topone"]
        assert "s2f.projectx.com" in PROJECTX_STREAMING_URLS["topone"]
    
    def test_get_config_with_builtin_urls(self):
        """Test configuration using built-in URLs"""
        with patch.dict(os.environ, {}, clear=True):
            # Clear environment, should get built-in URLs
            config = get_projectx_config("topone")
            
            assert config["base_url"] == PROJECTX_BASE_URLS["topone"]
            assert config["streaming_base_url"] == PROJECTX_STREAMING_URLS["topone"]
            assert config["firm"] == "TOPONE"
            assert config["api_key"] is None  # No env var set
            assert config["username"] is None  # No env var set
    
    def test_get_config_with_env_override(self):
        """Test configuration with environment variable overrides"""
        test_env = {
            "PROJECTX_TOPONE_API_KEY": "test_key",
            "PROJECTX_TOPONE_USERNAME": "test_user",
            "PROJECTX_TOPONE_BASE_URL": "https://custom.example.com/",
            "PROJECTX_TOPONE_STREAMING_BASE_URL": "https://custom-stream.example.com/",
            "PROJECTX_TOPONE_PREFERRED_ACCOUNT_NAME": "test_account"
        }
        
        with patch.dict(os.environ, test_env, clear=True):
            config = get_projectx_config("topone")
            
            # Should use environment overrides
            assert config["base_url"] == "https://custom.example.com/"
            assert config["streaming_base_url"] == "https://custom-stream.example.com/"
            assert config["api_key"] == "test_key"
            assert config["username"] == "test_user"
            assert config["preferred_account_name"] == "test_account"
    
    def test_get_config_partial_env_override(self):
        """Test configuration with partial environment variable overrides"""
        test_env = {
            "PROJECTX_TOPONE_API_KEY": "test_key",
            "PROJECTX_TOPONE_USERNAME": "test_user",
            "PROJECTX_TOPONE_BASE_URL": "https://custom.example.com/",
            # No streaming URL override - should use built-in
        }
        
        with patch.dict(os.environ, test_env, clear=True):
            config = get_projectx_config("topone")
            
            # Should use environment override for base URL
            assert config["base_url"] == "https://custom.example.com/"
            # Should use built-in for streaming URL
            assert config["streaming_base_url"] == PROJECTX_STREAMING_URLS["topone"]
            assert config["api_key"] == "test_key"
            assert config["username"] == "test_user"
    
    def test_get_config_topstepx_special_case(self):
        """Test configuration for TopStepX (uses different URL pattern)"""
        with patch.dict(os.environ, {}, clear=True):
            config = get_projectx_config("topstepx")
            
            assert config["base_url"] == "https://api.topstepx.com/"
            assert config["streaming_base_url"] == PROJECTX_STREAMING_URLS["topstepx"]
            assert config["firm"] == "TOPSTEPX"
    
    def test_get_config_unknown_firm(self):
        """Test configuration for unknown firm"""
        with patch.dict(os.environ, {}, clear=True):
            config = get_projectx_config("unknownfirm")
            
            # Should return config with None URLs for unknown firm
            assert config["base_url"] is None
            assert config["streaming_base_url"] is None
            assert config["firm"] == "UNKNOWNFIRM"
    
    def test_auto_detection_with_multiple_firms(self):
        """Test auto-detection when multiple firms are configured"""
        test_env = {
            "PROJECTX_TOPONE_API_KEY": "test_key1",
            "PROJECTX_TOPSTEPX_API_KEY": "test_key2",
        }
        
        with patch.dict(os.environ, test_env, clear=True):
            # Should auto-detect first available firm
            config = get_projectx_config()
            
            # Should pick one of the configured firms
            assert config["firm"] in ["TOPONE", "TOPSTEPX"]
            assert config["base_url"] is not None


class TestProjectXBrokerValidation:
    """Test ProjectX broker validation with new URL mappings"""

    def test_broker_validation_success(self):
        """Test broker validation with valid configuration"""
        from lumibot.brokers.projectx import ProjectX

        config = {
            "firm": "TOPONE",
            "api_key": "test_key",
            "username": "test_user",
            "base_url": "https://gateway-api-toponefutures.s2f.projectx.com/",
            "preferred_account_name": "test_account"
        }

        # Should not raise any exceptions during validation (mock the client initialization)
        with patch('lumibot.tools.projectx_helpers.ProjectXAuth.get_auth_token', return_value="mock_token"):
            with patch('lumibot.tools.projectx_helpers.ProjectX'):
                # Mock the data source
                with patch('lumibot.data_sources.projectx_data.ProjectXData') as mock_data_source:
                    mock_data_instance = mock_data_source.return_value
                    broker = ProjectX(config=config, data_source=mock_data_instance)
                    assert broker is not None
                assert broker.firm == "TOPONE"
    
    def test_broker_validation_missing_fields(self):
        """Test broker validation with missing required fields"""
        from lumibot.brokers.projectx import ProjectX
        
        config = {
            "firm": "TOPONE",
            "api_key": "test_key",
            # Missing username and base_url
        }
        
        with pytest.raises(ValueError) as excinfo:
            ProjectX(config=config)
        
        assert "Missing required ProjectX configuration" in str(excinfo.value)
        assert "username" in str(excinfo.value)
        assert "base_url" in str(excinfo.value)

    def test_short_firm_name_env_vars_only(self):
        """Test that only short firm names are required for environment variables (no legacy names)"""
        from lumibot.credentials import get_projectx_config, get_available_projectx_firms
        
        # Test that PROJECTX_TOPONE_* env vars work (not PROJECTX_TOPONEFUTURES_*)
        with patch.dict(os.environ, {
            'PROJECTX_TOPONE_API_KEY': 'test_topone_key',
            'PROJECTX_TOPONE_USERNAME': 'test_topone_user',
            'PROJECTX_TOPONE_PREFERRED_ACCOUNT_NAME': 'test_account'
        }, clear=False):
            
            # Should auto-detect the TOPONE firm
            available_firms = get_available_projectx_firms()
            assert 'TOPONE' in available_firms
            
            # Should get correct config for 'topone' (case insensitive)
            config = get_projectx_config('topone')
            assert config['firm'] == 'TOPONE'
            assert config['api_key'] == 'test_topone_key'
            assert config['username'] == 'test_topone_user'
            assert config['preferred_account_name'] == 'test_account'
            assert config['base_url'] == 'https://gateway-api-toponefutures.s2f.projectx.com/'
            assert config['streaming_base_url'] == 'https://gateway-rtc-demo.s2f.projectx.com/'
    
    def test_no_legacy_firm_names_in_mappings(self):
        """Test that legacy firm names like 'toponefutures' are not in the URL mappings"""
        from lumibot.credentials import PROJECTX_BASE_URLS, PROJECTX_STREAMING_URLS
        
        # Ensure no legacy firm names exist in mappings
        assert 'toponefutures' not in PROJECTX_BASE_URLS
        assert 'toponefutures' not in PROJECTX_STREAMING_URLS
        
        # Ensure short firm name exists
        assert 'topone' in PROJECTX_BASE_URLS
        assert 'topone' in PROJECTX_STREAMING_URLS
        
        # Ensure all mappings use the v2 gateway pattern
        for firm, url in PROJECTX_BASE_URLS.items():
            if firm != 'topstepx':  # topstepx has different URL pattern
                assert 'gateway-api-' in url or 's2f.projectx.com' in url
        
        for firm, url in PROJECTX_STREAMING_URLS.items():
            if firm != 'topstepx':  # topstepx has different URL pattern  
                assert 'gateway-rtc-' in url or 's2f.projectx.com' in url


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
