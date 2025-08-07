"""
Test for the Alpaca authentication error handling fix.
"""

import pytest
from unittest.mock import Mock, patch
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities import Asset


class TestAlpacaAuthFix:
    """Test the authentication error handling improvements."""
    
    def setup_method(self):
        """Setup test with mock config."""
        self.config = {
            "API_KEY": "test_key", 
            "API_SECRET": "test_secret"
        }
    
    def test_auth_failed_flag_not_set_for_network_errors(self):
        """Test that network errors don't permanently disable the data source."""
        data_source = AlpacaData(self.config)
        
        # Simulate a network error (not auth error)
        network_error = Exception("Connection timeout")
        
        # This should not set the _auth_failed flag
        with pytest.raises(Exception):
            data_source._handle_auth_error(network_error, "test operation")
        
        # The auth_failed flag should still be False
        assert not data_source._auth_failed
    
    def test_auth_failed_flag_set_for_real_auth_errors(self):
        """Test that real authentication errors do set the flag."""
        data_source = AlpacaData(self.config)
        
        # Simulate a real auth error
        auth_error = Exception("401 Unauthorized")
        
        # This should set the _auth_failed flag
        with pytest.raises(ValueError, match="Authentication failed"):
            data_source._handle_auth_error(auth_error, "test operation")
        
        # The auth_failed flag should now be True
        assert data_source._auth_failed
    
    def test_reset_auth_failure_method(self):
        """Test that the reset method properly clears the auth failure state."""
        data_source = AlpacaData(self.config)
        
        # Set auth failed state
        data_source._auth_failed = True
        data_source._option_client = Mock()
        
        # Reset the auth failure
        data_source.reset_auth_failure()
        
        # Check that everything is reset
        assert not data_source._auth_failed
        assert data_source._option_client is None
    
    @patch('lumibot.data_sources.alpaca_data.OptionHistoricalDataClient')
    def test_client_initialization_with_network_error(self, mock_client_class):
        """Test that network errors during client init don't permanently disable the client."""
        data_source = AlpacaData(self.config)
        
        # Mock a network error during client initialization
        mock_client_class.side_effect = Exception("Connection refused")
        
        # First call should raise the exception but not set auth_failed
        with pytest.raises(Exception, match="Connection refused"):
            data_source._get_option_client()
        
        # Auth should not be marked as failed
        assert not data_source._auth_failed
        
        # Reset the mock to succeed on next call
        mock_client_class.side_effect = None
        mock_client_class.return_value = Mock()
        
        # Should be able to retry and succeed
        client = data_source._get_option_client()
        assert client is not None
    
    @patch('lumibot.data_sources.alpaca_data.OptionHistoricalDataClient')
    def test_client_initialization_with_auth_error(self, mock_client_class):
        """Test that auth errors during client init do set the auth failed flag."""
        data_source = AlpacaData(self.config)
        
        # Mock an auth error during client initialization
        mock_client_class.side_effect = Exception("401 Unauthorized")
        
        # First call should raise auth error and set the flag
        with pytest.raises(ValueError, match="Authentication failed"):
            data_source._get_option_client()
        
        # Auth should be marked as failed
        assert data_source._auth_failed
    
    @patch('lumibot.data_sources.alpaca_data.OptionHistoricalDataClient')
    def test_get_chains_blocked_after_auth_failure(self, mock_client_class):
        """Test that get_chains is blocked after authentication failure."""
        data_source = AlpacaData(self.config)
        
        # Set auth failed state
        data_source._auth_failed = True
        
        # Mock the client to avoid actual API calls
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # get_chains should reset the flag and retry (not block anymore)
        asset = Asset("SPY", Asset.AssetType.STOCK)
        
        # Mock successful response after retry
        mock_client.get_option_chain.return_value = {
            "option_chains": {}
        }
        
        # Should not raise error - it resets and retries
        result = data_source.get_chains(asset)
        
        # Verify flag was reset
        assert not data_source._auth_failed
        
        # Verify it returned a result (even if empty)
        assert result is not None
    
    def test_different_auth_error_patterns(self):
        """Test that various authentication error patterns are detected."""
        data_source = AlpacaData(self.config)
        
        auth_error_messages = [
            "401 Unauthorized",
            "403 Forbidden", 
            "Invalid credentials provided",
            "Authentication failed for user",
            "Invalid API key",
            "Invalid token"
        ]
        
        for error_msg in auth_error_messages:
            # Reset state for each test
            data_source._auth_failed = False
            
            error = Exception(error_msg)
            with pytest.raises(ValueError, match="Authentication failed"):
                data_source._handle_auth_error(error, "test")
            
            assert data_source._auth_failed, f"Failed to detect auth error: {error_msg}"


if __name__ == "__main__":
    pytest.main([__file__])
