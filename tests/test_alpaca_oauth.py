#!/usr/bin/env python3

"""
Comprehensive OAuth tests for Alpaca integration.
These tests verify OAuth functionality without requiring real API credentials.
"""

import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.trading_builtins import PollingStream
from alpaca.trading.stream import TradingStream


class TestAlpacaOAuth(unittest.TestCase):
    """Test Alpaca OAuth functionality."""
    
    def setUp(self):
        """Set up test configurations."""
        self.oauth_config = {
            "OAUTH_TOKEN": "test_oauth_token_12345",
            "PAPER": True
        }
        
        self.api_config = {
            "API_KEY": "test_api_key_12345",
            "API_SECRET": "test_api_secret_12345",
            "PAPER": True
        }
        
        self.mixed_config = {
            "OAUTH_TOKEN": "test_oauth_token_12345",
            "API_KEY": "test_api_key_12345", 
            "API_SECRET": "test_api_secret_12345",
            "PAPER": True
        }

    def test_oauth_data_source_initialization(self):
        """Test that AlpacaData can be initialized with OAuth token only."""
        data_source = AlpacaData(self.oauth_config)
        
        # Verify OAuth token is set correctly
        self.assertEqual(data_source.oauth_token, "test_oauth_token_12345")
        self.assertIsNone(data_source.api_key)
        self.assertIsNone(data_source.api_secret)
        self.assertTrue(data_source.is_paper)

    def test_api_key_data_source_initialization(self):
        """Test that AlpacaData can be initialized with API key/secret."""
        data_source = AlpacaData(self.api_config)
        
        # Verify API credentials are set correctly
        self.assertIsNone(data_source.oauth_token)
        self.assertEqual(data_source.api_key, "test_api_key_12345")
        self.assertEqual(data_source.api_secret, "test_api_secret_12345")
        self.assertTrue(data_source.is_paper)

    def test_mixed_credentials_data_source(self):
        """Test that AlpacaData works with both OAuth and API credentials."""
        data_source = AlpacaData(self.mixed_config)
        
        # When both OAuth token and API credentials are present, OAuth takes precedence
        self.assertEqual(data_source.oauth_token, "test_oauth_token_12345")
        # API key/secret are None because OAuth token is present and takes precedence
        self.assertIsNone(data_source.api_key)
        self.assertIsNone(data_source.api_secret)

    def test_no_credentials_error(self):
        """Test that AlpacaData raises error when no credentials provided."""
        empty_config = {"PAPER": True}
        
        with self.assertRaises(ValueError) as context:
            AlpacaData(empty_config)
        
        self.assertIn("Either OAuth token or API key/secret must be provided", str(context.exception))

    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_oauth_broker_initialization(self, mock_trading_client):
        """Test that Alpaca broker can be initialized with OAuth token only."""
        mock_trading_client.return_value = Mock()
        
        broker = Alpaca(self.oauth_config, connect_stream=False)
        
        # Verify OAuth configuration
        self.assertEqual(broker.oauth_token, "test_oauth_token_12345")
        self.assertEqual(broker.api_key, "")
        self.assertEqual(broker.api_secret, "")
        self.assertTrue(broker.is_oauth_only)
        self.assertTrue(broker.is_paper)
        
        # Verify TradingClient was called with OAuth token
        mock_trading_client.assert_called_with(oauth_token="test_oauth_token_12345", paper=True)

    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_api_key_broker_initialization(self, mock_trading_client):
        """Test that Alpaca broker can be initialized with API key/secret."""
        mock_trading_client.return_value = Mock()
        
        broker = Alpaca(self.api_config, connect_stream=False)
        
        # Verify API key configuration
        self.assertEqual(broker.oauth_token, "")
        self.assertEqual(broker.api_key, "test_api_key_12345")
        self.assertEqual(broker.api_secret, "test_api_secret_12345")
        self.assertFalse(broker.is_oauth_only)
        
        # Verify TradingClient was called with API credentials
        mock_trading_client.assert_called_with("test_api_key_12345", "test_api_secret_12345", paper=True)

    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_mixed_credentials_broker(self, mock_trading_client):
        """Test that Alpaca broker works with both OAuth and API credentials."""
        mock_trading_client.return_value = Mock()
        
        broker = Alpaca(self.mixed_config, connect_stream=False)
        
        # Should use OAuth when both are available
        self.assertEqual(broker.oauth_token, "test_oauth_token_12345")
        self.assertEqual(broker.api_key, "test_api_key_12345")
        self.assertEqual(broker.api_secret, "test_api_secret_12345")
        self.assertFalse(broker.is_oauth_only)  # Both credentials available
        
        # Should initialize with OAuth token
        mock_trading_client.assert_called_with(oauth_token="test_oauth_token_12345", paper=True)

    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_oauth_only_stream_object(self, mock_trading_client):
        """Test that OAuth-only configurations use PollingStream."""
        mock_trading_client.return_value = Mock()
        
        broker = Alpaca(self.oauth_config, connect_stream=False)
        stream = broker._get_stream_object()
        
        # Should return PollingStream for OAuth-only
        self.assertIsInstance(stream, PollingStream)
        self.assertEqual(stream.polling_interval, 5.0)  # Default polling interval

    @patch('lumibot.brokers.alpaca.TradingClient')
    @patch('lumibot.brokers.alpaca.TradingStream')
    def test_api_key_stream_object(self, mock_trading_stream, mock_trading_client):
        """Test that API key configurations use TradingStream."""
        mock_trading_client.return_value = Mock()
        mock_trading_stream.return_value = Mock()
        
        broker = Alpaca(self.api_config, connect_stream=False)
        stream = broker._get_stream_object()
        
        # Should use TradingStream for API key/secret
        mock_trading_stream.assert_called_with("test_api_key_12345", "test_api_secret_12345", paper=True)

    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_oauth_polling_interval_custom(self, mock_trading_client):
        """Test that custom polling interval is respected for OAuth-only configurations."""
        mock_trading_client.return_value = Mock()
        
        broker = Alpaca(self.oauth_config, connect_stream=False, polling_interval=10.0)
        stream = broker._get_stream_object()
        
        self.assertIsInstance(stream, PollingStream)
        self.assertEqual(stream.polling_interval, 10.0)

    def test_oauth_constants(self):
        """Test that OAuth constants are properly defined."""
        from lumibot.credentials import ALPACA_OAUTH_CONFIG
        
        self.assertIn("CALLBACK_URL", ALPACA_OAUTH_CONFIG)
        self.assertIn("CLIENT_ID", ALPACA_OAUTH_CONFIG)
        self.assertIn("REDIRECT_URL", ALPACA_OAUTH_CONFIG)
        
        # Verify they contain expected values
        self.assertIn("alpaca", ALPACA_OAUTH_CONFIG["CALLBACK_URL"])
        self.assertIn("alpaca", ALPACA_OAUTH_CONFIG["REDIRECT_URL"])

    @patch('lumibot.data_sources.alpaca_data.StockHistoricalDataClient')
    def test_oauth_client_creation(self, mock_stock_client):
        """Test that OAuth tokens are properly passed to Alpaca clients."""
        mock_stock_client.return_value = Mock()
        
        data_source = AlpacaData(self.oauth_config)
        client = data_source._get_stock_client()
        
        # Verify OAuth token was passed to client
        mock_stock_client.assert_called_with(oauth_token="test_oauth_token_12345")

    @patch('lumibot.data_sources.alpaca_data.StockHistoricalDataClient')
    def test_api_key_client_creation(self, mock_stock_client):
        """Test that API credentials are properly passed to Alpaca clients."""
        mock_stock_client.return_value = Mock()
        
        data_source = AlpacaData(self.api_config)
        client = data_source._get_stock_client()
        
        # Verify API credentials were passed to client
        mock_stock_client.assert_called_with("test_api_key_12345", "test_api_secret_12345")

    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_oauth_error_handling_stops_execution(self, mock_trading_client):
        """Test that authentication errors stop execution immediately."""
        mock_trading_client.return_value = Mock()
        
        broker = Alpaca(self.oauth_config, connect_stream=False)
        
        # Mock the sync_positions to raise an authentication error
        with patch.object(broker, 'sync_positions') as mock_sync:
            mock_sync.side_effect = Exception("401 Unauthorized")
            
            # Should raise ValueError to stop execution
            with self.assertRaises(ValueError) as context:
                broker.do_polling()
            
            # Verify the error message contains authentication information
            self.assertIn("401 Unauthorized", str(context.exception))

    @patch('lumibot.brokers.alpaca.TradingClient')
    def test_strategy_none_handling(self, mock_trading_client):
        """Test that polling handles None strategy gracefully."""
        mock_trading_client.return_value = Mock()
        
        broker = Alpaca(self.oauth_config, connect_stream=False)
        
        # Mock the necessary methods to avoid real API calls
        with patch.object(broker, '_pull_broker_positions') as mock_positions:
            with patch.object(broker, '_pull_broker_all_orders') as mock_orders:
                mock_positions.return_value = []
                mock_orders.return_value = []
                
                # Should handle None strategy without errors
                try:
                    broker.do_polling()
                except Exception as e:
                    # Should not get AttributeError: 'NoneType' object has no attribute 'name'
                    self.assertNotIn("'NoneType' object has no attribute 'name'", str(e))


if __name__ == '__main__':
    unittest.main() 