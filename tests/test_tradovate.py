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
- Symbol conversion for continuous futures contracts
- Order submission functionality

All tests use appropriate mocking to prevent actual API calls during testing,
making them suitable for CI/CD environments like GitHub Actions.
"""

import pytest
import os
from unittest.mock import patch, MagicMock, Mock
import logging
import time
import requests


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
        from unittest.mock import patch
        
        # Test with empty/minimal config
        empty_config = {}
        
        # Mock the requests to avoid actual API calls
        with patch('requests.post') as mock_post:
            # Mock a failure response that would happen with missing credentials
            mock_response = mock_post.return_value
            mock_response.status_code = 400
            mock_response.json.return_value = {"errorText": "Missing credentials"}
            mock_response.raise_for_status.side_effect = Exception("Bad Request")
            
            try:
                broker = Tradovate(config=empty_config)
                # Should not reach here with empty config
                assert False, "Broker should have failed with empty config"
            except Exception as e:
                # Should fail due to authentication/credentials issue
                error_msg = str(e).lower()
                # The error should be about authentication or bad request
                assert any(keyword in error_msg for keyword in ['authentication', 'failed', 'bad request', 'credentials'])


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


class TestTradovateSymbolConversion:
    """Test symbol conversion functionality for continuous futures."""
    
    def test_continuous_futures_symbol_resolution(self):
        """Test that continuous futures symbols are resolved to specific contracts."""
        from lumibot.entities import Asset
        
        # Test MES conversion
        mes_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        mes_resolved = mes_asset.resolve_continuous_futures_contract()
        
        # Should resolve to a specific contract like MESU25, MESZ25, etc.
        assert mes_resolved != "MES"
        assert "MES" in mes_resolved
        assert len(mes_resolved) >= 5  # Should be like MESU25
        
        # Test ES conversion
        es_asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
        es_resolved = es_asset.resolve_continuous_futures_contract()
        
        assert es_resolved != "ES"
        assert "ES" in es_resolved
        assert len(es_resolved) >= 4  # Should be like ESU25
        
        # Test that specific futures contracts don't get converted 
        # (they should use the symbol as-is)
        specific_asset = Asset("MESU25", asset_type=Asset.AssetType.FUTURE)
        # For specific contracts, we just use the symbol directly
        assert specific_asset.symbol == "MESU25"  # Should remain unchanged
        
        print(f"✅ Symbol conversion test passed:")
        print(f"   MES -> {mes_resolved}")
        print(f"   ES -> {es_resolved}")
        print(f"   MESU25 -> MESU25 (specific contract, unchanged)")

    def test_order_symbol_extraction(self):
        """Test that orders use the correct symbol for submission."""
        from lumibot.entities import Asset, Order
        from unittest.mock import MagicMock
        
        # Create continuous futures asset
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Create a mock strategy for the order
        mock_strategy = MagicMock()
        
        # Create order
        order = Order(
            strategy=mock_strategy,
            asset=asset,
            quantity=1,
            side="buy",
            order_type=Order.OrderType.MARKET
        )
        
        # Test symbol extraction logic used in order submission
        if order.asset.asset_type == order.asset.AssetType.CONT_FUTURE:
            symbol = order.asset.resolve_continuous_futures_contract()
        else:
            symbol = order.asset.symbol
        
        # Should be resolved to specific contract
        assert symbol != "MES"
        assert "MES" in symbol
        assert len(symbol) >= 5
        
        print(f"✅ Order symbol extraction test passed: MES -> {symbol}")


class TestTradovateIntegration:
    """Integration tests that combine multiple components."""
    
    def test_end_to_end_order_flow(self):
        """Test the complete order flow from asset creation to submission."""
        from lumibot.entities import Asset, Order
        
        # Step 1: Create continuous futures asset
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        assert asset.symbol == "MES"
        assert asset.asset_type == Asset.AssetType.CONT_FUTURE
        
        # Step 2: Resolve symbol for trading
        resolved_symbol = asset.resolve_continuous_futures_contract()
        assert resolved_symbol != "MES"
        assert "MES" in resolved_symbol
        
        # Step 3: Create order
        mock_strategy = MagicMock()
        order = Order(
            strategy=mock_strategy,
            asset=asset,
            quantity=1,
            side="buy",
            order_type=Order.OrderType.MARKET
        )
        
        # Step 4: Verify order has correct symbol resolution
        if order.asset.asset_type == order.asset.AssetType.CONT_FUTURE:
            order_symbol = order.asset.resolve_continuous_futures_contract()
        else:
            order_symbol = order.asset.symbol
            
        assert order_symbol == resolved_symbol
        assert order_symbol != "MES"
        
        print(f"✅ End-to-end order flow test passed: MES -> {order_symbol}")


class TestTradovateSymbolResolution:
    """Test Tradovate-specific symbol resolution with 1-digit year format."""
    
    def test_tradovate_symbol_format(self):
        """Test that Tradovate broker generates correct symbol format with 1-digit year."""
        from lumibot.brokers.tradovate import Tradovate
        from unittest.mock import patch
        
        # Mock datetime to control the current date
        with patch('datetime.datetime') as mock_datetime:
            # Set current date to July 9, 2025
            mock_datetime.now.return_value.month = 7
            mock_datetime.now.return_value.year = 2025
            
            # Create a mock Tradovate broker instance
            broker = Tradovate.__new__(Tradovate)
            
            # Create a mock asset
            from lumibot.entities import Asset
            asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
            
            # Test the Tradovate-specific symbol resolution
            symbol = broker._resolve_tradovate_futures_symbol(asset)
            
            # July should resolve to September (U) with 1-digit year (5 for 2025)
            expected_symbol = "MNQU5"
            assert symbol == expected_symbol, f"Expected {expected_symbol}, got {symbol}"
            
            print(f"✅ Tradovate symbol format test passed: MNQ -> {symbol}")
    
    def test_tradovate_symbol_different_months(self):
        """Test symbol resolution for different months."""
        from lumibot.brokers.tradovate import Tradovate
        from unittest.mock import patch
        
        broker = Tradovate.__new__(Tradovate)
        from lumibot.entities import Asset
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        test_cases = [
            (1, 2025, "MESM5"),   # January -> June (M5)
            (3, 2025, "MESM5"),   # March -> June (M5) 
            (5, 2025, "MESU5"),   # May -> September (U5)
            (7, 2025, "MESU5"),   # July -> September (U5)
            (9, 2025, "MESU5"),   # September -> September (U5)
            (11, 2025, "MESZ5"),  # November -> December (Z5)
            (12, 2026, "MESZ6"),  # December 2026 -> December (Z6)
        ]
        
        for month, year, expected in test_cases:
            with patch('datetime.datetime') as mock_datetime:
                mock_datetime.now.return_value.month = month
                mock_datetime.now.return_value.year = year
                
                symbol = broker._resolve_tradovate_futures_symbol(asset)
                assert symbol == expected, f"Month {month}/{year}: Expected {expected}, got {symbol}"
        
        print("✅ Tradovate symbol resolution for different months test passed")
    
    def test_tradovate_vs_standard_symbol_difference(self):
        """Test that Tradovate symbols differ from standard 2-digit year format."""
        from lumibot.brokers.tradovate import Tradovate
        from lumibot.entities import Asset
        from unittest.mock import patch
        
        broker = Tradovate.__new__(Tradovate)
        asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value.month = 7
            mock_datetime.now.return_value.year = 2025
            
            # Get Tradovate-specific symbol (1-digit year)
            tradovate_symbol = broker._resolve_tradovate_futures_symbol(asset)
            
            # Get standard symbol (2-digit year)
            standard_symbol = asset.resolve_continuous_futures_contract()
            
            # They should be different
            assert tradovate_symbol != standard_symbol
            assert tradovate_symbol == "MNQU5"  # 1-digit year
            assert standard_symbol == "MNQU25"  # 2-digit year
            
        print(f"✅ Symbol format difference test passed: Tradovate={tradovate_symbol}, Standard={standard_symbol}")


class TestTradovateAPIPayload:
    """Test Tradovate API payload format and field names."""
    
    def test_limit_order_payload_format(self):
        """Test that limit orders use 'price' field not 'limitPrice'."""
        from lumibot.brokers.tradovate import Tradovate
        from lumibot.entities import Asset, Order
        from unittest.mock import MagicMock, patch
        
        # Mock the broker initialization
        broker = Tradovate.__new__(Tradovate)
        broker.account_spec = "TEST_ACCOUNT"
        broker.account_id = 12345
        broker.trading_token = "fake_token"
        broker.trading_api_url = "https://demo.tradovateapi.com/v1"
        
        # Create test order
        asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        mock_strategy = MagicMock()
        order = Order(
            strategy=mock_strategy,
            asset=asset,
            quantity=1,
            side="buy",
            order_type=Order.OrderType.LIMIT,
            limit_price=20000.0
        )
        
        # Mock the symbol resolution
        with patch.object(broker, '_resolve_tradovate_futures_symbol', return_value='MNQU5'):
            with patch.object(broker, '_get_headers') as mock_headers:
                with patch('lumibot.brokers.tradovate.requests.post') as mock_post:
                    # Mock successful response
                    mock_response = MagicMock()
                    mock_response.status_code = 200
                    mock_response.json.return_value = {"orderId": 123456}
                    mock_post.return_value = mock_response
                    
                    # Submit the order
                    result = broker._submit_order(order)
                    
                    # Check that the request was made with correct payload
                    assert mock_post.called
                    call_args = mock_post.call_args
                    payload = call_args[1]['json']  # Get the JSON payload
                    
                    # Verify correct field names per Tradovate API
                    assert 'price' in payload, "Limit orders should use 'price' field"
                    assert 'limitPrice' not in payload, "Should not use 'limitPrice' field"
                    assert payload['price'] == 20000.0
                    assert payload['symbol'] == 'MNQU5'
                    assert payload['orderType'] == 'Limit'
                    
        print("✅ Limit order payload format test passed")
    
    def test_stop_order_payload_format(self):
        """Test that stop orders use 'stopPrice' field."""
        from lumibot.brokers.tradovate import Tradovate
        from lumibot.entities import Asset, Order
        from unittest.mock import MagicMock, patch
        
        # Mock the broker initialization
        broker = Tradovate.__new__(Tradovate)
        broker.account_spec = "TEST_ACCOUNT"
        broker.account_id = 12345
        broker.trading_token = "fake_token"
        broker.trading_api_url = "https://demo.tradovateapi.com/v1"
        
        # Create test order
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        mock_strategy = MagicMock()
        order = Order(
            strategy=mock_strategy,
            asset=asset,
            quantity=1,
            side="sell",
            order_type=Order.OrderType.STOP,
            stop_price=4500.0
        )
        
        # Mock the symbol resolution and submission
        with patch.object(broker, '_resolve_tradovate_futures_symbol', return_value='MESU5'):
            with patch.object(broker, '_get_headers') as mock_headers:
                with patch('lumibot.brokers.tradovate.requests.post') as mock_post:
                    # Mock successful response
                    mock_response = MagicMock()
                    mock_response.status_code = 200
                    mock_response.json.return_value = {"orderId": 123457}
                    mock_post.return_value = mock_response
                    
                    # Submit the order
                    result = broker._submit_order(order)
                    
                    # Check payload
                    call_args = mock_post.call_args
                    payload = call_args[1]['json']
                    
                    # Verify stop price field
                    assert 'stopPrice' in payload
                    assert payload['stopPrice'] == 4500.0
                    assert payload['symbol'] == 'MESU5'
                    assert payload['orderType'] == 'Stop'
                    
        print("✅ Stop order payload format test passed")


class TestTradovateTokenRenewal:
    """Test the token renewal functionality."""
    
    def test_token_renewal_on_expiry(self):
        """Test that tokens are renewed when they're about to expire."""
        from lumibot.brokers.tradovate import Tradovate
        from unittest.mock import patch, MagicMock
        
        # Mock the broker initialization
        with patch.object(Tradovate, '_get_tokens') as mock_get_tokens, \
             patch.object(Tradovate, '_get_account_info') as mock_get_account_info, \
             patch.object(Tradovate, '_get_user_info') as mock_get_user_info:
            
            # Initial token response
            mock_get_tokens.return_value = {
                'accessToken': 'initial_token',
                'marketToken': 'initial_market_token',
                'hasMarketData': True
            }
            
            mock_get_account_info.return_value = {
                'accountSpec': 'TEST_ACCOUNT',
                'accountId': 12345
            }
            
            mock_get_user_info.return_value = 'test_user_id'
            
            # Create broker instance
            config = {
                "USERNAME": "test_user",
                "DEDICATED_PASSWORD": "test_pass",
                "CID": "test_cid",
                "SECRET": "test_secret",
                "IS_PAPER": True
            }
            
            broker = Tradovate(config=config)
            
            # Verify initial token
            assert broker.trading_token == 'initial_token'
            assert broker.market_token == 'initial_market_token'
            
            # Mock time to simulate token aging
            original_time = broker.token_acquired_time
            broker.token_acquired_time = original_time - (broker.token_lifetime * 0.95)  # 95% expired
            
            # Update mock to return new tokens
            mock_get_tokens.return_value = {
                'accessToken': 'renewed_token',
                'marketToken': 'renewed_market_token',
                'hasMarketData': True
            }
            
            # Call token check method
            broker._check_and_renew_token()
            
            # Verify tokens were renewed
            assert broker.trading_token == 'renewed_token'
            assert broker.market_token == 'renewed_market_token'
            assert broker.token_acquired_time > original_time
            
        print("✅ Token renewal on expiry test passed")
    
    def test_automatic_retry_on_401(self):
        """Test that API requests automatically retry on 401 errors."""
        from lumibot.brokers.tradovate import Tradovate
        from unittest.mock import patch, MagicMock
        
        # Mock the broker initialization
        with patch.object(Tradovate, '_get_tokens') as mock_get_tokens, \
             patch.object(Tradovate, '_get_account_info') as mock_get_account_info, \
             patch.object(Tradovate, '_get_user_info') as mock_get_user_info:
            
            # Initial setup
            mock_get_tokens.return_value = {
                'accessToken': 'expired_token',
                'marketToken': 'expired_market_token',
                'hasMarketData': True
            }
            
            mock_get_account_info.return_value = {
                'accountSpec': 'TEST_ACCOUNT', 
                'accountId': 12345
            }
            
            mock_get_user_info.return_value = 'test_user_id'
            
            config = {
                "USERNAME": "test_user",
                "DEDICATED_PASSWORD": "test_pass",
                "CID": "test_cid",
                "SECRET": "test_secret",
                "IS_PAPER": True
            }
            
            broker = Tradovate(config=config)
            
            # Force token to be expired
            broker.token_acquired_time = time.time() - (broker.token_lifetime * 0.95)
            
            # Create a mock request function that fails with 401 first time
            call_count = 0
            def mock_request_func():
                nonlocal call_count
                call_count += 1
                
                if call_count == 1:
                    # First call: simulate 401 error
                    response = Mock()
                    response.status_code = 401
                    error = requests.exceptions.HTTPError()
                    error.response = response
                    raise error
                else:
                    # Second call: success after token renewal
                    response = Mock()
                    response.json.return_value = {"success": True}
                    return response
            
            # Update the mock to return new tokens when called again
            mock_get_tokens.return_value = {
                'accessToken': 'new_token',
                'marketToken': 'new_market_token', 
                'hasMarketData': True
            }
            
            # Test the retry mechanism
            result = broker._handle_api_request(mock_request_func)
            
            # Verify it retried and succeeded
            assert call_count == 2
            assert result.json() == {"success": True}
            assert broker.trading_token == 'new_token'
            
        print("✅ Automatic retry on 401 test passed")
    
    def test_get_balances_with_token_renewal(self):
        """Test that _get_balances_at_broker handles token renewal correctly."""
        from lumibot.brokers.tradovate import Tradovate
        from lumibot.entities import Asset
        from unittest.mock import patch, MagicMock, Mock
        
        # Mock the broker initialization
        with patch.object(Tradovate, '_get_tokens') as mock_get_tokens, \
             patch.object(Tradovate, '_get_account_info') as mock_get_account_info, \
             patch.object(Tradovate, '_get_user_info') as mock_get_user_info:
            
            # Initial setup
            mock_get_tokens.return_value = {
                'accessToken': 'initial_token',
                'marketToken': 'initial_market_token',
                'hasMarketData': True
            }
            
            mock_get_account_info.return_value = {
                'accountSpec': 'TEST_ACCOUNT',
                'accountId': 12345
            }
            
            mock_get_user_info.return_value = 'test_user_id'
            
            config = {
                "USERNAME": "test_user",
                "DEDICATED_PASSWORD": "test_pass",
                "CID": "test_cid",
                "SECRET": "test_secret",
                "IS_PAPER": True
            }
            
            broker = Tradovate(config=config)
            
            # Mock requests.post to simulate 401 then success
            call_count = 0
            def mock_post(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                
                response = Mock()
                if call_count == 1:
                    # First call: 401 error
                    response.status_code = 401
                    response.raise_for_status.side_effect = requests.exceptions.HTTPError()
                    response.raise_for_status.side_effect.response = response
                    return response
                else:
                    # Second call: success
                    response.status_code = 200
                    response.json.return_value = {
                        "totalCashValue": 100000,
                        "netLiq": 105000
                    }
                    response.raise_for_status.return_value = None
                    return response
            
            # Force token to be expired  
            broker.token_acquired_time = time.time() - (broker.token_lifetime * 0.95)
            
            # Update the mock to return new tokens when called again
            mock_get_tokens.return_value = {
                'accessToken': 'renewed_token',
                'marketToken': 'renewed_market_token',
                'hasMarketData': True
            }
            
            with patch('requests.post', side_effect=mock_post):
                # Call get_balances
                quote_asset = Asset("USD", asset_type=Asset.AssetType.FOREX)
                cash, positions_value, portfolio_value = broker._get_balances_at_broker(quote_asset, None)
                
                # Verify results
                assert cash == 100000
                assert positions_value == 5000  # netLiq - cash
                assert portfolio_value == 105000
                assert call_count == 2  # Should have retried
                assert broker.trading_token == 'renewed_token'
            
        print("✅ Get balances with token renewal test passed")
    
    def test_proactive_token_check(self):
        """Test the public check_token_expiry method."""
        from lumibot.brokers.tradovate import Tradovate
        from unittest.mock import patch
        
        with patch.object(Tradovate, '_get_tokens') as mock_get_tokens, \
             patch.object(Tradovate, '_get_account_info') as mock_get_account_info, \
             patch.object(Tradovate, '_get_user_info') as mock_get_user_info:
            
            # Initial setup
            mock_get_tokens.return_value = {
                'accessToken': 'initial_token',
                'marketToken': 'initial_market_token',
                'hasMarketData': True
            }
            
            mock_get_account_info.return_value = {
                'accountSpec': 'TEST_ACCOUNT',
                'accountId': 12345
            }
            
            mock_get_user_info.return_value = 'test_user_id'
            
            config = {
                "USERNAME": "test_user",
                "DEDICATED_PASSWORD": "test_pass",
                "CID": "test_cid",
                "SECRET": "test_secret",
                "IS_PAPER": True
            }
            
            broker = Tradovate(config=config)
            
            # Age the token
            broker.token_acquired_time = time.time() - (broker.token_lifetime * 0.95)
            
            # Mock renewal response
            mock_get_tokens.return_value = {
                'accessToken': 'renewed_token',
                'marketToken': 'renewed_market_token',
                'hasMarketData': True
            }
            
            # Call public method
            broker.check_token_expiry()
            
            # Verify renewal happened
            assert broker.trading_token == 'renewed_token'
            assert broker.market_token == 'renewed_market_token'
            
        print("✅ Proactive token check test passed")
    
    def test_token_not_renewed_when_fresh(self):
        """Test that tokens are NOT renewed when they're still fresh."""
        from lumibot.brokers.tradovate import Tradovate
        from unittest.mock import patch
        
        with patch.object(Tradovate, '_get_tokens') as mock_get_tokens, \
             patch.object(Tradovate, '_get_account_info') as mock_get_account_info, \
             patch.object(Tradovate, '_get_user_info') as mock_get_user_info:
            
            # Initial setup
            initial_call_count = 0
            def token_getter():
                nonlocal initial_call_count
                initial_call_count += 1
                return {
                    'accessToken': f'token_{initial_call_count}',
                    'marketToken': f'market_token_{initial_call_count}',
                    'hasMarketData': True
                }
            
            mock_get_tokens.side_effect = token_getter
            
            mock_get_account_info.return_value = {
                'accountSpec': 'TEST_ACCOUNT',
                'accountId': 12345
            }
            
            mock_get_user_info.return_value = 'test_user_id'
            
            config = {
                "USERNAME": "test_user",
                "DEDICATED_PASSWORD": "test_pass",
                "CID": "test_cid",
                "SECRET": "test_secret",
                "IS_PAPER": True
            }
            
            broker = Tradovate(config=config)
            
            # Token should be fresh (just created)
            assert broker.trading_token == 'token_1'
            assert initial_call_count == 1
            
            # Call check method - should NOT renew
            broker._check_and_renew_token()
            
            # Verify no renewal happened
            assert broker.trading_token == 'token_1'  # Still the same
            assert initial_call_count == 1  # No additional calls
            
        print("✅ Token not renewed when fresh test passed")


if __name__ == "__main__":
    # Run all tests when file is executed directly
    pytest.main([__file__, "-v"])
