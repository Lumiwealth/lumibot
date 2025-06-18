import pytest
from unittest.mock import MagicMock

from lumibot.entities import Asset, Order
from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.credentials import ALPACA_TEST_CONFIG

from datetime import datetime, timedelta

import math

from lumibot.tools import get_trading_days

if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


class TestAlpacaBroker:

    def test_initialize_broker_legacy(self):
        """
        This test to make sure the legacy way of initializing the broker still works.
        """
        broker = Alpaca(ALPACA_TEST_CONFIG)
        strategy = BuyAndHold(
            broker=broker,
        )

        # Assert that strategy.broker is the same as broker
        assert strategy.broker == broker

        # Assert that strategy.data_source is AlpacaData object
        assert isinstance(strategy.broker.data_source, AlpacaData)

    def test_submit_order_calls_conform_order(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        broker._conform_order = MagicMock()
        order = Order(asset=Asset("SPY"), quantity=10, side=Order.OrderSide.BUY, strategy='abc')
        broker.submit_order(order=order)
        broker._conform_order.assert_called_once()

    def test_limit_order_conforms_when_limit_price_gte_one_dollar(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        order = Order(asset=Asset("SPY"), quantity=10, side=Order.OrderSide.BUY, limit_price=1.123455, strategy='abc')
        broker._conform_order(order)
        assert order.limit_price == 1.12

    def test_limit_order_conforms_when_limit_price_lte_one_dollar(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        order = Order(asset=Asset("SPY"), quantity=10, side=Order.OrderSide.BUY, limit_price=0.12345, strategy='abc')
        broker._conform_order(order)
        assert order.limit_price == 0.1235

    # The tests below exist to make sure the BROKER calls pass through the data source correctly.
    # Testing that the DATA is CORRECT (vs just existing) happens in test_alpaca_data.

    @pytest.mark.skip(reason="This test is doesn't work.")
    def test_option_get_last_price(self):
        broker = Alpaca(
            ALPACA_TEST_CONFIG,
        )
        # calculate the last calendar day before today
        trading_days = get_trading_days(
            start_date=(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        )
        dte = trading_days.index[-1]
        spy_price = broker.get_last_price(asset=Asset('SPY'))
        price = broker.get_last_price(asset=Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL'))
        assert price != 0

    def test_stock_get_last_price(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        price = broker.get_last_price(asset=Asset('SPY'))
        assert price != 0
        price = broker.get_last_price(asset=Asset(symbol='SPY'))
        assert price != 0

    def test_crypto_get_last_price(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        base = Asset(symbol="BTC", asset_type=Asset.AssetType.CRYPTO)
        quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
        price = broker.get_last_price(asset=base, quote=quote)
        assert price != 0

    def test_get_historical_prices(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)
        asset = Asset('SPY', Asset.AssetType.STOCK)
        bars = broker.data_source.get_historical_prices(asset, 10, "day")
        assert len(bars.df) > 0

    def test_option_get_historical_prices(self):
        broker = Alpaca(ALPACA_TEST_CONFIG)

        # calculate the last calendar day before today
        trading_days = get_trading_days(
            start_date=(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        )
        dte = trading_days.index[-1]
        
        spy_price = broker.get_last_price(asset=Asset('SPY'))
        asset = Asset('SPY', Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')
        print(asset)
        bars = broker.data_source.get_historical_prices(asset, 10, "day")
        assert len(bars.df) > 0

    # ============= OAuth Tests =============
    
    def test_oauth_broker_initialization(self):
        """Test that Alpaca broker can be initialized with OAuth token only."""
        oauth_config = {
            "OAUTH_TOKEN": "test_oauth_token",
            "PAPER": True
        }
        
        broker = Alpaca(oauth_config, connect_stream=False)
        assert broker.oauth_token == "test_oauth_token"
        assert broker.api_key == ""
        assert broker.api_secret == ""
        assert broker.is_paper == True
        assert broker.is_oauth_only == True
        
    def test_oauth_mixed_credentials(self):
        """Test that mixed OAuth + API credentials work correctly."""
        mixed_config = {
            "OAUTH_TOKEN": "test_oauth_token",
            "API_KEY": "test_api_key", 
            "API_SECRET": "test_api_secret",
            "PAPER": True
        }
        
        broker = Alpaca(mixed_config, connect_stream=False)
        assert broker.oauth_token == "test_oauth_token"
        assert broker.api_key == "test_api_key"
        assert broker.api_secret == "test_api_secret"
        assert broker.is_oauth_only == False  # Has both OAuth and API credentials
        
    def test_oauth_fallback_to_api_keys(self):
        """Test that broker falls back to API keys when OAuth token is empty."""
        fallback_config = {
            "OAUTH_TOKEN": "",  # Empty OAuth token
            "API_KEY": "test_api_key",
            "API_SECRET": "test_api_secret", 
            "PAPER": True
        }
        
        broker = Alpaca(fallback_config, connect_stream=False)
        assert broker.oauth_token == ""
        assert broker.api_key == "test_api_key"
        assert broker.api_secret == "test_api_secret"
        assert broker.is_oauth_only == False
        
    def test_oauth_error_on_missing_credentials(self):
        """Test that proper error is raised when no credentials are provided."""
        empty_config = {"PAPER": True}
        
        with pytest.raises(ValueError, match="Either OAuth token or API key/secret must be provided"):
            Alpaca(empty_config, connect_stream=False)
            
    def test_oauth_stream_object_creation(self):
        """Test that correct stream object is created for OAuth vs API key configurations."""
        from lumibot.trading_builtins import PollingStream
        from alpaca.trading.stream import TradingStream
        
        # OAuth-only should use PollingStream
        oauth_config = {
            "OAUTH_TOKEN": "test_oauth_token",
            "PAPER": True
        }
        broker_oauth = Alpaca(oauth_config, connect_stream=False)
        stream_oauth = broker_oauth._get_stream_object()
        assert isinstance(stream_oauth, PollingStream)
        
        # API key/secret should use TradingStream  
        api_config = {
            "API_KEY": "test_api_key",
            "API_SECRET": "test_api_secret",
            "PAPER": True
        }
        broker_api = Alpaca(api_config, connect_stream=False)
        stream_api = broker_api._get_stream_object()
        assert isinstance(stream_api, TradingStream)
        
    def test_oauth_polling_interval(self):
        """Test that polling interval is properly set."""
        oauth_config = {
            "OAUTH_TOKEN": "test_oauth_token", 
            "PAPER": True
        }
        
        # Test default polling interval
        broker = Alpaca(oauth_config, connect_stream=False)
        assert broker.polling_interval == 5.0
        
        # Test custom polling interval
        broker_custom = Alpaca(oauth_config, connect_stream=False, polling_interval=10.0)
        assert broker_custom.polling_interval == 10.0
