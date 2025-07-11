import logging
import os
import pytest
import math
import datetime as dt
from datetime import timedelta
import pytz
from unittest.mock import MagicMock

from lumibot.data_sources import AlpacaData, DataSource
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset, Quote
from lumibot.tools import get_trading_days, is_market_open
from lumibot.credentials import ALPACA_TEST_CONFIG
from tests.fixtures import BaseDataSourceTester

logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_display_precision()

if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


# @pytest.mark.skip()
class TestAlpacaData(BaseDataSourceTester):

    def _create_data_source(self, tzinfo: pytz.tzinfo = None, remove_incomplete_current_bar=False) -> DataSource:
        return AlpacaData(
            config=ALPACA_TEST_CONFIG,
            tzinfo=tzinfo,
            remove_incomplete_current_bar=remove_incomplete_current_bar
        )

    def test_get_last_price_crypto(self):
        data_source = self._create_data_source()
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        self.check_get_last_price(data_source, asset, quote_asset)
        # test tuple
        asset_tuple = (asset, quote_asset)
        self.check_get_last_price(data_source, asset_tuple)

    def test_get_last_price_stock(self):
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        self.check_get_last_price(data_source, asset, quote_asset)
        # test tuple
        asset_tuple = (asset, quote_asset)
        self.check_get_last_price(data_source, asset_tuple)

    def test_get_historical_prices_daily_bars_stock(self):
        data_source = self._create_data_source()
        asset = Asset("SPY")
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = 'NYSE'
        now = dt.datetime.now(data_source.tzinfo)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                time_check=dt.time(0,0),
                market=market
            )

    def test_get_historical_prices_daily_bars_stock_remove_incomplete_current_bar(self):
        data_source = self._create_data_source(remove_incomplete_current_bar=True)
        asset = Asset("SPY")
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = 'NYSE'
        now = dt.datetime.now(data_source.tzinfo)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                time_check=dt.time(0,0),
                market=market,
                remove_incomplete_current_bar=True
            )

    def test_get_historical_prices_daily_bars_stock_tuple(self):
        data_source = self._create_data_source()
        asset = Asset("SPY")
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = 'NYSE'
        now = dt.datetime.now(data_source.tzinfo)

        asset_tuple = (asset, quote_asset)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset_tuple,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                time_check=dt.time(0,0),
                market=market
            )


    def test_get_historical_prices_daily_bars_stock_split_adjusted(self):
        """Test that when get_historical_prices is called, it uses adjustment=Adjustment.ALL for stock bars."""
        from unittest.mock import patch, Mock
        from alpaca.data.enums import Adjustment
        import pandas as pd

        # Create a data source with auto_adjust=True to ensure it uses Adjustment.ALL
        data_source = self._create_data_source(remove_incomplete_current_bar=True)

        # Verify that auto_adjust is True
        assert data_source._auto_adjust is True, "Expected data_source._auto_adjust to be True"

        # Create a subclass of StockBarsRequest that we can use to check the adjustment parameter
        from alpaca.data.requests import StockBarsRequest

        class TestStockBarsRequest(StockBarsRequest):
            def __init__(self, *args, **kwargs):
                self.test_kwargs = kwargs  # Store the kwargs for testing
                super().__init__(*args, **kwargs)

        # Store the original StockBarsRequest class
        import alpaca.data.requests
        original_stock_bars_request = alpaca.data.requests.StockBarsRequest

        # Replace StockBarsRequest with our test class
        alpaca.data.requests.StockBarsRequest = TestStockBarsRequest

        # Create a mock for the client to avoid API calls
        mock_client = Mock()
        mock_barset = Mock()
        mock_barset.df = pd.DataFrame()  # Empty DataFrame
        mock_client.get_stock_bars.return_value = mock_barset

        # Store the original _get_stock_client method
        original_get_stock_client = data_source._get_stock_client

        # Replace _get_stock_client with our mock
        data_source._get_stock_client = lambda: mock_client

        # Create test parameters
        asset = Asset('UGL', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        length = 10

        # Create a variable to store the StockBarsRequest instance
        request_instance = None

        # Create a mock for get_stock_bars that captures the request
        original_get_stock_bars = mock_client.get_stock_bars

        def mock_get_stock_bars(request):
            nonlocal request_instance
            request_instance = request
            return mock_barset

        mock_client.get_stock_bars = mock_get_stock_bars

        try:
            # Call get_historical_prices
            data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset,
                include_after_hours=False,
            )

            # Verify that a request was created
            assert request_instance is not None, "No StockBarsRequest was created"

            # Verify that the request has the adjustment parameter set to Adjustment.ALL
            assert hasattr(request_instance, 'adjustment'), "StockBarsRequest does not have an adjustment attribute"
            assert request_instance.adjustment == Adjustment.ALL, f"Expected adjustment to be Adjustment.ALL, but got {request_instance.adjustment}"

        finally:
            # Restore the original classes and methods
            alpaca.data.requests.StockBarsRequest = original_stock_bars_request
            data_source._get_stock_client = original_get_stock_client
            mock_client.get_stock_bars = original_get_stock_bars

    @pytest.mark.xfail(reason="need to handle github timezone")
    def test_get_historical_prices_daily_bars_crypto(self):
        data_source = self._create_data_source()
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = '24/7'
        now = dt.datetime.now(data_source.tzinfo)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,

                # default crypto timezone is America/Chicago
                time_check=dt.time(1,0),
                market=market
            )

    @pytest.mark.xfail(reason="need to handle github timezone")
    def test_get_historical_prices_daily_bars_crypto_tuple(self):
        data_source = self._create_data_source()
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = '24/7'
        now = dt.datetime.now(data_source.tzinfo)

        asset_tuple = (asset, quote_asset)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset_tuple,
                length=length,
                timestep=timestep,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,

                # default crypto timezone is America/Chicago
                time_check=dt.time(1 ,0),
                market=market
            )

    def test_get_historical_prices_daily_bars_crypto_america_chicago(self):
        tzinfo = pytz.timezone('America/Chicago')
        data_source = self._create_data_source(tzinfo=tzinfo)
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = '24/7'
        now = dt.datetime.now(data_source.tzinfo)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset,
                include_after_hours=True
            )

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                time_check=dt.time(0 ,0),
                market=market
            )

    def test_get_historical_prices_minute_bars_stock_regular_hours(self):
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "minute"
        market = 'NYSE'
        now = dt.datetime.now(data_source.tzinfo)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset,
                include_after_hours=False,
            )
            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now - data_source._delay,
                data_source_tz=data_source.tzinfo,
                market=market,
            )

    def test_get_historical_prices_minute_bars_crypto_america_chicago(self):
        tzinfo = pytz.timezone('America/Chicago')
        data_source = self._create_data_source(tzinfo=tzinfo)
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "minute"
        market = '24/7'
        now = dt.datetime.now(data_source.tzinfo)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset
            )
            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source.tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
            )

    def test_get_historical_option_prices(self):
        length = 30
        ticker = 'SPY'
        asset = Asset("SPY")
        timestep = "day"
        data_source = self._create_data_source()
        now = dt.datetime.now(data_source.tzinfo)

        # Get a 0dte option
        # calculate the last calendar day before today
        trading_days = get_trading_days(
            start_date=(dt.datetime.now() - dt.timedelta(days=5)).strftime('%Y-%m-%d'),
            end_date=(dt.datetime.now() - dt.timedelta(days=1)).strftime('%Y-%m-%d')
        )
        dte = trading_days.index[-1]

        spy_price = data_source.get_last_price(asset=asset)
        o_asset = Asset(ticker, Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')

        bars = data_source.get_historical_prices(asset=o_asset, length=length, timestep=timestep)
        assert len(bars.df) > 0

    def check_quote_data(self, quote_data):
        """Helper method to check quote data structure"""
        from lumibot.entities import Quote
        assert quote_data is not None
        assert isinstance(quote_data, Quote)
        assert hasattr(quote_data, 'bid') and quote_data.bid is not None
        assert hasattr(quote_data, 'ask') and quote_data.ask is not None
        assert hasattr(quote_data, 'price') and quote_data.price is not None
        assert hasattr(quote_data, 'asset')

    def test_get_quote_stock(self):
        """Test get_quote for stock assets"""
        if not is_market_open(dt.datetime.now()): return
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')

        quote_data = data_source.get_quote(asset, quote_asset)
        self.check_quote_data(quote_data)

    def test_get_quote_crypto(self):
        """Test get_quote for crypto assets"""
        data_source = self._create_data_source()
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')

        quote_data = data_source.get_quote(asset, quote_asset)
        self.check_quote_data(quote_data)

    def test_get_quote_when_stock_bid_is_zero(self):
        """Test get_quote when stock bid is 0.0"""
        from lumibot.entities import Quote

        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Create a mock for the quote object
        mock_quote = MagicMock()
        mock_quote.bid_price = 0.0
        mock_quote.ask_price = 100.0  # Some non-zero value for ask

        # Store the original method
        original_get_stock_client = data_source._get_stock_client

        # Create a mock client
        mock_client = MagicMock()
        mock_client.get_stock_latest_quote.return_value = {asset.symbol: mock_quote}

        # Replace the _get_stock_client method temporarily
        data_source._get_stock_client = lambda: mock_client

        try:
            # Call get_quote
            quote_data = data_source.get_quote(asset)

            # Verify the results
            assert quote_data is not None
            assert isinstance(quote_data, Quote)
            assert quote_data.bid == 0.0
            assert quote_data.ask == 100.0
            # price should be None if bid or ask is zero
            assert quote_data.price is None
            assert quote_data.asset == asset

        finally:
            # Restore the original method
            data_source._get_stock_client = original_get_stock_client

    def test_get_quote_when_stock_ask_is_zero(self):
        """Test get_quote when stock ask is 0.0"""
        from lumibot.entities import Quote

        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Create a mock for the quote object
        mock_quote = MagicMock()
        mock_quote.bid_price = 100.0
        mock_quote.ask_price = 0.0

        # Store the original method
        original_get_stock_client = data_source._get_stock_client

        # Create a mock client
        mock_client = MagicMock()
        mock_client.get_stock_latest_quote.return_value = {asset.symbol: mock_quote}

        # Replace the _get_stock_client method temporarily
        data_source._get_stock_client = lambda: mock_client

        try:
            # Call get_quote
            quote_data = data_source.get_quote(asset)

            # Verify the results
            assert quote_data is not None
            assert isinstance(quote_data, Quote)
            assert quote_data.bid == 100.0
            assert quote_data.ask == 0.0
            # price should be None if bid or ask is zero
            assert quote_data.price is None
            assert quote_data.asset == asset

        finally:
            # Restore the original method
            data_source._get_stock_client = original_get_stock_client

    def test_get_last_price_stock_when_bid_is_zero(self):
        """Test get_last_price when stock bid is 0.0"""
        from lumibot.entities import Quote

        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Store the original method
        original_get_quote = data_source.get_quote

        # Create a mock Quote object
        mock_quote = Quote(
            asset=asset,
            price=None,
            bid=0.0,
            ask=100.0
        )

        # Replace the get_quote method temporarily
        data_source.get_quote = lambda a, q=None, e=None: mock_quote if a.symbol == asset.symbol else None

        try:
            # Call get_last_price
            price = data_source.get_last_price(asset)

            # Verify the results
            # Since price is None and bid is 0.0, it should return the ask value
            assert price == 100.0

        finally:
            # Restore the original method
            data_source.get_quote = original_get_quote

    def test_get_last_price_stock_when_ask_is_zero(self):
        """Test get_last_price when stock ask is 0.0"""
        from lumibot.entities import Quote

        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Store the original method
        original_get_quote = data_source.get_quote

        # Create a mock Quote object
        mock_quote = Quote(
            asset=asset,
            price=None,
            bid=100.0,  # Some non-zero value for bid
            ask=0.0
        )

        # Replace the get_quote method temporarily
        data_source.get_quote = lambda a, q=None, e=None: mock_quote if a.symbol == asset.symbol else None

        try:
            # Call get_last_price
            price = data_source.get_last_price(asset)

            # Verify the results
            # Since price is None and ask is 0.0 it should return the bid value (100.0)
            assert price == 100.0

        finally:
            # Restore the original method
            data_source.get_quote = original_get_quote

    def test_get_last_price_stock_when_last(self):
        """Test get_last_price when price is available"""
        from lumibot.entities import Quote

        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Store the original method
        original_get_quote = data_source.get_quote

        # Create a mock Quote object
        mock_quote = Quote(
            asset=asset,
            price=100.5,
            bid=100.0,
            ask=101.0
        )

        # Replace the get_quote method temporarily
        data_source.get_quote = lambda a, q=None, e=None: mock_quote if a.symbol == asset.symbol else None

        try:
            # Call get_last_price
            price = data_source.get_last_price(asset)

            # Verify the results - should return the price value
            assert price == 100.5

        finally:
            # Restore the original method
            data_source.get_quote = original_get_quote

    def test_get_quote_when_stock_bid_and_ask(self):
        """Test get_quote when stock ask and ask"""
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Create a mock for the quote object
        mock_quote = MagicMock()
        mock_quote.bid_price = 100.0
        mock_quote.ask_price = 101.0

        # Store the original method
        original_get_stock_client = data_source._get_stock_client

        # Create a mock client
        mock_client = MagicMock()
        mock_client.get_stock_latest_quote.return_value = {asset.symbol: mock_quote}

        # Replace the _get_stock_client method temporarily
        data_source._get_stock_client = lambda: mock_client

        try:
            # Call get_quote
            quote_data: Quote = data_source.get_quote(asset)

            # Verify the resultss
            assert quote_data is not None
            assert isinstance(quote_data, Quote)
            assert quote_data.bid == 100.0
            assert quote_data.ask == 101.0
            assert quote_data.price == 100.5
            assert quote_data.asset.symbol == asset.symbol

        finally:
            # Restore the original method
            data_source._get_stock_client = original_get_stock_client

    # ============= OAuth Tests for AlpacaData =============

    def test_oauth_data_source_initialization(self):
        """Test that AlpacaData can be initialized with OAuth token only."""
        oauth_config = {
            "OAUTH_TOKEN": "test_oauth_token_alpaca_data",
            "PAPER": True
        }

        data_source = AlpacaData(oauth_config)
        assert data_source.oauth_token == "test_oauth_token_alpaca_data"
        assert data_source.api_key is None
        assert data_source.api_secret is None
        assert data_source.is_paper == True

    def test_oauth_client_initialization(self):
        """Test that OAuth clients are properly initialized."""
        oauth_config = {
            "OAUTH_TOKEN": "test_oauth_token_clients",
            "PAPER": True
        }

        data_source = AlpacaData(oauth_config)

        # Test stock client
        stock_client = data_source._get_stock_client()
        assert stock_client is not None

        # Test crypto client
        crypto_client = data_source._get_crypto_client()
        assert crypto_client is not None

        # Test option client
        option_client = data_source._get_option_client()
        assert option_client is not None

    def test_oauth_priority_over_api_key(self):
        """Test that OAuth token takes priority over API key/secret."""
        mixed_config = {
            "OAUTH_TOKEN": "priority_oauth_token",
            "API_KEY": "should_not_be_used",
            "API_SECRET": "should_not_be_used_either",
            "PAPER": True
        }

        data_source = AlpacaData(mixed_config)
        assert data_source.oauth_token == "priority_oauth_token"
        assert data_source.api_key is None
        assert data_source.api_secret is None

    def test_oauth_empty_fallback_to_api_key(self):
        """Test fallback to API key when OAuth token is empty."""
        fallback_config = {
            "OAUTH_TOKEN": "",  # Empty OAuth token
            "API_KEY": "fallback_key",
            "API_SECRET": "fallback_secret",
            "PAPER": True
        }

        data_source = AlpacaData(fallback_config)
        assert data_source.oauth_token is None
        assert data_source.api_key == "fallback_key"
        assert data_source.api_secret == "fallback_secret"

    def test_oauth_none_fallback_to_api_key(self):
        """Test fallback to API key when OAuth token is None."""
        fallback_config = {
            "OAUTH_TOKEN": None,  # None OAuth token
            "API_KEY": "fallback_key_none",
            "API_SECRET": "fallback_secret_none",
            "PAPER": True
        }

        data_source = AlpacaData(fallback_config)
        assert data_source.oauth_token is None
        assert data_source.api_key == "fallback_key_none"
        assert data_source.api_secret == "fallback_secret_none"

    def test_oauth_no_credentials_error(self):
        """Test error when no authentication credentials provided."""
        empty_config = {
            "PAPER": True
        }

        with pytest.raises(ValueError, match="Either OAuth token or API key/secret must be provided"):
            AlpacaData(empty_config)

    def test_oauth_missing_api_secret_error(self):
        """Test error when API key provided but secret is missing."""
        incomplete_config = {
            "API_KEY": "key_without_secret",
            "PAPER": True
        }

        with pytest.raises(ValueError, match="API_SECRET not found in config when API_KEY is provided"):
            AlpacaData(incomplete_config)

    def test_default_delay_value(self):
        """Test that the default delay value is 16 minutes when not specified."""
        # Save the original environment variable value
        original_env_value = os.environ.get("DATA_SOURCE_DELAY")

        try:
            # Ensure the environment variable is not set
            if "DATA_SOURCE_DELAY" in os.environ:
                del os.environ["DATA_SOURCE_DELAY"]

            # Create a data source without specifying delay
            data_source = self._create_data_source()

            # Check that the delay is 16 minutes
            assert data_source._delay == timedelta(minutes=16), f"Expected delay to be 16 minutes, but got {data_source._delay}"

        finally:
            # Restore the original environment variable value
            if original_env_value is not None:
                os.environ["DATA_SOURCE_DELAY"] = original_env_value
            elif "DATA_SOURCE_DELAY" in os.environ:
                del os.environ["DATA_SOURCE_DELAY"]

    def test_data_source_delay_env_var(self):
        """Test that AlpacaData uses the DATA_SOURCE_DELAY environment variable when set."""
        # Save the original environment variable value
        original_env_value = os.environ.get("DATA_SOURCE_DELAY")

        try:
            # Set the environment variable to a test value
            test_delay = "25"
            os.environ["DATA_SOURCE_DELAY"] = test_delay

            # Create a data source without specifying delay
            data_source = self._create_data_source()

            # Check that the delay matches the environment variable value
            assert data_source._delay == timedelta(minutes=int(test_delay)), \
                f"Expected delay to be {test_delay} minutes, but got {data_source._delay}"

        finally:
            # Restore the original environment variable value
            if original_env_value is not None:
                os.environ["DATA_SOURCE_DELAY"] = original_env_value
            elif "DATA_SOURCE_DELAY" in os.environ:
                del os.environ["DATA_SOURCE_DELAY"]
