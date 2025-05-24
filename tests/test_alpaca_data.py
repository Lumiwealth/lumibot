import logging
import pytest
import math
from datetime import datetime, timedelta, time
import pytz
from unittest.mock import MagicMock

from lumibot.data_sources import AlpacaData, DataSource
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset
from lumibot.tools import get_trading_days
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
        now = datetime.now(data_source._tzinfo)

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
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source._tzinfo,
                time_check=time(0,0),
                market=market
            )

    def test_get_historical_prices_daily_bars_stock_remove_incomplete_current_bar(self):
        data_source = self._create_data_source(remove_incomplete_current_bar=True)
        asset = Asset("SPY")
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = 'NYSE'
        now = datetime.now(data_source._tzinfo)

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
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source._tzinfo,
                time_check=time(0,0),
                market=market,
                remove_incomplete_current_bar=True
            )

    def test_get_historical_prices_daily_bars_stock_tuple(self):
        data_source = self._create_data_source()
        asset = Asset("SPY")
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = 'NYSE'
        now = datetime.now(data_source._tzinfo)

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
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source._tzinfo,
                time_check=time(0,0),
                market=market
            )

    @pytest.mark.xfail(reason="need to handle github timezone")
    def test_get_historical_prices_daily_bars_crypto(self):
        data_source = self._create_data_source()
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = '24/7'
        now = datetime.now(data_source._tzinfo)

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
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source._tzinfo,

                # default crypto timezone is America/Chicago
                time_check=time(1,0),
                market=market
            )

    @pytest.mark.xfail(reason="need to handle github timezone")
    def test_get_historical_prices_daily_bars_crypto_tuple(self):
        data_source = self._create_data_source()
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = '24/7'
        now = datetime.now(data_source._tzinfo)

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
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source._tzinfo,

                # default crypto timezone is America/Chicago
                time_check=time(1 ,0),
                market=market
            )

    def test_get_historical_prices_daily_bars_crypto_america_chicago(self):
        tzinfo = pytz.timezone('America/Chicago')
        data_source = self._create_data_source(tzinfo=tzinfo)
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = '24/7'
        now = datetime.now(data_source._tzinfo)

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
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            self.check_daily_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source._tzinfo,
                time_check=time(0 ,0),
                market=market
            )

    def test_get_historical_prices_minute_bars_stock_regular_hours(self):
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "minute"
        market = 'NYSE'
        now = datetime.now(data_source._tzinfo)

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
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now - data_source._delay,
                data_source_tz=data_source._tzinfo,
                market=market,
            )
            
    def test_get_historical_prices_minute_bars_crypto_america_chicago(self):
        tzinfo = pytz.timezone('America/Chicago')
        data_source = self._create_data_source(tzinfo=tzinfo)
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "minute"
        market = '24/7'
        now = datetime.now(data_source._tzinfo)

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset
            )
            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source._tzinfo,
                market=market,
            )

    def test_get_historical_option_prices(self):
        length = 30
        ticker = 'SPY'
        asset = Asset("SPY")
        timestep = "day"
        data_source = self._create_data_source()
        now = datetime.now(data_source._tzinfo)

        # Get a 0dte option
        # calculate the last calendar day before today
        trading_days = get_trading_days(
            start_date=(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        )
        dte = trading_days.index[-1]

        spy_price = data_source.get_last_price(asset=asset)
        o_asset = Asset(ticker, Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')

        bars = data_source.get_historical_prices(asset=o_asset, length=length, timestep=timestep)
        assert len(bars.df) > 0

    def check_quote_data(self, quote_data):
        """Helper method to check quote data structure"""
        assert quote_data is not None
        assert isinstance(quote_data, dict)
        assert 'bid' in quote_data and quote_data['bid'] is not None
        assert 'ask' in quote_data and quote_data['ask'] is not None
        assert 'last' in quote_data and quote_data['last'] is not None
        assert 'symbol' in quote_data

    def test_get_quote_stock(self):
        """Test get_quote for stock assets"""
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
            assert isinstance(quote_data, dict)
            assert 'bid' in quote_data
            assert quote_data['bid'] == 0.0
            assert 'ask' in quote_data
            assert quote_data['ask'] == 100.0
            assert 'last' in quote_data
            # last should be None if bid or ask is zero
            assert quote_data['last'] == None
            assert 'symbol' in quote_data
            assert quote_data['symbol'] == asset.symbol

        finally:
            # Restore the original method
            data_source._get_stock_client = original_get_stock_client

    def test_get_quote_when_stock_ask_is_zero(self):
        """Test get_quote when stock ask is 0.0"""
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
            assert isinstance(quote_data, dict)
            assert 'bid' in quote_data
            assert quote_data['bid'] == 100.0
            assert 'ask' in quote_data
            assert quote_data['ask'] == 0.0
            assert 'last' in quote_data
            # last should be None if bid or ask is zero
            assert quote_data['last'] == None
            assert 'symbol' in quote_data
            assert quote_data['symbol'] == asset.symbol

        finally:
            # Restore the original method
            data_source._get_stock_client = original_get_stock_client

    def test_get_last_price_stock_when_bid_is_zero(self):
        """Test get_last_price when stock bid is 0.0"""
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Store the original method
        original_get_quote = data_source.get_quote

        # Create a mock for the get_quote method
        mock_quote_data = {
            'bid': 0.0,
            'ask': 100.0,
            'last': None,
            'symbol': asset.symbol
        }

        # Replace the get_quote method temporarily
        data_source.get_quote = lambda a, q=None, e=None: mock_quote_data if a.symbol == asset.symbol else None

        try:
            # Call get_last_price
            price = data_source.get_last_price(asset)

            # Verify the results
            # Since 'last' is None and bid is 0.0, it should return the ask value
            assert price == 100.0

        finally:
            # Restore the original method
            data_source.get_quote = original_get_quote

    def test_get_last_price_stock_when_ask_is_zero(self):
        """Test get_last_price when stock ask is 0.0"""
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Store the original method
        original_get_quote = data_source.get_quote

        # Create a mock for the get_quote method
        mock_quote_data = {
            'bid': 100.0,  # Some non-zero value for bid
            'ask': 0.0,
            'last': None,
            'symbol': asset.symbol
        }

        # Replace the get_quote method temporarily
        data_source.get_quote = lambda a, q=None, e=None: mock_quote_data if a.symbol == asset.symbol else None

        try:
            # Call get_last_price
            price = data_source.get_last_price(asset)

            # Verify the results
            # Since 'last' is None and ask is 0.0 it should return the bid value (100.0)
            assert price == 100.0

        finally:
            # Restore the original method
            data_source.get_quote = original_get_quote

    def test_get_last_price_stock_when_last(self):
        """Test get_last_price when stock ask is 0.0"""
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')

        # Store the original method
        original_get_quote = data_source.get_quote

        # Create a mock for the get_quote method
        mock_quote_data = {
            'bid': 100.0,  # Some non-zero value for bid
            'ask': 101.0,
            'last': 100.5,
            'symbol': asset.symbol
        }

        # Replace the get_quote method temporarily
        data_source.get_quote = lambda a, q=None, e=None: mock_quote_data if a.symbol == asset.symbol else None

        try:
            # Call get_last_price
            price = data_source.get_last_price(asset)

            # Verify the results
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
            quote_data = data_source.get_quote(asset)

            # Verify the results
            assert quote_data is not None
            assert isinstance(quote_data, dict)
            assert 'bid' in quote_data
            assert quote_data['bid'] == 100.0
            assert 'ask' in quote_data
            assert quote_data['ask'] == 101.0
            assert 'last' in quote_data
            assert quote_data['last'] == 100.5
            assert 'symbol' in quote_data
            assert quote_data['symbol'] == asset.symbol

        finally:
            # Restore the original method
            data_source._get_stock_client = original_get_stock_client