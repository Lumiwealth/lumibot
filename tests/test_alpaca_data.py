import logging
import pytest
import math
from datetime import datetime, timedelta, time
import pytz

import pandas as pd

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
    
    def _create_data_source(self, tzinfo: pytz.tzinfo = None) -> DataSource:
        return AlpacaData(ALPACA_TEST_CONFIG, tzinfo=tzinfo)

    def test_get_last_price_crypto(self):
        data_source = self._create_data_source()
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        price = data_source.get_last_price(asset=asset, quote=quote_asset)
        assert price is not None
        assert isinstance(price, float)

    def test_get_last_price_stock(self):
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        price = data_source.get_last_price(asset=asset, quote=quote_asset)
        assert price is not None
        assert isinstance(price, float)

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
                time_check=time(0 ,0),
                market=market,
            )

    def test_get_historical_prices_daily_bars_crypto(self):
        market = '24/7'
        timestep = "day"
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')

        data_source = self._create_data_source()
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
                time_check=time(1 ,0),
                market=market,
            )

    def test_get_historical_prices_daily_bars_crypto_utc(self):
        tzinfo = pytz.timezone('UTC')
        market = '24/7'
        timestep = "day"
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')

        data_source = self._create_data_source(tzinfo=tzinfo)
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
            # TODO: When asking for daily bars in UTC time, we don't get the incomplete daily bar for the
            # current day like we do for otherwise.
            # self.check_daily_bars(
            #     bars=bars,
            #     now=now,
            #     data_source_tz=data_source._tzinfo,
            #
            #     # Default alpaca crypto timezone is America/Chicago
            #     time_check=time(5 ,0),
            #     market=market,
            # )

    def test_get_historical_prices_daily_bars_crypto_america_chicago(self):
        tzinfo = pytz.timezone('America/Chicago')
        market = '24/7'
        timestep = "day"
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')

        data_source = self._create_data_source(tzinfo=tzinfo)
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
                market=market,
            )

    def test_get_historical_prices_minute_bars_stock_extended_hours(self):
        data_source = self._create_data_source()
        timestep = "minute"
        now = datetime.now(data_source._tzinfo)
        quote_asset = Asset('USD', asset_type='forex')
        market='NYSE'
        asset = Asset('SPY', asset_type='stock')

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset,
                include_after_hours=True
            )
            if not bars or bars.df.empty:
                # TODO: Sometimes there are no minute bars for every minute and the data_source doesn't forward fill.
                logger.warning(f"No minutes bars found for asset={asset} at: {now}")
                continue

            self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)

            # TODO: Need to create an Alpaca extended hours market
            # self.check_minute_bars(
            #     bars=bars,
            #     now=now - data_source._delay,
            #     data_source_tz=data_source._tzinfo,
            #     market=market,
            # )

    def test_get_historical_prices_minute_bars_stock_regular_hours(self):
        data_source = self._create_data_source()
        timestep = "minute"
        now = datetime.now(data_source._tzinfo)
        quote_asset = Asset('USD', asset_type='forex')
        market='NYSE'
        asset = Asset('SPY', asset_type='stock')

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset,
                include_after_hours=False,
            )
            if not bars or bars.df.empty:
                # TODO: Sometimes there are no minute bars for every minute and the data_source doesn't forward fill.
                logger.warning(f"No minutes bars found for asset={asset} at: {now}")
                continue

            # TODO: Sometimes there are no minute bars for every minute and the data_source doesn't forward fill.
            # This is a different behavior backtesting data sources which do forward fill dataframes
            # returned by get_historical_prices. We should consider making the data_source do the same.
            # self.check_length(bars=bars, length=length)
            
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
        timestep = "minute"
        data_source = self._create_data_source(tzinfo=tzinfo)
        now = datetime.now(data_source._tzinfo)
        quote_asset = Asset('USD', asset_type='forex')
        market='24/7'
        asset = Asset('BTC', asset_type='crypto')

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset
            )
            if not bars or bars.df.empty:
                # TODO: Sometimes there are no minute bars for every minute and the data_source doesn't forward fill.
                logger.warning(f"No minutes bars found for asset={asset} at: {now}")
                continue

            # TODO: Sometimes there are no minute bars for every minute and the data_source doesn't forward fill.
            # This is a different behavior backtesting data sources which do forward fill dataframes
            # returned by get_historical_prices. We should consider making the data_source do the same.
            # self.check_length(bars=bars, length=length)
            
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