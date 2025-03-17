import logging
import pytest
import math
from datetime import datetime, timedelta, time
import pytz

import pandas as pd

from lumibot.data_sources import AlpacaData
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset, Bars
from lumibot.tools import get_trading_days
from lumibot.credentials import ALPACA_TEST_CONFIG
from tests.fixtures import check_bars

logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_display_precision()

if not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>':
    pytest.skip("These tests requires an Alpaca API key", allow_module_level=True)


# @pytest.mark.skip()
class TestAlpacaData:

    def test_get_last_price_crypto(self):
        data_source = AlpacaData(ALPACA_TEST_CONFIG)
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        price = data_source.get_last_price(asset=asset, quote=quote_asset)
        assert price is not None
        assert isinstance(price, float)

    def test_get_last_price_stock(self):
        data_source = AlpacaData(ALPACA_TEST_CONFIG)
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        price = data_source.get_last_price(asset=asset, quote=quote_asset)
        assert price is not None
        assert isinstance(price, float)

    def test_get_historical_prices_daily_bars(self):
        asset = Asset("SPY")
        timestep = "day"

        data_source = AlpacaData(ALPACA_TEST_CONFIG)
        now = datetime.now(data_source._tzinfo)

        # This simulates what the backtesting_broker does when it tries to fill an order
        length = 1
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=data_source._tzinfo,
            time_check=time(0 ,0),
            timestep=timestep,
        )

        length = 30
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep)

        check_bars(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=data_source._tzinfo,
            time_check=time(0,0),
            timestep=timestep,
        )

    def test_get_historical_prices_minute_bars_stock(self):
        # TODO: this really checks extended hours and won't work before market hours.
        timestep = "minute"
        data_source = AlpacaData(ALPACA_TEST_CONFIG)
        now = datetime.now(data_source._tzinfo)
        quote_asset = Asset('USD', asset_type='forex')
        market='NYSE'
        asset = Asset('SPY', asset_type='stock')

        for length in [1, 30]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset
            )
            if bars:
                check_bars(
                    bars=bars,
                    now=now - data_source._delay,
                    length=length,
                    data_source_tz=data_source._tzinfo,
                    time_check=None,
                    market=market,
                    timestep=timestep,
                )

    def test_get_historical_prices_minute_bars_crypto(self):
        timestep = "minute"
        data_source = AlpacaData(ALPACA_TEST_CONFIG)
        now = datetime.now(data_source._tzinfo)
        quote_asset = Asset('USD', asset_type='forex')
        market='24/7'
        asset = Asset('BTC', asset_type='crypto')

        for length in [30, 1]:
            bars = data_source.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset
            )
            # if bars:
            check_bars(
                bars=bars,
                now=now,
                length=length,
                data_source_tz=data_source._tzinfo,
                time_check=None,
                market=market,
                timestep=timestep,
            )

    def test_get_historical_prices_daily_bars_crypto(self):
        length = 30
        timestep = "day"

        data_source = AlpacaData(ALPACA_TEST_CONFIG)
        now = datetime.now(data_source._tzinfo)
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, quote=quote_asset)

        # Alpaca ONLY returns crypto daily bars at midnight central time aka 1am ET
        check_bars(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=data_source._tzinfo,
            time_check=time(1,0),
            market='24/7',
            timestep=timestep,
        )

        # This simulates what the backtesting_broker does when it tries to fill an order
        length = 1
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, quote=quote_asset)
        check_bars(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=data_source._tzinfo,
            time_check=time(1,0),
            market='24/7',
            timestep=timestep,
        )

    def test_get_historical_prices_daily_bars_crypto_utc(self):
        length = 30
        timestep = "day"
        tzinfo = pytz.timezone('UTC')

        data_source = AlpacaData(ALPACA_TEST_CONFIG, tzinfo=tzinfo)
        now = datetime.now(tzinfo)
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, quote=quote_asset)

        # Alpaca ONLY returns crypto daily bars at midnight central time aka 5am UTC
        check_bars(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=data_source._tzinfo,
            time_check=time(5,0),
            market='24/7',
            timestep=timestep,
        )

        # This simulates what the backtesting_broker does when it tries to fill an order
        length = 1
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, quote=quote_asset)
        check_bars(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=data_source._tzinfo,
            time_check=time(5,0),
            market='24/7',
            timestep=timestep,
        )

    def test_get_historical_prices_daily_bars_crypto_america_chicago(self):
        length = 30
        timestep = "day"
        tzinfo = pytz.timezone('America/Chicago')

        data_source = AlpacaData(ALPACA_TEST_CONFIG, tzinfo=tzinfo)
        now = datetime.now(tzinfo)
        asset = Asset('BTC', asset_type='crypto')
        quote_asset = Asset('USD', asset_type='forex')
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, quote=quote_asset)

        # Alpaca ONLY returns crypto daily bars at midnight central time
        check_bars(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=data_source._tzinfo,
            time_check=time(0,0),
            market='24/7',
            timestep=timestep,
        )

        # This simulates what the backtesting_broker does when it tries to fill an order
        length = 1
        bars = data_source.get_historical_prices(asset=asset, length=length, timestep=timestep, quote=quote_asset)
        check_bars(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=data_source._tzinfo,
            time_check=time(0,0),
            market='24/7',
            timestep=timestep,
        )

    def test_get_historical_option_prices(self):
        length = 30
        ticker = 'SPY'
        asset = Asset("SPY")
        timestep = "day"
        data_source = AlpacaData(ALPACA_TEST_CONFIG)
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