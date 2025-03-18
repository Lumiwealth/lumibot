import logging
import pytest
import math
from datetime import datetime, timedelta, time
import pytz

import pandas as pd

from lumibot.data_sources import TradierData
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset, Bars
from lumibot.tools import get_trading_days
from lumibot.credentials import TRADIER_TEST_CONFIG
from tests.fixtures import check_bars_from_get_historical_prices

logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_display_precision()


if not TRADIER_TEST_CONFIG['ACCESS_TOKEN'] or TRADIER_TEST_CONFIG['ACCESS_TOKEN'] == '<your key here>':
    pytest.skip(reason="These tests require a Tradier API key")


@pytest.fixture
def tradier_ds():
    return TradierData(
        account_number=TRADIER_TEST_CONFIG['ACCOUNT_NUMBER'],
        access_token=TRADIER_TEST_CONFIG['ACCESS_TOKEN'],
        paper=True
    )


# @pytest.mark.skip()
class TestTradierData:

    def test_basics(self, tradier_ds):
        assert tradier_ds._account_number == TRADIER_TEST_CONFIG['ACCOUNT_NUMBER']

    def test_get_chains(self, tradier_ds):
        asset = Asset("SPY")
        chain = tradier_ds.get_chains(asset)
        assert isinstance(chain, dict)
        assert 'Chains' in chain
        assert "CALL" in chain['Chains']
        assert len(chain['Chains']['CALL']) > 0
        expir_date = list(chain['Chains']['CALL'].keys())[0]
        assert len(chain['Chains']['CALL'][expir_date]) > 0
        strike = chain['Chains']['CALL'][expir_date][0]
        assert strike > 0
        assert chain['Multiplier'] == 100

    def test_query_greeks(self, tradier_ds):
        asset = Asset("SPY")
        chains = tradier_ds.get_chains(asset)
        expir_date = list(chains['Chains']['CALL'].keys())[0]
        num_strikes = len(chains['Chains']['CALL'][expir_date])
        strike = chains['Chains']['CALL'][expir_date][num_strikes // 2]  # Get a strike price in the middle
        option_asset = Asset(asset.symbol, asset_type='option', expiration=expir_date, strike=strike, right='CALL')
        greeks = tradier_ds.query_greeks(option_asset)
        assert greeks
        assert 'delta' in greeks
        assert 'gamma' in greeks
        assert greeks['delta'] > 0

    def test_get_quote(self, tradier_ds):
        asset = Asset("AAPL")
        quote = tradier_ds.get_quote(asset)
        assert isinstance(quote, dict)
        assert 'last' in quote
        assert 'bid' in quote
        assert 'ask' in quote
        assert 'volume' in quote
        assert 'open' in quote
        assert 'high' in quote
        assert 'low' in quote
        assert 'close' in quote

    def test_get_chain_full_info(self, tradier_ds):
        asset = Asset("SPY")
        chains = tradier_ds.get_chains(asset)
        expir_date = list(chains['Chains']['CALL'].keys())[0]

        df = tradier_ds.get_chain_full_info(asset, expir_date)
        assert isinstance(df, pd.DataFrame)
        assert 'strike' in df.columns
        assert 'last' in df.columns
        assert 'greeks.delta' in df.columns
        assert len(df)

    def test_get_last_price_stock(self, tradier_ds):
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        price = tradier_ds.get_last_price(asset=asset, quote=quote_asset)
        assert price is not None

    def test_get_historical_prices_daily_bars(self, tradier_ds):
        length = 30
        asset = Asset("SPY")
        timestep = "day"

        now = datetime.now(tradier_ds._tzinfo)
        bars = tradier_ds.get_historical_prices(asset=asset, length=length, timestep=timestep)

        check_bars_from_get_historical_prices(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=tradier_ds._tzinfo,
            time_check=time(0,0),
            timestep=timestep,
        )

        # This simulates what the backtesting_broker does when it tries to fill an order
        length = 1
        bars = tradier_ds.get_historical_prices(asset=asset, length=length, timestep=timestep)
        check_bars_from_get_historical_prices(
            bars=bars,
            now=now,
            length=length,
            data_source_tz=tradier_ds._tzinfo,
            time_check=time(0 ,0),
            timestep=timestep,
        )

    def test_get_historical_prices_minute_bars_stock(self, tradier_ds):
        timestep = "minute"
        now = datetime.now(tradier_ds._tzinfo)
        quote_asset = Asset('USD', asset_type='forex')
        market='NYSE'
        asset = Asset('SPY', asset_type='stock')

        for length in [1, 30]:
            bars = tradier_ds.get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote_asset
            )
            if bars:
                check_bars_from_get_historical_prices(
                    bars=bars,
                    now=now,
                    length=length,
                    data_source_tz=tradier_ds._tzinfo,
                    time_check=None,
                    market=market,
                    timestep=timestep,
                )

    def test_get_historical_option_prices(self, tradier_ds):
        length = 30
        ticker = 'SPY'
        asset = Asset("SPY")
        timestep = "day"
        now = datetime.now(tradier_ds._tzinfo)

        # Get a 0dte option
        # calculate the last calendar day before today
        trading_days = get_trading_days(
            start_date=(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            end_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        )
        dte = trading_days.index[-1]

        spy_price = tradier_ds.get_last_price(asset=asset)
        o_asset = Asset(ticker, Asset.AssetType.OPTION, expiration=dte, strike=math.floor(spy_price), right='CALL')

        bars = tradier_ds.get_historical_prices(asset=o_asset, length=length, timestep=timestep)
        assert len(bars.df) > 0