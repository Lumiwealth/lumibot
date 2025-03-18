import logging
import pytest
import math
from datetime import datetime, timedelta, time

import pandas as pd

from lumibot.data_sources import TradierData, DataSource
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Asset
from lumibot.tools import get_trading_days
from lumibot.credentials import TRADIER_TEST_CONFIG
from tests.fixtures import BaseDataSourceTester

logger = logging.getLogger(__name__)
print_full_pandas_dataframes()
set_pandas_float_display_precision()


if not TRADIER_TEST_CONFIG['ACCESS_TOKEN'] or TRADIER_TEST_CONFIG['ACCESS_TOKEN'] == '<your key here>':
    pytest.skip(reason="These tests require a Tradier API key")


# @pytest.mark.skip()
class TestTradierData(BaseDataSourceTester):
    
    def _create_data_source(self) -> DataSource:
        return TradierData(
            account_number=TRADIER_TEST_CONFIG['ACCOUNT_NUMBER'],
            access_token=TRADIER_TEST_CONFIG['ACCESS_TOKEN'],
            paper=True
        )

    def test_basics(self):
        data_source = self._create_data_source()
        assert data_source._account_number == TRADIER_TEST_CONFIG['ACCOUNT_NUMBER']

    def test_get_chains(self):
        data_source = self._create_data_source()
        asset = Asset("SPY")
        chain = data_source.get_chains(asset)
        assert isinstance(chain, dict)
        assert 'Chains' in chain
        assert "CALL" in chain['Chains']
        assert len(chain['Chains']['CALL']) > 0
        expir_date = list(chain['Chains']['CALL'].keys())[0]
        assert len(chain['Chains']['CALL'][expir_date]) > 0
        strike = chain['Chains']['CALL'][expir_date][0]
        assert strike > 0
        assert chain['Multiplier'] == 100

    def test_query_greeks(self):
        data_source = self._create_data_source()
        asset = Asset("SPY")
        chains = data_source.get_chains(asset)
        expir_date = list(chains['Chains']['CALL'].keys())[0]
        num_strikes = len(chains['Chains']['CALL'][expir_date])
        strike = chains['Chains']['CALL'][expir_date][num_strikes // 2]  # Get a strike price in the middle
        option_asset = Asset(asset.symbol, asset_type='option', expiration=expir_date, strike=strike, right='CALL')
        greeks = data_source.query_greeks(option_asset)
        assert greeks
        assert 'delta' in greeks
        assert 'gamma' in greeks
        assert greeks['delta'] > 0

    def test_get_quote(self):
        data_source = self._create_data_source()
        asset = Asset("AAPL")
        quote = data_source.get_quote(asset)
        assert isinstance(quote, dict)
        assert 'last' in quote
        assert 'bid' in quote
        assert 'ask' in quote
        assert 'volume' in quote
        assert 'open' in quote
        assert 'high' in quote
        assert 'low' in quote
        assert 'close' in quote

    def test_get_chain_full_info(self):
        data_source = self._create_data_source()
        asset = Asset("SPY")
        chains = data_source.get_chains(asset)
        expir_date = list(chains['Chains']['CALL'].keys())[0]

        df = data_source.get_chain_full_info(asset, expir_date)
        assert isinstance(df, pd.DataFrame)
        assert 'strike' in df.columns
        assert 'last' in df.columns
        assert 'greeks.delta' in df.columns
        assert len(df)

    def test_get_last_price_stock(self):
        data_source = self._create_data_source()
        asset = Asset('SPY', asset_type='stock')
        quote_asset = Asset('USD', asset_type='forex')
        price = data_source.get_last_price(asset=asset, quote=quote_asset)
        assert price is not None

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
                logging.warning(f"No minutes bars found for asset={asset} at: {now}")
                continue

            # TODO: Sometimes there are no minute bars for every minute and the data_source doesn't forward fill.
            # This is a different behavior backtesting data sources which do forward fill dataframes
            # returned by get_historical_prices. We should consider making TradierData do the same.
            # self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)

            # TODO: Need to create an Tradier extended hours market
            # self.check_minute_bars(
            #     bars=bars,
            #     now=now,
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
                logging.warning(f"No minutes bars found for asset={asset} at: {now}")
                continue

            # TODO: Sometimes there are no minute bars for every minute and the data_source doesn't forward fill.
            # This is a different behavior backtesting data sources which do forward fill dataframes
            # returned by get_historical_prices. We should consider making TradierData do the same.
            # self.check_length(bars=bars, length=length)
            self.check_columns(bars=bars)
            self.check_index(bars=bars, data_source_tz=data_source._tzinfo)

            # TODO: TradierData doesn't send back the last N bars in this case. It sends back
            # the bars in between the start and end date, which are calculated incorrectly if
            # we wanted to get the last N bars.
            self.check_minute_bars(
                bars=bars,
                now=now,
                data_source_tz=data_source._tzinfo,
                market=market,
            )

    def test_get_historical_option_prices(self):
        data_source = self._create_data_source()
        length = 30
        ticker = 'SPY'
        asset = Asset("SPY")
        timestep = "day"
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