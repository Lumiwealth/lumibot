import logging
import os
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
    pytest.skip(reason="These tests require a Tradier API key", allow_module_level=True)


# @pytest.mark.skip()
class TestTradierData(BaseDataSourceTester):

    def _create_data_source(self, remove_incomplete_current_bar=False) -> DataSource:
        return TradierData(
            account_number=TRADIER_TEST_CONFIG['ACCOUNT_NUMBER'],
            access_token=TRADIER_TEST_CONFIG['ACCESS_TOKEN'],
            paper=True,
            remove_incomplete_current_bar=remove_incomplete_current_bar
        )

    def test_basics(self):
        data_source = self._create_data_source()
        assert data_source._account_number == TRADIER_TEST_CONFIG['ACCOUNT_NUMBER']

    def test_sanitize_base_and_quote_asset(self):
        """
        Test _sanitize_base_and_quote_asset method with various inputs:
        - Regular input with two assets
        - Tuple input
        - String input (should raise error)
        - String in tuple input (should raise error)
        """
        # Setup
        data_source = self._create_data_source()
        asset_a = Asset("AAPL")  # Create real Asset objects
        asset_b = Asset("USD")  # instead of Mocks

        # Test case 1: Regular input with two separate assets
        base, quote = data_source._sanitize_base_and_quote_asset(asset_a, asset_b)
        assert base == asset_a
        assert quote == asset_b

        # Test case 2: Input as tuple
        tuple_input = (asset_a, asset_b)
        base, quote = data_source._sanitize_base_and_quote_asset(tuple_input, None)
        assert base == asset_a
        assert quote == asset_b

        # Test case 3: String input should raise NotImplementedError
        with pytest.raises(NotImplementedError) as exc_info:
            data_source._sanitize_base_and_quote_asset("AAPL", asset_b)
        assert "TradierData doesn't support string assets" in str(exc_info.value)

        # Test case 4: String input in tuple should raise NotImplementedError
        with pytest.raises(NotImplementedError) as exc_info:
            data_source._sanitize_base_and_quote_asset(("AAPL", asset_b), None)
        assert "TradierData doesn't support string assets" in str(exc_info.value)

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
        from lumibot.entities import Quote

        data_source = self._create_data_source()
        asset = Asset("AAPL")
        quote = data_source.get_quote(asset)

        assert isinstance(quote, Quote)
        assert quote.asset == asset
        assert quote.price is not None
        assert quote.bid is not None
        assert quote.ask is not None
        assert quote.volume is not None
        assert quote.timestamp is not None

        # Check that raw_data contains the original quote dictionary
        assert quote.raw_data is not None
        assert 'open' in quote.raw_data
        assert 'high' in quote.raw_data
        assert 'low' in quote.raw_data
        assert 'close' in quote.raw_data

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
        # test tuple
        asset_tuple = (asset, quote_asset)
        self.check_get_last_price(data_source, asset_tuple)

    def test_get_historical_prices_daily_bars_stock(self):
        data_source = self._create_data_source()
        asset = Asset("SPY")
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = 'NYSE'

        for length in [1, 30]:
            now = datetime.now(data_source.tzinfo)
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
                time_check=time(0,0),
                market=market,
            )

        # test tuple
        asset_tuple = (asset, quote_asset)
        self.check_get_last_price(data_source, asset_tuple)
        for length in [1, 30]:
            now = datetime.now(data_source.tzinfo)
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
                time_check=time(0,0),
                market=market,
            )

    def test_get_historical_prices_daily_bars_stock_remove_incomplete_current_bar(self):
        data_source = self._create_data_source(remove_incomplete_current_bar=True)
        asset = Asset("SPY")
        quote_asset = Asset('USD', asset_type='forex')
        timestep = "day"
        market = 'NYSE'

        for length in [1, 30]:
            now = datetime.now(data_source.tzinfo)
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
                time_check=time(0,0),
                market=market,
                remove_incomplete_current_bar=True
            )

        # test tuple
        asset_tuple = (asset, quote_asset)
        self.check_get_last_price(data_source, asset_tuple)
        for length in [1, 30]:
            now = datetime.now(data_source.tzinfo)
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
                time_check=time(0,0),
                market=market,
                remove_incomplete_current_bar=True
            )

    def test_get_historical_prices_minute_bars_stock_regular_hours(self):
        data_source = self._create_data_source()
        timestep = "minute"
        quote_asset = Asset('USD', asset_type='forex')
        market='NYSE'
        asset = Asset('SPY', asset_type='stock')

        for length in [1, 30]:
            now = datetime.now(data_source.tzinfo)
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
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
            )

    def test_get_historical_prices_minute_bars_stock_regular_hours_remove_incomplete_current_bar(self):
        data_source = self._create_data_source(remove_incomplete_current_bar=True)
        timestep = "minute"
        quote_asset = Asset('USD', asset_type='forex')
        market='NYSE'
        asset = Asset('SPY', asset_type='stock')

        for length in [1, 30]:
            now = datetime.now(data_source.tzinfo)
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
                now=now,
                data_source_tz=data_source.tzinfo,
                market=market,
                remove_incomplete_current_bar=True
            )


    def test_get_historical_option_prices(self):
        data_source = self._create_data_source()
        length = 30
        ticker = 'SPY'
        asset = Asset("SPY")
        timestep = "day"
        now = datetime.now(data_source.tzinfo)

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

    def test_default_delay_value(self):
        """Test that the default delay value is 0 minutes when not specified."""
        # Save the original environment variable value
        original_env_value = os.environ.get("DATA_SOURCE_DELAY")

        try:
            # Ensure the environment variable is not set
            if "DATA_SOURCE_DELAY" in os.environ:
                del os.environ["DATA_SOURCE_DELAY"]

            # Create a data source without specifying delay
            data_source = self._create_data_source()

            # Check that the delay is 16 minutes
            assert data_source._delay == timedelta(minutes=0), f"Expected delay to be 0 minutes, but got {data_source._delay}"

        finally:
            # Restore the original environment variable value
            if original_env_value is not None:
                os.environ["DATA_SOURCE_DELAY"] = original_env_value
            elif "DATA_SOURCE_DELAY" in os.environ:
                del os.environ["DATA_SOURCE_DELAY"]
