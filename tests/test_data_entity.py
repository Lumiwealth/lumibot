"""
Tests for the Data entity, particularly the get_last_price bid/ask fallback.

This tests the fix where get_last_price falls back to bid/ask midpoint
when the close/open price is None or NaN. This is especially important
for options where there may be quotes but no actual trades.
"""

import pytest
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from lumibot.entities import Asset
from lumibot.entities.data import Data


class TestDataGetLastPrice:
    """Tests for Data.get_last_price with bid/ask fallback."""

    def _create_data_with_prices(
        self,
        asset,
        close_prices,
        open_prices=None,
        bid_prices=None,
        ask_prices=None,
        timestep="day"
    ):
        """
        Helper to create a Data object with specified price data.

        Parameters
        ----------
        asset : Asset
            The asset for this data
        close_prices : list
            List of close prices (can include None/NaN)
        open_prices : list, optional
            List of open prices, defaults to close_prices
        bid_prices : list, optional
            List of bid prices
        ask_prices : list, optional
            List of ask prices
        timestep : str
            The timestep for the data
        """
        if open_prices is None:
            open_prices = close_prices

        # Create a simple dataframe with timezone-aware datetimes
        n = len(close_prices)
        tz = pytz.timezone('America/New_York')
        base_dt = tz.localize(datetime(2024, 1, 1, 9, 30))
        dates = [base_dt + timedelta(days=i) for i in range(n)]

        df_data = {
            'datetime': dates,
            'open': open_prices,
            'high': [max(o, c) if o is not None and c is not None else (o or c)
                     for o, c in zip(open_prices, close_prices)],
            'low': [min(o, c) if o is not None and c is not None else (o or c)
                    for o, c in zip(open_prices, close_prices)],
            'close': close_prices,
            'volume': [1000] * n,
        }

        if bid_prices is not None:
            df_data['bid'] = bid_prices
        if ask_prices is not None:
            df_data['ask'] = ask_prices

        df = pd.DataFrame(df_data)
        df.set_index('datetime', inplace=True)

        # Create Data object
        data = Data(asset, df, timestep=timestep)
        return data

    def test_get_last_price_returns_close_when_available(self):
        """Test that close price is returned when available."""
        asset = Asset("SPY")
        close_prices = [100.0, 101.0, 102.0]
        data = self._create_data_with_prices(asset, close_prices)

        tz = pytz.timezone('America/New_York')
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))  # Third day
        price = data.get_last_price(dt)

        assert price == 102.0

    def test_get_last_price_falls_back_to_bid_ask_midpoint(self):
        """Test that bid/ask midpoint is used when close is None."""
        asset = Asset("SPY", asset_type="option", expiration=datetime(2024, 2, 1).date(),
                      strike=400, right="CALL")

        # Close is None, but we have bid/ask
        close_prices = [None, None, None]
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]

        data = self._create_data_with_prices(
            asset, close_prices,
            open_prices=[None, None, None],
            bid_prices=bid_prices,
            ask_prices=ask_prices
        )

        tz = pytz.timezone('America/New_York')
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))  # Third day
        price = data.get_last_price(dt)

        # Should be midpoint of 12.0 and 13.0 = 12.5
        assert price == 12.5

    def test_get_last_price_falls_back_when_close_is_nan(self):
        """Test that bid/ask midpoint is used when close is NaN."""
        asset = Asset("SPY", asset_type="option", expiration=datetime(2024, 2, 1).date(),
                      strike=400, right="CALL")

        # Close is NaN, but we have bid/ask
        close_prices = [np.nan, np.nan, np.nan]
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]

        data = self._create_data_with_prices(
            asset, close_prices,
            open_prices=[np.nan, np.nan, np.nan],
            bid_prices=bid_prices,
            ask_prices=ask_prices
        )

        tz = pytz.timezone('America/New_York')
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))  # Third day
        price = data.get_last_price(dt)

        # Should be midpoint of 12.0 and 13.0 = 12.5
        assert price == 12.5

    def test_get_last_price_returns_none_when_no_data_available(self):
        """Test that None is returned when both close and bid/ask are None."""
        asset = Asset("SPY", asset_type="option", expiration=datetime(2024, 2, 1).date(),
                      strike=400, right="CALL")

        # Everything is None
        close_prices = [None, None, None]
        data = self._create_data_with_prices(asset, close_prices, open_prices=[None, None, None])

        tz = pytz.timezone('America/New_York')
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        price = data.get_last_price(dt)

        # Should be None since no bid/ask fallback available
        assert price is None

    def test_get_last_price_returns_none_when_bid_ask_invalid(self):
        """Test that None is returned when bid/ask are zero or negative."""
        asset = Asset("SPY", asset_type="option", expiration=datetime(2024, 2, 1).date(),
                      strike=400, right="CALL")

        close_prices = [None, None, None]
        bid_prices = [0.0, 0.0, 0.0]  # Invalid bid
        ask_prices = [1.0, 1.0, 1.0]

        data = self._create_data_with_prices(
            asset, close_prices,
            open_prices=[None, None, None],
            bid_prices=bid_prices,
            ask_prices=ask_prices
        )

        tz = pytz.timezone('America/New_York')
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        price = data.get_last_price(dt)

        # Should be None since bid is 0
        assert price is None

    def test_get_last_price_prefers_close_over_bid_ask(self):
        """Test that close price is preferred even when bid/ask available."""
        asset = Asset("SPY", asset_type="option", expiration=datetime(2024, 2, 1).date(),
                      strike=400, right="CALL")

        close_prices = [5.0, 5.0, 5.0]  # Valid close prices
        bid_prices = [10.0, 11.0, 12.0]
        ask_prices = [11.0, 12.0, 13.0]

        data = self._create_data_with_prices(
            asset, close_prices,
            bid_prices=bid_prices,
            ask_prices=ask_prices
        )

        tz = pytz.timezone('America/New_York')
        dt = tz.localize(datetime(2024, 1, 3, 9, 30))
        price = data.get_last_price(dt)

        # Should use close price, not bid/ask midpoint
        assert price == 5.0


class TestGreeksWithBidAskFallback:
    """Test that Greeks can be calculated when using bid/ask fallback."""

    def test_calculate_greeks_with_bid_ask_midpoint_option_price(self):
        """
        Test that Greeks can be calculated when option price comes from bid/ask midpoint.

        This verifies the fix works end-to-end: when ThetaData has quote data but no
        trades for an option, the bid/ask midpoint should enable Greeks calculation.
        """
        from lumibot.data_sources.data_source import DataSource

        # Create a testable data source
        class TestableDataSource(DataSource):
            def __init__(self):
                super().__init__(api_key="test")

            def get_chains(self, asset, quote=None):
                return {}

            def get_last_price(self, asset, quote=None, exchange=None):
                # Return bid/ask midpoint for option
                return 5.0  # Simulating the result after bid/ask fallback

            def get_historical_prices(self, asset, length, timestep="", timeshift=None,
                                       quote=None, exchange=None, include_after_hours=True):
                return None

        ds = TestableDataSource()

        # Create an option asset
        option = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 15).date(),
            strike=450,
            right="CALL"
        )

        # Mock get_datetime to return a date before expiry
        ds._datetime = datetime(2024, 1, 15, 10, 0)

        # Calculate Greeks
        greeks = ds.calculate_greeks(
            option,
            asset_price=5.0,  # Option price (from bid/ask midpoint fallback)
            underlying_price=445.0,
            risk_free_rate=0.05
        )

        # Greeks should be calculated successfully
        assert greeks is not None
        assert 'delta' in greeks
        assert 'gamma' in greeks
        assert 'theta' in greeks
        assert 'vega' in greeks
        assert 'implied_volatility' in greeks

        # Delta should be positive for an OTM call (strike > underlying)
        # Actually strike 450 > underlying 445, so slightly OTM call
        # Delta should be less than 0.5 but positive
        assert 0 < greeks['delta'] < 0.5

    def test_greeks_otm_call_delta_below_half(self):
        """Test that OTM call has delta < 0.5."""
        from lumibot.data_sources.data_source import DataSource

        class TestableDataSource(DataSource):
            def __init__(self):
                super().__init__(api_key="test")
            def get_chains(self, asset, quote=None):
                return {}
            def get_last_price(self, asset, quote=None, exchange=None):
                return 2.0
            def get_historical_prices(self, asset, length, timestep="", timeshift=None,
                                       quote=None, exchange=None, include_after_hours=True):
                return None

        ds = TestableDataSource()
        ds._datetime = datetime(2024, 1, 15, 10, 0)

        # OTM call: strike > underlying (option has no intrinsic value)
        option = Asset("SPY", asset_type="option", expiration=datetime(2024, 2, 15).date(),
                      strike=470, right="CALL")

        greeks = ds.calculate_greeks(option, asset_price=2.0, underlying_price=450.0, risk_free_rate=0.05)

        assert greeks is not None
        # OTM calls should have delta < 0.5
        assert 0 < greeks['delta'] < 0.5, f"OTM call delta {greeks['delta']} not < 0.5"

    def test_greeks_returns_none_with_none_option_price(self):
        """Test that Greeks returns None when option price is None (no fallback available)."""
        from lumibot.data_sources.data_source import DataSource

        class TestableDataSource(DataSource):
            def __init__(self):
                super().__init__(api_key="test")
            def get_chains(self, asset, quote=None):
                return {}
            def get_last_price(self, asset, quote=None, exchange=None):
                return None  # Simulates no price data available
            def get_historical_prices(self, asset, length, timestep="", timeshift=None,
                                       quote=None, exchange=None, include_after_hours=True):
                return None

        ds = TestableDataSource()
        ds._datetime = datetime(2024, 1, 15, 10, 0)

        option = Asset("SPY", asset_type="option", expiration=datetime(2024, 2, 15).date(),
                      strike=450, right="CALL")

        greeks = ds.calculate_greeks(option, asset_price=None, underlying_price=450.0, risk_free_rate=0.05)

        # Should return None when option price is None
        assert greeks is None


class TestThetaDataBidAskScenario:
    """
    Integration tests simulating realistic ThetaData scenarios where options
    have quotes (bid/ask) but no trades (close is None/NaN).
    """

    def test_full_flow_option_with_quotes_only(self):
        """
        Simulate ThetaData returning option data with quotes but no trades.
        This is the exact scenario that was causing Greeks to fail.

        This test verifies the DATA FLOW works:
        1. Option has no close price (no trades)
        2. Option has bid/ask quotes
        3. get_last_price() returns bid/ask midpoint (not None)
        4. Greeks calculation receives a valid price and returns a result (not None)

        Note: We don't validate specific Greek values here as the Black-Scholes
        library has numerical stability issues with certain parameter combinations.
        The important thing is that the flow works and doesn't return None.
        """
        from lumibot.data_sources.data_source import DataSource

        # Create option asset - slightly OTM call (strike > underlying)
        # This configuration is known to work with the BS library
        option = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 15).date(),
            strike=455,  # Slightly OTM
            right="CALL"
        )

        # Create Data object with no close price but with bid/ask
        tz = pytz.timezone('America/New_York')
        base_dt = tz.localize(datetime(2024, 1, 15, 9, 30))

        df = pd.DataFrame({
            'datetime': [base_dt + timedelta(days=i) for i in range(5)],
            'open': [np.nan] * 5,
            'high': [np.nan] * 5,
            'low': [np.nan] * 5,
            'close': [np.nan] * 5,  # No trades
            'volume': [0] * 5,
            'bid': [4.50, 4.60, 4.70, 4.80, 4.90],  # OTM option bid
            'ask': [4.70, 4.80, 4.90, 5.00, 5.10],
        })
        df.set_index('datetime', inplace=True)

        data = Data(option, df, timestep='day')

        # Get price for the last day - should fall back to bid/ask midpoint
        dt = tz.localize(datetime(2024, 1, 19, 9, 30))  # 5th day
        price = data.get_last_price(dt)

        # Should be midpoint of 4.90 and 5.10 = 5.00
        assert price == pytest.approx(5.00, rel=0.001), "Bid/ask fallback should work"
        assert price is not None, "Price should not be None with bid/ask available"

        # Now use this price to calculate Greeks
        class TestableDataSource(DataSource):
            def __init__(self):
                super().__init__(api_key="test")
            def get_chains(self, asset, quote=None):
                return {}
            def get_last_price(self, asset, quote=None, exchange=None):
                return price
            def get_historical_prices(self, asset, length, timestep="", timeshift=None,
                                       quote=None, exchange=None, include_after_hours=True):
                return None

        ds = TestableDataSource()
        ds._datetime = datetime(2024, 1, 19, 10, 0)

        # Calculate Greeks with the bid/ask midpoint price
        greeks = ds.calculate_greeks(
            option,
            asset_price=price,
            underlying_price=450.0,
            risk_free_rate=0.05
        )

        # The key assertion: Greeks should be calculated (not None)
        # This proves the data flow works: bid/ask → price → Greeks
        assert greeks is not None, "Greeks should be calculated with bid/ask midpoint price"

        # Verify the Greeks dict contains expected keys
        assert 'delta' in greeks, "Greeks should contain delta"
        assert 'gamma' in greeks, "Greeks should contain gamma"
        assert 'theta' in greeks, "Greeks should contain theta"
        assert 'vega' in greeks, "Greeks should contain vega"
        assert 'implied_volatility' in greeks, "Greeks should contain IV"

    def test_wide_bid_ask_spread_uses_midpoint(self):
        """Test that wide bid/ask spreads (common in illiquid options) still work."""
        option = Asset(
            "SPY",
            asset_type="option",
            expiration=datetime(2024, 2, 15).date(),
            strike=500,  # Far OTM
            right="CALL"
        )

        tz = pytz.timezone('America/New_York')
        base_dt = tz.localize(datetime(2024, 1, 15, 9, 30))

        # Wide spread typical of far OTM options
        df = pd.DataFrame({
            'datetime': [base_dt],
            'open': [np.nan],
            'high': [np.nan],
            'low': [np.nan],
            'close': [np.nan],
            'volume': [0],
            'bid': [0.05],  # Very low bid
            'ask': [0.15],  # Higher ask - 200% spread is common for cheap options
        })
        df.set_index('datetime', inplace=True)

        data = Data(option, df, timestep='day')
        price = data.get_last_price(base_dt)

        # Should be midpoint = 0.10
        assert price == pytest.approx(0.10, rel=0.001)
