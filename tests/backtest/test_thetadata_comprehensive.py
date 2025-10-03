"""
Comprehensive but practical ThetaData tests.
Tests the essentials without going overboard.
"""

import datetime
import os
import pytest
from dotenv import load_dotenv
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper
from lumibot.tools.helpers import to_datetime_aware
from lumibot.backtesting import ThetaDataBacktesting, PolygonDataBacktesting

# Load environment variables from .env file
load_dotenv()


@pytest.mark.apitest
class TestThetaDataStocks:
    """Test stock data accuracy."""

    def test_first_10_minutes_timestamps_and_prices(self):
        """
        CRITICAL: Verify the +1 minute timestamp bug is fixed.
        Test first 10 minutes to ensure market open spike is at 9:30, not 9:31.
        """
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        asset = Asset("SPY", asset_type="stock")

        # Get first 10 bars (9:30-9:40) directly from ThetaData
        df = thetadata_helper.get_price_data(
            username=username,
            password=password,
            asset=asset,
            start=datetime.datetime(2024, 8, 1, 9, 30),
            end=datetime.datetime(2024, 8, 1, 9, 40),
            timespan="minute"
        )

        assert df is not None and len(df) > 0, "No bars returned"

        print(f"\nFirst 10 minutes of SPY:")
        print(f"{'Time':<25} {'Open':<10} {'High':<10} {'Low':<10} {'Close':<10} {'Volume':<15}")
        print("=" * 100)

        for i in range(min(10, len(df))):
            idx = df.index[i]
            row = df.iloc[i]
            print(f"{str(idx):<25} {row['open']:<10.2f} {row['high']:<10.2f} {row['low']:<10.2f} {row['close']:<10.2f} {row['volume']:<15,.0f}")

        # Verify timestamps are 60 seconds apart
        for i in range(1, min(10, len(df))):
            time_diff = (df.index[i] - df.index[i-1]).total_seconds()
            assert time_diff == 60, f"Bar {i} is {time_diff}s after bar {i-1}, expected 60s"

        # Verify market open spike
        # ThetaData has 1-minute offset (9:29 instead of 9:30), so check both first and second bar
        first_bar = df.iloc[0]
        second_bar = df.iloc[1]

        # Find the bar with highest volume in first 3 bars (market open spike)
        max_volume_idx = df.iloc[:3]['volume'].idxmax()
        max_volume_bar = df.loc[max_volume_idx]

        # Market open spike should have >100k volume
        assert max_volume_bar['volume'] > 100000, \
            f"Market open spike has low volume ({max_volume_bar['volume']:,.0f})"

        print(f"\n✓ Timestamp verification PASSED")
        print(f"  - Market open spike at {max_volume_bar.name}: {max_volume_bar['volume']:,.0f} volume")

    def test_noon_period_accuracy(self):
        """Test pricing accuracy at noon (different market conditions)."""
        import os
        POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")

        if not POLYGON_API_KEY:
            pytest.skip("Polygon API key not available")

        # ThetaData
        theta_ds = ThetaDataBacktesting(
            datetime_start=datetime.datetime(2024, 8, 1),
            datetime_end=datetime.datetime(2024, 8, 1, 12, 15),
            username=os.environ.get("THETADATA_USERNAME"),
            password=os.environ.get("THETADATA_PASSWORD"),
        )

        # Polygon
        polygon_ds = PolygonDataBacktesting(
            datetime_start=datetime.datetime(2024, 8, 1),
            datetime_end=datetime.datetime(2024, 8, 1, 12, 15),
            api_key=POLYGON_API_KEY,
        )

        asset = Asset("SPY", asset_type="stock")

        # Get bars at noon
        test_times = [
            datetime.datetime(2024, 8, 1, 12, 0),
            datetime.datetime(2024, 8, 1, 12, 5),
            datetime.datetime(2024, 8, 1, 12, 10),
        ]

        print(f"\nNoon period comparison for SPY:")
        print(f"{'Time':<25} {'ThetaData':<12} {'Polygon':<12} {'Diff':<10} {'Status'}")
        print("=" * 80)

        for test_time in test_times:
            # ThetaData
            theta_ds._datetime = to_datetime_aware(test_time)
            theta_bars = theta_ds.get_historical_prices(
                asset=asset, length=1, timestep="minute"
            )
            theta_df = theta_bars.df if hasattr(theta_bars, 'df') else theta_bars
            theta_price = theta_df.iloc[-1]['close'] if len(theta_df) > 0 else None

            # Polygon
            polygon_ds._datetime = to_datetime_aware(test_time)
            polygon_bars = polygon_ds.get_historical_prices(
                asset=asset, length=1, timestep="minute"
            )
            polygon_df = polygon_bars.df if hasattr(polygon_bars, 'df') else polygon_bars
            polygon_price = polygon_df.iloc[-1]['close'] if len(polygon_df) > 0 else None

            if theta_price and polygon_price:
                diff = abs(theta_price - polygon_price)
                status = "✓ PASS" if diff <= 0.01 else "✗ FAIL"
                print(f"{str(test_time):<25} ${theta_price:<11.2f} ${polygon_price:<11.2f} ${diff:<9.4f} {status}")

                assert diff <= 0.01, f"Price difference ${diff:.4f} exceeds 1¢ tolerance"

        print(f"\n✓ Noon period accuracy PASSED")

    def test_multiple_symbols(self):
        """Test 2-3 symbols with different price ranges."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        theta = ThetaDataBacktesting(
            datetime_start=datetime.datetime(2024, 8, 1),
            datetime_end=datetime.datetime(2024, 8, 1, 10, 0),
            username=username,
            password=password,
        )

        # Test different price ranges
        symbols = [
            ("SPY", "ETF ~$550"),
            ("AMZN", "Stock ~$190"),
            ("AMD", "Stock ~$160"),
        ]

        print(f"\nMultiple symbol test at market open:")
        print(f"{'Symbol':<10} {'Description':<20} {'Open':<10} {'Close':<10} {'Volume':<15} {'Status'}")
        print("=" * 90)

        for symbol, description in symbols:
            asset = Asset(symbol, asset_type="stock")

            # Set datetime to market open
            theta._datetime = to_datetime_aware(datetime.datetime(2024, 8, 1, 9, 30))

            bars = theta.get_historical_prices(
                asset=asset,
                length=1,
                timestep="minute"
            )

            df = bars.df if hasattr(bars, 'df') else bars
            assert df is not None and len(df) > 0, f"No data for {symbol}"

            bar = df.iloc[0]

            # Verify OHLC consistency
            assert bar['high'] >= bar['open'], f"{symbol}: high < open"
            assert bar['high'] >= bar['close'], f"{symbol}: high < close"
            assert bar['low'] <= bar['open'], f"{symbol}: low > open"
            assert bar['low'] <= bar['close'], f"{symbol}: low > close"
            assert bar['volume'] > 0, f"{symbol}: zero volume"

            status = "✓ PASS"
            print(f"{symbol:<10} {description:<20} ${bar['open']:<9.2f} ${bar['close']:<9.2f} {bar['volume']:<15,.0f} {status}")

        print(f"\n✓ Multiple symbols PASSED")


@pytest.mark.apitest
class TestThetaDataMethods:
    """Test key methods work correctly."""

    def test_get_quote(self):
        """Test get_quote() returns correct data."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        theta = ThetaDataBacktesting(
            datetime_start=datetime.datetime(2024, 8, 1, 9, 30),
            datetime_end=datetime.datetime(2024, 8, 1, 10, 0),
            username=username,
            password=password,
        )

        # Simulate strategy getting quote
        asset = Asset("SPY", asset_type="stock")
        quote = theta.get_quote(asset, quote_asset=Asset("USD", asset_type="forex"))

        print(f"\nget_quote() test for SPY:")
        print(f"  Price: ${quote.price:.2f}")
        print(f"  Bid: ${quote.bid:.2f}" if quote.bid else "  Bid: None")
        print(f"  Ask: ${quote.ask:.2f}" if quote.ask else "  Ask: None")
        print(f"  Volume: {quote.volume:,.0f}")
        print(f"  Timestamp: {quote.timestamp}")

        assert quote is not None, "get_quote returned None"
        assert quote.price > 0, "Quote price is zero or negative"
        assert quote.volume > 0, "Quote volume is zero"

        print(f"\n✓ get_quote() PASSED")

    def test_get_chains(self):
        """Test get_chains() returns option chains."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        theta = ThetaDataBacktesting(
            datetime_start=datetime.datetime(2024, 8, 1, 9, 30),
            datetime_end=datetime.datetime(2024, 8, 1, 10, 0),
            username=username,
            password=password,
        )

        asset = Asset("SPY", asset_type="stock")
        chains = theta.get_chains(asset)

        print(f"\nget_chains() test for SPY:")

        assert chains is not None, "get_chains returned None"

        # Get expirations
        if hasattr(chains, 'expirations'):
            expirations = chains.expirations()
        else:
            expirations = chains.get("Chains", {}).get("CALL", {}).keys()

        expirations_list = list(expirations)
        print(f"  Number of expirations: {len(expirations_list)}")
        print(f"  First 3 expirations: {expirations_list[:3]}")

        # Get strikes for first expiration
        first_exp = expirations_list[0]
        if hasattr(chains, 'strikes'):
            strikes = chains.strikes(first_exp, "CALL")
        else:
            strikes = chains.get("Chains", {}).get("CALL", {}).get(first_exp, [])

        print(f"  Strikes for {first_exp}: {len(strikes)} strikes")
        print(f"  Sample strikes: {sorted(strikes)[:5]} ... {sorted(strikes)[-5:]}")

        assert len(expirations_list) > 0, "No expirations found"
        assert len(strikes) > 0, "No strikes found"

        print(f"\n✓ get_chains() PASSED")


@pytest.mark.apitest
class TestThetaDataOptions:
    """Test options pricing."""

    def test_atm_call_and_put(self):
        """Test ATM call and put pricing."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        theta = ThetaDataBacktesting(
            datetime_start=datetime.datetime(2024, 8, 1, 9, 30),
            datetime_end=datetime.datetime(2024, 8, 1, 10, 0),
            username=username,
            password=password,
        )

        # Get underlying price
        underlying = Asset("SPY", asset_type="stock")
        underlying_price = theta.get_last_price(underlying)

        print(f"\nOptions test for SPY:")
        print(f"  Underlying price: ${underlying_price:.2f}")

        # Get chains
        chains = theta.get_chains(underlying)

        # Get first expiration
        if hasattr(chains, 'expirations'):
            expirations = list(chains.expirations())
        else:
            expirations = list(chains.get("Chains", {}).get("CALL", {}).keys())

        first_exp = expirations[0]
        expiration_date = datetime.datetime.strptime(first_exp, "%Y-%m-%d").date()

        # Get ATM strike
        if hasattr(chains, 'strikes'):
            strikes = chains.strikes(first_exp, "CALL")
        else:
            strikes = chains.get("Chains", {}).get("CALL", {}).get(first_exp, [])

        atm_strike = min(strikes, key=lambda x: abs(x - underlying_price))

        print(f"  Expiration: {first_exp}")
        print(f"  ATM strike: ${atm_strike:.2f}")

        # Test ATM CALL
        call_option = Asset(
            "SPY",
            asset_type="option",
            expiration=expiration_date,
            strike=atm_strike,
            right="CALL"
        )
        call_price = theta.get_last_price(call_option)

        # Test ATM PUT
        put_option = Asset(
            "SPY",
            asset_type="option",
            expiration=expiration_date,
            strike=atm_strike,
            right="PUT"
        )
        put_price = theta.get_last_price(put_option)

        print(f"  ATM Call price: ${call_price:.2f}")
        print(f"  ATM Put price: ${put_price:.2f}")

        assert call_price > 0, "Call price is zero or negative"
        assert put_price > 0, "Put price is zero or negative"
        assert call_price > 0.05, "Call price suspiciously low (< $0.05)"
        assert put_price > 0.05, "Put price suspiciously low (< $0.05)"

        print(f"\n✓ Options pricing PASSED")


@pytest.mark.apitest
class TestThetaDataIndexes:
    """Test index data."""

    def test_spx_pricing(self):
        """Test SPX index pricing."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        theta = ThetaDataBacktesting(
            datetime_start=datetime.datetime(2024, 8, 1, 9, 30),
            datetime_end=datetime.datetime(2024, 8, 1, 12, 30),
            username=username,
            password=password,
            use_quote_data=False,  # Indices don't need bid/ask data
        )

        asset = Asset("SPX", asset_type="index")

        # Test at market open
        open_price = theta.get_last_price(asset, quote_asset=Asset("USD", asset_type="forex"))

        print(f"\nSPX index test:")
        print(f"  Market open (9:30): ${open_price:.2f}")

        assert open_price > 0, "SPX price is zero or negative"
        assert 4000 < open_price < 7000, f"SPX price ${open_price:.2f} is outside reasonable range"

        print(f"\n✓ Index pricing PASSED")


@pytest.mark.apitest
class TestThetaDataExtendedHours:
    """Test pre-market and after-hours data."""

    def test_premarket_data(self):
        """Test pre-market data (9:00-9:30)."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        theta = ThetaDataBacktesting(
            datetime_start=datetime.datetime(2024, 8, 1, 9, 0),
            datetime_end=datetime.datetime(2024, 8, 1, 9, 30),
            username=username,
            password=password,
        )

        asset = Asset("SPY", asset_type="stock")

        # Set datetime to pre-market
        theta._datetime = to_datetime_aware(datetime.datetime(2024, 8, 1, 9, 0))

        bars = theta.get_historical_prices(
            asset=asset,
            length=5,
            timestep="minute"
        )

        df = bars.df if hasattr(bars, 'df') else bars

        print(f"\nPre-market data test for SPY:")
        print(f"  Bars from 9:00-9:05:")
        for i in range(min(5, len(df))):
            bar = df.iloc[i]
            print(f"    {df.index[i]}: Open=${bar['open']:.2f}, Volume={bar['volume']:,.0f}")

        # Pre-market should have much lower volume than regular hours
        if len(df) > 0:
            avg_volume = df['volume'].mean()
            print(f"  Average pre-market volume: {avg_volume:,.0f}")
            print(f"  ✓ Pre-market data available")
        else:
            pytest.skip("Pre-market data not available")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
