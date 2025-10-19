"""
Consolidated pandas vs polars parity tests for ThetaData backtesting.

This test suite ensures that both ThetaDataBacktesting (polars) and
ThetaDataBacktestingPandas return identical results for:
- Stock historical prices and momentum calculations
- Option quote lookups at specific datetimes
- Full backtest runs over multi-day windows

All tests should PASS when pandas and polars have perfect parity.
"""

import os
import pytest
import pytz
from datetime import datetime
import pandas as pd

from lumibot.backtesting import ThetaDataBacktesting, ThetaDataBacktestingPandas
from lumibot.entities import Asset


def test_option_quote_aug1_isolated():
    """
    Reproduce the time-travel bug where polars uses expiration date for quote lookup.

    BUG: Polars backtester returns option quotes from expiration date instead of current backtest date.

    Example:
    - Date: Aug 1, 2024 @ 9:30 AM
    - Option: PLTR CALL 27.0 exp 2024-08-16
    - Expected (pandas): bid=1.92, ask=2.15, mid=2.035
    - Actual (polars): bid=5.05, ask=5.25, mid=5.15 (from Aug 16, not Aug 1!)

    This test should FAIL before the fix and PASS after the fix.
    """

    # Set up environment
    os.environ.setdefault("BACKTESTING_QUIET_LOGS", "false")
    os.environ.setdefault("IS_BACKTESTING", "true")

    ny = pytz.timezone("America/New_York")

    # Test window - need to cover option listing through expiration
    start = datetime(2024, 7, 1)
    end = datetime(2024, 9, 1)

    # The critical moment: Aug 1, 2024 @ 9:30 AM (backtest is "at" this time)
    current_dt = ny.localize(datetime(2024, 8, 1, 9, 30))

    # The option that caused the divergence
    option_asset = Asset(
        symbol="PLTR",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime(2024, 8, 16).date(),
        strike=27.0,
        right="CALL"
    )

    # Create both broker types
    pandas_broker = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False
    )

    polars_broker = ThetaDataBacktesting(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False
    )

    # Advance both brokers to Aug 1, 2024 @ 9:30 AM
    pandas_broker._update_datetime(current_dt)
    polars_broker._update_datetime(current_dt)

    # Get quotes for the option at this point in time
    print(f"\n{'='*80}")
    print(f"Getting option quote for {option_asset.symbol} on {current_dt}")
    print(f"Option expiration: {option_asset.expiration}")
    print(f"{'='*80}\n")

    pandas_quote = pandas_broker.get_quote(option_asset, timestep="minute")
    polars_quote = polars_broker.get_quote(option_asset, timestep="minute")

    # Print results
    print(f"\nPandas quote:")
    print(f"  bid={pandas_quote.bid}, ask={pandas_quote.ask}, mid={pandas_quote.mid_price}")

    print(f"\nPolars quote:")
    print(f"  bid={polars_quote.bid}, ask={polars_quote.ask}, mid={polars_quote.mid_price}")

    print(f"\n{'='*80}\n")

    # ASSERTIONS
    # The quotes should be identical for the same asset at the same datetime
    assert pandas_quote is not None, "Pandas quote should not be None"
    assert polars_quote is not None, "Polars quote should not be None"

    # Expected values from cache file analysis:
    # Aug 1 @ 00:00: bid=1.92, ask=2.15 (pandas uses this)
    # Aug 16 @ 20:00: bid=5.05, ask=5.25 (polars incorrectly uses this!)

    # Check that both brokers return the Aug 1 prices, not the Aug 16 prices
    assert abs(pandas_quote.bid - 1.92) < 0.01, f"Pandas bid should be ~1.92, got {pandas_quote.bid}"
    assert abs(pandas_quote.ask - 2.15) < 0.01, f"Pandas ask should be ~2.15, got {pandas_quote.ask}"

    # This will FAIL before the fix (polars returns Aug 16 prices: bid=5.05, ask=5.25)
    assert abs(polars_quote.bid - 1.92) < 0.01, (
        f"POLARS TIME-TRAVEL BUG: Expected bid=1.92 (Aug 1), got {polars_quote.bid} (likely from Aug 16!)"
    )
    assert abs(polars_quote.ask - 2.15) < 0.01, (
        f"POLARS TIME-TRAVEL BUG: Expected ask=2.15 (Aug 1), got {polars_quote.ask} (likely from Aug 16!)"
    )

    # Quotes should match exactly
    assert abs(pandas_quote.bid - polars_quote.bid) < 0.01, (
        f"Bid mismatch: pandas={pandas_quote.bid}, polars={polars_quote.bid}"
    )
    assert abs(pandas_quote.ask - polars_quote.ask) < 0.01, (
        f"Ask mismatch: pandas={pandas_quote.ask}, polars={polars_quote.ask}"
    )
    assert abs(pandas_quote.mid_price - polars_quote.mid_price) < 0.01, (
        f"Mid price mismatch: pandas={pandas_quote.mid_price}, polars={polars_quote.mid_price}"
    )


def test_get_historical_prices_parity_aug15_pltr():
    """Compare pandas vs polars for PLTR on August 15, 2024."""

    # Set up environment
    os.environ.setdefault("BACKTESTING_QUIET_LOGS", "false")  # Show logs for debugging
    os.environ.setdefault("IS_BACKTESTING", "true")

    ny = pytz.timezone("America/New_York")

    # Test window
    start = datetime(2024, 7, 1)
    end = datetime(2024, 11, 1)
    target_dt = ny.localize(datetime(2024, 8, 15, 9, 30))

    # Asset
    asset = Asset("PLTR", asset_type=Asset.AssetType.STOCK)

    # Create both broker types
    pandas_broker = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False
    )

    polars_broker = ThetaDataBacktesting(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False
    )

    # Set datetime to August 15
    pandas_broker._update_datetime(target_dt)
    polars_broker._update_datetime(target_dt)

    # Get 63 days of historical data (same as the strategy)
    pandas_bars = pandas_broker.get_historical_prices(
        asset=asset,
        length=63,
        timestep="day"
    )

    polars_bars = polars_broker.get_historical_prices(
        asset=asset,
        length=63,
        timestep="day"
    )

    # Get DataFrames
    pandas_df = pandas_bars.df
    polars_df = polars_bars.df

    # Print diagnostics
    print(f"\n{'='*80}")
    print(f"PLTR Data Comparison (August 15, 2024)")
    print(f"{'='*80}")
    print(f"\nPandas shape: {pandas_df.shape}")
    print(f"Polars shape: {polars_df.shape}")
    print(f"\nPandas date range: {pandas_df.index[0]} to {pandas_df.index[-1]}")
    print(f"Polars date range: {polars_df.index[0]} to {polars_df.index[-1]}")

    # Filter non-missing rows
    pandas_valid = pandas_df[pandas_df['missing'] == False]
    polars_valid = polars_df[polars_df['missing'] == False]

    print(f"\nValid rows: pandas={len(pandas_valid)}, polars={len(polars_valid)}")

    # Compare last 5 rows
    print(f"\nPandas last 5 rows:")
    print(pandas_valid[['close', 'return']].tail())

    print(f"\nPolars last 5 rows:")
    print(polars_valid[['close', 'return']].tail())

    # Calculate momentum
    pandas_first = pandas_valid['close'].iloc[0]
    pandas_last = pandas_valid['close'].iloc[-1]
    polars_first = polars_valid['close'].iloc[0]
    polars_last = polars_valid['close'].iloc[-1]

    pandas_momentum = (pandas_last - pandas_first) / pandas_first
    polars_momentum = (polars_last - polars_first) / polars_first

    print(f"\nPandas: ${pandas_first:.2f} → ${pandas_last:.2f} = {pandas_momentum:.4f} ({pandas_momentum*100:.2f}%)")
    print(f"Polars: ${polars_first:.2f} → ${polars_last:.2f} = {polars_momentum:.4f} ({polars_momentum*100:.2f}%)")
    print(f"{'='*80}")

    # ASSERTIONS
    assert len(pandas_df) == len(polars_df), f"Row count mismatch: pandas={len(pandas_df)}, polars={len(polars_df)}"
    assert len(pandas_valid) == len(polars_valid), f"Valid row count mismatch"

    # Compare close prices (should be identical)
    pd.testing.assert_series_equal(
        pandas_valid['close'].reset_index(drop=True),
        polars_valid['close'].reset_index(drop=True),
        check_names=False,
        rtol=1e-9,
        atol=1e-9
    )

    # Compare momentum
    assert abs(pandas_momentum - polars_momentum) < 0.0001, (
        f"Momentum mismatch: pandas={pandas_momentum:.6f}, polars={polars_momentum:.6f}"
    )


def test_get_historical_prices_parity_aug15_app():
    """Compare pandas vs polars for APP on August 15, 2024."""

    os.environ.setdefault("BACKTESTING_QUIET_LOGS", "false")
    os.environ.setdefault("IS_BACKTESTING", "true")

    ny = pytz.timezone("America/New_York")

    start = datetime(2024, 7, 1)
    end = datetime(2024, 11, 1)
    target_dt = ny.localize(datetime(2024, 8, 15, 9, 30))

    asset = Asset("APP", asset_type=Asset.AssetType.STOCK)

    pandas_broker = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False
    )

    polars_broker = ThetaDataBacktesting(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False
    )

    pandas_broker._update_datetime(target_dt)
    polars_broker._update_datetime(target_dt)

    pandas_bars = pandas_broker.get_historical_prices(
        asset=asset,
        length=63,
        timestep="day"
    )

    polars_bars = polars_broker.get_historical_prices(
        asset=asset,
        length=63,
        timestep="day"
    )

    pandas_df = pandas_bars.df
    polars_df = polars_bars.df

    print(f"\n{'='*80}")
    print(f"APP Data Comparison (August 15, 2024)")
    print(f"{'='*80}")
    print(f"\nPandas shape: {pandas_df.shape}")
    print(f"Polars shape: {polars_df.shape}")

    pandas_valid = pandas_df[pandas_df['missing'] == False]
    polars_valid = polars_df[polars_df['missing'] == False]

    print(f"\nValid rows: pandas={len(pandas_valid)}, polars={len(polars_valid)}")

    pandas_first = pandas_valid['close'].iloc[0]
    pandas_last = pandas_valid['close'].iloc[-1]
    polars_first = polars_valid['close'].iloc[0]
    polars_last = polars_valid['close'].iloc[-1]

    pandas_momentum = (pandas_last - pandas_first) / pandas_first
    polars_momentum = (polars_last - polars_first) / polars_first

    print(f"\nPandas: ${pandas_first:.2f} → ${pandas_last:.2f} = {pandas_momentum:.4f} ({pandas_momentum*100:.2f}%)")
    print(f"Polars: ${polars_first:.2f} → ${polars_last:.2f} = {polars_momentum:.4f} ({polars_momentum*100:.2f}%)")
    print(f"{'='*80}")

    # ASSERTIONS
    assert len(pandas_df) == len(polars_df), f"Row count mismatch"
    assert len(pandas_valid) == len(polars_valid), f"Valid row count mismatch"

    pd.testing.assert_series_equal(
        pandas_valid['close'].reset_index(drop=True),
        polars_valid['close'].reset_index(drop=True),
        check_names=False,
        rtol=1e-9,
        atol=1e-9
    )

    assert abs(pandas_momentum - polars_momentum) < 0.0001, (
        f"Momentum mismatch: pandas={pandas_momentum:.6f}, polars={polars_momentum:.6f}"
    )


def test_aug1_full_backtest():
    """
    Run a focused 3-day backtest (July 31 - Aug 2) to capture the Aug 1 option quote divergence.

    This test runs both pandas and polars backtests over the critical Aug 1 window
    with DEBUG_OPTION_QUOTES=1 enabled to capture detailed logging of option quote lookups.

    The bug manifests during full backtest runs but not in isolated quote lookups.
    This test should reveal the smoking gun in the logs.
    """

    # Enable verbose option quote logging
    os.environ["DEBUG_OPTION_QUOTES"] = "1"
    os.environ["BACKTESTING_QUIET_LOGS"] = "false"
    os.environ["IS_BACKTESTING"] = "true"

    ny = pytz.timezone("America/New_York")

    # Focused 3-day window around Aug 1
    start = datetime(2024, 7, 31)
    end = datetime(2024, 8, 2)

    # Create a simple strategy that queries the problematic option
    from lumibot.strategies import Strategy

    class Aug1TestStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.target_option = Asset(
                symbol="PLTR",
                asset_type=Asset.AssetType.OPTION,
                expiration=datetime(2024, 8, 16).date(),
                strike=27.0,
                right="CALL"
            )

        def on_trading_iteration(self):
            # Query the option quote - this should trigger the instrumented logging
            current_time = self.get_datetime()
            print(f"\n{'='*80}")
            print(f"[STRATEGY] on_trading_iteration called at {current_time}")
            print(f"{'='*80}\n")

            quote = self.get_quote(self.target_option, timestep="minute")

            if quote:
                print(f"[STRATEGY] Got quote: bid={quote.bid}, ask={quote.ask}, mid={quote.mid_price}")
            else:
                print(f"[STRATEGY] Got None quote")

    # Run pandas backtest
    print(f"\n{'='*80}")
    print(f"RUNNING PANDAS BACKTEST (July 31 - Aug 2)")
    print(f"{'='*80}\n")

    pandas_strategy = Aug1TestStrategy(broker=ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False
    ))
    pandas_result = pandas_strategy.run_backtest()

    # Run polars backtest
    print(f"\n{'='*80}")
    print(f"RUNNING POLARS BACKTEST (July 31 - Aug 2)")
    print(f"{'='*80}\n")

    polars_strategy = Aug1TestStrategy(broker=ThetaDataBacktesting(
        datetime_start=start,
        datetime_end=end,
        show_progress_bar=False
    ))
    polars_result = polars_strategy.run_backtest()

    # Compare results
    print(f"\n{'='*80}")
    print(f"BACKTEST RESULTS COMPARISON")
    print(f"{'='*80}")
    print(f"Pandas ending value: {pandas_result}")
    print(f"Polars ending value: {polars_result}")
    print(f"{'='*80}\n")

    # The backtests should produce identical results if parity is achieved
    # For now, we just verify they both complete successfully
    # TODO: Add strict result comparison once the bug is fixed
    assert pandas_result is not None, "Pandas backtest should complete successfully"
    assert polars_result is not None, "Polars backtest should complete successfully"


if __name__ == "__main__":
    pytest.main([__file__, "-xvs"])
