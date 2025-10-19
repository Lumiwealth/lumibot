"""Debug script to compare pandas vs polars data for August 15, 2024 divergence."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime

# Add lumibot to path
current_file = Path(__file__).resolve()
lumibot_root = current_file.parent.parent.parent
if str(lumibot_root) not in sys.path:
    sys.path.insert(0, str(lumibot_root))

from lumibot.entities import Asset
from lumibot.backtesting import ThetaDataBacktesting, ThetaDataBacktestingPandas

def compare_data(symbol: str, date_str: str):
    """Compare pandas vs polars data for a specific symbol and date."""

    target_dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    target_dt = target_dt.replace(tzinfo=None)  # Will be localized by broker

    asset = Asset(symbol, Asset.AssetType.STOCK)

    print(f"\n{'='*80}")
    print(f"Comparing {symbol} data for {date_str}")
    print(f"{'='*80}\n")

    # Create both broker types - start early enough to have lookback data
    # For August 15 with 63-day lookback, we need data from ~March 2024
    pandas_broker = ThetaDataBacktestingPandas(
        datetime_start=datetime(2024, 3, 1),
        datetime_end=datetime(2024, 11, 1),
    )

    polars_broker = ThetaDataBacktesting(
        datetime_start=datetime(2024, 3, 1),
        datetime_end=datetime(2024, 11, 1),
    )

    # Advance broker to target datetime
    # This simulates being "at" August 15 in a backtest
    pandas_broker.datetime = target_dt
    polars_broker.datetime = target_dt

    print(f"Pandas broker datetime: {pandas_broker.datetime}")
    print(f"Polars broker datetime: {polars_broker.datetime}")

    # Get historical prices (63 days lookback)
    try:
        pandas_bars = pandas_broker.get_historical_prices(
            asset=asset,
            length=63,
            timestep="day",
        )

        polars_bars = polars_broker.get_historical_prices(
            asset=asset,
            length=63,
            timestep="day",
        )

        if pandas_bars is None:
            print(f"❌ PANDAS returned None for {symbol}")
            return
        if polars_bars is None:
            print(f"❌ POLARS returned None for {symbol}")
            return

        # Get dataframes
        pandas_df = pandas_bars.df
        polars_df = polars_bars.df

        print(f"Pandas shape: {pandas_df.shape}")
        print(f"Polars shape: {polars_df.shape}")

        # Compare row counts
        if len(pandas_df) != len(polars_df):
            print(f"\n❌ ROW COUNT MISMATCH: pandas={len(pandas_df)}, polars={len(polars_df)}")
        else:
            print(f"\n✓ Row counts match: {len(pandas_df)}")

        # Compare date ranges
        print(f"\nPandas date range: {pandas_df.index[0]} to {pandas_df.index[-1]}")
        print(f"Polars date range: {polars_df.index[0]} to {polars_df.index[-1]}")

        # Filter out rows with missing=True
        pandas_valid = pandas_df[pandas_df['missing'] == False] if 'missing' in pandas_df.columns else pandas_df
        polars_valid = polars_df[polars_df['missing'] == False] if 'missing' in polars_df.columns else polars_df

        print(f"\nValid (non-missing) rows: pandas={len(pandas_valid)}, polars={len(polars_valid)}")

        # Compare last valid close price and return
        if len(pandas_valid) > 0 and len(polars_valid) > 0:
            pandas_last_close = pandas_valid['close'].iloc[-1]
            polars_last_close = polars_valid['close'].iloc[-1]

            pandas_last_return = pandas_valid['return'].iloc[-1] if 'return' in pandas_valid.columns else None
            polars_last_return = polars_valid['return'].iloc[-1] if 'return' in polars_valid.columns else None

            print(f"\nLast valid close: pandas={pandas_last_close:.4f}, polars={polars_last_close:.4f}")

            if pandas_last_return is not None and polars_last_return is not None:
                print(f"Last return: pandas={pandas_last_return:.6f}, polars={polars_last_return:.6f}")

                if abs(pandas_last_return - polars_last_return) > 0.0001:
                    print(f"\n❌ RETURN MISMATCH! Difference: {abs(pandas_last_return - polars_last_return):.6f}")
                else:
                    print(f"\n✓ Returns match")

            # Calculate momentum (first to last price change)
            pandas_first_close = pandas_valid['close'].iloc[0]
            polars_first_close = polars_valid['close'].iloc[0]

            pandas_momentum = (pandas_last_close - pandas_first_close) / pandas_first_close
            polars_momentum = (polars_last_close - polars_first_close) / polars_first_close

            print(f"\n63-day momentum: pandas={pandas_momentum:.4f} ({pandas_momentum*100:.2f}%)")
            print(f"63-day momentum: polars={polars_momentum:.4f} ({polars_momentum*100:.2f}%)")

            if abs(pandas_momentum - polars_momentum) > 0.0001:
                print(f"\n❌ MOMENTUM MISMATCH! Difference: {abs(pandas_momentum - polars_momentum):.6f}")
            else:
                print(f"\n✓ Momentum calculations match")

        # Show tail of both dataframes
        print(f"\n--- Pandas last 5 rows (with returns) ---")
        print(pandas_valid[['close', 'return']].tail())

        print(f"\n--- Polars last 5 rows (with returns) ---")
        print(polars_valid[['close', 'return']].tail())

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Compare the two symbols that diverged on August 15
    compare_data("PLTR", "2024-08-15 09:30:00")
    compare_data("APP", "2024-08-15 09:30:00")
