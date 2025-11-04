#!/usr/bin/env python3
"""
Download GC data EXACTLY like gc_futures_optimized.py does it
Then save to CSV and plot with analyze_missing_minutes.py
"""

import os
from datetime import datetime

import pandas as pd

from lumibot.backtesting import DataBentoDataBacktesting
from lumibot.entities import Asset

# Load API key from environment
api_key = os.getenv("DATABENTO_API_KEY")
if not api_key:
    print("❌ ERROR: DATABENTO_API_KEY not found")
    exit(1)

print("=" * 80)
print("Downloading GC data using EXACT SAME CODE as gc_futures_optimized.py")
print("=" * 80)

# Define the asset EXACTLY as in the example
asset = Asset(symbol="GC", asset_type="cont_future")  # Continuous futures for seamless backtesting

# Set up dates EXACTLY as in the example (but using 2025)
backtest_start = datetime(2025, 1, 1)
backtest_end = datetime(2025, 10, 31)

print(f"Asset: {asset}")
print(f"Period: {backtest_start.date()} to {backtest_end.date()}")
print()

# Create DataBento data source EXACTLY as the example does it
print("Creating DataBento data source...")
data_source = DataBentoDataBacktesting(api_key=api_key, datetime_start=backtest_start, datetime_end=backtest_end)

# Initialize data for backtest EXACTLY as the example does it
print("Initializing data for backtest (prefetch)...")
data_source.initialize_data_for_backtest(
    strategy_assets=[asset], timestep="minute"  # MUST use minute-level data as in example
)

print("Data prefetch complete")

# Now get the data from the prefetched cache
print("\nExtracting prefetched data...")

# The data is stored in data_source.data_store after prefetch
# This is a dictionary with asset symbols as keys
if hasattr(data_source, "data_store") and data_source.data_store:
    # Get the data for our asset
    gc_data = None
    for key in data_source.data_store:
        if "GC" in str(key):
            gc_data = data_source.data_store[key]
            print(f"Found GC data under key: {key}")
            break

    if gc_data is not None:
        # Convert to pandas DataFrame if needed
        if hasattr(gc_data, "to_pandas"):
            df = gc_data.to_pandas()
        else:
            df = gc_data

        print(f"Data shape: {df.shape}")
        print(f"Data range: {df.index[0]} to {df.index[-1]}")

        # Convert to Build Alpha format
        print("\nConverting to Build Alpha format...")
        ba_df = pd.DataFrame()

        # Ensure we have a datetime index
        if "datetime" in df.columns:
            df = df.set_index("datetime")

        # Convert to Build Alpha format
        ba_df["Date"] = df.index.strftime("%m/%d/%Y")
        ba_df["Time"] = df.index.strftime("%H:%M:%S")
        ba_df["Open"] = df["open"]
        ba_df["High"] = df["high"]
        ba_df["Low"] = df["low"]
        ba_df["Close"] = df["close"]
        ba_df["Vol"] = df.get("volume", 1)
        ba_df["OI"] = 1

        # Save to CSV
        output_file = "GC_from_example_method.csv"
        ba_df.to_csv(output_file, index=False)
        print(f"✅ Saved to: {output_file}")
        print(f"   Total rows: {len(ba_df)}")

        # Show sample
        print("\nSample data (first 5 rows):")
        print(ba_df.head())

        print("\n" + "=" * 80)
        print("SUCCESS! Data downloaded using EXACT SAME METHOD as gc_futures_optimized.py")
        print(f"File saved: {output_file}")
        print("\nNow you can plot it with:")
        print(f"  python analyze_missing_minutes.py {output_file}")
        print("=" * 80)

    else:
        print("❌ Could not find GC data in data_store")
else:
    print("❌ No data_store found after prefetch")

    # Alternative: Try to get data directly
    print("\nTrying alternative method...")
    try:
        # Try getting historical prices directly
        bars = data_source.get_historical_prices(
            asset=asset, length=1000000, timestep="minute"  # Get as much as possible
        )

        if bars and hasattr(bars, "df"):
            df = bars.df
            print(f"Got {len(df)} bars via get_historical_prices")

            # Convert to Build Alpha format
            ba_df = pd.DataFrame()
            ba_df["Date"] = df.index.strftime("%m/%d/%Y")
            ba_df["Time"] = df.index.strftime("%H:%M:%S")
            ba_df["Open"] = df["open"]
            ba_df["High"] = df["high"]
            ba_df["Low"] = df["low"]
            ba_df["Close"] = df["close"]
            ba_df["Vol"] = df.get("volume", 1)
            ba_df["OI"] = 1

            output_file = "GC_from_example_method.csv"
            ba_df.to_csv(output_file, index=False)
            print(f"✅ Saved to: {output_file}")

    except Exception as e:
        print(f"Alternative method failed: {e}")
