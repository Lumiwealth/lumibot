#!/usr/bin/env python3
"""
Download fresh GC data and save to CSV
"""

import os
from datetime import datetime

import pandas as pd

from lumibot.entities import Asset
from lumibot.tools import databento_helper_polars as databento_helper

# Load API key
api_key = os.getenv("DATABENTO_API_KEY")
if not api_key:
    print("❌ ERROR: DATABENTO_API_KEY not found")
    exit(1)

print("=" * 80)
print("DOWNLOADING FRESH GC DATA")
print("=" * 80)

# Define asset - continuous futures
asset = Asset(symbol="GC", asset_type="cont_future")

# Date range
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 10, 31)

print(f"Asset: {asset}")
print(f"Period: {start_date.date()} to {end_date.date()}")
print("Note: futures_roll.py has NO GC rule (will use quarterly default)")
print()

# Download data - force fresh download
print("Downloading from DataBento (forcing cache update)...")
df = databento_helper.get_price_data_from_databento(
    api_key=api_key,
    asset=asset,
    start=start_date,
    end=end_date,
    timestep="minute",
    venue=None,
    force_cache_update=True,  # FORCE FRESH DOWNLOAD
)

if df is None:
    print("❌ Failed to download data")
    exit(1)

# Convert to pandas if needed
if hasattr(df, "to_pandas"):
    df = df.to_pandas()

# Ensure datetime index
if "datetime" in df.columns:
    df.set_index("datetime", inplace=True)

print(f"✅ Downloaded {len(df)} bars")
print(f"Date range: {df.index[0]} to {df.index[-1]}")

# Convert to Build Alpha format
print("\nConverting to Build Alpha format...")
ba_df = pd.DataFrame()
ba_df["Date"] = df.index.strftime("%m/%d/%Y")
ba_df["Time"] = df.index.strftime("%H:%M:%S")
ba_df["Open"] = df["open"]
ba_df["High"] = df["high"]
ba_df["Low"] = df["low"]
ba_df["Close"] = df["close"]
ba_df["Vol"] = df.get("volume", 1)
ba_df["OI"] = 1

# Save to CSV
output_file = "GC_FRESH_DATA.csv"
ba_df.to_csv(output_file, index=False)
print(f"✅ Saved to: {output_file}")

# Show sample
print("\nSample data (first 5 rows):")
print(ba_df.head())

print("\n" + "=" * 80)
print("SUCCESS! Fresh data downloaded and saved.")
print(f"File: {output_file}")
print("=" * 80)
