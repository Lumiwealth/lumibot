#!/usr/bin/env python3
"""
Test what DataBento actually returns
"""

import os
from datetime import datetime, timedelta
from lumibot.entities import Asset
from lumibot.tools import databento_helper_polars as databento_helper

api_key = os.getenv("DATABENTO_API_KEY")
if not api_key:
    print("❌ DATABENTO_API_KEY not found")
    exit(1)

gc_asset = Asset(symbol="GC", asset_type="cont_future")
start_datetime = datetime(2025, 1, 1) - timedelta(days=5)  # Dec 27
end_datetime = datetime(2025, 1, 31) + timedelta(days=1)   # Feb 1

print("="*80)
print(f"Testing DataBento fetch for GC continuous futures")
print(f"Requesting: {start_datetime.date()} to {end_datetime.date()}")
print("="*80)

# Get data from DataBento
df = databento_helper.get_price_data_from_databento(
    api_key=api_key,
    asset=gc_asset,
    start=start_datetime,
    end=end_datetime,
    timestep="minute",
    venue=None,
    force_cache_update=True  # Force fresh download
)

print(f"\nDataBento returned type: {type(df)}")

if df is not None:
    if hasattr(df, 'height'):  # Polars
        print(f"Total rows: {df.height}")
        if df.height > 0:
            print(f"First row datetime: {df['datetime'][0]}")
            print(f"Last row datetime: {df['datetime'][-1]}")
            print(f"Columns: {df.columns}")
    elif hasattr(df, 'shape'):  # Pandas
        print(f"Total rows: {len(df)}")
        if len(df) > 0:
            print(f"First row datetime: {df.index[0]}")
            print(f"Last row datetime: {df.index[-1]}")
            print(f"Columns: {list(df.columns)}")
else:
    print("❌ DataBento returned None!")

print("="*80)
