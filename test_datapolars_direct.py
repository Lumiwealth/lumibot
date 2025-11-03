#!/usr/bin/env python3
"""
Test DataPolars construction directly
"""

import os
import datetime
from datetime import timedelta
from lumibot.entities import Asset
from lumibot.entities.data_polars import DataPolars
from lumibot.tools import databento_helper_polars as databento_helper

api_key = os.getenv("DATABENTO_API_KEY")
gc_asset = Asset(symbol="GC", asset_type="cont_future")
start_datetime = datetime.datetime(2025, 1, 1)
end_datetime = datetime.datetime(2025, 1, 31)

# Fetch data
df = databento_helper.get_price_data_from_databento(
    api_key=api_key,
    asset=gc_asset,
    start=start_datetime - timedelta(days=5),
    end=end_datetime + timedelta(days=1),
    timestep="minute",
    venue=None,
    force_cache_update=False
)

print(f"Fetched DataFrame: {df.height} rows")
print(f"Range: {df['datetime'][0]} to {df['datetime'][-1]}")

# Test 1: Create DataPolars WITHOUT date_start/date_end (old way)
print("\n" + "="*80)
print("TEST 1: DataPolars WITHOUT date_start/date_end")
print("="*80)
data_obj1 = DataPolars(
    gc_asset,
    df=df,
    timestep="minute",
    quote=Asset("USD", "forex"),
)
print(f"Result: {len(data_obj1.df)} rows")
if len(data_obj1.df) > 0:
    print(f"Range: {data_obj1.df.index[0]} to {data_obj1.df.index[-1]}")

# Test 2: Create DataPolars WITH date_start/date_end (new way)
print("\n" + "="*80)
print("TEST 2: DataPolars WITH date_start/date_end")
print("="*80)
data_obj2 = DataPolars(
    gc_asset,
    df=df,
    timestep="minute",
    quote=Asset("USD", "forex"),
    date_start=start_datetime,
    date_end=end_datetime,
)
print(f"Result: {len(data_obj2.df)} rows")
if len(data_obj2.df) > 0:
    print(f"Range: {data_obj2.df.index[0]} to {data_obj2.df.index[-1]}")
