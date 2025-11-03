#!/usr/bin/env python3
"""Debug script to trace date_start/date_end in DataPolars initialization"""

import os
from datetime import datetime, timedelta
from lumibot.backtesting import DataBentoDataBacktestingPolars
from lumibot.entities import Asset
import pytz

# Set API key
api_key = os.getenv("DATABENTO_API_KEY")
if not api_key:
    print("ERROR: DATABENTO_API_KEY not set")
    exit(1)

# Create backtest period: Jan 1-31, 2025
tz = pytz.timezone("America/New_York")
start = tz.localize(datetime(2025, 1, 1))
end = tz.localize(datetime(2025, 1, 31))

print(f"BACKTEST PERIOD: {start} to {end}")
print()

# Create data source
data_source = DataBentoDataBacktestingPolars(
    datetime_start=start,
    datetime_end=end,
    api_key=api_key
)

# Create asset
asset = Asset("MES", "future")

# Update pandas data (fetch data from DataBento)
print("Fetching data...")
data_source._update_pandas_data(asset, Asset("USD", "forex"), length=100, timestep="minute")

# Check what data we got
search_key = (asset, Asset("USD", "forex"))
if search_key in data_source.pandas_data:
    data_obj = data_source.pandas_data[search_key]
    
    print(f"\nDataPolars object attributes:")
    print(f"  date_start: {data_obj.date_start}")
    print(f"  date_end: {data_obj.date_end}")
    print(f"  datetime_start: {data_obj.datetime_start}")
    print(f"  datetime_end: {data_obj.datetime_end}")
    print(f"  polars_df.height: {data_obj.polars_df.height}")
    
    if data_obj.polars_df.height > 0:
        print(f"  polars_df datetime range: {data_obj.polars_df['datetime'].min()} to {data_obj.polars_df['datetime'].max()}")
        print(f"\nFirst 5 bars:")
        print(data_obj.polars_df.head(5))
        print(f"\nLast 5 bars:")
        print(data_obj.polars_df.tail(5))
        
        # Check how many bars in January vs December
        import polars as pl
        dec_bars = data_obj.polars_df.filter(pl.col("datetime").dt.month() == 12).height
        jan_bars = data_obj.polars_df.filter(pl.col("datetime").dt.month() == 1).height
        print(f"\nBars by month:")
        print(f"  December: {dec_bars}")
        print(f"  January: {jan_bars}")
        
        print(json.dumps({
            "success": jan_bars > 0 and dec_bars == 0,
            "data": {
                "total_bars": data_obj.polars_df.height,
                "december_bars": dec_bars,
                "january_bars": jan_bars,
                "date_start": str(data_obj.date_start),
                "date_end": str(data_obj.date_end)
            }
        }))
else:
    print("ERROR: No data found")
    import json
    print(json.dumps({"success": False, "data": {"error": "No data found"}}))
