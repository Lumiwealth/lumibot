#!/usr/bin/env python3
import os
import logging
from datetime import datetime
import pytz
from lumibot.backtesting import DataBentoDataBacktestingPolars
from lumibot.entities import Asset

# Enable DEBUG logging
logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')

api_key = os.getenv('DATABENTO_API_KEY')
gc_asset = Asset(symbol='GC', asset_type='cont_future')
est = pytz.timezone('America/New_York')

data_source = DataBentoDataBacktestingPolars(
    api_key=api_key,
    datetime_start=est.localize(datetime(2025, 1, 1)),
    datetime_end=est.localize(datetime(2025, 1, 31)),
    clear_cache=True
)

print("Initializing...")
data_source.initialize_data_for_backtest([gc_asset], 'minute')

print("\nCalling get_historical_prices...")
data_source.datetime = est.localize(datetime(2025, 1, 3, 10, 0))
bars = data_source.get_historical_prices(gc_asset, length=40, timestep='minute')

print(f"\nResult: {bars}")
if bars and hasattr(bars, 'df'):
    print(f"Bars: {len(bars.df)}")
