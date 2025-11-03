#!/usr/bin/env python3
import os
from datetime import datetime
import pytz
from lumibot.backtesting import DataBentoDataBacktestingPolars
from lumibot.entities import Asset

api_key = os.getenv('DATABENTO_API_KEY')
gc_asset = Asset(symbol='GC', asset_type='cont_future')
est = pytz.timezone('America/New_York')

data_source = DataBentoDataBacktestingPolars(
    api_key=api_key,
    datetime_start=est.localize(datetime(2025, 1, 1)),
    datetime_end=est.localize(datetime(2025, 1, 31)),
    clear_cache=True
)
data_source.initialize_data_for_backtest([gc_asset], 'minute')

print(f'pandas_data keys: {list(data_source.pandas_data.keys())}')
print(f'_data_store keys: {list(data_source._data_store.keys())}')
print(f'Are they the same object? {data_source.pandas_data is data_source._data_store}')

# Check what find_asset_in_data_store returns
quote_asset = Asset('USD', 'forex')
search_key = data_source.find_asset_in_data_store(gc_asset, quote_asset, 'minute')
print(f'find_asset_in_data_store returned: {search_key}')
print(f'Is it in _data_store? {search_key in data_source._data_store}')

# Now test get_historical_prices
data_source.datetime = est.localize(datetime(2025, 1, 3, 10, 0))
print(f'\nTesting get_historical_prices...')
bars = data_source.get_historical_prices(gc_asset, length=40, timestep='minute')
print(f'Result: {bars}')
if bars:
    print(f'  Has df: {hasattr(bars, "df")}')
    if hasattr(bars, 'df'):
        print(f'  Bars: {len(bars.df)}')
