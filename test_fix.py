#!/usr/bin/env python3
"""
Quick test to verify the fix for the 15-bar bug
"""

import os
from datetime import datetime
import pytz
from lumibot.backtesting import DataBentoDataBacktestingPolars
from lumibot.entities import Asset

# Get API key
api_key = os.getenv("DATABENTO_API_KEY")
if not api_key:
    print("❌ DATABENTO_API_KEY not found")
    exit(1)

# Test parameters
gc_asset = Asset(symbol="GC", asset_type="cont_future")
est = pytz.timezone('America/New_York')
backtest_start = est.localize(datetime(2025, 1, 1))
backtest_end = est.localize(datetime(2025, 1, 31))

print("="*80)
print("Testing Fix: get_historical_prices() should return 40 bars (not 15)")
print("="*80)

# Initialize data source
print("Initializing DataBento data source (clearing cache)...")
data_source = DataBentoDataBacktestingPolars(
    api_key=api_key,
    datetime_start=backtest_start,
    datetime_end=backtest_end,
    clear_cache=True  # Force fresh download
)

# Initialize data
print("Loading data for GC continuous futures...")
try:
    data_source.initialize_data_for_backtest(
        strategy_assets=[gc_asset],
        timestep="minute"
    )
    print(f"Data loaded. pandas_data keys: {list(data_source.pandas_data.keys())}")
    for key, data_obj in data_source.pandas_data.items():
        if hasattr(data_obj, 'df'):
            print(f"  {key}: {len(data_obj.df)} rows")
except Exception as e:
    print(f"ERROR during initialize: {e}")
    import traceback
    traceback.print_exc()

# Test dates
test_dates = [
    est.localize(datetime(2025, 1, 3, 10, 0)),
    est.localize(datetime(2025, 1, 15, 14, 30)),
]

print("\nTesting get_historical_prices() at different dates:")
print("-" * 80)

for test_dt in test_dates:
    data_source.datetime = test_dt

    try:
        bars = data_source.get_historical_prices(
            asset=gc_asset,
            length=40,
            timestep="minute"
        )

        if bars and hasattr(bars, 'df'):
            df = bars.df
            num_bars = len(df)

            if num_bars > 0:
                first_date = str(df.index[0])
                last_date = str(df.index[-1])

                status = "✅ PASS" if num_bars == 40 else "❌ FAIL"
                print(f"\nTest datetime: {test_dt}")
                print(f"  Bars returned: {num_bars} (requested 40) {status}")
                print(f"  Date range: {first_date} to {last_date}")

                if num_bars != 40:
                    print(f"  ⚠️  Expected 40 bars, got {num_bars}")
            else:
                print(f"\n❌ FAIL: No bars returned for {test_dt}")
        else:
            print(f"\n❌ FAIL: No bars object for {test_dt}")
            print(f"  bars type: {type(bars)}")
            print(f"  bars value: {bars}")
    except Exception as e:
        print(f"\n❌ EXCEPTION for {test_dt}: {e}")
        import traceback
        traceback.print_exc()

print("\n" + "="*80)
print("Test complete!")
print("="*80)
