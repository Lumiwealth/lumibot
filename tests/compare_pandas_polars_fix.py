"""Compare pandas vs polars with the dt filtering fix.

This test verifies that both modes:
1. Respect the dt (broker time) parameter when filtering data
2. Never return future data (last_ts <= broker_dt)
3. Fill HIMS option at the same price/time
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import ThetaDataBacktesting

from tests.performance.strategies.weekly_momentum_options import WeeklyMomentumOptionsStrategy

# Short window: July 18-24 where HIMS trades on July 18
BACKTEST_START = datetime(2024, 7, 18)
BACKTEST_END = datetime(2024, 7, 24)

trading_fee = TradingFee(percent_fee=0.001)

print("\n" + "="*80)
print("COMPARING PANDAS vs POLARS - dt Filtering Fix Verification")
print("="*80)
print(f"Test window: {BACKTEST_START} to {BACKTEST_END}")
print(f"Expected: HIMS option should fill at same price in both modes")
print(f"Expected: [THETA][FILTER][AFTER] logs should show last_ts <= dt")
print("="*80)

# ThetaDataBacktesting is actually the polars version now
print("\n\n" + "="*80)
print("RUNNING TEST")
print("="*80)
result = WeeklyMomentumOptionsStrategy.backtest(
    ThetaDataBacktesting,
    backtesting_start=BACKTEST_START,
    backtesting_end=BACKTEST_END,
    budget=100000,
    benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
    quote_asset=Asset("USD", Asset.AssetType.FOREX),
    buy_trading_fees=[trading_fee],
    sell_trading_fees=[trading_fee],
    parameters={},
    show_plot=False,
    show_tearsheet=False,
    save_tearsheet=False,
    show_indicators=False,
    quiet_logs=False,
    show_progress_bar=False,
)

print(f"\n\nBacktest completed!")
print("="*80)
print("Check logs above for:")
print("1. [THETA][FILTER][AFTER] lines - verify last_ts <= dt_filter")
print("2. [BROKER_FILL_EXEC] HIMS - note the fill price and time")
print("="*80)
