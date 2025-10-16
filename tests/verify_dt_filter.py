"""Quick test to verify dt filtering is working for HIMS option on July 18, 2024."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
os.environ['BACKTESTING_QUIET_LOGS'] = 'false'

from datetime import datetime
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import ThetaDataBacktesting

from tests.performance.strategies.weekly_momentum_options import WeeklyMomentumOptionsStrategy

# Just July 18 - the day HIMS trades
BACKTEST_START = datetime(2024, 7, 18)
BACKTEST_END = datetime(2024, 7, 19)

trading_fee = TradingFee(percent_fee=0.001)

print("\n" + "="*80)
print("VERIFYING DT FILTER - POLARS MODE")
print("="*80)
print(f"Date: {BACKTEST_START} to {BACKTEST_END}")
print(f"Looking for:")
print(f"  1. HIMS option minute data fetch at broker_dt=2024-07-18 09:30:00")
print(f"  2. Check if last_ts > broker_dt (would indicate bug)")
print(f"  3. Check for BROKER_FILL_BAR with HIMS")
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

print("\n" + "="*80)
print("TEST COMPLETE - Check logs above for:")
print("  - [THETA][RETURN] lines showing last_ts")
print("  - [BROKER_FILL_BAR] showing HIMS fill")
print("="*80)
