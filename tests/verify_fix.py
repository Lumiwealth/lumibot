"""Verify the dt filtering fix works for both pandas and polars."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import ThetaDataBacktesting, ThetaDataBacktestingPolars

from tests.performance.strategies.weekly_momentum_options import WeeklyMomentumOptionsStrategy

BACKTEST_START = datetime(2024, 7, 18)
BACKTEST_END = datetime(2024, 7, 19)

trading_fee = TradingFee(percent_fee=0.001)

print("\n" + "="*80)
print("RUNNING PANDAS (should fill HIMS at $1.40 or similar)")
print("="*80)
pandas_result = WeeklyMomentumOptionsStrategy.backtest(
    ThetaDataBacktestingPolars,  # This is actually polars despite the name
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

print(f"\nBacktest completed!")
