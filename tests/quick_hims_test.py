"""Quick test to verify HIMS option fill matches between pandas and polars."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import ThetaDataBacktesting

# Import strategy
from tests.performance.strategies.weekly_momentum_options import WeeklyMomentumOptionsStrategy

# Test the week where HIMS option trades (July 11-18, 2024)
# Need the full week so the strategy has enough historical data to rank stocks
BACKTEST_START = datetime(2024, 7, 11)
BACKTEST_END = datetime(2024, 7, 19)  # Through July 18 to verify the fill

trading_fee = TradingFee(percent_fee=0.001)

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

print(f"\nBacktest completed!")
