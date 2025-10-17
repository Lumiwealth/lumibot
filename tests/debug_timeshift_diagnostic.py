"""
Diagnostic test to compare timeshift bar selection between pandas and polars.

Runs a very short backtest (1 hour) and logs detailed timeshift information
to compare how each backend selects bars.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pytz

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPandas, DataBentoDataBacktestingPolars
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")

class DiagnosticStrategy(Strategy):
    """Simple strategy that just buys on first bar to trigger fills"""

    def initialize(self):
        self.asset = Asset("MES", asset_type="future")
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        # Only buy on first iteration to keep logs minimal
        if self.first_iteration:
            self.create_order(self.asset, 1, "buy")

def run_diagnostic(backend_name):
    """Run diagnostic backtest with specified backend"""

    print(f"\n{'='*80}")
    print(f"DIAGNOSTIC: {backend_name.upper()} BACKEND")
    print(f"{'='*80}\n")

    # Very short period - just 1 hour
    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2024, 1, 3, 9, 30))
    end = tz.localize(datetime(2024, 1, 3, 10, 30))

    # Create data source
    datasource_cls = DataBentoDataBacktestingPolars if backend_name == "polars" else DataBentoDataBacktestingPandas
    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Create broker and strategy
    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = DiagnosticStrategy(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    # Run backtest
    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)
    result = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    return result

if __name__ == "__main__":
    # Run pandas first
    print("\n" + "="*80)
    print("RUNNING PANDAS DIAGNOSTIC")
    print("="*80)
    pandas_result = run_diagnostic("pandas")

    # Run polars
    print("\n" + "="*80)
    print("RUNNING POLARS DIAGNOSTIC")
    print("="*80)
    polars_result = run_diagnostic("polars")

    print("\n" + "="*80)
    print("DIAGNOSTIC COMPLETE")
    print("="*80)
    print("\nGrep logs for [TIMESHIFT_PANDAS] and [TIMESHIFT_POLARS] to compare")
    print("Expected: Both should return the same bar timestamps")
