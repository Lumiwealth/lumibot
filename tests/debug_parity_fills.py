"""
Debug script to compare pandas vs polars fills bar-by-bar.
This will show exactly where they diverge.
"""
import sys
from datetime import datetime
import pytz

from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPandas
from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars
from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")

class SimpleMESTrader(Strategy):
    """Simple strategy that buys and sells MES to test fill accuracy"""

    def initialize(self):
        self.sleeptime = "15M"
        self.set_market("us_futures")
        self.asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        self.state = "BUY"

    def on_trading_iteration(self):
        position = self.get_position(self.asset)

        if self.state == "BUY" and not position:
            order = self.create_order(self.asset, 1, "buy")
            self.submit_order(order)
            self.state = "SELL"
        elif self.state == "SELL" and position:
            order = self.create_order(self.asset, 1, "sell")
            self.submit_order(order)
            self.state = "DONE"

def run_backtest(mode: str):
    """Run backtest and capture fills"""
    print(f"\n{'='*80}")
    print(f"RUNNING {mode.upper()} BACKEND")
    print(f"{'='*80}\n")

    datasource_cls = DataBentoDataBacktestingPolars if mode == "polars" else DataBentoDataBacktestingPandas

    # Very short test period - just 2 hours
    tzinfo = pytz.timezone("America/New_York")
    start = tzinfo.localize(datetime(2025, 10, 14, 9, 30))
    end = tzinfo.localize(datetime(2025, 10, 14, 11, 30))

    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = SimpleMESTrader(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)
    results = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    print(f"\n{'='*80}")
    print(f"{mode.upper()} RESULTS:")
    print(f"Final Portfolio Value: ${results['strategy_results']['SimpleMESTrader']['cagr']}")
    print(f"{'='*80}\n")

    return results

if __name__ == "__main__":
    if not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>':
        print("ERROR: DATABENTO_API_KEY not configured")
        sys.exit(1)

    print("="*80)
    print("PARITY FILL COMPARISON TEST")
    print("="*80)
    print("\nThis will run both backends and log every [BROKER_FILL_BAR]")
    print("Look for differences in timestamps and prices between pandas and polars\n")

    # Run both backends
    pandas_results = run_backtest("pandas")
    polars_results = run_backtest("polars")

    print("\n" + "="*80)
    print("COMPARISON COMPLETE")
    print("="*80)
    print("\nNow grep the output above for [BROKER_FILL_BAR] and compare timestamps/prices")
    print("Any difference indicates where the divergence starts")
