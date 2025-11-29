"""Profile DataBento comprehensive backtest - realistic trading simulation."""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path

import pytz
import yappi

from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPandas, DataBentoDataBacktestingPolars
from lumibot.credentials import DATABENTO_CONFIG
from lumibot.entities import Asset, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader

OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


class MultiInstrumentTrader(Strategy):
    """Trading strategy that tests multiple data operations"""

    def initialize(self):
        self.sleeptime = "15M"  # Every 15 minutes
        self.set_market("us_futures")
        self.instruments = [
            Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE),
            Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE),
            Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE),
        ]
        self.current_idx = 0
        self.phase = "BUY"

    def on_trading_iteration(self):
        if self.current_idx >= len(self.instruments):
            return

        asset = self.instruments[self.current_idx]

        # Get last price (tests get_last_price)
        self.get_last_price(asset)

        # Get historical data (tests filtering)
        self.get_historical_prices(asset, 20, timestep="minute")

        # Simple trading logic
        position = self.get_position(asset)

        if self.phase == "BUY" and not position:
            order = self.create_order(asset, 1, "buy")
            self.submit_order(order)
            self.phase = "SELL"
        elif self.phase == "SELL" and position:
            order = self.create_order(asset, 1, "sell")
            self.submit_order(order)
            self.current_idx += 1
            self.phase = "BUY"


def run_comprehensive_profile(mode: str) -> float:
    """Run full backtest and profile it"""
    datasource_cls = DataBentoDataBacktestingPolars if mode == "polars" else DataBentoDataBacktestingPandas
    label = f"databento_comprehensive_{mode}"
    profile_path = OUTPUT_DIR / f"{label}.prof"

    # 2 trading days
    tzinfo = pytz.timezone("America/New_York")
    start = tzinfo.localize(datetime(2024, 1, 3, 9, 30))
    end = tzinfo.localize(datetime(2024, 1, 4, 16, 0))

    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start()
    wall_start = time.time()

    # Run backtest
    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = MultiInstrumentTrader(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)
    trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    elapsed = time.time() - wall_start
    yappi.stop()

    # Save profile
    yappi.get_func_stats().save(str(profile_path), type="pstat")

    # Print results
    print(f"\n{'='*60}")
    print(f"MODE: {mode.upper()}")
    print(f"{'='*60}")
    print(f"Elapsed time: {elapsed:.2f}s")
    print(f"Iterations: {strat.broker.iteration_count if hasattr(strat.broker, 'iteration_count') else 'unknown'}")
    print(f"Profile saved: {profile_path}")
    print(f"{'='*60}\n")

    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile comprehensive DataBento backtest")
    parser.add_argument("--mode", choices=["pandas", "polars", "both"], default="both")
    args = parser.parse_args()

    if not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>':
        print("ERROR: DATABENTO_API_KEY not configured")
        return

    modes = [args.mode] if args.mode != "both" else ["pandas", "polars"]
    results = {}

    for m in modes:
        elapsed = run_comprehensive_profile(m)
        results[m] = elapsed

    # Print comparison
    if len(results) > 1:
        print(f"\n{'='*60}")
        print("COMPARISON")
        print(f"{'='*60}")
        pandas_time = results.get("pandas", 0)
        polars_time = results.get("polars", 0)
        if pandas_time > 0 and polars_time > 0:
            speedup = pandas_time / polars_time
            print(f"Pandas: {pandas_time:.2f}s")
            print(f"Polars: {polars_time:.2f}s")
            print(f"Speedup: {speedup:.2f}x")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
