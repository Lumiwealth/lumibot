"""Performance harness for AAPL Deep Dip Calls strategy (ThetaData pandas vs Polars)."""

from __future__ import annotations

import importlib.util
import time
from datetime import datetime
from pathlib import Path

import pytest
import yappi

from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import ThetaDataBacktesting, ThetaDataBacktestingPandas

STRATEGY_PATH = Path('/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/AAPL Deep Dip Calls (Copy 2).py')
OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BACKTEST_START = datetime(2025, 8, 18)
BACKTEST_END = datetime(2025, 9, 1)
STRATEGY_PARAMS = {
    "symbol": "TSLA",
    "decline_trigger_pct": 0.25,
    "hold_duration_days": 365,
    "otm_pct": 0.20,
    "target_exp_days": 730,
    "risk_per_trade": 0.95,
    "max_contracts": 50,
    "stop_loss_pct": 0.50,
    "min_days_to_expiry_exit": 30,
    "max_spread_pct": 0.35,
    "sleeptime": "1D",
}


def _load_strategy():
    spec = importlib.util.spec_from_file_location("aapl_deep_dip_copy2", STRATEGY_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.AAPLDeepDipCalls


def _run_profile(mode: str) -> float:
    strategy_cls = _load_strategy()
    datasource = ThetaDataBacktesting if mode == "polars" else ThetaDataBacktestingPandas
    label = f"aapl_deep_dip_{mode}"
    profile_path = OUTPUT_DIR / f"{label}.prof"

    trading_fee = TradingFee(percent_fee=0.001)

    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start()
    start = time.time()

    strategy_cls.backtest(
        datasource,
        backtesting_start=BACKTEST_START,
        backtesting_end=BACKTEST_END,
        budget=100000,
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        quote_asset=Asset("USD", Asset.AssetType.FOREX),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters=STRATEGY_PARAMS,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
        show_progress_bar=False,
        save_stats_file=False,
        save_logfile=False,
    )

    elapsed = time.time() - start
    yappi.stop()
    yappi.get_func_stats().save(str(profile_path), type="pstat")
    return elapsed


@pytest.mark.performance
def test_aapl_deep_dip_pandas_vs_polars():
    pandas_elapsed = _run_profile("pandas")
    polars_elapsed = _run_profile("polars")

    pandas_profile = OUTPUT_DIR / "aapl_deep_dip_pandas.prof"
    polars_profile = OUTPUT_DIR / "aapl_deep_dip_polars.prof"

    assert pandas_profile.exists()
    assert polars_profile.exists()
    # Log timings for manual inspection; don't enforce a hard ratio yet.
    print(f"pandas_elapsed={pandas_elapsed:.2f}s polars_elapsed={polars_elapsed:.2f}s")
