"""Profile pandas vs Polars backtests across major data sources using YAPPI.

Runs tailored profiling strategies for Polygon, ThetaData, DataBento, and Yahoo
under both pandas and Polars data pipelines, capturing profiler output and
logging execution metrics to the historical CSV for regression tracking.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple, Type

import numpy as np
import yappi

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lumibot.backtesting import (  # noqa: E402
    BacktestingBroker,
    ThetaDataBacktestingPandas,
    ThetaDataBacktestingPolars,
    DataBentoDataBacktestingPandas,
    DataBentoDataBacktestingPolars,
)
from lumibot.entities import Asset  # noqa: E402
from lumibot.strategies import Strategy  # noqa: E402
from lumibot.traders import Trader  # noqa: E402
from lumibot.tools.lumibot_logger import get_logger  # noqa: E402
from lumibot.tools.polars_utils import resample_polars_ohlc  # noqa: E402

from tests.backtest.performance_tracker import record_backtest_performance  # noqa: E402

logger = get_logger(__name__)

LOG_DIR = Path("tests/backtest/logs/polars_profiles")
LOG_DIR.mkdir(parents=True, exist_ok=True)


class BaseProfilingStrategy(Strategy):
    """Shared behaviour for profiling strategies."""

    asset: Asset
    order_size: int = 5
    resample_multiplier: int = 5
    base_unit: str = "minute"
    base_timestep: str = "minute"

    def initialize(self):
        self.sleeptime = "15M"
        self._iteration = 0
        self._did_buy = False
        self._did_sell = False
        self._latest_bars = None
        if not getattr(self, "base_timestep", None):
            self.base_timestep = self.base_unit

    def _manual_resample(self, bars):
        if not bars:
            return
        try:
            if getattr(self, "_use_polars_default", False) and hasattr(bars, "polars_df"):
                resample_polars_ohlc(
                    bars.polars_df,
                    multiplier=self.resample_multiplier,
                    base_unit=self.base_unit,
                    length=3,
                )
            elif hasattr(bars, "pandas_df"):
                df = bars.pandas_df
                if hasattr(df, "resample"):
                    suffix = "min" if self.base_unit == "minute" else "D"
                    rule = f"{self.resample_multiplier}{suffix}"
                    df.resample(rule, label="left", closed="left").agg({
                        "open": "first",
                        "high": "max",
                        "low": "min",
                        "close": "last",
                        "volume": "sum",
                    }).dropna()
        except Exception as exc:  # pragma: no cover - diagnostics only
            self.log_message(f"[profiling] manual resample failed: {exc}")

    def _exercise_data_pipeline(self):
        primary_asset = self.asset
        bars = self.get_historical_prices(primary_asset, 30, self.base_timestep)
        self._latest_bars = bars
        self._manual_resample(bars)
        _ = self.get_last_price(primary_asset)
        _ = self.get_portfolio_value()
        _ = self.get_cash()
        _ = self.get_orders()
        _ = self.get_positions()

    def on_trading_iteration(self):
        self._exercise_data_pipeline()

        if not self._did_buy:
            order = self.create_order(self.asset, self.order_size, "buy")
            self.submit_order(order)
            self._did_buy = True
        elif not self._did_sell:
            position = self.get_position(self.asset)
            if position and position.quantity > 0:
                order = self.create_order(self.asset, position.quantity, "sell")
                self.submit_order(order)
                self._did_sell = True

        self._iteration += 1


class ThetaProfilingStrategy(BaseProfilingStrategy):
    parameters = {"symbol": "AMZN"}

    def initialize(self):
        self.asset = Asset(self.parameters["symbol"], asset_type=Asset.AssetType.STOCK)
        self.base_timestep = "day"
        self.base_unit = "day"
        self.resample_multiplier = 2
        super().initialize()
        self._chains_checked = False

    def on_trading_iteration(self):
        if not self._chains_checked:
            try:
                self.get_chains(self.asset)
            except Exception as exc:  # pragma: no cover - diagnostic only
                self.log_message(f"[profiling] get_chains failed: {exc}")
            self._chains_checked = True
        super().on_trading_iteration()


class DataBentoProfilingStrategy(BaseProfilingStrategy):
    parameters = {"symbol": "MES"}
    order_size = 2

    def initialize(self):
        self.asset = Asset(self.parameters["symbol"], asset_type=Asset.AssetType.CONT_FUTURE)
        super().initialize()


StrategyFactory = Callable[[bool], Type[Strategy]]


@dataclass
class ProviderConfig:
    name: str
    pandas_ds: Type
    polars_ds: Type
    strategy_factory: StrategyFactory
    start: datetime
    end: datetime
    cache_dirs: Tuple[Path, ...]
    ds_kwargs_factory: Callable[[str], Optional[Dict]]
    description: str


def build_strategy(base_cls: Type[Strategy], use_polars: bool) -> Type[Strategy]:
    """Inject default return_polars behaviour into strategy."""

    class ProfilingStrategy(base_cls):  # type: ignore
        _use_polars_default = use_polars

        def get_historical_prices(self, asset, length, timestep="", timeshift=None,
                                   quote=None, exchange=None, include_after_hours=True,
                                   return_polars=None):
            if return_polars is None:
                return_polars = bool(self._use_polars_default)
            return super().get_historical_prices(
                asset,
                length,
                timestep=timestep,
                timeshift=timeshift,
                quote=quote,
                exchange=exchange,
                include_after_hours=include_after_hours,
                return_polars=return_polars,
            )

    ProfilingStrategy.__name__ = f"{base_cls.__name__}{'Polars' if use_polars else 'Pandas'}Profile"
    return ProfilingStrategy


def clear_cache(dirs: Tuple[Path, ...]):
    for directory in dirs:
        if directory.exists():
            shutil.rmtree(directory, ignore_errors=True)


def execute_backtest(strategy_cls: Type[Strategy], datasource_cls: Type,
                      start: datetime, end: datetime, ds_kwargs: Dict,
                      notes: str) -> Tuple[float, Strategy]:
    data_source = datasource_cls(datetime_start=start, datetime_end=end, **ds_kwargs)
    broker = BacktestingBroker(data_source=data_source)
    strategy = strategy_cls(broker=broker, backtesting_start=start, backtesting_end=end)

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)

    start_ts = time.time()
    trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
        tearsheet_file="",
    )
    elapsed = time.time() - start_ts

    trading_days = int(np.busday_count(start.date(), end.date()))
    record_backtest_performance(
        test_name=f"profiling_{strategy.__class__.__name__}",
        data_source=data_source.SOURCE if hasattr(data_source, "SOURCE") else datasource_cls.__name__,
        execution_time_seconds=elapsed,
        trading_days=trading_days,
        strategy_name=strategy_cls.__name__,
        start_date=start,
        end_date=end,
        sleeptime=getattr(strategy, "sleeptime", None),
        notes=notes,
    )

    return elapsed, strategy


def run_profile(provider: ProviderConfig, mode: str, warm: bool,
                skip_cache_clear: bool, output: Dict) -> None:
    use_polars = mode == "polars"
    ds_cls = provider.polars_ds if use_polars else provider.pandas_ds
    creds_kwargs = provider.ds_kwargs_factory(mode)
    if creds_kwargs is None:
        logger.warning("Skipping %s %s run due to missing credentials", provider.name, mode)
        return

    if not warm and not skip_cache_clear:
        clear_cache(provider.cache_dirs)

    strategy_cls = build_strategy(provider.strategy_factory(use_polars), use_polars)

    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start()

    label = f"{provider.name}_{mode}_{'warm' if warm else 'cold'}"
    try:
        elapsed, strategy = execute_backtest(
            strategy_cls,
            ds_cls,
            provider.start,
            provider.end,
            creds_kwargs,
            notes=f"{provider.description} | mode={mode} | {'warm' if warm else 'cold'}",
        )
    except Exception as exc:
        yappi.stop()
        logger.error("Profiling run failed for %s (%s, %s): %s", provider.name, mode, 'warm' if warm else 'cold', exc)
        output[label] = {
            "error": str(exc),
        }
        return
    else:
        yappi.stop()

    profile_path = LOG_DIR / f"{label}.prof"
    func_stats = yappi.get_func_stats()
    func_stats.save(str(profile_path), type="pstat")

    output[label] = {
        "elapsed_seconds": elapsed,
        "orders": len(strategy.get_orders()) if hasattr(strategy, "get_orders") else None,
        "portfolio_value": float(strategy.get_portfolio_value()) if hasattr(strategy, "get_portfolio_value") else None,
        "profile": str(profile_path),
    }


def build_configs() -> Dict[str, ProviderConfig]:
    home = Path.home()
    theta_user = os.environ.get("THETADATA_USERNAME")
    theta_pass = os.environ.get("THETADATA_PASSWORD")
    databento_key = os.environ.get("DATABENTO_API_KEY") or os.environ.get("DATABENTO_APIKEY")

    def thetadata_kwargs(_mode: str) -> Optional[Dict]:
        if not theta_user or not theta_pass:
            return None
        return {"username": theta_user, "password": theta_pass, "show_progress_bar": False}

    def databento_kwargs(_mode: str) -> Optional[Dict]:
        if not databento_key:
            return None
        return {"api_key": databento_key, "show_progress_bar": False}

    return {
        "thetadata": ProviderConfig(
            name="thetadata",
            pandas_ds=ThetaDataBacktestingPandas,
            polars_ds=ThetaDataBacktestingPolars,
            strategy_factory=lambda use_polars: ThetaProfilingStrategy,
            start=datetime(2025, 8, 18),
            end=datetime(2025, 9, 1),
            cache_dirs=(home / ".lumibot" / "thetadata",),
            ds_kwargs_factory=thetadata_kwargs,
            description="ThetaData equity profiling strategy",
        ),
        "databento": ProviderConfig(
            name="databento",
            pandas_ds=DataBentoDataBacktestingPandas,
            polars_ds=DataBentoDataBacktestingPolars,
            strategy_factory=lambda use_polars: DataBentoProfilingStrategy,
            start=datetime(2025, 9, 1),
            end=datetime(2025, 9, 15),
            cache_dirs=(home / ".lumibot" / "databento",),
            ds_kwargs_factory=databento_kwargs,
            description="DataBento futures profiling strategy",
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="Profile pandas vs Polars backtests with YAPPI")
    parser.add_argument("--providers", nargs="*", default=["thetadata", "databento"],
                        help="Subset of providers to profile")
    parser.add_argument("--modes", nargs="*", default=["pandas", "polars"], choices=["pandas", "polars"],
                        help="Which data frame modes to run")
    parser.add_argument("--skip-warm", action="store_true", help="Skip warm cache runs")
    parser.add_argument("--skip-cold", action="store_true", help="Skip cold cache runs")
    parser.add_argument("--skip-cache-clear", action="store_true",
                        help="Do not clear cache between cold runs")

    args = parser.parse_args()

    configs = build_configs()
    output_summary: Dict[str, Dict] = {}

    for provider_name in args.providers:
        provider = configs.get(provider_name)
        if not provider:
            logger.warning("Unknown provider %s; skipping", provider_name)
            continue

        for mode in args.modes:
            if not args.skip_cold:
                run_profile(provider, mode, warm=False, skip_cache_clear=args.skip_cache_clear,
                            output=output_summary)
            if not args.skip_warm:
                run_profile(provider, mode, warm=True, skip_cache_clear=True, output=output_summary)

    summary_file = LOG_DIR / "summary.json"
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(output_summary, f, indent=2, default=str)
    print(f"Profiling summary saved to {summary_file}")


if __name__ == "__main__":
    main()
