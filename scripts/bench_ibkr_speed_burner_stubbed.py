#!/usr/bin/env python3
from __future__ import annotations

"""
bench_ibkr_speed_burner_stubbed.py

> Stubbed (no network) speed burner benchmark for IBKR-like backtests.

**IMPORTANT:** This script forces imports from the *checked-out source tree* (not an installed
`lumibot` package) so the numbers track current branch changes.
"""

import os
import sys
import logging
from datetime import date
from decimal import Decimal
from pathlib import Path
from time import perf_counter

import pandas as pd


def _force_source_tree_imports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))


def _lock_down_env() -> None:
    # SAFETY: this benchmark does not need the real downloader. Force placeholders so local `.env`
    # values (private hostnames / API keys) never appear in logs from import-time config.
    os.environ["DATADOWNLOADER_BASE_URL"] = "http://localhost:8080"
    os.environ["DATADOWNLOADER_API_KEY"] = "<redacted>"
    os.environ.setdefault("DATADOWNLOADER_API_KEY_HEADER", "X-Downloader-Key")

    # Avoid recursive `.env` discovery (latency + accidental secrets loading) during benchmarks.
    os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "true")

    # Reduce logging noise for benchmarks.
    os.environ.setdefault("IS_BACKTESTING", "true")
    os.environ.setdefault("BACKTESTING_QUIET_LOGS", "true")
    os.environ.setdefault("BACKTESTING_LOG_ITERATION_HEARTBEAT", "false")
    os.environ.setdefault("BACKTESTING_CAPTURE_LOCALS", "false")


def _minute_df(index: pd.DatetimeIndex, start_price: float) -> pd.DataFrame:
    prices = start_price + (pd.Series(range(len(index))) * 0.01).to_numpy()
    return pd.DataFrame(
        {"open": prices, "high": prices + 0.01, "low": prices - 0.01, "close": prices, "volume": 1000},
        index=index,
    )


def _day_df(index: pd.DatetimeIndex, start_price: float) -> pd.DataFrame:
    prices = start_price + (pd.Series(range(len(index))) * 1.0).to_numpy()
    return pd.DataFrame(
        {"open": prices, "high": prices + 1.0, "low": prices - 1.0, "close": prices, "volume": 1_000_000},
        index=index,
    )


def _multi_minute_df(source: pd.DataFrame, minutes: int) -> pd.DataFrame:
    return (
        source.resample(f"{minutes}min")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna()
    )


def main() -> int:
    _lock_down_env()
    _force_source_tree_imports()

    # Keep benchmark output readable. LumiBot logging is designed for interactive runs, but in
    # microbenchmarks the per-order INFO logs dominate stdout and can skew timing.
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("lumibot").setLevel(logging.WARNING)

    from lumibot.backtesting import BacktestingBroker
    from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
    from lumibot.entities import Asset
    from lumibot.entities.order import Order
    from lumibot.strategies.strategy import Strategy
    import lumibot.tools.ibkr_helper as ibkr_helper

    class _SpeedBurnerBase(Strategy):
        def initialize(self, parameters=None):
            self.sleeptime = "1M"
            self._i = 0
            self.include_cash_positions = True

        def _burn_one_asset(self, asset: Asset):
            _ = self.get_last_price(asset)
            _ = self.get_historical_prices(asset, length=100, timestep="minute").df
            _ = self.get_historical_prices(asset, length=20, timestep="day").df

    class _FuturesSpeedBurnerStrategy(_SpeedBurnerBase):
        def initialize(self, parameters=None):
            super().initialize(parameters=parameters)
            self.futs = list(parameters["futs"])

        def on_trading_iteration(self):
            for fut in self.futs:
                self._burn_one_asset(fut)

            side = Order.OrderSide.BUY if (self._i % 2 == 0) else Order.OrderSide.SELL
            for fut in self.futs:
                order = self.create_order(fut, Decimal("1"), side, order_type=Order.OrderType.MARKET)
                self.submit_order(order)
            self._i += 1

    class _CryptoSpeedBurnerStrategy(_SpeedBurnerBase):
        def initialize(self, parameters=None):
            super().initialize(parameters=parameters)
            self.coins = list(parameters["coins"])

        def on_trading_iteration(self):
            for coin in self.coins:
                self._burn_one_asset(coin)

            side = Order.OrderSide.BUY if (self._i % 2 == 0) else Order.OrderSide.SELL
            for coin in self.coins:
                order = self.create_order(coin, Decimal("0.01"), side, order_type=Order.OrderType.MARKET)
                self.submit_order(order)
            self._i += 1

    iterations = int(os.environ.get("LUMIBOT_BENCH_ITERATIONS", "1000"))

    tz = "America/New_York"
    # Keep enough minute bars for longer runs. A 200-iteration benchmark with a shared broker
    # overweights one-time lazy series loads; 1000 iterations gives a steadier runtime signal.
    minute_periods = max(4000, (iterations * 2) + 300)
    minute_index = pd.date_range("2025-12-08 09:30", periods=minute_periods, freq="1min", tz=tz)
    # Provide enough daily history for `length=20` at the chosen intraday timestamps.
    day_index = pd.date_range("2025-09-01 00:00", periods=200, freq="1D", tz=tz)

    fut_mes = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut_mnq = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=2)
    btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    eth = Asset("ETH", asset_type=Asset.AssetType.CRYPTO)
    sol = Asset("SOL", asset_type=Asset.AssetType.CRYPTO)

    datasets: dict[tuple[str, str], pd.DataFrame] = {
        ("MES", "minute"): _minute_df(minute_index, 6400.0),
        ("MES", "day"): _day_df(day_index, 6300.0),
        ("MNQ", "minute"): _minute_df(minute_index, 17000.0),
        ("MNQ", "day"): _day_df(day_index, 16500.0),
        ("BTC", "minute"): _minute_df(minute_index, 40000.0),
        ("BTC", "day"): _day_df(day_index, 39000.0),
        ("ETH", "minute"): _minute_df(minute_index, 2000.0),
        ("ETH", "day"): _day_df(day_index, 1900.0),
        ("SOL", "minute"): _minute_df(minute_index, 100.0),
        ("SOL", "day"): _day_df(day_index, 95.0),
    }
    datasets[("MES", "15minute")] = _multi_minute_df(datasets[("MES", "minute")], 15)

    calls: dict[tuple[str, str], int] = {}

    def fake_get_price_data(*, asset, quote, timestep, start_dt, end_dt, exchange=None, include_after_hours=True, source=None):
        sym = getattr(asset, "symbol", "")
        key = (sym, str(timestep))
        calls[key] = calls.get(key, 0) + 1
        df = datasets.get(key)
        if df is None:
            raise RuntimeError(f"Missing stub dataset for {key}")
        return df

    ibkr_helper.get_price_data = fake_get_price_data  # type: ignore[assignment]

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=minute_index[0].to_pydatetime(),
        datetime_end=minute_index[-1].to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False
    broker._update_datetime(minute_index[200].to_pydatetime())

    futures = _FuturesSpeedBurnerStrategy(
        broker=broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={"futs": [fut_mes, fut_mnq]},
    )
    futures._first_iteration = False
    futures.initialize(parameters={"futs": [fut_mes, fut_mnq]})

    crypto = _CryptoSpeedBurnerStrategy(
        broker=broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={"coins": [btc, eth, sol]},
    )
    crypto._first_iteration = False
    crypto.initialize(parameters={"coins": [btc, eth, sol]})

    # Ensure multi-timeframe paths are exercised.
    _ = futures.get_historical_prices(fut_mes, length=10, timestep="15min")

    t0 = perf_counter()
    for _ in range(iterations):
        futures.on_trading_iteration()
        broker.process_pending_orders(futures)
        futures._executor.process_queue()
        broker._update_datetime(60)
    t1 = perf_counter()

    for _ in range(iterations):
        crypto.on_trading_iteration()
        broker.process_pending_orders(crypto)
        crypto._executor.process_queue()
        broker._update_datetime(60)
    t2 = perf_counter()

    futures_s = t1 - t0
    crypto_s = t2 - t1
    print("IBKR speed burner (stubbed, source tree)")
    print(f"futures: {iterations} iters in {futures_s:.3f}s ({iterations / max(futures_s, 1e-9):.1f} it/s)")
    print(f"crypto:  {iterations} iters in {crypto_s:.3f}s ({iterations / max(crypto_s, 1e-9):.1f} it/s)")
    print(f"loads: {sum(calls.values())} series loads")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
