#!/usr/bin/env python3
from __future__ import annotations

"""
bench_ibkr_speed_burner_warm_cache.py

Cache-only (queue-free) speed burner benchmark for IBKR REST backtesting.

This benchmark is designed to measure the *warm-cache* requirement:
- zero downloader queue requests
- bounded wall time

It intentionally stresses the hot path each iteration:
- get_last_price() per asset
- get_historical_prices(..., 100, "minute") per asset
- get_historical_prices(..., 20, "day") per asset
- frequent market orders (alternating BUY/SELL)

If required cache objects are missing, this script will fail fast rather than silently hitting the
downloader (because warm-cache speed is the metric we care about).
"""

import argparse
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter


def _force_source_tree_imports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))


def _lock_down_env() -> None:
    # Avoid recursive `.env` discovery (latency + accidental secrets loading).
    os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "true")

    # Make logs quiet for benchmarking.
    os.environ.setdefault("IS_BACKTESTING", "true")
    os.environ.setdefault("BACKTESTING_QUIET_LOGS", "true")

    # Keep benchmark artifacts/caches inside the repo so we don't write into user cache folders
    # outside `~/Documents/Development/`.
    os.environ.setdefault("LUMIBOT_CACHE_FOLDER", "tests/backtest/_ibkr_speed_burner_cache")

    # SAFETY: this benchmark must not require the downloader; set placeholders so we do not print
    # private hostnames in logs if any import-time config emits them.
    os.environ.setdefault("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    os.environ.setdefault("DATADOWNLOADER_API_KEY", "<redacted>")
    os.environ.setdefault("DATADOWNLOADER_API_KEY_HEADER", "X-Downloader-Key")


def main() -> int:
    _lock_down_env()
    _force_source_tree_imports()

    parser = argparse.ArgumentParser(description="IBKR warm-cache speed burner (cache-only) benchmark.")
    parser.add_argument("--iterations", type=int, default=200, help="Iterations per loop (futures + crypto).")
    parser.add_argument(
        "--profile-yappi-csv",
        default="",
        help="Optional path to write a yappi CSV profile (matches LumiBot *_profile_yappi.csv format).",
    )
    parser.add_argument(
        "--assert-futures-max-s",
        type=float,
        default=0.0,
        help="If >0, exit non-zero when futures loop exceeds this wall time (seconds).",
    )
    parser.add_argument(
        "--assert-crypto-max-s",
        type=float,
        default=0.0,
        help="If >0, exit non-zero when crypto loop exceeds this wall time (seconds).",
    )
    args = parser.parse_args()

    import lumibot.tools.ibkr_helper as ibkr_helper
    from lumibot.backtesting import BacktestingBroker
    from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
    from lumibot.entities import Asset
    from lumibot.entities.order import Order
    from lumibot.strategies.strategy import Strategy

    class _SpeedBurnerBase(Strategy):
        def initialize(self, parameters=None):
            self.sleeptime = "1M"
            self._i = 0
            self.include_cash_positions = True

        def _burn_one_asset(self, asset: Asset):
            last = self.get_last_price(asset)
            _ = self.get_historical_prices(asset, length=100, timestep="minute")
            _ = self.get_historical_prices(asset, length=20, timestep="day")
            return last

    class _FuturesSpeedBurnerStrategy(_SpeedBurnerBase):
        def initialize(self, parameters=None):
            super().initialize(parameters=parameters)
            self.futs = list(parameters["futs"])

        def on_trading_iteration(self):
            prices: dict[Asset, float | None] = {}
            for fut in self.futs:
                prices[fut] = self._burn_one_asset(fut)

            side = Order.OrderSide.BUY if (self._i % 2 == 0) else Order.OrderSide.SELL
            for fut in self.futs:
                # In a real strategy, you would not submit market orders when the market is closed
                # or when the data source has no bar at the current timestamp. Keep the benchmark
                # realistic so long-iteration runs don't accumulate unfillable orders and explode
                # runtime (O(n^2) order scans).
                px = prices.get(fut)
                if px is None:
                    continue
                order = self.create_order(fut, Decimal("1"), side, order_type=Order.OrderType.MARKET)
                self.submit_order(order)
            self._i += 1

    class _CryptoSpeedBurnerStrategy(_SpeedBurnerBase):
        def initialize(self, parameters=None):
            super().initialize(parameters=parameters)
            self.coins = list(parameters["coins"])

        def on_trading_iteration(self):
            prices: dict[Asset, float | None] = {}
            for coin in self.coins:
                prices[coin] = self._burn_one_asset(coin)

            side = Order.OrderSide.BUY if (self._i % 2 == 0) else Order.OrderSide.SELL
            for coin in self.coins:
                px = prices.get(coin)
                if px is None:
                    continue
                order = self.create_order(coin, Decimal("0.01"), side, order_type=Order.OrderType.MARKET)
                self.submit_order(order)
            self._i += 1

    # Deterministic window (CME futures contract + common crypto symbols).
    #
    # IMPORTANT: This script is cache-only. If your local cache does not already contain the
    # required parquet files for this window, the script will fail (by design).
    import pytz

    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2025, 12, 8, 9, 30))
    # IBKR minute bars represent the last *completed* minute. Using a slightly earlier end avoids
    # chasing the final partial minute which can force unnecessary downloader fetch attempts.
    end = tz.localize(datetime(2025, 12, 8, 19, 28))

    fut_mes = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=5)
    fut_mnq = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 12, 19), multiplier=2)
    btc = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    eth = Asset("ETH", asset_type=Asset.AssetType.CRYPTO)
    sol = Asset("SOL", asset_type=Asset.AssetType.CRYPTO)

    # Hard guard: never hit the downloader in this benchmark (warm-cache invariant).
    downloader_attempts: list[dict] = []

    def _no_queue(*args, **kwargs):
        downloader_attempts.append({"args": args, "kwargs": kwargs})
        raise AssertionError("Downloader queue_request called during warm-cache benchmark")

    ibkr_helper.queue_request = _no_queue  # type: ignore[assignment]

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=start,
        datetime_end=end,
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False
    broker._update_datetime(start.replace(hour=12, minute=0))  # ensure some lookback exists
    wrap_dt = start.replace(hour=12, minute=0)

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

    # Preflight: ensure ALL required series are present in cache and we did not attempt to hit
    # the downloader. If this fails, the benchmark is meaningless.
    def _require_bars(asset: Asset, *, length: int, timestep: str) -> None:
        bars = futures.get_historical_prices(asset, length=length, timestep=timestep)
        if bars is None or getattr(bars, "df", None) is None or bars.df is None or bars.df.empty:
            raise RuntimeError(f"Missing cached bars for {asset} timestep={timestep} (bars empty)")
        if len(bars.df) < length:
            raise RuntimeError(f"Missing cached bars for {asset} timestep={timestep} (len={len(bars.df)} < {length})")

    for fut in (fut_mes, fut_mnq):
        _require_bars(fut, length=100, timestep="minute")
        _require_bars(fut, length=20, timestep="day")
    _require_bars(fut_mes, length=10, timestep="15min")

    for coin in (btc, eth, sol):
        bars = crypto.get_historical_prices(coin, length=100, timestep="minute")
        if bars is None or getattr(bars, "df", None) is None or bars.df is None or bars.df.empty:
            raise RuntimeError(f"Missing cached bars for {coin} timestep=minute (bars empty)")
        if len(bars.df) < 100:
            raise RuntimeError(f"Missing cached bars for {coin} timestep=minute (len={len(bars.df)} < 100)")

        bars = crypto.get_historical_prices(coin, length=20, timestep="day")
        if bars is None or getattr(bars, "df", None) is None or bars.df is None or bars.df.empty:
            raise RuntimeError(f"Missing cached bars for {coin} timestep=day (bars empty)")
        if len(bars.df) < 20:
            raise RuntimeError(f"Missing cached bars for {coin} timestep=day (len={len(bars.df)} < 20)")

    if downloader_attempts:
        raise RuntimeError(
            f"Warm-cache preflight failed: attempted to call downloader {len(downloader_attempts)} times. "
            "Warm-cache benchmark requires the series to already exist in parquet cache."
        )

    iterations = int(args.iterations)

    yappi = None
    if args.profile_yappi_csv:
        import yappi as _yappi

        yappi = _yappi
        yappi.set_clock_type("wall")
        yappi.start()

    # Phase A: includes in-process prefetch reads (from warm disk cache) on first access.
    t0 = perf_counter()
    for _ in range(iterations):
        futures.on_trading_iteration()
        broker.process_pending_orders(futures)
        futures._executor.process_queue()
        next_dt = broker.datetime + timedelta(minutes=1)
        if next_dt >= end:
            next_dt = wrap_dt
        broker._update_datetime(next_dt)
    t1 = perf_counter()

    for _ in range(iterations):
        crypto.on_trading_iteration()
        broker.process_pending_orders(crypto)
        crypto._executor.process_queue()
        next_dt = broker.datetime + timedelta(minutes=1)
        if next_dt >= end:
            next_dt = wrap_dt
        broker._update_datetime(next_dt)
    t2 = perf_counter()

    futures_s = t1 - t0
    crypto_s = t2 - t1

    if yappi is not None:
        try:
            yappi.stop()
            stats = yappi.get_func_stats()
            stats.sort("ttot", "desc")

            out_path = Path(args.profile_yappi_csv).expanduser()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            import csv

            with out_path.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "full_name",
                        "module",
                        "lineno",
                        "name",
                        "ncall",
                        "nactualcall",
                        "ttot_s",
                        "tsub_s",
                        "tavg_s",
                        "ctx_name",
                    ]
                )
                for entry in stats:
                    writer.writerow(
                        [
                            getattr(entry, "full_name", ""),
                            getattr(entry, "module", ""),
                            getattr(entry, "lineno", ""),
                            getattr(entry, "name", ""),
                            getattr(entry, "ncall", ""),
                            getattr(entry, "nactualcall", ""),
                            getattr(entry, "ttot", ""),
                            getattr(entry, "tsub", ""),
                            getattr(entry, "tavg", ""),
                            getattr(entry, "ctx_name", ""),
                        ]
                    )
            print(f"profile: yappi csv={out_path}")
        finally:
            try:
                yappi.clear_stats()
            except Exception:
                pass

    print("IBKR speed burner (warm-cache, cache-only)")
    print(f"futures: {iterations} iters in {futures_s:.3f}s ({iterations / max(futures_s, 1e-9):.1f} it/s)")
    print(f"crypto:  {iterations} iters in {crypto_s:.3f}s ({iterations / max(crypto_s, 1e-9):.1f} it/s)")
    print("note: this benchmark asserts queue-free behavior by monkeypatching queue_request to raise")

    failed = False
    if float(args.assert_futures_max_s or 0.0) > 0.0 and futures_s > float(args.assert_futures_max_s):
        print(f"FAIL: futures wall time {futures_s:.3f}s exceeds max {float(args.assert_futures_max_s):.3f}s")
        failed = True
    if float(args.assert_crypto_max_s or 0.0) > 0.0 and crypto_s > float(args.assert_crypto_max_s):
        print(f"FAIL: crypto wall time {crypto_s:.3f}s exceeds max {float(args.assert_crypto_max_s):.3f}s")
        failed = True
    if failed:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
