from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
import requests

from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy

pytestmark = pytest.mark.apitest


class _IbkrCryptoBuyAndHold(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True
        self._entered = False

    def on_trading_iteration(self):
        if not self._entered:
            base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
            quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
            # Ensure the data source has bars loaded (avoids "market order remained open" situations).
            bars = self.get_historical_prices(base, length=2, timestep="minute", quote=quote)
            if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
                return
            last_price = float(bars.df["close"].iloc[-1])
            if not last_price or last_price <= 0:
                return
            qty = Decimal(str((self.cash * 0.95) / last_price)).quantize(Decimal("0.0001"))
            if qty <= 0:
                return
            order = self.create_order(
                base,
                qty,
                Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
                quote=quote,
            )
            self.submit_order(order)
            self._entered = True


class _IbkrCryptoRoundTrip(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True
        self._entered_at = None

    def on_trading_iteration(self):
        base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

        # Ensure the data source has bars loaded.
        bars = self.get_historical_prices(base, length=2, timestep="minute", quote=quote)
        if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
            return
        last_price = float(bars.df["close"].iloc[-1])
        if not last_price or last_price <= 0:
            return

        now = self.get_datetime()
        if self._entered_at is None:
            qty = Decimal(str((self.cash * 0.95) / last_price)).quantize(Decimal("0.0001"))
            if qty <= 0:
                return
            order = self.create_order(
                base,
                qty,
                Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
                quote=quote,
            )
            self.submit_order(order)
            self._entered_at = now
            return

        if now >= (self._entered_at + timedelta(minutes=60)):
            position = self.get_position(self.crypto_assets_to_tuple(base, quote))
            qty = Decimal(str(getattr(position, "quantity", 0) or 0))
            if qty <= 0:
                return
            order = self.create_order(
                base,
                qty,
                Order.OrderSide.SELL,
                order_type=Order.OrderType.MARKET,
                quote=quote,
            )
            self.submit_order(order)


def _require_ibkr_downloader() -> tuple[str, str]:
    base_url = (os.environ.get("DATADOWNLOADER_BASE_URL") or "").strip().rstrip("/")
    api_key = (os.environ.get("DATADOWNLOADER_API_KEY") or "").strip()
    api_key_header = (os.environ.get("DATADOWNLOADER_API_KEY_HEADER") or "X-Downloader-Key").strip()

    if not base_url or not api_key:
        pytest.skip("Missing DATADOWNLOADER_BASE_URL / DATADOWNLOADER_API_KEY")

    try:
        resp = requests.get(
            f"{base_url}/healthz",
            headers={api_key_header: api_key},
            timeout=5,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        pytest.skip(f"Local downloader not reachable/healthy: {exc}")

    ibkr = payload.get("ibkr") if isinstance(payload, dict) else None
    if not isinstance(ibkr, dict) or not ibkr.get("enabled"):
        pytest.skip("Local downloader is running but IBKR is not enabled")
    if ibkr.get("authenticated") is not True:
        pytest.skip("Local downloader is running but IBKR is not authenticated")

    return base_url, api_key


def _derive_known_good_window_from_cached_bars(
    *,
    cache_folder: str,
    window_days: int = 2,
    seed_minutes: int = 120,
) -> tuple[datetime, datetime]:
    import lumibot.tools.ibkr_helper as ibkr_helper

    # Force all IBKR caches into a temp root.
    ibkr_helper.LUMIBOT_CACHE_FOLDER = cache_folder  # type: ignore[attr-defined]
    os.environ["IBKR_CRYPTO_VENUE"] = (os.environ.get("IBKR_CRYPTO_VENUE") or "ZEROHASH").strip().upper()

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    now = datetime.now(UTC)
    # This may return empty (weekend trap), but should still write bars to cache.
    ibkr_helper.get_price_data(
        asset=base,
        quote=quote,
        timestep="minute",
        start_dt=now - timedelta(minutes=seed_minutes),
        end_dt=now,
        exchange=None,
        include_after_hours=True,
    )

    parquet_files = list(Path(cache_folder).rglob("*.parquet"))
    bars = [p for p in parquet_files if "ibkr" in p.parts and "bars" in p.parts]
    if not bars:
        raise AssertionError("No IBKR bars parquet written; cannot derive window")

    df = pd.read_parquet(bars[0])
    assert isinstance(df.index, pd.DatetimeIndex)
    assert not df.empty

    end_ts = df.index.max()
    assert isinstance(end_ts, pd.Timestamp)
    if end_ts.tz is None:
        end_ts = end_ts.tz_localize("America/New_York")
    window_end = end_ts.to_pydatetime()
    # IMPORTANT: Keep the backtest window fully inside the cached data to avoid long
    # "no data" spans (which can make the strategy look broken even when it isn't).
    start_ts = df.index.min()
    assert isinstance(start_ts, pd.Timestamp)
    if start_ts.tz is None:
        start_ts = start_ts.tz_localize("America/New_York")

    desired_start = end_ts - pd.Timedelta(days=window_days)
    window_start = max(desired_start, start_ts).to_pydatetime()
    return window_start, window_end


def test_ibkr_crypto_backtest_produces_tearsheet_and_trade_artifacts(monkeypatch, tmp_path):
    _require_ibkr_downloader()
    monkeypatch.setenv("MARKET", "24/7")

    # Keep artifacts and cache isolated and easy to inspect.
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Derive a window guaranteed to have data (based on cached bars).
    window_start, window_end = _derive_known_good_window_from_cached_bars(
        cache_folder=cache_dir.as_posix(),
        window_days=2,
    )

    logfile = (log_dir / "ibkr_crypto_backtest.log").as_posix()
    stats_file = (log_dir / "ibkr_crypto_stats.csv").as_posix()
    settings_file = (log_dir / "ibkr_crypto_settings.json").as_posix()
    tearsheet_file = (log_dir / "ibkr_crypto_tearsheet.html").as_posix()

    _IbkrCryptoBuyAndHold.run_backtest(
        datasource_class=InteractiveBrokersRESTBacktesting,
        backtesting_start=window_start,
        backtesting_end=window_end,
        market="24/7",
        analyze_backtest=True,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=True,
        show_indicators=False,
        quiet_logs=False,
        name="IBKR_CRYPTO_BUYHOLD",
        budget=100_000.0,
        benchmark_asset=Asset("BTC", asset_type=Asset.AssetType.CRYPTO),
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
        logfile=logfile,
        stats_file=stats_file,
        settings_file=settings_file,
        tearsheet_file=tearsheet_file,
    )

    assert Path(tearsheet_file).exists(), f"Missing {tearsheet_file}"
    assert Path(stats_file).exists(), f"Missing {stats_file}"
    assert list(log_dir.glob("*_trade_events.csv")), f"No trade_events.csv found in {log_dir}"

    trades = list(log_dir.glob("*_trades.csv"))
    assert trades, f"No trades.csv found in {log_dir}"
    trades_df = pd.read_csv(trades[0])
    assert "status" in trades_df.columns
    assert (trades_df["status"].astype(str).str.lower() == "fill").any(), f"No filled trades in {trades[0]}"

    stats_df = pd.read_csv(stats_file)
    assert "portfolio_value" in stats_df.columns
    # Portfolio value should move with BTC price during the window (not stay stuck on a single mark).
    assert stats_df["portfolio_value"].nunique() > 10, f"Portfolio value did not move as expected; see {stats_file}"


def test_ibkr_crypto_backtest_roundtrip_produces_tearsheet_and_trades(monkeypatch, tmp_path):
    _require_ibkr_downloader()
    monkeypatch.setenv("MARKET", "24/7")

    # Keep artifacts and cache isolated and easy to inspect.
    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    window_start, window_end = _derive_known_good_window_from_cached_bars(
        cache_folder=cache_dir.as_posix(),
        window_days=1,
    )

    logfile = (log_dir / "ibkr_crypto_backtest_roundtrip.log").as_posix()
    stats_file = (log_dir / "ibkr_crypto_roundtrip_stats.csv").as_posix()
    settings_file = (log_dir / "ibkr_crypto_roundtrip_settings.json").as_posix()
    tearsheet_file = (log_dir / "ibkr_crypto_roundtrip_tearsheet.html").as_posix()
    _IbkrCryptoRoundTrip.run_backtest(
        datasource_class=InteractiveBrokersRESTBacktesting,
        backtesting_start=window_start,
        backtesting_end=window_end,
        market="24/7",
        analyze_backtest=True,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=True,
        show_indicators=False,
        quiet_logs=False,
        name="IBKR_CRYPTO_ROUNDTRIP",
        budget=100_000.0,
        benchmark_asset=Asset("BTC", asset_type=Asset.AssetType.CRYPTO),
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
        logfile=logfile,
        stats_file=stats_file,
        settings_file=settings_file,
        tearsheet_file=tearsheet_file,
    )

    assert list(log_dir.glob("*_trade_events.csv")), f"No trade_events.csv found in {log_dir}"
    assert Path(tearsheet_file).exists(), f"Missing {tearsheet_file}"


def test_ibkr_crypto_warm_backtest_does_not_touch_downloader(monkeypatch, tmp_path):
    _require_ibkr_downloader()
    monkeypatch.setenv("MARKET", "24/7")

    import lumibot.tools.ibkr_helper as ibkr_helper

    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Cold seed: allow downloader calls and write cache.
    window_start, window_end = _derive_known_good_window_from_cached_bars(
        cache_folder=cache_dir.as_posix(),
        window_days=1,
    )

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Warm the full requested window so the subsequent backtest shouldn't need downloader calls.
    # IMPORTANT: the strategy uses `length=2` at the very start of the window; warm the cache with
    # a small lookback so the first iteration doesn't need to fetch "one bar earlier".
    warmed = ibkr_helper.get_price_data(
        asset=base,
        quote=quote,
        timestep="minute",
        start_dt=window_start - timedelta(minutes=5),
        end_dt=window_end,
        exchange=None,
        include_after_hours=True,
    )
    assert warmed is not None and not warmed.empty

    # Warm invariant: if we have cache coverage, backtests must not call the downloader.
    def _no_downloader(*args, **kwargs):
        raise AssertionError("Warm IBKR backtest unexpectedly attempted to call the downloader")

    monkeypatch.setattr(ibkr_helper, "queue_request", _no_downloader)

    logfile = (log_dir / "ibkr_crypto_backtest_warm.log").as_posix()
    _IbkrCryptoBuyAndHold.run_backtest(
        datasource_class=InteractiveBrokersRESTBacktesting,
        backtesting_start=window_start,
        backtesting_end=window_end,
        market="24/7",
        analyze_backtest=True,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=True,
        show_indicators=False,
        quiet_logs=False,
        name="IBKR_CRYPTO_BUYHOLD_WARM",
        budget=100_000.0,
        benchmark_asset=Asset("BTC", asset_type=Asset.AssetType.CRYPTO),
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
        logfile=logfile,
    )

    assert list(log_dir.glob("*_tearsheet.html")), f"No tearsheet.html found in {log_dir}"
