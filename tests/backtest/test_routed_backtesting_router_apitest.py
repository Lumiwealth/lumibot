from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd
import pytest
import requests

from lumibot.credentials import POLYGON_API_KEY
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy

pytestmark = pytest.mark.apitest


def _require_downloader() -> tuple[str, str, str]:
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
        resp.json()
    except Exception as exc:
        pytest.skip(f"Downloader not reachable/healthy: {exc}")

    return base_url, api_key, api_key_header


def _wrap_queue_request(monkeypatch) -> list[str]:
    """Capture urls passed to queue_request (IBKR + Theta both use this)."""
    import lumibot.tools.data_downloader_queue_client as queue
    import lumibot.tools.ibkr_helper as ibkr_helper

    calls: list[str] = []
    original = queue.queue_request

    def wrapped(url: str, querystring: dict[str, Any] | None, headers=None, timeout=None):
        calls.append(url)
        return original(url=url, querystring=querystring, headers=headers, timeout=timeout)

    monkeypatch.setattr(queue, "queue_request", wrapped)
    # ibkr_helper imports queue_request directly, so patch the local reference too.
    monkeypatch.setattr(ibkr_helper, "queue_request", wrapped)
    return calls


class _CryptoBuyHold(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True
        self._entered = False

    def on_trading_iteration(self):
        if self._entered:
            return

        base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
        quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

        bars = self.get_historical_prices(base, length=2, timestep="minute", quote=quote)
        if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
            return
        last_price = float(bars.df["close"].iloc[-1])
        if not last_price or last_price <= 0:
            return

        qty = Decimal(str((self.cash * 0.95) / last_price)).quantize(Decimal("0.0001"))
        if qty <= 0:
            return

        self.submit_order(
            self.create_order(
                base,
                qty,
                Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
                quote=quote,
            )
        )
        self._entered = True


class _StockFetchOnly(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
        bars = self.get_historical_prices(asset, length=2, timestep="minute")
        if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
            return


def test_router_crypto_ibkr_uses_ibkr_queue_paths(monkeypatch, tmp_path):
    _require_downloader()

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("MARKET", "24/7")
    monkeypatch.setenv("DATADOWNLOADER_SKIP_LOCAL_START", "true")
    monkeypatch.setenv("IBKR_CRYPTO_VENUE", "ZEROHASH")
    monkeypatch.setenv(
        "BACKTESTING_DATA_SOURCE",
        '{"default":"thetadata","crypto":"ibkr"}',
    )

    # Isolate IBKR cache writes.
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())

    calls = _wrap_queue_request(monkeypatch)

    now = datetime.now(UTC)
    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    ibkr_helper.get_price_data(
        asset=base,
        quote=quote,
        timestep="minute",
        start_dt=now - timedelta(minutes=240),
        end_dt=now,
        exchange=None,
        include_after_hours=True,
    )

    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert parquet_files, "Expected IBKR parquet cache to be written"
    cached = pd.read_parquet(parquet_files[0])
    assert isinstance(cached.index, pd.DatetimeIndex)
    assert not cached.empty

    window_end = cached.index.max()
    assert isinstance(window_end, pd.Timestamp)
    window_start = window_end - pd.Timedelta(minutes=60)

    results, strategy = _CryptoBuyHold.run_backtest(
        datasource_class=None,
        backtesting_start=window_start.to_pydatetime(),
        backtesting_end=window_end.to_pydatetime(),
        market="24/7",
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
        name="ROUTER_IBKR_CRYPTO_SMOKE",
        budget=10_000.0,
        quote_asset=quote,
        benchmark_asset=base,
    )

    # Confirm the env override actually selected the router.
    assert strategy is not None
    assert strategy.broker is not None
    assert strategy.broker.data_source.__class__.__name__ == "RoutedBacktestingPandas"

    assert any("/ibkr/" in url for url in calls), f"Expected IBKR queue paths, got {calls[:5]}"
    assert any("/ibkr/iserver/marketdata/history" in url for url in calls), "Expected IBKR history endpoint calls"


def test_router_stock_thetadata_uses_theta_queue_paths(monkeypatch, tmp_path):
    _require_downloader()

    monkeypatch.setenv("MARKET", "NYSE")
    monkeypatch.setenv("DATADOWNLOADER_SKIP_LOCAL_START", "true")
    monkeypatch.setenv(
        "BACKTESTING_DATA_SOURCE",
        '{"default":"thetadata","stock":"thetadata"}',
    )

    calls = _wrap_queue_request(monkeypatch)

    # Use a fixed, known weekday during RTH to avoid weekend/holiday traps.
    window_start = datetime(2025, 1, 2, 14, 35, tzinfo=UTC)  # 09:35 ET
    window_end = datetime(2025, 1, 2, 14, 50, tzinfo=UTC)

    results, strategy = _StockFetchOnly.run_backtest(
        datasource_class=None,
        backtesting_start=window_start,
        backtesting_end=window_end,
        market="NYSE",
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
        name="ROUTER_THETA_STOCK_SMOKE",
        budget=10_000.0,
        benchmark_asset=Asset("SPY", asset_type=Asset.AssetType.STOCK),
    )

    assert strategy is not None
    assert strategy.broker is not None
    assert strategy.broker.data_source.__class__.__name__ == "RoutedBacktestingPandas"

    # Theta requests should go through non-IBKR paths.
    assert any("/ibkr/" not in url for url in calls), "Expected at least one non-IBKR queue request"
    assert any(("/v3/" in url) or ("/v2/" in url) for url in calls), f"Expected Theta API endpoints, got {calls[:5]}"


def test_router_crypto_polygon_calls_polygon_helper(monkeypatch, tmp_path):
    _require_downloader()

    polygon_key = (os.environ.get("POLYGON_API_KEY") or POLYGON_API_KEY or "").strip()
    if not polygon_key:
        pytest.skip("Missing POLYGON_API_KEY for polygon router apitest")

    import lumibot.tools.polygon_helper as polygon_helper

    monkeypatch.setenv("MARKET", "24/7")
    monkeypatch.setenv("DATADOWNLOADER_SKIP_LOCAL_START", "true")
    monkeypatch.setenv(
        "BACKTESTING_DATA_SOURCE",
        '{"default":"thetadata","crypto":"polygon"}',
    )

    calls: list[tuple[str, str]] = []
    original: Callable[..., Any] = polygon_helper.get_price_data_from_polygon

    def wrapped(*args, **kwargs):
        asset = kwargs.get("asset")
        quote = kwargs.get("quote_asset")
        calls.append((getattr(asset, "symbol", ""), getattr(quote, "symbol", "")))
        return original(*args, **kwargs)

    monkeypatch.setattr(polygon_helper, "get_price_data_from_polygon", wrapped)

    # Short window to keep it fast and avoid large downloads.
    now = datetime.now(UTC)
    window_start = now - timedelta(hours=2)
    window_end = now - timedelta(hours=1)

    results, strategy = _CryptoBuyHold.run_backtest(
        datasource_class=None,
        backtesting_start=window_start,
        backtesting_end=window_end,
        market="24/7",
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
        name="ROUTER_POLYGON_CRYPTO_SMOKE",
        budget=10_000.0,
        quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
        benchmark_asset=Asset("BTC", asset_type=Asset.AssetType.CRYPTO),
        polygon_api_key=polygon_key,
    )

    assert strategy is not None
    assert strategy.broker is not None
    assert strategy.broker.data_source.__class__.__name__ == "RoutedBacktestingPandas"

    assert calls, "Expected router->polygon to call polygon_helper.get_price_data_from_polygon()"
