from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pandas as pd
import pytest
import requests

from lumibot.backtesting import PolygonDataBacktesting
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.credentials import POLYGON_API_KEY
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.tools import polygon_helper

pytestmark = pytest.mark.apitest


def _require_ibkr_downloader() -> tuple[str, str, str]:
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

    return base_url, api_key, api_key_header


@pytest.mark.parametrize("symbol", ["BTC", "ETH"])
def test_ibkr_crypto_minute_close_series_matches_polygon_reasonably(monkeypatch, tmp_path, symbol: str):
    _require_ibkr_downloader()

    polygon_key = (os.environ.get("POLYGON_API_KEY") or POLYGON_API_KEY or "").strip()
    if not polygon_key:
        pytest.skip("Missing POLYGON_API_KEY for parity apitest")

    import lumibot.tools.ibkr_helper as ibkr_helper

    # Isolate caches
    ibkr_helper.LUMIBOT_CACHE_FOLDER = tmp_path.as_posix()  # type: ignore[attr-defined]
    monkeypatch.setattr(polygon_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setenv("IBKR_CRYPTO_VENUE", "ZEROHASH")

    base = Asset(symbol, asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    now = datetime.now(UTC)
    # Seed cache with latest available bars.
    ibkr_helper.get_price_data(
        asset=base,
        quote=quote,
        timestep="minute",
        start_dt=now - timedelta(minutes=120),
        end_dt=now,
        exchange=None,
        include_after_hours=True,
    )

    parquet_files = list(tmp_path.rglob("*.parquet"))
    bars = [p for p in parquet_files if "ibkr" in p.parts and "bars" in p.parts]
    if not bars:
        pytest.skip("Unable to seed IBKR bars cache; cannot run parity check")
    df_cached = pd.read_parquet(bars[0])
    if not isinstance(df_cached.index, pd.DatetimeIndex) or df_cached.empty:
        pytest.skip("IBKR cache parquet invalid/empty")

    end_ts = df_cached.index.max()
    if end_ts.tz is None:
        end_ts = end_ts.tz_localize("America/New_York")
    end_dt = end_ts.to_pydatetime()
    start_dt = (end_ts - pd.Timedelta(days=2)).to_pydatetime()

    df_ibkr = ibkr_helper.get_price_data(
        asset=base,
        quote=quote,
        timestep="minute",
        start_dt=start_dt,
        end_dt=end_dt,
        exchange=None,
        include_after_hours=True,
    )
    if df_ibkr is None or df_ibkr.empty:
        pytest.skip("IBKR returned no overlapping bars for derived window")

    df_polygon = polygon_helper.get_price_data_from_polygon(
        api_key=polygon_key,
        asset=base,
        quote_asset=quote,
        start=start_dt,
        end=end_dt,
        timespan="minute",
        force_cache_update=False,
        max_workers=4,
    )
    if df_polygon is None or df_polygon.empty:
        pytest.skip(f"Polygon returned no data for {symbol}/USD minute parity window")

    s_ibkr = df_ibkr["close"].copy()
    s_poly = df_polygon["close"].copy()

    # Normalize indexes to UTC for alignment.
    if isinstance(s_ibkr.index, pd.DatetimeIndex):
        if s_ibkr.index.tz is None:
            s_ibkr.index = s_ibkr.index.tz_localize("America/New_York").tz_convert("UTC")
        else:
            s_ibkr.index = s_ibkr.index.tz_convert("UTC")
    if isinstance(s_poly.index, pd.DatetimeIndex):
        if s_poly.index.tz is None:
            s_poly.index = s_poly.index.tz_localize("UTC")
        else:
            s_poly.index = s_poly.index.tz_convert("UTC")

    joined = pd.concat(
        [s_ibkr.rename("ibkr"), s_poly.rename("polygon")],
        axis=1,
        join="inner",
    ).dropna()

    # Require a reasonable overlap for a 2-day window.
    assert len(joined) >= 500, f"Too little overlap (rows={len(joined)})"

    corr = joined["ibkr"].corr(joined["polygon"])
    assert corr is not None
    assert corr >= 0.98


class _ParityBuyHold(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True
        self._entered = False

    def on_trading_iteration(self):
        if self._entered:
            return
        base = self.parameters["base"]
        quote = self.parameters["quote"]

        bars = self.get_historical_prices(base, length=2, timestep="minute", quote=quote)
        if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
            return
        last_price = float(bars.df["close"].iloc[-1])
        if not last_price or last_price <= 0:
            return

        qty = float(self.cash * 0.95) / last_price
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


@pytest.mark.parametrize("symbol", ["BTC", "ETH"])
def test_ibkr_crypto_buyhold_return_is_close_to_polygon(monkeypatch, tmp_path, symbol: str):
    _require_ibkr_downloader()

    polygon_key = (os.environ.get("POLYGON_API_KEY") or POLYGON_API_KEY or "").strip()
    if not polygon_key:
        pytest.skip("Missing POLYGON_API_KEY for parity apitest")

    import lumibot.tools.ibkr_helper as ibkr_helper

    ibkr_helper.LUMIBOT_CACHE_FOLDER = tmp_path.as_posix()  # type: ignore[attr-defined]
    monkeypatch.setattr(polygon_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setenv("IBKR_CRYPTO_VENUE", "ZEROHASH")
    monkeypatch.setenv("MARKET", "24/7")

    base = Asset(symbol, asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    now = datetime.now(UTC)
    # Seed and derive a window that overlaps known IBKR data.
    ibkr_helper.get_price_data(
        asset=base,
        quote=quote,
        timestep="minute",
        start_dt=now - timedelta(minutes=720),
        end_dt=now,
        exchange=None,
        include_after_hours=True,
    )

    parquet_files = list(tmp_path.rglob("*.parquet"))
    bars = [p for p in parquet_files if "ibkr" in p.parts and "bars" in p.parts]
    if not bars:
        pytest.skip("Unable to seed IBKR bars cache; cannot derive parity window")
    df_cached = pd.read_parquet(bars[0])
    if not isinstance(df_cached.index, pd.DatetimeIndex) or df_cached.empty:
        pytest.skip("IBKR cache parquet invalid/empty")

    end_ts = df_cached.index.max()
    start_ts = df_cached.index.min()
    if end_ts.tz is None:
        end_ts = end_ts.tz_localize("America/New_York")
    if start_ts.tz is None:
        start_ts = start_ts.tz_localize("America/New_York")
    # Keep it short for stability/cost while still enough to trade and compute stats.
    desired_start = end_ts - pd.Timedelta(hours=4)
    window_start = max(desired_start, start_ts).to_pydatetime()
    window_end = end_ts.to_pydatetime()

    ibkr_stats = (tmp_path / f"ibkr_{symbol}_stats.csv").as_posix()
    poly_stats = (tmp_path / f"poly_{symbol}_stats.csv").as_posix()

    _ParityBuyHold.backtest(
        datasource_class=InteractiveBrokersRESTBacktesting,
        backtesting_start=window_start,
        backtesting_end=window_end,
        market="24/7",
        analyze_backtest=True,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        quiet_logs=True,
        name=f"IBKR_{symbol}_BUYHOLD",
        budget=100_000.0,
        quote_asset=quote,
        benchmark_asset=base,
        stats_file=ibkr_stats,
        parameters={"base": base, "quote": quote},
    )

    _ParityBuyHold.backtest(
        datasource_class=PolygonDataBacktesting,
        backtesting_start=window_start,
        backtesting_end=window_end,
        market="24/7",
        analyze_backtest=True,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        quiet_logs=True,
        name=f"POLY_{symbol}_BUYHOLD",
        budget=100_000.0,
        quote_asset=quote,
        benchmark_asset=base,
        stats_file=poly_stats,
        polygon_api_key=polygon_key,
        parameters={"base": base, "quote": quote},
    )

    df_ibkr_stats = pd.read_csv(ibkr_stats)
    df_poly_stats = pd.read_csv(poly_stats)

    for df_stats, path in ((df_ibkr_stats, ibkr_stats), (df_poly_stats, poly_stats)):
        assert "portfolio_value" in df_stats.columns, f"Missing portfolio_value in {path}"
        assert len(df_stats) >= 50, f"Too few rows in {path}"

    ibkr_return = (df_ibkr_stats["portfolio_value"].iloc[-1] / df_ibkr_stats["portfolio_value"].iloc[0]) - 1
    poly_return = (df_poly_stats["portfolio_value"].iloc[-1] / df_poly_stats["portfolio_value"].iloc[0]) - 1

    # Not identical (different feeds + different quote-fill semantics), but should be directionally close.
    assert abs(ibkr_return - poly_return) <= 0.02, (
        f"Return mismatch too large: ibkr={ibkr_return:.4%} poly={poly_return:.4%}"
    )
