from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

os.environ.setdefault("LUMIBOT_DISABLE_DOTENV", "1")

import pytest
import requests

import lumibot
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy


pytestmark = [pytest.mark.apitest, pytest.mark.downloader, pytest.mark.ibkr]

REPO_ROOT = Path(__file__).resolve().parents[2]
LUMIBOT_PACKAGE_ROOT = (REPO_ROOT / "lumibot").resolve()


class _IbkrMultiTimestepSmoke(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True
        self.fetch_counts: dict[str, int] = {}
        self.fetch_last_close: dict[str, float] = {}
        self.fetch_completed = False
        self._entered = False

    def _fetch_bars(self, asset: Asset, timestep: str, quote: Asset | None):
        kwargs = {
            "length": 2,
            "timestep": timestep,
        }
        if quote is not None:
            kwargs["quote"] = quote
        bars = self.get_historical_prices(asset, **kwargs)
        if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
            return None
        return bars.df

    def on_trading_iteration(self):
        asset: Asset = self.parameters["asset"]
        quote: Asset | None = self.parameters.get("quote")
        place_order = bool(self.parameters.get("place_order"))

        for timestep in ("minute", "hour", "day"):
            df = self._fetch_bars(asset, timestep, quote)
            if df is None:
                return
            self.fetch_counts[timestep] = len(df)
            self.fetch_last_close[timestep] = float(df["close"].iloc[-1])

        self.fetch_completed = True

        if not place_order or self._entered:
            return

        if asset.asset_type == Asset.AssetType.CRYPTO:
            last_price = self.fetch_last_close["minute"]
            qty = Decimal(str((self.cash * 0.25) / last_price)).quantize(Decimal("0.0001"))
            if qty <= 0:
                return
            order = self.create_order(
                asset,
                qty,
                Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
                quote=quote,
            )
        else:
            order = self.create_order(
                asset,
                1,
                Order.OrderSide.BUY,
                order_type=Order.OrderType.MARKET,
            )

        self.submit_order(order)
        self._entered = True


def _assert_repo_lumibot_import_path():
    resolved = Path(lumibot.__file__).resolve()
    assert resolved.is_relative_to(LUMIBOT_PACKAGE_ROOT), (
        f"Expected repo LumiBot import under {LUMIBOT_PACKAGE_ROOT}, got {resolved}"
    )


def _require_prod_ibkr_downloader() -> tuple[str, str, str]:
    base_url = (os.environ.get("DATADOWNLOADER_BASE_URL") or "").strip().rstrip("/")
    api_key = (os.environ.get("DATADOWNLOADER_API_KEY") or "").strip()
    api_key_header = (os.environ.get("DATADOWNLOADER_API_KEY_HEADER") or "X-Downloader-Key").strip()

    if not base_url or not api_key:
        pytest.skip("Missing DATADOWNLOADER_BASE_URL / DATADOWNLOADER_API_KEY")

    if any(host in base_url for host in ("localhost", "127.0.0.1", "0.0.0.0")):
        pytest.skip(f"Refusing local downloader base URL for this smoke: {base_url}")

    try:
        resp = requests.get(
            f"{base_url}/healthz",
            headers={api_key_header: api_key},
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        pytest.skip(f"Production downloader not reachable/healthy: {exc}")

    ibkr = payload.get("ibkr") if isinstance(payload, dict) else None
    if not isinstance(ibkr, dict) or not ibkr.get("enabled"):
        pytest.skip("Production downloader reachable but IBKR is not enabled")
    if ibkr.get("authenticated") is not True:
        pytest.skip("Production downloader reachable but IBKR is not authenticated")

    return base_url, api_key, api_key_header


def _run_smoke(
    monkeypatch,
    tmp_path: Path,
    *,
    asset: Asset,
    market: str,
    start: datetime,
    end: datetime,
    quote: Asset | None = None,
    place_order: bool = False,
):
    import lumibot.tools.ibkr_helper as ibkr_helper

    _assert_repo_lumibot_import_path()
    _require_prod_ibkr_downloader()

    monkeypatch.setenv("DATADOWNLOADER_SKIP_LOCAL_START", "true")
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.setenv("MARKET", market)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    if asset.asset_type == Asset.AssetType.CRYPTO:
        monkeypatch.setenv("IBKR_CRYPTO_VENUE", "ZEROHASH")

    run_kwargs = {
        "datasource_class": InteractiveBrokersRESTBacktesting,
        "backtesting_start": start,
        "backtesting_end": end,
        "market": market,
        "analyze_backtest": False,
        "show_plot": False,
        "show_tearsheet": False,
        "show_indicators": False,
        "quiet_logs": True,
        "name": f"IBKR_{asset.symbol}_LOCAL_REPO_SMOKE",
        "budget": 100_000.0,
        "benchmark_asset": asset,
        "parameters": {
            "asset": asset,
            "quote": quote,
            "place_order": place_order,
        },
    }
    if quote is not None:
        run_kwargs["quote_asset"] = quote

    results, strategy = _IbkrMultiTimestepSmoke.run_backtest(**run_kwargs)

    assert strategy is not None
    assert strategy.broker is not None
    assert strategy.broker.data_source.__class__.__name__ == "InteractiveBrokersRESTBacktesting"
    assert strategy.fetch_completed, f"{asset.symbol} smoke never completed minute/hour/day fetches"
    assert strategy.fetch_counts["minute"] >= 1
    assert strategy.fetch_counts["hour"] >= 1
    assert strategy.fetch_counts["day"] >= 1
    assert strategy.fetch_last_close["minute"] > 0
    assert strategy.fetch_last_close["hour"] > 0
    assert strategy.fetch_last_close["day"] > 0

    if place_order:
        assert strategy.cash < 100_000.0, f"{asset.symbol} smoke never filled the buy order"

    return results, strategy


def test_local_repo_lumibot_ibkr_index_smoke(monkeypatch, tmp_path):
    asset = Asset("SPX", asset_type=Asset.AssetType.INDEX)
    start = datetime(2026, 4, 7, 13, 35, tzinfo=timezone.utc)
    end = datetime(2026, 4, 9, 19, 55, tzinfo=timezone.utc)
    _run_smoke(
        monkeypatch,
        tmp_path,
        asset=asset,
        market="NYSE",
        start=start,
        end=end,
        place_order=False,
    )


def test_local_repo_lumibot_ibkr_stock_smoke(monkeypatch, tmp_path):
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    start = datetime(2026, 4, 7, 13, 35, tzinfo=timezone.utc)
    end = datetime(2026, 4, 9, 19, 55, tzinfo=timezone.utc)
    _run_smoke(
        monkeypatch,
        tmp_path,
        asset=asset,
        market="NYSE",
        start=start,
        end=end,
        place_order=True,
    )


def test_local_repo_lumibot_ibkr_crypto_smoke(monkeypatch, tmp_path):
    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    start = datetime(2026, 4, 7, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 9, 0, 0, tzinfo=timezone.utc)
    _run_smoke(
        monkeypatch,
        tmp_path,
        asset=asset,
        quote=quote,
        market="24/7",
        start=start,
        end=end,
        place_order=True,
    )
