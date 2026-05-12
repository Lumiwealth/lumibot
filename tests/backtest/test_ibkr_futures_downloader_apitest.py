from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytest
import requests

from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy

pytestmark = pytest.mark.apitest


def _require_ibkr_downloader() -> str:
    base_url = (os.environ.get("DATADOWNLOADER_BASE_URL") or "").strip().rstrip("/")
    api_key = (os.environ.get("DATADOWNLOADER_API_KEY") or "").strip()

    if not base_url or not api_key:
        pytest.skip("Missing DATADOWNLOADER_BASE_URL / DATADOWNLOADER_API_KEY for IBKR apitest")

    try:
        resp = requests.get(
            f"{base_url}/healthz",
            headers={"X-Downloader-Key": api_key},
            timeout=5,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        pytest.skip(f"Downloader not reachable/healthy: {exc}")

    ibkr = payload.get("ibkr") if isinstance(payload, dict) else None
    if not isinstance(ibkr, dict) or not ibkr.get("enabled"):
        pytest.skip("Downloader is running but IBKR is not enabled")
    if ibkr.get("authenticated") is not True:
        pytest.skip("Downloader is running but IBKR is not authenticated")

    return base_url


def test_ibkr_futures_can_fetch_minute_bars_via_downloader(monkeypatch, tmp_path):
    _require_ibkr_downloader()

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setenv("IBKR_FUTURES_EXCHANGE", "CME")

    # Use auto-expiry so this remains valid over time (apitest is allowed to be non-deterministic).
    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, auto_expiry=Asset.AutoExpiry.FRONT_MONTH)

    now = datetime.now(UTC)
    df = ibkr_helper.get_price_data(
        asset=fut,
        quote=None,
        timestep="minute",
        start_dt=now - timedelta(minutes=30),
        end_dt=now,
        exchange=None,
        include_after_hours=True,
    )

    assert df is not None and not df.empty
    assert {"open", "high", "low", "close"}.issubset(set(df.columns))

    # Ensure it is cached locally (and therefore eligible for S3 mirroring).
    parquet_files = list(tmp_path.rglob("*.parquet"))
    assert parquet_files
    cached = pd.read_parquet(parquet_files[0])
    assert not cached.empty


class _IbkrFuturesWarmRoundTrip(Strategy):
    def initialize(self, parameters=None):
        self.set_market("us_futures")
        self.sleeptime = "1M"
        self.include_cash_positions = True
        self._entered_at = None
        self.future = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=datetime(2025, 12, 19).date())

    def on_trading_iteration(self):
        bars = self.get_historical_prices(self.future, length=2, timestep="minute")
        if bars is None or getattr(bars, "df", None) is None or bars.df.empty:
            return
        last_price = float(bars.df["close"].iloc[-1])
        if not last_price or last_price <= 0:
            return

        now = self.get_datetime()
        if self._entered_at is None:
            self.submit_order(
                self.create_order(
                    self.future,
                    Decimal("1"),
                    Order.OrderSide.BUY,
                    order_type=Order.OrderType.MARKET,
                )
            )
            self._entered_at = now
            return

        if now >= (self._entered_at + timedelta(minutes=30)):
            pos = self.get_position(self.future)
            qty = Decimal(str(getattr(pos, "quantity", 0) or 0)) if pos is not None else Decimal("0")
            if qty <= 0:
                return
            self.submit_order(
                self.create_order(
                    self.future,
                    Decimal(str(abs(qty))),
                    Order.OrderSide.SELL,
                    order_type=Order.OrderType.MARKET,
                )
            )


def test_ibkr_futures_warm_backtest_does_not_touch_downloader(monkeypatch, tmp_path):
    _require_ibkr_downloader()

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setenv("IBKR_FUTURES_EXCHANGE", "CME")
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())

    log_dir = tmp_path / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Fixed, deterministic window so this test is stable across weekdays/weekends.
    window_start = datetime(2025, 12, 8, 15, 0, tzinfo=UTC)
    window_end = datetime(2025, 12, 8, 16, 0, tzinfo=UTC)

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=datetime(2025, 12, 19).date())
    warmed = ibkr_helper.get_price_data(
        asset=fut,
        quote=None,
        timestep="minute",
        start_dt=window_start - timedelta(minutes=5),
        end_dt=window_end,
        exchange=None,
        include_after_hours=True,
    )
    assert warmed is not None and not warmed.empty

    def _no_downloader(*args, **kwargs):
        raise AssertionError("Warm IBKR futures backtest unexpectedly attempted to call the downloader")

    monkeypatch.setattr(ibkr_helper, "queue_request", _no_downloader)

    logfile = (log_dir / "ibkr_futures_backtest_warm.log").as_posix()
    _IbkrFuturesWarmRoundTrip.run_backtest(
        datasource_class=InteractiveBrokersRESTBacktesting,
        backtesting_start=window_start,
        backtesting_end=window_end,
        market="us_futures",
        analyze_backtest=False,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=False,
        name="IBKR_FUTURES_WARM_ROUNDTRIP",
        budget=100_000.0,
        logfile=logfile,
    )

    assert list(log_dir.glob("*.log")), f"No logs produced in {log_dir}"


def test_ibkr_futures_contract_info_includes_trading_hours(monkeypatch, tmp_path):
    """Truth probe (read-only): fetch IBKR contract info and assert trading hours metadata exists.

    This does not place any orders. It is a safe probe that helps keep our session-gap modeling
    grounded in what the broker reports for the instrument.
    """
    _require_ibkr_downloader()

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setenv("IBKR_FUTURES_EXCHANGE", "CME")

    fut = Asset("MES", asset_type=Asset.AssetType.FUTURE, auto_expiry=Asset.AutoExpiry.FRONT_MONTH)
    try:
        conid = ibkr_helper._resolve_conid(asset=fut, quote=None, exchange="CME")
    except Exception as exc:
        pytest.skip(f"Unable to resolve IBKR conid for MES front month: {exc}")

    info = ibkr_helper._fetch_contract_info(int(conid))
    assert isinstance(info, dict) and info, "Empty IBKR contract info payload"
    # IBKR contract info payloads typically include a 'tradingHours' field, but field naming can
    # vary across instruments/gateways. Accept either direct or nested representations.
    has_hours = (
        "tradingHours" in info
        or "trading_hours" in info
        or any(isinstance(v, dict) and ("tradingHours" in v or "trading_hours" in v) for v in info.values())
    )
    assert has_hours, f"IBKR contract info missing trading hours fields (keys={sorted(info.keys())[:20]})"
