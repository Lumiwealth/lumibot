from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytest
import requests

from lumibot.backtesting import BacktestingBroker
from lumibot.backtesting.interactive_brokers_rest_backtesting import InteractiveBrokersRESTBacktesting
from lumibot.entities import Asset
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy

pytestmark = pytest.mark.apitest


class _DummyIbkrCryptoStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"
        self.include_cash_positions = True

    def on_trading_iteration(self):
        return


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
        pytest.skip(f"Local downloader not reachable/healthy: {exc}")

    ibkr = payload.get("ibkr") if isinstance(payload, dict) else None
    if not isinstance(ibkr, dict) or not ibkr.get("enabled"):
        pytest.skip("Local downloader is running but IBKR is not enabled")
    if ibkr.get("authenticated") is not True:
        pytest.skip("Local downloader is running but IBKR is not authenticated")

    return base_url


@pytest.mark.parametrize("symbol", ["BTC", "ETH"])
def test_ibkr_crypto_backtest_smoke_local_fills_market_order(monkeypatch, tmp_path, symbol: str):
    _require_ibkr_downloader()

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setenv("IBKR_CRYPTO_VENUE", "ZEROHASH")

    base = Asset(symbol, asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Seed the cache with "latest available" bars (may not overlap now on weekends).
    now = datetime.now(UTC)
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
    assert parquet_files, "Expected IBKR parquet cache to be written"
    cached = pd.read_parquet(parquet_files[0])
    assert not cached.empty, "Expected cached bars to contain data"

    # Pick a window that is guaranteed to overlap cached bars.
    window_end: pd.Timestamp | None = cached.index.max() if isinstance(cached.index, pd.DatetimeIndex) else None
    assert window_end is not None
    window_start = window_end - pd.Timedelta(minutes=30)

    df = ibkr_helper.get_price_data(
        asset=base,
        quote=quote,
        timestep="minute",
        start_dt=window_start.to_pydatetime(),
        end_dt=window_end.to_pydatetime(),
        exchange=None,
        include_after_hours=True,
    )
    assert not df.empty
    assert "open" in df.columns
    assert "bid" in df.columns
    assert "ask" in df.columns
    # Quotes should have at least some non-zero spread so market buys hit ask and sells hit bid.
    spread = (pd.to_numeric(df["ask"], errors="coerce") - pd.to_numeric(df["bid"], errors="coerce")).dropna()
    assert (spread >= 0).all()
    assert (spread > 0).any(), "Expected some non-zero bid/ask spread for IBKR crypto"

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=window_start.to_pydatetime(),
        datetime_end=(window_end + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrCryptoStrategy(
        broker=broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    # Ensure the data source has bars loaded for the asset before placing orders.
    data_source.get_historical_prices_between_dates(
        (base, quote),
        timestep="minute",
        quote=quote,
        start_date=window_start.to_pydatetime(),
        end_date=window_end.to_pydatetime(),
    )

    order = strategy.create_order(
        base,
        Decimal("0.01"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        quote=quote,
    )

    # Capture the quote at submit time so we can assert market orders fill at ask.
    quote_snapshot = broker.get_quote(base, quote=quote)
    expected_ask = float(quote_snapshot.ask)

    strategy.submit_order(order)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    # Advance one minute to allow next-bar market fill if needed.
    broker._update_datetime(broker.datetime + timedelta(minutes=1))
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()

    assert strategy.cash < 100_000.0
    assert order.is_filled()
    assert order.get_fill_price() == pytest.approx(expected_ask, rel=1e-6)


def test_ibkr_crypto_backtest_smoke_local_fills_marketable_limit_orders(monkeypatch, tmp_path):
    _require_ibkr_downloader()

    import lumibot.tools.ibkr_helper as ibkr_helper

    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setenv("IBKR_CRYPTO_VENUE", "ZEROHASH")

    base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    now = datetime.now(UTC)
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
    assert parquet_files, "Expected IBKR parquet cache to be written"
    cached = pd.read_parquet(parquet_files[0])
    assert not cached.empty, "Expected cached bars to contain data"

    window_end: pd.Timestamp | None = cached.index.max() if isinstance(cached.index, pd.DatetimeIndex) else None
    assert window_end is not None
    window_start = window_end - pd.Timedelta(minutes=30)

    data_source = InteractiveBrokersRESTBacktesting(
        datetime_start=window_start.to_pydatetime(),
        datetime_end=(window_end + pd.Timedelta(minutes=1)).to_pydatetime(),
        market="24/7",
        show_progress_bar=False,
        log_backtest_progress_to_file=False,
    )
    data_source.load_data()

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())
    broker._first_iteration = False

    strategy = _DummyIbkrCryptoStrategy(
        broker=broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={},
    )
    strategy._first_iteration = False

    data_source.get_historical_prices_between_dates(
        (base, quote),
        timestep="minute",
        quote=quote,
        start_date=window_start.to_pydatetime(),
        end_date=window_end.to_pydatetime(),
    )

    # Marketable BUY limit should fill at ask.
    quote0 = broker.get_quote(base, quote=quote)
    expected_ask = float(quote0.ask)
    buy = strategy.create_order(
        base,
        Decimal("0.01"),
        Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=Decimal("999999999"),
        quote=quote,
    )
    strategy.submit_order(buy)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert buy.is_filled()
    assert buy.get_fill_price() == pytest.approx(expected_ask, rel=1e-6)

    # Advance one minute then marketable SELL limit should fill at bid.
    broker._update_datetime(broker.datetime + timedelta(minutes=1))
    quote1 = broker.get_quote(base, quote=quote)
    expected_bid = float(quote1.bid)
    sell = strategy.create_order(
        base,
        Decimal("0.01"),
        Order.OrderSide.SELL,
        order_type=Order.OrderType.LIMIT,
        limit_price=Decimal("0"),
        quote=quote,
    )
    strategy.submit_order(sell)
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    assert sell.is_filled()
    assert sell.get_fill_price() == pytest.approx(expected_bid, rel=1e-6)
