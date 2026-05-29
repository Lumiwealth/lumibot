import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order


pytestmark = pytest.mark.apitest


def _schwab_broker(monkeypatch):
    token_path = Path("schwab_token.json").resolve()
    if not token_path.exists():
        pytest.skip("Missing local schwab_token.json token file.")

    account_number = (os.environ.get("SCHWAB_ACCOUNT_NUMBER") or "").strip()
    if not account_number:
        pytest.skip(
            "Missing existing SCHWAB_ACCOUNT_NUMBER credential. "
            "This live apitest may place and cancel a real Schwab order."
        )

    monkeypatch.setattr(Schwab, "_launch_stream", lambda self: None)
    return Schwab(
        config={
            "SCHWAB_TOKEN_PATH": str(token_path),
            "SCHWAB_ACCOUNT_NUMBER": account_number,
            "MARKET": "NASDAQ",
        }
    )


def _pull_all_for_order(broker, order_id, strategy_name):
    raw_orders = broker._pull_broker_all_orders()
    raw = next((row for row in raw_orders if str(row.get("orderId")) == str(order_id)), None)
    parsed = broker._parse_broker_order(raw, strategy_name) if raw else None
    return raw, parsed


def _poll_all_for_order(broker, order_id, strategy_name, *, timeout_seconds=4.0):
    deadline = time.perf_counter() + timeout_seconds
    while True:
        raw, parsed = _pull_all_for_order(broker, order_id, strategy_name)
        if raw or time.perf_counter() >= deadline:
            return raw, parsed
        time.sleep(0.25)


def test_schwab_live_submit_read_cancel_refresh(monkeypatch):
    """Places one TSLL limit order, verifies fresh reads, and cancels it.

    This is intentionally marked apitest because it uses the real Schwab API.
    It skips during market hours to avoid an immediate fill from the at-price
    limit order.
    """

    broker = _schwab_broker(monkeypatch)
    if broker.is_market_open():
        pytest.skip("Market is open; not placing an at-price TSLL limit order.")

    strategy_name = "schwab-live-refresh-apitest"
    asset = Asset("TSLL", asset_type=Asset.AssetType.STOCK)
    quote = broker.data_source.get_quote(asset)
    last_price = float(getattr(quote, "price", None) or broker.data_source.get_last_price(asset))
    assert last_price > 0

    order = Order(
        strategy=strategy_name,
        asset=asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=round(last_price, 2),
        time_in_force="day",
        tag=strategy_name,
    )

    submitted = None
    try:
        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier

        raw_single = broker._pull_broker_order(submitted.identifier)
        parsed_single = broker._parse_broker_order(raw_single, strategy_name)
        assert raw_single is not None
        assert parsed_single is not None
        assert parsed_single.identifier == submitted.identifier

        raw_all, parsed_all = _poll_all_for_order(broker, submitted.identifier, strategy_name)
        assert raw_all is not None
        assert parsed_all is not None
        assert parsed_all.identifier == submitted.identifier

        broker.cancel_order(submitted)

        post_cancel_single = broker._pull_broker_order(submitted.identifier)
        post_cancel_parsed = broker._parse_broker_order(post_cancel_single, strategy_name)
        assert post_cancel_single["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_parsed.is_canceled()

        post_cancel_all, post_cancel_all_parsed = _poll_all_for_order(
            broker, submitted.identifier, strategy_name
        )
        assert post_cancel_all["status"] in {"CANCELED", "CANCELLED"}
        assert post_cancel_all_parsed.is_canceled()
    finally:
        if submitted and submitted.identifier:
            try:
                raw = broker._pull_broker_order(submitted.identifier)
                status = str(raw.get("status", "") if raw else "").upper()
                if status not in {"CANCELED", "CANCELLED", "FILLED", "REJECTED", "EXPIRED"}:
                    broker.cancel_order(submitted)
            except Exception:
                pass
