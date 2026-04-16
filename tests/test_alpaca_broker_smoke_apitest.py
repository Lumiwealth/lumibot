import time

import pytest
from alpaca.common.exceptions import APIError

from lumibot.brokers.alpaca import Alpaca
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.entities import Asset, CashEvent, Order


pytestmark = pytest.mark.apitest


def _alpaca() -> Alpaca:
    if not ALPACA_TEST_CONFIG.get("API_KEY") or not ALPACA_TEST_CONFIG.get("API_SECRET"):
        pytest.skip("Missing ALPACA_TEST_API_KEY / ALPACA_TEST_API_SECRET in .env")

    # Smoke tests don't need a stream thread.
    return Alpaca(ALPACA_TEST_CONFIG, max_workers=1, connect_stream=False)


def test_alpaca_smoke_balances_positions_orders():
    broker = _alpaca()
    try:
        cash, positions_value, portfolio_value = broker._get_balances_at_broker(Asset("USD", asset_type=Asset.AssetType.FOREX), None)
        assert isinstance(cash, float)
        assert isinstance(positions_value, float)
        assert isinstance(portfolio_value, float)
        assert cash >= 0
        assert portfolio_value >= 0

        positions = broker._pull_broker_positions(strategy="apitest")
        assert positions is not None

        orders = broker._pull_broker_all_orders()
        assert orders is not None
    finally:
        broker.cleanup_streams()


def test_alpaca_smoke_place_and_cancel_day_limit_order():
    broker = _alpaca()
    try:
        # Ultra-conservative limit buy so the order should never fill, even if markets are open.
        order = Order(
            "apitest",
            Asset("AAPL", asset_type=Asset.AssetType.STOCK),
            quantity=1,
            side=Order.OrderSide.BUY,
            limit_price=0.01,
            time_in_force="day",
            order_type=Order.OrderType.LIMIT,
        )

        submitted = broker._submit_order(order)
        assert submitted is not None
        assert submitted.identifier, "Order submission did not return an order id"

        broker.cancel_order(submitted)

        status = None
        for _ in range(40):
            record = broker._pull_broker_order(submitted.identifier)
            status = str(getattr(record, "status", "")).lower()
            if status in {"canceled", "cancelled"} or status.endswith(".canceled") or status.endswith(".cancelled"):
                break
            time.sleep(0.25)

        assert status in {"canceled", "cancelled"} or status.endswith(".canceled") or status.endswith(".cancelled")
    finally:
        broker.cleanup_streams()


def test_alpaca_smoke_cash_events_read_path():
    broker = _alpaca()
    try:
        try:
            events = broker.get_cash_events(limit=10)
        except APIError as exc:
            if "unauthorized" in str(exc).lower():
                pytest.skip("Alpaca paper credentials are not authorized for account activities in this environment.")
            raise
        assert isinstance(events, list)
        assert all(isinstance(event, CashEvent) for event in events)
    finally:
        broker.cleanup_streams()
