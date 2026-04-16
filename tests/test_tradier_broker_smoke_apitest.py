import time

import pytest

from lumibot.brokers.tradier import Tradier
from lumibot.credentials import TRADIER_CONFIG, TRADIER_TEST_CONFIG
from lumibot.entities import Asset, CashEvent, Order


pytestmark = pytest.mark.apitest


def _tradier() -> Tradier:
    if not TRADIER_TEST_CONFIG.get("ACCOUNT_NUMBER") or not TRADIER_TEST_CONFIG.get("ACCESS_TOKEN"):
        pytest.skip("Missing TRADIER_TEST_ACCOUNT_NUMBER / TRADIER_TEST_ACCESS_TOKEN in .env")

    # Smoke tests don't need a stream thread.
    return Tradier(
        account_number=TRADIER_TEST_CONFIG["ACCOUNT_NUMBER"],
        access_token=TRADIER_TEST_CONFIG["ACCESS_TOKEN"],
        paper=True,
        connect_stream=False,
    )

def _tradier_live_or_skip() -> Tradier:
    if not TRADIER_CONFIG.get("ACCOUNT_NUMBER") or not TRADIER_CONFIG.get("ACCESS_TOKEN"):
        pytest.skip("Missing TRADIER_ACCOUNT_NUMBER / TRADIER_ACCESS_TOKEN in .env")
    if TRADIER_CONFIG.get("PAPER", True):
        pytest.skip("Tradier order-lifecycle smoke requires TRADIER_IS_PAPER=false (live API).")

    # Stream is not needed for a submit/cancel/poll lifecycle check.
    return Tradier(
        account_number=TRADIER_CONFIG["ACCOUNT_NUMBER"],
        access_token=TRADIER_CONFIG["ACCESS_TOKEN"],
        paper=False,
        connect_stream=False,
    )


def test_tradier_smoke_balances_positions_orders():
    broker = _tradier()
    try:
        cash, positions_value, portfolio_value = broker._get_balances_at_broker(Asset("USD", asset_type=Asset.AssetType.FOREX), None)
        assert isinstance(cash, float)
        assert isinstance(positions_value, float)
        assert isinstance(portfolio_value, float)
        assert cash >= 0
        assert portfolio_value >= 0

        positions = broker._pull_positions("apitest")
        assert positions is not None

        orders = broker._pull_broker_all_orders()
        assert isinstance(orders, list)
    finally:
        broker.cleanup_streams()


def test_tradier_smoke_place_and_cancel_day_limit_order():
    broker = _tradier_live_or_skip()
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
            record = broker._pull_broker_order(submitted.identifier) or {}
            status = str(record.get("status", "")).lower()
            if status in {"canceled", "cancelled"}:
                break
            time.sleep(0.25)

        assert status in {"canceled", "cancelled"}
    finally:
        broker.cleanup_streams()


def test_tradier_smoke_cash_events_read_path():
    broker = _tradier_live_or_skip()
    try:
        events = broker.get_cash_events(limit=10)
        assert isinstance(events, list)
        assert all(isinstance(event, CashEvent) for event in events)
    finally:
        broker.cleanup_streams()
