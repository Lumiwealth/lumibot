"""Small real-broker gate for changes that can affect live trading startup."""

import time
from math import isfinite
from types import SimpleNamespace

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.brokers.tradier import Tradier
from lumibot.credentials import ALPACA_TEST_CONFIG, TRADIER_TEST_CONFIG
from lumibot.entities import Asset, Order

pytestmark = pytest.mark.apitest


def _required(value, name: str):
    assert value and value != "<your key here>", f"{name} is required for the live broker gate"
    return value


def _assert_account_reads(broker, strategy) -> None:
    cash, positions_value, total_value = broker._get_balances_at_broker(
        Asset("USD", asset_type=Asset.AssetType.FOREX),
        strategy,
    )
    assert all(isfinite(float(value)) for value in (cash, positions_value, total_value))
    assert float(total_value) >= 0
    assert isinstance(broker._pull_positions(strategy), list)
    assert isinstance(broker._pull_broker_all_orders(), list)


def _non_marketable_limit_order(strategy_name: str, price: float) -> Order:
    assert isfinite(float(price)) and float(price) > 0
    return Order(
        strategy=strategy_name,
        asset=Asset("AAPL"),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=max(0.01, round(float(price) * 0.1, 2)),
        time_in_force="day",
    )


def _assert_submit_read_cancel(broker, strategy) -> None:
    price = broker.get_last_price(Asset("AAPL"))
    order = _non_marketable_limit_order(strategy.name, price)
    submitted = broker._submit_order(order)
    assert submitted is not None
    assert submitted.identifier

    try:
        assert broker._pull_broker_order(submitted.identifier) is not None
        all_orders = broker._pull_broker_all_orders()
        assert any(str(row.get("id")) == str(submitted.identifier) for row in all_orders) if (
            all_orders and isinstance(all_orders[0], dict)
        ) else any(str(getattr(row, "id", "")) == str(submitted.identifier) for row in all_orders)
    finally:
        broker.cancel_order(submitted)

    for _ in range(15):
        current = broker._pull_broker_order(submitted.identifier)
        raw_status = current.get("status") if isinstance(current, dict) else getattr(current, "status", None)
        status = str(getattr(raw_status, "value", raw_status)).lower()
        if status in {"cancelled", "canceled"}:
            break
        time.sleep(1)
    else:
        pytest.fail(f"broker did not confirm cancellation; final status={status!r}")


def test_alpaca_paper_account_positions_orders_and_cancel() -> None:
    config = dict(ALPACA_TEST_CONFIG)
    _required(config.get("API_KEY"), "ALPACA_TEST_API_KEY")
    _required(config.get("API_SECRET"), "ALPACA_TEST_API_SECRET")
    assert config.get("PAPER") is True, "Alpaca live-broker gate must use paper trading"

    broker = Alpaca(
        config,
        connect_stream=False,
        start_orders_thread=False,
    )
    strategy = SimpleNamespace(name="ci-alpaca-paper-gate")
    _assert_account_reads(broker, strategy)
    _assert_submit_read_cancel(broker, strategy)


def test_tradier_paper_account_positions_orders_and_cancel() -> None:
    account_number = _required(
        TRADIER_TEST_CONFIG.get("ACCOUNT_NUMBER"),
        "TRADIER_TEST_ACCOUNT_NUMBER",
    )
    access_token = _required(
        TRADIER_TEST_CONFIG.get("ACCESS_TOKEN"),
        "TRADIER_TEST_ACCESS_TOKEN",
    )

    broker = Tradier(
        account_number=account_number,
        access_token=access_token,
        paper=True,
        connect_stream=False,
    )
    strategy = SimpleNamespace(name="ci-tradier-paper-gate")
    _assert_account_reads(broker, strategy)
    _assert_submit_read_cancel(broker, strategy)
