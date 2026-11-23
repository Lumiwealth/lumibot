"""Schwab short sales that never chose a time in force go out as DAY.

Schwab refuses good-till-canceled short sales on hard-to-borrow stocks with
"Good until canceled short sale orders are not accepted for hard to borrow
securities", while its own trading screens default to DAY. ``create_order``
defaults to GTC, so a strategy that simply shorts a stock had every
hard-to-borrow short rejected. These tests submit through the real Schwab order
serializer (no network) and read the order spec Schwab would receive.
"""

import importlib.util
from pathlib import Path

import pytest

from lumibot.entities import Asset, Order
from tests.backtest.test_backtesting_broker_processing import setup_strategy_with_prices

SPEC = importlib.util.spec_from_file_location(
    "schwab_lifecycle_transport_day_short",
    Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py",
)
TRANSPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRANSPORT)


@pytest.fixture(autouse=True)
def no_external_requests(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("SCHWAB_CANCEL_DIAGNOSTICS", "false")

    def reject(*_args, **_kwargs):
        raise AssertionError("External network is forbidden in this test")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


@pytest.fixture
def strategy():
    flat = (16.8, 16.9, 16.7, 16.8)
    strat, _broker, _ = setup_strategy_with_prices(
        Asset("LW"), Asset("USD", Asset.AssetType.FOREX), [flat] * 4
    )
    return strat


def _sent_duration(order):
    transport = TRANSPORT.SchwabLifecycleTransport()
    broker, _queues = TRANSPORT.data_only_schwab_broker(transport)
    assert broker.submit_order(order) is order
    TRANSPORT.drain_stream_events(broker)
    return transport.attempted_orders[-1]["order_spec"]["duration"]


@pytest.mark.parametrize("order_type,limit", [("market", None), ("limit", 16.81)])
def test_default_stock_short_sale_goes_out_day(strategy, order_type, limit):
    order = strategy.create_order(
        Asset("LW"), 100, Order.OrderSide.SELL_SHORT, limit_price=limit
    )
    assert order.time_in_force == "gtc"  # the Strategy API default is unchanged
    assert order.time_in_force_defaulted is True
    assert _sent_duration(order) == "DAY"


def test_explicit_gtc_short_sale_stays_gtc(strategy):
    order = strategy.create_order(
        Asset("LW"), 100, Order.OrderSide.SELL_SHORT, time_in_force="gtc"
    )
    assert order.time_in_force_defaulted is False
    assert _sent_duration(order) == "GOOD_TILL_CANCEL"


def test_default_long_sell_and_buy_keep_gtc(strategy):
    for side in (Order.OrderSide.SELL, Order.OrderSide.BUY, Order.OrderSide.BUY_TO_COVER):
        order = strategy.create_order(Asset("LW"), 100, side, limit_price=10.0)
        assert _sent_duration(order) == "GOOD_TILL_CANCEL", side


def test_default_option_sell_to_open_is_unchanged(strategy):
    from datetime import date

    option = Asset("LW", Asset.AssetType.OPTION, expiration=date(2026, 12, 18), strike=20, right="CALL")
    order = strategy.create_order(option, 1, Order.OrderSide.SELL_TO_OPEN, limit_price=1.0)
    assert _sent_duration(order) == "GOOD_TILL_CANCEL"


def test_order_built_directly_keeps_its_own_day_default():
    order = Order(
        strategy="fixture",
        asset=Asset("LW"),
        quantity=100,
        side=Order.OrderSide.SELL_SHORT,
        order_type=Order.OrderType.MARKET,
        time_in_force="gtc",
    )
    # Not created through create_order: no "defaulted" flag, so the caller's GTC stands.
    assert _sent_duration(order) == "GOOD_TILL_CANCEL"
