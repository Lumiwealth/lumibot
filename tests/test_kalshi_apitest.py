"""Opt-in real Demo API qualification; never uses production credentials."""

import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.tools.kalshi_client import decimal_value

pytestmark = [pytest.mark.apitest, pytest.mark.kalshi]


class DemoCheck(Strategy):
    def on_trading_iteration(self):
        pass


@pytest.fixture
def demo(request, monkeypatch):
    key_id = os.environ.get("KALSHI_TEST_API_KEY_ID")
    pem = os.environ.get("KALSHI_TEST_PRIVATE_KEY")
    path = os.environ.get("KALSHI_TEST_PRIVATE_KEY_PATH")
    if not key_id or not (pem or path):
        pytest.skip("Configure separate KALSHI_TEST_API_KEY_ID and KALSHI_TEST_PRIVATE_KEY or _PATH")
    if os.environ.get("KALSHI_TEST_IS_DEMO", "").lower() != "true":
        pytest.fail("KALSHI_TEST_IS_DEMO=true is required; production API testing is forbidden")
    mutations = request.node.get_closest_marker("kalshi_demo_mutation") is not None
    fills = request.node.get_closest_marker("kalshi_demo_fill") is not None
    if (mutations or fills) and os.environ.get("KALSHI_TEST_ENABLE_ORDER_MUTATIONS", "").lower() != "true":
        pytest.skip("Demo order mutation tests require KALSHI_TEST_ENABLE_ORDER_MUTATIONS=true")
    if fills and os.environ.get("KALSHI_TEST_ENABLE_FILL", "").lower() != "true":
        pytest.skip("Demo fill tests require KALSHI_TEST_ENABLE_FILL=true")
    # Test the real standard credentials factory, with an explicit complete
    # Demo-only config. Never fall back to the developer's production settings.
    monkeypatch.setenv("LUMIBOT_LAZY_CREDENTIALS", "true")
    monkeypatch.setenv("LUMIBOT_CONNECT_STREAM", "true" if mutations or fills else "false")
    import lumibot.credentials as credentials

    monkeypatch.setattr(credentials, "trading_broker_name", "kalshi")
    monkeypatch.setattr(credentials, "data_source_name", None)
    monkeypatch.setattr(
        credentials,
        "KALSHI_CONFIG",
        {
            "API_KEY_ID": key_id,
            "PRIVATE_KEY": pem,
            "PRIVATE_KEY_PATH": path,
            "IS_DEMO": True,
            "SUBACCOUNT": 0,
        },
    )
    broker, source = credentials._build_default_live_credentials()
    assert broker._client.is_demo and ".demo.kalshi.co" in broker._client.base_url
    strategy = DemoCheck(broker=broker, analyze_backtest=False, synchronize_broker_on_start=False)
    try:
        yield strategy
    finally:
        broker.cleanup_streams()
        source.shutdown()


def _eligible_contract(strategy):
    """Test fixture only: pick a fresh tradable ticker, never a public API."""
    client = strategy.broker._client
    override = os.environ.get("KALSHI_TEST_TICKER")
    rows = client.pages("/markets", "markets", params={"status": "open", "limit": 1000}, authenticated=False)
    if override:
        rows = [client.request("GET", f"/markets/{client.path_id(override)}", authenticated=False)["market"]]
    for index, market in enumerate(rows):
        if index >= 5000:
            break
        if market.get("market_type") != "binary" or market.get("mve_collection_ticker"):
            continue
        close = datetime.fromisoformat(market["close_time"].replace("Z", "+00:00"))
        if close < datetime.now(timezone.utc) + timedelta(hours=1):
            continue
        ask = decimal_value(market.get("yes_ask_dollars", "0"))
        bid = decimal_value(market.get("yes_bid_dollars", "0"))
        if not Decimal("0.03") < ask < Decimal("0.95") or bid <= 0:
            continue
        for band in market.get("price_ranges", []):
            start, end, step = (decimal_value(band[k]) for k in ("start", "end", "step"))
            low = max(start, step)
            if step > 0 and low + step < min(ask, end) and low > 0:
                return Asset(market["ticker"], asset_type="prediction_contract"), float(low), float(low + step)
    pytest.skip("No eligible liquid, unexpired Demo contract was found; retry or set KALSHI_TEST_TICKER")


def _wait(strategy, order, predicate, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = strategy.get_order(order.identifier)
        if current is not None and predicate(current):
            return current
        time.sleep(0.25)
    pytest.fail("Kalshi Demo order did not reach the expected state within 20 seconds")


def _cleanup_demo_orders_and_position(strategy, asset, orders, maximum_quantity):
    """Only for tests that established the selected market was initially empty.

    Recover an uncertain submit by client ID before canceling. Closing attempts
    are bounded and never continue after an uncertain close submission.
    """
    strategy.broker.sync_orders(strategy.name)
    errors = []
    for obj in orders:
        try:
            if not obj.was_transmitted():
                if getattr(obj, "_kalshi_client_order_id", None) in strategy.broker._pending_submissions:
                    raise RuntimeError("Demo submit outcome remains unknown; inspect the Demo account")
                continue
            current = strategy.get_order(obj.identifier)
            if current is None:
                raise RuntimeError("Demo test order could not be located for cleanup")
            if current.is_active():
                strategy.cancel_order(obj)
                _wait(strategy, obj, lambda value: value.is_canceled() or value.is_filled())
        except (Exception, pytest.fail.Exception) as exc:
            errors.append(type(exc).__name__)
    # Never let one failed cancellation prevent attempts on the remaining orders.
    # Do not trade against unresolved open orders or unexpected preexisting size.
    if errors:
        pytest.fail("Demo order cleanup incomplete; inspect open orders before closing positions")
    for _ in range(3):
        position = strategy.get_position(asset)
        if position is None or position.quantity == 0:
            return
        if not 0 < position.quantity <= maximum_quantity:
            pytest.fail("Unexpected Demo position size; manual cleanup required")
        bid = strategy.get_quote(asset).bid
        if bid is None or bid <= 0:
            pytest.fail("No executable Demo bid for cleanup; manual cleanup required")
        close = strategy.create_order(asset, position.quantity, "sell", limit_price=bid, time_in_force="ioc")
        strategy.submit_order(close)
        _wait(strategy, close, lambda value: value.is_filled() or value.is_canceled())
    remaining = strategy.get_position(asset)
    assert remaining is None or remaining.quantity == 0, "Demo cleanup left a position; close it manually"


def test_demo_authentication_cash_positions_orders(demo):
    assert demo.update_broker_balances()
    assert demo.get_cash() >= 0
    assert demo.get_portfolio_value() >= demo.get_cash()
    positions = demo.get_positions()
    assert isinstance(positions, list)
    for position in positions:
        if position.asset.asset_type == "prediction_contract":
            assert demo.get_position(position.asset).quantity == position.quantity
    orders = demo.get_orders()
    for obj in orders[:3]:
        assert demo.get_order(obj.identifier).identifier == obj.identifier


def test_demo_price_quote_and_singular_plural_history(demo):
    asset, _, _ = _eligible_contract(demo)
    assert 0 <= demo.get_last_price(asset) <= 1
    assert 0 <= demo.get_last_prices([asset])[asset] <= 1
    quote = demo.get_quote(asset)
    assert 0 <= quote.bid <= quote.ask <= 1
    assert demo.get_historical_prices(asset, 5, timestep="hour") is not None
    assert demo.get_historical_prices_for_assets([asset], 5, timestep="hour")[asset] is not None


@pytest.mark.kalshi_demo_mutation
def test_demo_submit_read_modify_cancel_and_ioc_fok(demo):
    asset, low, modified = _eligible_contract(demo)
    initial = demo.get_position(asset)
    if initial is not None and initial.quantity:
        pytest.skip("Selected Demo market already has a position; select an empty market")
    created = []
    try:
        obj = demo.create_order(asset, 1, "buy", limit_price=low, time_in_force="gtc")
        created.append(obj)
        demo.submit_order(obj)
        _wait(demo, obj, lambda current: current.is_active())
        assert any(o.identifier == obj.identifier for o in demo.get_orders())
        demo.modify_order(obj, limit_price=modified)
        _wait(demo, obj, lambda current: abs(current.limit_price - modified) < 0.00001)
        demo.cancel_order(obj)
        _wait(demo, obj, lambda current: current.is_canceled())
        for tif in ("ioc", "fok"):
            quote = demo.get_quote(asset)
            if quote.ask <= low:
                pytest.fail("Demo market moved through the non-marketable test price")
            obj = demo.create_order(asset, 1, "buy", limit_price=low, time_in_force=tif)
            created.append(obj)
            demo.submit_order(obj)
            _wait(demo, obj, lambda current: current.is_canceled())
        assert demo.broker._is_stream_subscribed, "Authenticated Kalshi stream did not subscribe"
    finally:
        _cleanup_demo_orders_and_position(demo, asset, created, maximum_quantity=len(created))
        if any(obj.transactions for obj in created):
            pytest.fail("Demo mutation test unexpectedly filled; cleanup completed, retry at a non-crossing price")


@pytest.mark.kalshi_demo_fill
def test_demo_one_contract_fill_positions_cash_and_cleanup(demo):
    asset, _, _ = _eligible_contract(demo)
    initial = demo.get_position(asset)
    if initial is not None and initial.quantity:
        pytest.skip("Selected Demo market already has a position; select an empty market")
    assert demo.update_broker_balances()
    cash_before = demo.get_cash()
    buy = demo.create_order(asset, 1, "buy", limit_price=demo.get_quote(asset).ask, time_in_force="ioc")
    try:
        demo.submit_order(buy)
        _wait(demo, buy, lambda obj: obj.is_filled() or obj.is_canceled())
        assert buy.transactions, "Demo contract lacked executable liquidity"
        position = demo.get_position(asset)
        assert position and 0 < position.quantity <= 1
        assert demo.update_broker_balances()
        assert demo.get_cash() < cash_before
        assert demo.get_portfolio_value() >= demo.get_cash()
        assert demo.broker._is_stream_subscribed
    finally:
        _cleanup_demo_orders_and_position(demo, asset, [buy], maximum_quantity=1)
