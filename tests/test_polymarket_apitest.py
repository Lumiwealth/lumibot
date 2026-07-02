import asyncio
import json
import os

import pytest

from lumibot.brokers.broker import LumibotBrokerAPIError
from lumibot.brokers.polymarket import Polymarket
from lumibot.data_sources.polymarket_data import PolymarketData
from lumibot.entities import Asset, Order


pytestmark = [pytest.mark.apitest, pytest.mark.polymarket]


def _token_or_skip():
    token_id = os.environ.get("POLYMARKET_TEST_TOKEN_ID")
    if not token_id:
        pytest.skip("Missing POLYMARKET_TEST_TOKEN_ID")
    return token_id


def test_polymarket_public_quote_smoke():
    token_id = _token_or_skip()
    data = PolymarketData()
    asset = Asset(token_id, asset_type=Asset.AssetType.PREDICTION_CONTRACT)

    quote = data.get_quote(asset)

    assert quote.asset == asset
    assert quote.bid is not None or quote.ask is not None or quote.price is not None


def test_polymarket_public_websocket_smoke():
    token_id = _token_or_skip()

    async def _run():
        import websockets

        events = []
        async with websockets.connect(PolymarketData.MARKET_WS_URL, ping_interval=20, close_timeout=5) as ws:
            await ws.send(json.dumps({"assets_ids": [token_id], "type": "market", "custom_feature_enabled": True}))
            deadline = asyncio.get_running_loop().time() + 8
            while asyncio.get_running_loop().time() < deadline and not events:
                try:
                    events.append(json.loads(await asyncio.wait_for(ws.recv(), timeout=1.0)))
                except asyncio.TimeoutError:
                    continue
        return events

    events = asyncio.run(_run())

    assert events


@pytest.mark.polymarket_credentials
def test_polymarket_account_snapshot_smoke(monkeypatch):
    monkeypatch.setattr("lumibot.brokers.broker.Broker._start_orders_thread", lambda self: None)
    broker = Polymarket(connect_stream=False)
    try:
        cash, positions_value, portfolio_value = broker._get_balances_at_broker(
            Asset("USD", asset_type=Asset.AssetType.FOREX),
            None,
        )
        positions = broker._pull_positions("apitest")
        orders = broker._pull_broker_all_orders()

        assert isinstance(cash, float)
        assert cash < 1_000_000
        assert isinstance(positions_value, float)
        assert isinstance(portfolio_value, float)
        assert isinstance(positions, list)
        assert isinstance(orders, list)
    finally:
        broker.cleanup_streams()


@pytest.mark.polymarket_credentials
@pytest.mark.polymarket_live_trading
def test_polymarket_tiny_market_buy_smoke(monkeypatch):
    token_id = _token_or_skip()
    amount = os.environ.get("POLYMARKET_TEST_ORDER_AMOUNT", "1.00")
    max_price = os.environ.get("POLYMARKET_TEST_MAX_PRICE", "0.99")
    monkeypatch.setattr("lumibot.brokers.broker.Broker._start_orders_thread", lambda self: None)
    broker = Polymarket(connect_stream=False)
    asset = Asset(token_id, asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order(
        "apitest",
        asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        custom_params={"amount": amount, "price": max_price, "order_type": "FAK"},
    )
    try:
        try:
            submitted = broker._submit_order(order)
        except LumibotBrokerAPIError as exc:
            message = str(exc).lower()
            if (
                "deposit wallet" in message
                or "maker address not allowed" in message
                or "order signer address" in message
            ):
                pytest.xfail(f"Current Polymarket account is blocked by deposit-wallet/API-key binding: {exc}")
            raise

        assert submitted.identifier
        broker._get_balances_at_broker(Asset("USD", asset_type=Asset.AssetType.FOREX), None)
        broker._pull_positions("apitest")
        broker._pull_broker_order(submitted.identifier)
    finally:
        broker.cleanup_streams()
