from lumibot.brokers.broker import Broker
from lumibot.brokers.polymarket import Polymarket, PolymarketCLOBStream, PolymarketCredentialStore
from lumibot.data_sources.polymarket_data import PolymarketData
from lumibot.entities import Asset, Order, Position


class FakeDataClient:
    def get(self, url, params=None):
        if url.endswith("/positions"):
            return [
                {
                    "asset": "111",
                    "size": "3",
                    "avgPrice": "0.35",
                    "currentValue": "1.26",
                    "curPrice": "0.42",
                    "cashPnl": "0.21",
                    "outcome": "Yes",
                    "slug": "test-market",
                }
            ]
        if url.endswith("/value"):
            return [{"value": "1.26"}]
        raise AssertionError(f"Unexpected data API URL: {url}")


class FakeMarketDataClient:
    def get(self, url, params=None):
        if url.endswith("/tick-size"):
            return {"minimum_tick_size": "0.01"}
        if url.endswith("/book"):
            return {
                "bids": [{"price": "0.40", "size": "10"}],
                "asks": [{"price": "0.45", "size": "10"}],
                "tick_size": "0.01",
                "neg_risk": True,
            }
        raise AssertionError(f"Unexpected market-data URL: {url}")


class FakeSecureClient:
    def __init__(self):
        self.market_orders = []
        self.market_order_kwargs = []
        self.limit_orders = []
        self.limit_order_kwargs = []
        self.canceled = []

    def get_balance_allowance(self, *args, **kwargs):
        return {"balance": "12.34"}

    def get_portfolio_values(self):
        return {"portfolio_value": "13.60", "positions_value": "1.26"}

    def list_positions(self):
        return [
            {
                "asset": "111",
                "size": "3",
                "avgPrice": "0.35",
                "currentValue": "1.26",
                "curPrice": "0.42",
            }
        ]

    def get_open_orders(self):
        return [{"id": "order-1", "asset_id": "111", "side": "BUY", "price": "0.42", "size": "2", "status": "live"}]

    def get_order(self, identifier):
        return {"id": identifier, "asset_id": "111", "side": "BUY", "price": "0.42", "size": "2", "status": "live"}

    def create_and_post_market_order(self, payload, **kwargs):
        self.market_orders.append(payload)
        self.market_order_kwargs.append(kwargs)
        if isinstance(payload, dict):
            return {
                "id": "market-order",
                "asset_id": payload["token_id"],
                "side": payload["side"],
                "type": "market",
                "size": payload.get("amount") or payload.get("shares"),
                "status": "filled",
                "price": payload.get("price", "0.42"),
            }
        return {
            "id": "market-order",
            "asset_id": payload.token_id,
            "side": payload.side,
            "type": "market",
            "size": payload.amount,
            "status": "filled",
            "price": payload.price,
        }

    def create_and_post_order(self, payload, **kwargs):
        self.limit_orders.append(payload)
        self.limit_order_kwargs.append(kwargs)
        if isinstance(payload, dict):
            return {
                "id": "limit-order",
                "asset_id": payload["token_id"],
                "side": payload["side"],
                "type": "limit",
                "size": payload["size"],
                "status": "live",
                "price": payload["price"],
            }
        return {
            "id": "limit-order",
            "asset_id": payload.token_id,
            "side": payload.side,
            "type": "limit",
            "size": payload.size,
            "status": "live",
            "price": payload.price,
        }

    def cancel_order(self, payload):
        self.canceled.append(payload)
        return {"canceled": True}


def _broker(monkeypatch):
    monkeypatch.setattr(Broker, "_start_orders_thread", lambda self: None)
    data_source = PolymarketData(client=FakeMarketDataClient())
    return Polymarket(
        {
            "WALLET_ADDRESS": "0xwallet",
            "MAX_MARKET_ORDER_NOTIONAL": "5",
        },
        data_source=data_source,
        secure_client=FakeSecureClient(),
        data_api_client=FakeDataClient(),
        connect_stream=False,
    )


def test_polymarket_credential_store_redacts_secrets():
    store = PolymarketCredentialStore({"PRIVATE_KEY": "secret", "WALLET_ADDRESS": "0xwallet"})

    assert store.redacted_config()["PRIVATE_KEY"] == "<redacted>"
    assert store.redacted_config()["WALLET_ADDRESS"] == "0xwallet"


def test_polymarket_balances_positions_orders(monkeypatch):
    broker = _broker(monkeypatch)
    try:
        cash, positions_value, portfolio_value = broker._get_balances_at_broker(
            Asset("USD", asset_type=Asset.AssetType.FOREX),
            None,
        )
        positions = broker._pull_positions("unit")
        orders = broker._pull_broker_all_orders()

        assert cash == 12.34
        assert positions_value == 1.26
        assert portfolio_value == 13.60
        assert isinstance(positions[0], Position)
        assert positions[0].asset.asset_type == Asset.AssetType.PREDICTION_CONTRACT
        assert positions[0].quantity == 3.0
        assert orders[0]["id"] == "order-1"
    finally:
        broker.cleanup_streams()


def test_polymarket_raw_collateral_balance_scales_from_usdc_units(monkeypatch):
    broker = _broker(monkeypatch)
    try:
        broker._secure_client.get_balance_allowance = lambda *_, **__: {"balance": "29185517"}

        cash = broker._get_collateral_cash()

        assert cash == 29.185517
    finally:
        broker.cleanup_streams()


def test_polymarket_parse_broker_order(monkeypatch):
    broker = _broker(monkeypatch)
    try:
        order = broker._parse_broker_order(
            {"id": "o1", "asset_id": "111", "side": "SELL", "price": "0.44", "size": "2", "status": "confirmed"},
            "unit",
        )

        assert order.identifier == "o1"
        assert order.side == Order.OrderSide.SELL
        assert order.status == Order.OrderStatus.FILLED
        assert order.asset.asset_type == Asset.AssetType.PREDICTION_CONTRACT
    finally:
        broker.cleanup_streams()


def test_polymarket_market_buy_requires_amount(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order("unit", asset, quantity=1, side=Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    try:
        try:
            broker._submit_order(order)
        except ValueError as exc:
            assert 'custom_params["amount"]' in str(exc)
        else:
            raise AssertionError("Expected missing market amount to fail")
    finally:
        broker.cleanup_streams()


def test_polymarket_market_buy_uses_custom_amount_not_quantity(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order(
        "unit",
        asset,
        quantity=999,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        custom_params={"amount": "1.00", "price": "0.45", "order_type": "FAK"},
    )
    try:
        submitted = broker._submit_order(order)

        assert submitted.identifier == "market-order"
        assert broker._secure_client.market_orders[0].amount == 1.0
        assert broker._secure_client.market_orders[0].token_id == "111"
        assert broker._secure_client.market_order_kwargs[0]["options"].tick_size == "0.01"
        assert broker._secure_client.market_order_kwargs[0]["options"].neg_risk is True
    finally:
        broker.cleanup_streams()


def test_polymarket_market_buy_notional_cap(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order(
        "unit",
        asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        custom_params={"amount": "6.00", "price": "0.45", "order_type": "FAK"},
    )
    try:
        try:
            broker._submit_order(order)
        except ValueError as exc:
            assert "exceeds configured cap" in str(exc)
        else:
            raise AssertionError("Expected market order cap to fail")
    finally:
        broker.cleanup_streams()


def test_polymarket_limit_order_and_cancel(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order(
        "unit",
        asset,
        quantity=2,
        side=Order.OrderSide.BUY,
        limit_price=0.42,
        order_type=Order.OrderType.LIMIT,
        time_in_force="gtc",
    )
    try:
        submitted = broker._submit_order(order)
        broker.cancel_order(submitted)

        assert submitted.identifier == "limit-order"
        assert broker._secure_client.limit_orders
        assert broker._secure_client.limit_order_kwargs[0]["options"].tick_size == "0.01"
        assert broker._secure_client.limit_order_kwargs[0]["options"].neg_risk is True
        assert broker._secure_client.canceled
        assert submitted.status == Order.OrderStatus.CANCELED
    finally:
        broker.cleanup_streams()


def test_polymarket_wraps_known_deposit_wallet_order_blocker(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order(
        "unit",
        asset,
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        custom_params={"amount": "1.00", "price": "0.45", "order_type": "FAK"},
    )
    broker._secure_client.create_and_post_market_order = lambda *_, **__: (_ for _ in ()).throw(
        RuntimeError("maker address not allowed, please use the deposit wallet flow")
    )
    try:
        try:
            broker._submit_order(order)
        except Exception as exc:
            assert "deposit-wallet flow" in str(exc) or "deposit wallet" in str(exc)
            assert "Raw Polymarket error" in str(exc)
        else:
            raise AssertionError("Expected platform order blocker to be surfaced")
    finally:
        broker.cleanup_streams()


def test_polymarket_stream_user_event_dispatch(monkeypatch):
    broker = _broker(monkeypatch)
    stream = PolymarketCLOBStream(broker, use_websocket=False)
    seen = []
    stream.add_action(broker.FILLED_ORDER)(lambda order, **_: seen.append(order.identifier))

    stream.handle_user_event({"id": "filled-1", "asset_id": "111", "side": "BUY", "status": "confirmed", "size": "1"})

    stream._process_queue_event(broker.FILLED_ORDER, {"order": broker._parse_broker_order({"id": "filled-1"}, "unit")})
    assert seen == ["filled-1"]


def test_polymarket_stream_subscription_payloads():
    market_payload = PolymarketCLOBStream._market_subscription_payload(["111", "222"])
    user_payload = PolymarketCLOBStream._user_subscription_payload(
        {"apiKey": "key", "secret": "secret", "passphrase": "passphrase"}
    )

    assert market_payload == {"assets_ids": ["111", "222"], "type": "market", "custom_feature_enabled": True}
    assert user_payload["type"] == "user"
    assert user_payload["auth"]["apiKey"] == "key"
