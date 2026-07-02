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
        if url.endswith("/trades"):
            return [{"id": "trade-1", "asset": "111", "price": "0.42", "size": "2"}]
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
        self.cancel_many_payloads = []
        self.cancel_all_called = False
        self.cancel_market_payloads = []
        self.balance_allowance_updates = []

    def get_balance_allowance(self, *args, **kwargs):
        return {"balance": "12.34"}

    def update_balance_allowance(self, payload):
        self.balance_allowance_updates.append(payload)
        return {"updated": True}

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

    def cancel_orders(self, payload):
        self.cancel_many_payloads.append(payload)
        return {"canceled": payload, "not_canceled": {}}

    def cancel_all(self):
        self.cancel_all_called = True
        return {"canceled": ["all"], "not_canceled": {}}

    def cancel_market_orders(self, payload):
        self.cancel_market_payloads.append(payload)
        return {"canceled": ["market"], "not_canceled": {}}


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


def test_polymarket_signature_type_honors_explicit_deposit_wallet_config(monkeypatch):
    monkeypatch.setattr(Broker, "_start_orders_thread", lambda self: None)

    class SignatureTypes:
        POLY_PROXY = 1
        POLY_1271 = 3

    broker = Polymarket(
        {
            "OWNER_ADDRESS": "0xowner",
            "WALLET_ADDRESS": "0xdepositwallet",
            "SIGNATURE_TYPE": "3",
        },
        data_source=PolymarketData(client=FakeMarketDataClient()),
        secure_client=FakeSecureClient(),
        data_api_client=FakeDataClient(),
        connect_stream=False,
    )
    try:
        assert broker._configured_signature_type(SignatureTypes) == 3
    finally:
        broker.cleanup_streams()


def test_polymarket_signature_type_infers_proxy_when_no_explicit_value(monkeypatch):
    monkeypatch.setattr(Broker, "_start_orders_thread", lambda self: None)
    monkeypatch.delenv("POLYMARKET_SIGNATURE_TYPE", raising=False)

    class SignatureTypes:
        POLY_PROXY = 1
        POLY_1271 = 3

    broker = Polymarket(
        {
            "OWNER_ADDRESS": "0xowner",
            "WALLET_ADDRESS": "0xproxywallet",
        },
        data_source=PolymarketData(client=FakeMarketDataClient()),
        secure_client=FakeSecureClient(),
        data_api_client=FakeDataClient(),
        connect_stream=False,
    )
    try:
        assert broker._configured_signature_type(SignatureTypes) == 1
    finally:
        broker.cleanup_streams()


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


def test_polymarket_parse_matched_market_order_response(monkeypatch):
    broker = _broker(monkeypatch)
    try:
        order = broker._parse_broker_order(
            {
                "orderID": "0xabc",
                "status": "matched",
                "makingAmount": "0.999999",
                "takingAmount": "35.714284",
            },
            "unit",
        )

        assert order.identifier == "0xabc"
        assert order.status == Order.OrderStatus.FILLED
        assert order.quantity == 35.714284
        assert round(order.avg_fill_price, 6) == 0.028
    finally:
        broker.cleanup_streams()


def test_polymarket_parse_matched_sell_order_response(monkeypatch):
    broker = _broker(monkeypatch)
    try:
        order = broker._parse_broker_order(
            {
                "orderID": "0xsell",
                "side": "SELL",
                "status": "matched",
                "makingAmount": "5",
                "takingAmount": "0.14",
            },
            "unit",
        )

        assert order.identifier == "0xsell"
        assert order.status == Order.OrderStatus.FILLED
        assert order.quantity == 5.0
        assert round(order.avg_fill_price, 6) == 0.028
    finally:
        broker.cleanup_streams()


def test_polymarket_submit_response_uses_original_sell_side_for_matched_amounts(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    original = Order(
        "unit",
        asset,
        quantity=5,
        side=Order.OrderSide.SELL,
        order_type=Order.OrderType.MARKET,
        custom_params={"order_type": "FOK", "price": "0.01"},
    )
    try:
        parsed = broker._order_from_submit_response(
            {
                "orderID": "0xsell",
                "status": "matched",
                "makingAmount": "5",
                "takingAmount": "0.14",
            },
            original,
        )

        assert parsed.identifier == "0xsell"
        assert parsed.side == Order.OrderSide.SELL
        assert parsed.status == Order.OrderStatus.FILLED
        assert parsed.quantity == 5.0
        assert round(parsed.avg_fill_price, 6) == 0.028
    finally:
        broker.cleanup_streams()


def test_polymarket_parse_live_limit_order_response_with_empty_amounts(monkeypatch):
    broker = _broker(monkeypatch)
    try:
        order = broker._parse_broker_order(
            {
                "orderID": "0xlimit",
                "status": "live",
                "makingAmount": "",
                "takingAmount": "",
            },
            "unit",
        )

        assert order.identifier == "0xlimit"
        assert order.status == Order.OrderStatus.OPEN
        assert order.quantity == 0
        assert order.avg_fill_price is None
    finally:
        broker.cleanup_streams()


def test_polymarket_submit_response_preserves_original_limit_price(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    original = Order(
        "unit",
        asset,
        quantity=5,
        side=Order.OrderSide.BUY,
        limit_price=0.01,
        order_type=Order.OrderType.LIMIT,
    )
    try:
        parsed = broker._order_from_submit_response({"orderID": "0xlimit", "status": "live"}, original)

        assert parsed.limit_price == 0.01
        assert parsed.order_type == Order.OrderType.LIMIT
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


def test_polymarket_market_fok_buy_and_sell_semantics(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    buy = Order(
        "unit",
        asset,
        quantity=999,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.MARKET,
        custom_params={"amount": "1.00", "price": "0.45", "order_type": "FOK"},
    )
    sell = Order(
        "unit",
        asset,
        quantity=3,
        side=Order.OrderSide.SELL,
        order_type=Order.OrderType.MARKET,
        custom_params={"price": "0.40", "order_type": "FOK"},
    )
    try:
        broker._submit_order(buy)
        broker._submit_order(sell)

        assert broker._secure_client.market_orders[0].amount == 1.0
        assert broker._secure_client.market_orders[0].side == "BUY"
        assert broker._secure_client.market_orders[1].amount == 3.0
        assert broker._secure_client.market_orders[1].side == "SELL"
        assert str(broker._secure_client.market_order_kwargs[0]["order_type"]).upper().endswith("FOK")
        assert broker._secure_client.balance_allowance_updates[-1].asset_type == "CONDITIONAL"
        assert broker._secure_client.balance_allowance_updates[-1].token_id == "111"
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


def test_polymarket_gtd_limit_order_passes_expiration(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order(
        "unit",
        asset,
        quantity=2,
        side=Order.OrderSide.BUY,
        limit_price=0.42,
        order_type=Order.OrderType.LIMIT,
        time_in_force="gtd",
        custom_params={"expiration": 1_800_000_000},
    )
    try:
        broker._submit_order(order)

        assert broker._secure_client.limit_orders[0].expiration == 1_800_000_000
        assert str(broker._secure_client.limit_order_kwargs[0]["order_type"]).upper().endswith("GTD")
    finally:
        broker.cleanup_streams()


def test_polymarket_gtd_limit_order_requires_expiration(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order(
        "unit",
        asset,
        quantity=2,
        side=Order.OrderSide.BUY,
        limit_price=0.42,
        order_type=Order.OrderType.LIMIT,
        time_in_force="gtd",
    )
    try:
        try:
            broker._submit_order(order)
        except ValueError as exc:
            assert "GTD" in str(exc)
        else:
            raise AssertionError("Expected GTD without expiration to fail")
    finally:
        broker.cleanup_streams()


def test_polymarket_limit_order_supports_post_only(monkeypatch):
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
        custom_params={"post_only": True},
    )
    try:
        broker._submit_order(order)

        assert broker._secure_client.limit_order_kwargs[0]["post_only"] is True
    finally:
        broker.cleanup_streams()


def test_polymarket_cancel_helpers_use_native_clob_methods(monkeypatch):
    broker = _broker(monkeypatch)
    order1 = Order("unit", Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT), 1, Order.OrderSide.BUY)
    order1.identifier = "order-1"
    order2 = Order("unit", Asset("222", asset_type=Asset.AssetType.PREDICTION_CONTRACT), 1, Order.OrderSide.SELL)
    order2.identifier = "order-2"
    try:
        broker.cancel_orders([order1, order2])
        broker.cancel_all_orders()
        broker.cancel_market_orders(market="0x" + "1" * 64, asset_id="111")

        assert broker._secure_client.cancel_many_payloads == [["order-1", "order-2"]]
        assert broker._secure_client.cancel_all_called is True
        assert broker._secure_client.cancel_market_payloads
        assert order1.status == Order.OrderStatus.CANCELED
        assert order2.status == Order.OrderStatus.CANCELED
    finally:
        broker.cleanup_streams()


def test_polymarket_recent_trades_from_data_api(monkeypatch):
    broker = _broker(monkeypatch)
    try:
        broker._secure_client.get_trades = lambda: None

        trades = broker.get_recent_trades(limit=5)

        assert trades[0]["id"] == "trade-1"
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


def test_polymarket_wraps_post_only_rejection(monkeypatch):
    broker = _broker(monkeypatch)
    asset = Asset("111", asset_type=Asset.AssetType.PREDICTION_CONTRACT)
    order = Order(
        "unit",
        asset,
        quantity=5,
        side=Order.OrderSide.BUY,
        limit_price=0.80,
        order_type=Order.OrderType.LIMIT,
        time_in_force="gtc",
        custom_params={"post_only": True},
    )
    broker._secure_client.create_and_post_order = lambda *_, **__: (_ for _ in ()).throw(
        RuntimeError("invalid post-only order: order crosses book")
    )
    try:
        try:
            broker._submit_order(order)
        except Exception as exc:
            assert "post-only" in str(exc)
            assert "cross" in str(exc)
            assert "Raw Polymarket error" in str(exc)
        else:
            raise AssertionError("Expected post-only rejection to be wrapped")
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


def test_polymarket_stream_events_process_through_broker_lifecycle(monkeypatch):
    broker = _broker(monkeypatch)
    stream = PolymarketCLOBStream(broker, use_websocket=False)
    broker.stream = stream
    seen = []

    def fake_process_trade_event(order, event, **kwargs):
        seen.append((order.identifier, event, kwargs.get("price"), kwargs.get("filled_quantity")))

    broker._process_trade_event = fake_process_trade_event
    broker._register_stream_events()

    event = {
        "id": "filled-1",
        "asset_id": "111",
        "side": "BUY",
        "status": "matched",
        "price": "0.42",
        "size": "2",
        "updated_at": "2026-07-01T00:00:00Z",
    }
    stream.handle_user_event(event)
    duplicate = dict(event)
    duplicate["updated_at"] = "2026-07-01T00:00:01Z"
    stream.handle_user_event(duplicate)

    while not stream._queue.empty():
        queued_event, payload = stream._queue.get_nowait()
        stream._process_queue_event(queued_event, payload)
        stream._queue.task_done()

    assert seen == [("filled-1", broker.FILLED_ORDER, 0.42, 2.0)]


def test_polymarket_user_stream_waits_until_events_can_route(monkeypatch):
    broker = _broker(monkeypatch)
    stream = PolymarketCLOBStream(broker, use_websocket=False)
    try:
        broker._subscribers = []
        broker._strategy_name = ""
        assert stream._can_route_user_events() is False

        broker.set_strategy_name("unit")
        assert stream._can_route_user_events() is True
    finally:
        broker.cleanup_streams()


def test_polymarket_stream_subscription_payloads():
    market_payload = PolymarketCLOBStream._market_subscription_payload(["111", "222"])
    user_payload = PolymarketCLOBStream._user_subscription_payload(
        {"apiKey": "key", "secret": "secret", "passphrase": "passphrase"}
    )

    assert market_payload == {"assets_ids": ["111", "222"], "type": "market", "custom_feature_enabled": True}
    assert user_payload["type"] == "user"
    assert user_payload["auth"]["apiKey"] == "key"
