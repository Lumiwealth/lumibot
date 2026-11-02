"""MetaMask broker contract and lifecycle regressions; no external credentials."""

from decimal import Decimal, localcontext
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import requests

from lumibot.brokers import MetaMask
from lumibot.entities import Asset, Order, Position
from lumibot.strategies.strategy import Strategy
from lumibot.tools.metamask_client import MetaMaskAPIError, MetaMaskClient

WALLET = "0x" + "1" * 40
CONFIG = {"WALLET_ADDRESS": WALLET, "NETWORK": "testnet", "AGENT_ID": "agent"}


@pytest.fixture
def broker():
    instance = MetaMask(CONFIG, connect_stream=False, start_orders_thread=False)
    instance.client = Mock()
    instance.client.verify.return_value = {"wallet": WALLET, "network": "testnet"}
    yield instance
    instance._stop_event.set()


def order(**kwargs):
    return Order(
        "test-strategy",
        Asset("BTC", asset_type="crypto_future"),
        Decimal("0.001"),
        "buy",
        quote=Asset("USDC", asset_type="crypto"),
        **kwargs,
    )


def observation(item, state, quantity="0", price=None):
    result = {"id": str(item.identifier), "request": {"kind": "perp"}, "state": state, "filledQuantity": quantity}
    if price is not None:
        result["averagePrice"] = price
    return result


def test_chain_identity_roundtrip_and_legacy_shape():
    native = Asset("ETH", asset_type="crypto", chain_id=11155111, token_address="native", decimals=18)
    other = Asset("ETH", asset_type="crypto", chain_id=421614, token_address="native", decimals=18)
    assert native != other
    assert len({native, other}) == 2
    assert Asset.from_dict(native.to_dict()) == native
    assert native.to_minimal_dict()["chain_id"] == 11155111
    assert "chain_id" not in Asset("ETH", asset_type="crypto").to_dict()
    assert native != Asset("ETH", asset_type="crypto")


def test_chain_identity_survives_shared_order_and_position_serialization():
    asset = Asset("TOKEN", asset_type="crypto", chain_id=11155111, token_address="0x" + "2" * 40, decimals=6)
    trade = Order("neutral", asset, Decimal("1.25"), "buy", quote=Asset("USDC", asset_type="crypto"))
    restored_trade = Order.from_dict(trade.to_dict())
    assert restored_trade.quote == trade.quote
    restored = [
        Asset.from_dict(asset.to_minimal_dict()),
        restored_trade.asset,
        Position.from_dict(Position("neutral", asset, Decimal("1.25")).to_dict()).asset,
    ]
    for value in restored:
        assert value == asset
        assert value.decimals == 6
        assert value.chain_id == 11155111
        assert value.token_address == asset.token_address


@pytest.mark.parametrize(
    "overrides", [{"chain_id": 0}, {"decimals": -1}, {"token_address": "ETH"}, {"asset_type": "stock"}]
)
def test_invalid_token_identity(overrides):
    args = {
        "symbol": "TOKEN",
        "asset_type": "crypto",
        "chain_id": 11155111,
        "token_address": "0x" + "2" * 40,
        "decimals": 6,
        **overrides,
    }
    with pytest.raises(ValueError):
        Asset(**args)


def test_perp_submission_uses_base_quantity_and_durable_id(broker):
    item = order()
    broker.client.submit.return_value = observation(item, "submitted")
    assert broker._submit_order(item) is item
    payload = broker.client.submit.call_args.args[0]
    assert payload["quantity"] == "0.001"
    assert payload["id"] == str(item.identifier)
    assert payload["reduceOnly"] is False
    assert item.was_transmitted()


def test_lost_response_stays_transmitted_and_does_not_retry(broker):
    item = order()
    broker.client.submit.side_effect = requests.Timeout()
    broker._submit_order(item)
    assert item.was_transmitted()
    assert item._raw["state"] == "unknown"
    assert broker.client.submit.call_count == 1


def test_unsupported_order_rejected_before_transport(broker):
    with pytest.raises(ValueError, match="conditional"):
        broker._submit_order(order(order_type="stop", stop_price=100))
    broker.client.submit.assert_not_called()


def test_spot_buy_not_reinterpreted_as_exact_input(broker):
    item = Order("test", Asset("ETH", asset_type="crypto"), 1, "buy", quote=Asset("USDC", asset_type="crypto"))
    with pytest.raises(ValueError, match="swap"):
        broker._submit_order(item)
    broker.client.submit.assert_not_called()


def test_fill_deltas_and_prices_are_not_double_counted(broker):
    item = order()
    broker._process_trade_event = Mock()
    broker._observe(item, observation(item, "partial_fill", "0.0004", "50000"))
    broker._observe(item, observation(item, "partial_fill", "0.0004", "50000"))
    broker._observe(item, observation(item, "filled", "0.001", "50600"))
    calls = broker._process_trade_event.call_args_list
    assert len(calls) == 2
    assert calls[0].kwargs["filled_quantity"] == Decimal("0.0004")
    assert calls[1].kwargs["filled_quantity"] == Decimal("0.0006")
    assert calls[1].kwargs["price"] == Decimal("51000")
    assert calls[1].args[1] == broker.FILLED_ORDER


@pytest.mark.parametrize("restore", [False, True])
def test_fill_deltas_ignore_strategy_decimal_context(broker, restore):
    item = order()
    broker._process_trade_event = Mock()
    first = {
        **observation(item, "partial_fill", "0.0004", "50000.12345"),
        "request": {
            "kind": "perp",
            "symbol": "BTC",
            "quantity": "0.001",
            "side": "buy",
            "orderType": "market",
            "leverage": 1,
            "tif": "ioc",
        },
    }
    with localcontext() as context:
        context.prec = 6
        context.Emax = 2
        if restore:
            item = broker._parse_broker_order(first, item.strategy)
        else:
            broker._observe(item, first)
        final = observation(item, "filled", "0.001", "50600.54321")
        broker._observe(item, final)
        broker._observe(item, final)
    assert broker._process_trade_event.call_count == (1 if restore else 2)
    last = broker._process_trade_event.call_args
    assert last.kwargs["filled_quantity"] == Decimal("0.0006")
    assert last.kwargs["price"] == Decimal("51000.82305")


def test_incomplete_fill_history_cannot_reverse_fill(broker):
    item = order()
    broker._process_trade_event = Mock()
    broker._observe(item, observation(item, "partial_fill", "0.0004", "50000"))
    with pytest.raises(MetaMaskAPIError, match="FILL_REGRESSION"):
        broker._observe(item, observation(item, "open"))


def test_cancel_sends_even_when_lumibot_status_is_cancelling(broker):
    item = order()
    item.status = "cancelling"
    broker.client.cancel.return_value = observation(item, "canceled")
    broker._process_trade_event = Mock()
    broker.cancel_order(item)
    broker.client.cancel.assert_called_once_with(str(item.identifier))
    assert broker._process_trade_event.call_args.args[1] == broker.CANCELED_ORDER


def test_pending_approval_does_not_emit_fill_or_error(broker):
    item = order()
    broker._process_trade_event = Mock()
    broker._observe(item, observation(item, "awaiting_approval"))
    broker._process_trade_event.assert_not_called()


@pytest.mark.parametrize("custom", [{}, {"reduce_only": False}])
def test_canonical_reduce_only_cannot_be_weakened_by_custom_params(broker, custom):
    item = order(reduce_only=True, custom_params=custom)
    broker.client.submit.return_value = observation(item, "awaiting_approval")
    broker.submit_order(item)
    assert broker.client.submit.call_args.args[0]["reduceOnly"] is True


def test_shared_close_position_keeps_reduce_only_on_wire_and_restore(broker):
    item = order()
    broker._filled_positions.append(Position(item.strategy, item.asset, item.quantity))
    broker.quote_assets = {item.quote}
    broker.client.submit.side_effect = lambda payload: {
        "id": payload["id"],
        "request": payload,
        "state": "submitted",
        "filledQuantity": "0",
    }
    closed = broker.close_position(item.strategy, item.asset)
    assert closed is not None and closed.reduce_only is True
    request = broker.client.submit.call_args.args[0]
    assert request["reduceOnly"] is True
    assert request["side"] == "sell"
    restored = broker._parse_broker_order(
        {"id": "restored-close", "request": request, "state": "open", "filledQuantity": "0"}, item.strategy
    )
    assert restored.reduce_only is True


@pytest.mark.parametrize("state,tracker", [("canceled", "_canceled_orders"), ("rejected", "_error_orders")])
def test_restored_terminal_order_is_not_processed_again_by_polling(broker, state, tracker):
    class RestoreStrategy(Strategy):
        def initialize(self, parameters=None):
            self.sleeptime = "1S"

        def on_trading_iteration(self):
            pass

    response = {
        "id": "restored-terminal",
        "request": {
            "kind": "perp",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.001",
            "orderType": "limit",
            "limitPrice": "40000",
            "leverage": 1,
            "tif": "gtc",
        },
        "state": state,
        "filledQuantity": "0",
    }

    def read(method, path, **kwargs):
        if path == "/v1/account":
            return {"cash": "100", "positionsValue": "0", "equity": "100", "currency": "USDC", "positions": []}
        assert path == "/v1/orders"
        return [response]

    broker.client.account.side_effect = lambda kind: read("GET", "/v1/account")
    broker.client.orders.side_effect = lambda: read("GET", "/v1/orders")
    strategy = RestoreStrategy(
        broker=broker,
        name="restore-strategy",
        quote_asset=Asset("USDC", asset_type="crypto"),
        benchmark_asset=None,
        analyze_backtest=False,
    )
    strategy._executor.sync_broker()
    broker.do_polling()
    broker.do_polling()

    # Startup already restored terminal history; later polls must not append it again.
    assert [item.identifier for item in getattr(broker, tracker)] == [response["id"]]


@pytest.mark.parametrize("state,filled", [("partial_fill", "0.0004"), ("filled", "0.001")])
def test_executor_restores_exact_fill_history_without_replaying_quantity(broker, state, filled):
    class RestoreStrategy(Strategy):
        def initialize(self, parameters=None):
            self.sleeptime = "1S"

        def on_trading_iteration(self):
            pass

    response = {
        "id": "restored-fill",
        "request": {
            "kind": "perp",
            "symbol": "BTC",
            "side": "buy",
            "quantity": "0.001",
            "orderType": "limit",
            "limitPrice": "40000",
            "leverage": 1,
            "tif": "gtc",
        },
        "state": state,
        "filledQuantity": filled,
        "averagePrice": "40000",
    }

    def read(method, path, **kwargs):
        if path == "/v1/account":
            return {
                "cash": "60",
                "positionsValue": "40",
                "equity": "100",
                "currency": "USDC",
                "positions": [
                    {
                        "symbol": "BTC",
                        "quantity": filled,
                        "entryPrice": "40000",
                        "marketValue": "40",
                        "unrealizedPnl": "0",
                    }
                ],
            }
        assert path == "/v1/orders"
        return [response]

    broker.client.account.side_effect = lambda kind: read("GET", "/v1/account")
    broker.client.orders.side_effect = lambda: read("GET", "/v1/orders")
    strategy = RestoreStrategy(
        broker=broker,
        name="restore-strategy",
        quote_asset=Asset("USDC", asset_type="crypto"),
        benchmark_asset=None,
        analyze_backtest=False,
    )
    strategy._executor.sync_broker()
    restored = broker.get_tracked_order(response["id"])
    assert restored is not None
    # A venue snapshot already contains cumulative fills. Startup must not add total order size again.
    assert sum(Decimal(str(t.quantity)) for t in restored.transactions) == Decimal(filled)
    if state == "partial_fill":
        assert restored.status == Order.OrderStatus.PARTIALLY_FILLED
    else:
        assert restored.is_filled()
    broker.do_polling()
    assert sum(Decimal(str(t.quantity)) for t in restored.transactions) == Decimal(filled)


def test_order_snapshot_cannot_consume_unprocessed_fill_delta(broker):
    item = order()
    subscriber = SimpleNamespace(
        name=item.strategy,
        add_event=Mock(),
        NEW_ORDER="new",
        PARTIALLY_FILLED_ORDER="partial_fill",
        FILLED_ORDER="fill",
    )
    broker._add_subscriber(subscriber)
    broker.client.submit.return_value = observation(item, "partial_fill", "0.0004", "50000")
    broker.submit_order(item)
    final = {
        **observation(item, "filled", "0.001", "50600"),
        "request": {
            "kind": "perp",
            "symbol": "BTC",
            "quantity": "0.001",
            "side": "buy",
            "orderType": "market",
            "leverage": 1,
            "tif": "ioc",
        },
    }

    # Executor synchronization parses snapshots before the stream processes callbacks.
    broker._parse_broker_order(final, item.strategy)
    broker.client.orders.return_value = [final]
    broker.do_polling()

    assert item.is_filled()
    assert len(item.transactions) == 2
    assert subscriber.add_event.call_count == 2
    assert subscriber.add_event.call_args.args[0] == subscriber.FILLED_ORDER
    # Shared broker callbacks expose floats; compare the reported decimal quantity.
    assert Decimal(str(subscriber.add_event.call_args.args[1]["quantity"])) == Decimal("0.0006")


@pytest.mark.parametrize("state", ["filled", "canceled"])
def test_order_refresh_reconciles_fills_before_terminal_state(broker, state):
    item = order()
    subscriber = SimpleNamespace(
        name=item.strategy,
        add_event=Mock(),
        NEW_ORDER="new",
        PARTIALLY_FILLED_ORDER="partial_fill",
        FILLED_ORDER="fill",
        CANCELED_ORDER="canceled",
    )
    broker._add_subscriber(subscriber)
    broker.client.submit.return_value = observation(item, "open")
    broker.submit_order(item)
    quantity = "0.001" if state == "filled" else "0.0004"
    broker.client.orders.return_value = [
        {
            **observation(item, state, quantity, "50000"),
            "request": {
                "kind": "perp",
                "symbol": "BTC",
                "quantity": "0.001",
                "side": "buy",
                "orderType": "market",
                "leverage": 1,
                "tif": "ioc",
            },
        }
    ]
    broker.sync_orders(item.strategy)
    assert sum(Decimal(str(t.quantity)) for t in item.transactions) == Decimal(quantity)
    assert item.get_fill_price() == 50000
    assert item._closed_event.is_set()
    assert item.is_filled() if state == "filled" else item.is_canceled()
    event_count = subscriber.add_event.call_count
    broker.do_polling()
    broker.sync_orders(item.strategy)
    assert len(item.transactions) == 1
    assert subscriber.add_event.call_count == event_count


@pytest.mark.parametrize("state,tracker", [("canceled", "_canceled_orders"), ("rejected", "_error_orders")])
@pytest.mark.parametrize("reconstructed", [False, True])
def test_late_terminal_fills_emit_only_new_delta_without_reopening(broker, state, tracker, reconstructed):
    item = order()
    delivered_statuses = []
    subscriber = SimpleNamespace(
        name=item.strategy,
        add_event=Mock(side_effect=lambda event, payload: delivered_statuses.append(payload["order"].status)),
        NEW_ORDER="new",
        PARTIALLY_FILLED_ORDER="partial_fill",
        FILLED_ORDER="fill",
        CANCELED_ORDER="canceled",
        ERROR_ORDER="error",
    )
    broker._add_subscriber(subscriber)
    broker._unprocessed_orders.append(item)
    broker._observe(item, observation(item, state))
    terminal_calls = subscriber.add_event.call_count
    late = observation(item, state, "0.0004", "50000")
    broker._observe(order(identifier=item.identifier) if reconstructed else item, late)
    broker._observe(item, late)
    broker._observe(item, {**late, "state": "open"})
    assert sum(Decimal(str(t.quantity)) for t in item.transactions) == Decimal("0.0004")
    assert subscriber.add_event.call_count == terminal_calls + 1
    call = subscriber.add_event.call_args
    assert call.args[0] == "partial_fill" and Decimal(str(call.args[1]["quantity"])) == Decimal("0.0004")
    assert item.status == ("canceled" if state == "canceled" else "error")
    assert item._closed_event.is_set()
    assert list(getattr(broker, tracker)) == [item]
    assert item not in broker._partially_filled_orders
    assert item not in broker._new_orders
    assert item._raw["state"] == state
    assert delivered_statuses == [item.status, item.status]
    broker.client.submit.return_value = late
    assert broker.submit_order(order(identifier=item.identifier)) is item
    assert delivered_statuses == [item.status, item.status]


def test_swap_retains_request_id_on_uncertain_response(broker):
    broker.mode = "spot"
    source = Asset("ETH", asset_type="crypto", chain_id=11155111, token_address="native", decimals=18)
    dest = Asset("TOKEN", asset_type="crypto", chain_id=11155111, token_address="0x" + "2" * 40, decimals=6)
    broker.client.submit.side_effect = requests.Timeout()
    result = broker.swap(source, dest, "0.001", identifier="durable-swap")
    assert result["id"] == "durable-swap"
    assert result["state"] == "unknown"
    assert result["request"]["inputAmount"] == "0.001"


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com",
        "http://127.0.0.1:8766?token=secret",
        "http://user:pass@localhost:8766",
        "http://127.0.0.1:8766/v1",
    ],
)
def test_transport_cannot_redirect_credentials_to_other_hosts(url):
    with pytest.raises(ValueError):
        MetaMaskClient({**CONFIG, "BROKER_URL": url})


def test_legacy_service_configuration_is_rejected_without_network_calls():
    with pytest.raises(ValueError, match="runs in Python"):
        MetaMaskClient({**CONFIG, "BROKER_TOKEN": "retired-token"})


def test_real_broker_fill_lifecycle(broker):
    item = order()
    subscriber = SimpleNamespace(
        name=item.strategy,
        add_event=Mock(),
        NEW_ORDER="new",
        PARTIALLY_FILLED_ORDER="partial_fill",
        FILLED_ORDER="fill",
    )
    broker._add_subscriber(subscriber)
    broker.client.submit.return_value = observation(item, "open")
    broker.submit_order(item)
    assert broker.get_tracked_order(item.identifier) is item
    for response in (
        observation(item, "partial_fill", "0.0004", "50000"),
        observation(item, "filled", "0.001", "50600"),
    ):
        broker.client.orders.return_value = [response]
        broker.do_polling()
    assert item.is_filled()
    assert item.get_fill_price() == pytest.approx(50600)
    broker._observe(item, observation(item, "filled", "0.001", "50600"))
    assert len(item.transactions) == 2
    fills = [call for call in subscriber.add_event.call_args_list if call.args[0] in ("partial_fill", "fill")]
    assert [call.args[1]["quantity"] for call in fills] == pytest.approx([0.0004, 0.0006])
    # Live positions are reconciled from provider balances, not invented from fills.
    broker.client.account.return_value = {
        "positions": [
            {"symbol": "BTC", "quantity": "0.001", "entryPrice": "50600", "marketValue": "50.6", "unrealizedPnl": "0"}
        ]
    }
    assert broker._pull_position(item.strategy, item.asset).quantity == pytest.approx(0.001)


def test_limit_day_is_not_silently_changed_to_gtc(broker):
    with pytest.raises(ValueError, match="time_in_force"):
        broker._submit_order(order(order_type="limit", limit_price=100))
    broker.client.submit.assert_not_called()


def test_terminal_status_after_final_partial_closes_order(broker):
    item = order()
    broker._unprocessed_orders.append(item)
    broker._observe(item, observation(item, "partial_fill", "0.001", "50000"))
    broker._observe(item, observation(item, "filled", "0.001", "50000"))
    assert item.is_filled()
    assert sum(Decimal(str(t.quantity)) for t in item.transactions) == Decimal("0.001")


def test_contract_identity_does_not_depend_on_ticker_alias():
    first = Asset("TOKEN", asset_type="crypto", chain_id=11155111, token_address="0x" + "2" * 40, decimals=6)
    alias = Asset("ALIAS", asset_type="crypto", chain_id=11155111, token_address="0x" + "2" * 40, decimals=6)
    assert first == alias
    assert hash(first) == hash(alias)


def test_pre_chain_identity_serialized_assets_remain_readable():
    asset = Asset("ETH", asset_type="crypto")
    del asset.chain_id
    del asset.token_address
    del asset.decimals
    assert asset == Asset("ETH", asset_type="crypto")
    assert "chain_id" not in asset.to_dict()


def test_small_token_amounts_use_plain_decimal_wire_values(broker):
    broker.mode = "spot"
    source = Asset("ETH", asset_type="crypto", chain_id=11155111, token_address="native", decimals=18)
    dest = Asset("TOKEN", asset_type="crypto", chain_id=11155111, token_address="0x" + "2" * 40, decimals=6)
    broker.client.submit.return_value = {"state": "submitted"}
    broker.swap(source, dest, Decimal("1e-10"))
    assert broker.client.submit.call_args.args[0]["inputAmount"] == "0.0000000001"


@pytest.mark.parametrize("state", ["filled", "rejected", "canceled"])
def test_terminal_submission_response_updates_public_order(broker, state):
    item = order()
    broker.client.submit.return_value = observation(
        item, state, "0.001" if state == "filled" else "0", "50000" if state == "filled" else None
    )
    broker.submit_order(item)
    assert item not in broker._unprocessed_orders.get_list()
    assert item.status == {"filled": "fill", "rejected": "error", "canceled": "canceled"}[state]


def test_batch_rejects_atomic_legs_before_submission(broker):
    with pytest.raises(NotImplementedError, match="independent"):
        broker.submit_orders([order(), order()], is_multileg=True)
    broker.client.submit.assert_not_called()


def test_batch_independent_orders_register_in_input_order(broker):
    items = [order(), order()]
    broker.client.submit.side_effect = [observation(item, "open") for item in items]
    assert broker.submit_orders(items, is_multileg=False) == items
    assert all(broker.get_tracked_order(item.identifier) is item for item in items)
