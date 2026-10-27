"""Bitunix submission contracts; all exchange traffic is intercepted locally."""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from lumibot.brokers.bitunix import Bitunix
from lumibot.entities import Asset, Order, Position
from lumibot.tools.bitunix_helpers import BitUnixClient


@pytest.fixture
def submission():
    client = BitUnixClient(api_key="test-key", secret_key="test-secret")
    client.get_trading_pairs = MagicMock(
        return_value={
            "code": 0,
            "data": [{"symbol": "BTCUSDT", "basePrecision": 4, "quotePrecision": 1, "minTradeVolume": "0.0001"}],
        }
    )
    client.change_position_mode = MagicMock(
        return_value={
            "code": 0,
            "data": [{"positionMode": "HEDGE"}],
        }
    )
    client.get_position_mode = MagicMock(return_value={"code": 0, "data": {"positionMode": "HEDGE"}})
    client.change_leverage = MagicMock(return_value={"code": 0})
    client.get_positions = MagicMock(
        return_value={
            "code": 0,
            "data": [
                {
                    "symbol": "BTCUSDT",
                    "side": "LONG",
                    "positionId": "test-long",
                    "qty": "0.02",
                    "positionMode": "HEDGE",
                },
                {
                    "symbol": "BTCUSDT",
                    "side": "SHORT",
                    "positionId": "test-short",
                    "qty": "0.02",
                    "positionMode": "HEDGE",
                },
            ],
        }
    )
    with patch("lumibot.brokers.bitunix.BitUnixClient", return_value=client), patch(
        "lumibot.brokers.bitunix.BitunixData"
    ), patch("requests.request") as request:
        request.return_value.json.return_value = {"code": 0, "data": {"orderId": "test-order"}}
        broker = Bitunix({"API_KEY": "test-key", "API_SECRET": "test-secret"}, connect_stream=False)
        broker._process_trade_event = MagicMock()
        yield broker, client, request


def make_order(quantity="0.008868641", **kwargs):
    return Order(
        "test-strategy", Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE, leverage=10), Decimal(quantity), **kwargs
    )


@pytest.mark.parametrize("mode", ["HEDGE", "ONE_WAY"])
def test_submission_reads_account_mode_without_switching_it(submission, mode):
    broker, client, request = submission
    # Official GET position_mode response; account mode is a user choice.
    client.get_position_mode = MagicMock(return_value={"code": 0, "data": {"positionMode": mode}})
    order = broker._submit_order(make_order())
    assert order.status == Order.OrderStatus.SUBMITTED
    client.change_position_mode.assert_not_called()
    client.get_position_mode.assert_called_once_with()
    if mode == "ONE_WAY":
        assert order.position_side is None
        assert "tradeSide" not in json.loads(request.call_args.kwargs["data"])


@pytest.mark.parametrize("side,wire_side", [(Order.OrderSide.SELL, "SELL"), (Order.OrderSide.BUY, "BUY")])
def test_one_way_reduce_only_preserves_execution_side(submission, side, wire_side):
    broker, client, request = submission
    client.get_position_mode = MagicMock(return_value={"code": 0, "data": {"positionMode": "ONE_WAY"}})
    for position in client.get_positions.return_value["data"]:
        position["positionMode"] = "ONE_WAY"
    order = make_order(side=side)
    order.reduce_only = True
    assert broker._submit_order(order).status == Order.OrderStatus.SUBMITTED
    body = json.loads(request.call_args.kwargs["data"])
    assert body["side"] == wire_side
    assert body["reduceOnly"] is True
    assert "tradeSide" not in body
    assert order.position_side is None
    client.change_position_mode.assert_not_called()


def test_one_way_snapshot_and_order_use_net_position_identity(submission):
    broker, client, _ = submission
    client.get_positions.return_value["data"] = [
        {"symbol": "BTCUSDT", "side": "LONG", "positionId": "net-position", "qty": "0.02",
         "positionMode": "ONE_WAY", "avgOpenPrice": "60000"}]
    position = broker._pull_positions(None)[0]
    assert position.position_side is None
    order = broker._parse_broker_order({"orderId": "net-order", "symbol": "BTCUSDT", "side": "SELL",
                                         "positionMode": "ONE_WAY", "orderType": "MARKET",
                                         "status": "FILLED", "qty": "0.01", "reduceOnly": True}, "strategy")
    assert order.position_side is None
    assert order.side == Order.OrderSide.SELL
    assert order.reduce_only


def test_client_position_mode_read_uses_documented_get_endpoint():
    client = BitUnixClient(api_key="test-key", secret_key="test-secret")
    client._request = MagicMock(return_value={"code": 0, "data": {"positionMode": "ONE_WAY"}})
    assert client.get_position_mode()["data"]["positionMode"] == "ONE_WAY"
    client._request.assert_called_once_with(method="GET", endpoint="/api/v1/futures/account/position_mode")


@pytest.mark.parametrize("hedged,native", [(True, "HEDGE"), (False, "ONE_WAY")])
def test_position_mode_changes_are_explicit(submission, hedged, native):
    broker, client, request = submission
    broker.set_position_mode(hedged)
    client.change_position_mode.assert_called_once_with(native)
    request.assert_not_called()


def test_generic_broker_submit_orders_rejects_a_null_entry(submission):
    broker, _, request = submission

    with pytest.raises(ValueError, match="null order"):
        broker.submit_orders([None])

    request.assert_not_called()


@pytest.mark.parametrize("value", [0.00000001, Decimal("0.00000001"), "0.00000001"])
def test_client_serializes_numeric_fields_as_plain_decimal_strings(value):
    client = BitUnixClient(api_key="test-key", secret_key="test-secret")
    with patch("requests.request") as request:
        client.place_order(
            "BTCUSDT", "BUY", "LIMIT", value, price=value, take_profit_price=value, stop_loss_price=value
        )
    body = json.loads(request.call_args.kwargs["data"])
    for field in ("qty", "price", "tpPrice", "slPrice"):
        assert body[field] == "0.00000001"


def test_client_normalizes_native_price_kwargs():
    client = BitUnixClient(api_key="test-key", secret_key="test-secret")
    client._request = MagicMock()
    client.place_order("BTCUSDT", "BUY", "MARKET", "0.01", tpPrice=61000.1, slPrice=59000.1)
    body = client._request.call_args.kwargs["json_body"]
    assert body["tpPrice"] == "61000.1"
    assert body["slPrice"] == "59000.1"


def test_client_defaults_reduce_only_to_close():
    client = BitUnixClient(api_key="test-key", secret_key="test-secret")
    client._request = MagicMock()
    client.place_order("BTCUSDT", "BUY", "MARKET", "0.01", reduceOnly=True)
    assert client._request.call_args.kwargs["json_body"]["tradeSide"] == "CLOSE"


@pytest.mark.parametrize("wire_side,execution_side", [("BUY", Order.OrderSide.SELL), ("SELL", Order.OrderSide.BUY)])
def test_parse_close_preserves_lumibot_execution_side(submission, wire_side, execution_side):
    broker, _, _ = submission
    order = broker._parse_broker_order(
        {
            "orderId": "test-order",
            "symbol": "BTCUSDT",
            "side": wire_side,
            "tradeSide": "CLOSE",
            "orderType": "MARKET",
            "status": "FILLED",
            "qty": "0.01",
        },
        "test-strategy",
    )
    assert order.side == execution_side
    assert order.reduce_only is True


def test_crypto_future_constructor_preserves_leverage():
    assert Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE, leverage=10).leverage == 10
    assert Asset("SPY", leverage=10).leverage == 1


def test_submit_quantizes_wire_quantity_and_tracked_order(submission):
    broker, client, request = submission
    order = broker._submit_order(make_order())
    body = json.loads(request.call_args.kwargs["data"])
    assert body["qty"] == "0.0088"
    assert order.quantity == Decimal("0.0088")
    assert order.status == Order.OrderStatus.SUBMITTED
    assert body["tradeSide"] == "OPEN"
    client.change_leverage.assert_called_once_with(symbol="BTCUSDT", leverage=10, margin_coin="USDT")
    broker._submit_order(make_order("0.01"))
    client.get_trading_pairs.assert_called_once_with(symbols="BTCUSDT")


def test_submit_quantizes_all_prices(submission):
    broker, _, request = submission
    order = make_order(order_type=Order.OrderType.LIMIT, limit_price=Decimal("60000.19"))
    order.take_profit_price = Decimal("61000.19")
    order.stop_loss_price = Decimal("59000.19")
    broker._submit_order(order)
    body = json.loads(request.call_args.kwargs["data"])
    assert body["price"] == "60000.1"
    assert body["tpPrice"] == "61000.1"
    assert body["slPrice"] == "59000.1"
    assert order.limit_price == Decimal("60000.1")


def test_simulated_exchange_rejects_overprecision_or_numeric_quantity(submission):
    broker, _, request = submission

    def exchange_response(**kwargs):
        body = json.loads(kwargs["data"])
        qty = body["qty"]
        valid = isinstance(qty, str) and Decimal(qty).as_tuple().exponent >= -4
        response = MagicMock()
        response.json.return_value = (
            {"code": 0, "data": {"orderId": "test-order"}} if valid else {"code": 10002, "msg": "Parameter error"}
        )
        return response

    request.side_effect = exchange_response
    assert broker._submit_order(make_order()).status == Order.OrderStatus.SUBMITTED


@pytest.mark.parametrize("quantity", ["0.00009999", "0"])
def test_under_minimum_is_rejected_locally(submission, quantity):
    broker, client, request = submission
    order = broker._submit_order(make_order(quantity))
    assert order.status == Order.OrderStatus.ERROR
    assert "minTradeVolume" in str(order._error)
    assert "BTCUSDT" in str(order._error)
    request.assert_not_called()
    client.change_leverage.assert_not_called()


@pytest.mark.parametrize(
    "response",
    [
        None,
        {"code": 10001},
        {"code": 0, "data": []},
        {"code": 0, "data": [{"symbol": "BTCUSDT", "basePrecision": -1, "minTradeVolume": "0.0001"}]},
    ],
)
def test_missing_or_invalid_pair_rules_fail_closed_and_retry(submission, response):
    broker, client, request = submission
    valid = client.get_trading_pairs.return_value
    client.get_trading_pairs.side_effect = [response, valid]
    order = broker._submit_order(make_order())
    assert order.status == Order.OrderStatus.ERROR
    assert "trading pair" in str(order._error).lower()
    request.assert_not_called()
    assert broker._submit_order(make_order()).status == Order.OrderStatus.SUBMITTED


@pytest.mark.parametrize(
    "failure", [RuntimeError("mode unavailable"), {"code": 20009}, {"code": 0, "data": {"positionMode": "INVALID"}}]
)
def test_failed_mode_read_blocks_unidentified_open_and_retries(submission, failure):
    broker, client, request = submission
    # No implicit account mutation: unreadable/invalid mode is retried as a read.
    client.get_position_mode.side_effect = [failure, client.get_position_mode.return_value]
    order = broker._submit_order(make_order())
    assert order.status == Order.OrderStatus.ERROR
    assert "position mode" in str(order._error)
    request.assert_not_called()
    assert broker._submit_order(make_order()).status == Order.OrderStatus.SUBMITTED


def test_confirmed_existing_hedge_position_allows_close_after_mode_read_failure(submission):
    broker, client, request = submission
    client.get_position_mode.side_effect = RuntimeError("mode read unavailable")
    order = make_order(side=Order.OrderSide.SELL)
    order.reduce_only = True

    result = broker._submit_order(order)

    assert result.status == Order.OrderStatus.SUBMITTED
    assert json.loads(request.call_args.kwargs["data"])["positionId"] == "test-long"


@pytest.mark.parametrize("position_mode", [None, "ONE_WAY"])
def test_reduce_only_close_uses_confirmed_live_position_mode_after_read_failure(
    submission, position_mode
):
    broker, client, request = submission
    client.get_position_mode.side_effect = RuntimeError("mode unavailable")
    for position in client.get_positions.return_value["data"]:
        if position_mode is None:
            position.pop("positionMode", None)
        else:
            position["positionMode"] = position_mode
    order = make_order(side=Order.OrderSide.SELL)
    order.reduce_only = True

    result = broker._submit_order(order)
    # ONE_WAY is a supported mode, not an initialization failure. Preserve the
    # rejection for a genuinely unidentified close protocol (missing mode).
    if position_mode is None:
        assert result.status == Order.OrderStatus.ERROR
        request.assert_not_called()
    else:
        assert result.status == Order.OrderStatus.SUBMITTED
        assert json.loads(request.call_args.kwargs["data"])["side"] == "SELL"


@pytest.mark.parametrize(
    "side,wire_side,position_id",
    [
        (Order.OrderSide.SELL, "BUY", "test-long"),
        (Order.OrderSide.BUY, "SELL", "test-short"),
    ],
)
def test_reduce_only_closes_use_hedge_position_contract(submission, side, wire_side, position_id):
    broker, _, request = submission
    order = make_order(side=side)
    order.reduce_only = True
    broker._submit_order(order)
    body = json.loads(request.call_args.kwargs["data"])
    assert body["tradeSide"] == "CLOSE"
    assert body["side"] == wire_side
    assert body["positionId"] == position_id
    assert body["reduceOnly"] is True
    assert order.side == side  # LumiBot execution side remains opposite to the position.


@pytest.mark.parametrize("quantity,wire_side", [("0.02", "BUY"), ("-0.02", "SELL")])
def test_close_position_fraction_reaches_wire(submission, quantity, wire_side):
    broker, _, request = submission
    asset = Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE)
    broker.get_tracked_position = MagicMock(return_value=Position("test-strategy", asset, Decimal(quantity)))
    broker.submit_order = broker._submit_order
    order = broker.close_position("test-strategy", asset, fraction=0.5)
    assert order.status == Order.OrderStatus.SUBMITTED
    body = json.loads(request.call_args.kwargs["data"])
    assert body["qty"] == "0.0100"
    assert body["tradeSide"] == "CLOSE"
    assert body["side"] == wire_side


@pytest.mark.parametrize("quantity,wire_side", [("0.02", "BUY"), ("-0.02", "SELL")])
@pytest.mark.parametrize("fraction", [0.4, 1.0])
@pytest.mark.parametrize("cached_leverage", [None, 10])
def test_close_position_preserves_exchange_leverage(submission, quantity, wire_side, fraction, cached_leverage):
    broker, client, request = submission
    # Reconstructed assets default to 1x; closing must not apply that default
    # to an existing position, including after a restart with an empty cache.
    asset = Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE)
    broker.get_tracked_position = MagicMock(return_value=Position("test-strategy", asset, Decimal(quantity)))
    broker.submit_order = broker._submit_order
    if cached_leverage is not None:
        broker.current_leverage[asset.symbol] = cached_leverage
    previous_leverage = broker.current_leverage.copy()

    order = broker.close_position("test-strategy", asset, fraction=fraction)

    client.change_leverage.assert_not_called()
    assert broker.current_leverage == previous_leverage
    assert order.status == Order.OrderStatus.SUBMITTED
    body = json.loads(request.call_args.kwargs["data"])
    assert body["tradeSide"] == "CLOSE"
    assert body["side"] == wire_side
    assert body["positionId"] == ("test-long" if wire_side == "BUY" else "test-short")
    assert Decimal(body["qty"]) == abs(Decimal(quantity)) * Decimal(str(fraction))


def test_close_without_matching_position_fails_closed(submission):
    broker, client, request = submission
    client.get_positions.return_value = {"code": 0, "data": []}
    order = make_order(side=Order.OrderSide.SELL)
    order.reduce_only = True
    broker._submit_order(order)
    assert order.status == Order.OrderStatus.ERROR
    assert "position" in str(order._error).lower()
    request.assert_not_called()


def test_minimum_quantity_is_accepted(submission):
    broker, _, request = submission
    assert broker._submit_order(make_order("0.0001")).status == Order.OrderStatus.SUBMITTED
    assert json.loads(request.call_args.kwargs["data"])["qty"] == "0.0001"


@pytest.mark.parametrize("fraction", [0, -0.1, 1.1, float("nan")])
def test_invalid_close_fraction_never_submits(submission, fraction):
    broker, _, request = submission
    asset = Asset("BTCUSDT", Asset.AssetType.CRYPTO_FUTURE)
    broker.get_tracked_position = MagicMock(return_value=Position("test-strategy", asset, Decimal("0.02")))
    with pytest.raises(ValueError, match="fraction"):
        broker.close_position("test-strategy", asset, fraction=fraction)
    request.assert_not_called()


@pytest.mark.parametrize("failure", ["ambiguous", "request_failed", "missing_id"])
def test_close_position_lookup_failures_never_submit(submission, failure):
    broker, client, request = submission
    if failure == "ambiguous":
        client.get_positions.return_value["data"].append(client.get_positions.return_value["data"][0].copy())
    elif failure == "request_failed":
        client.get_positions.return_value = {"code": 10001}
    else:
        del client.get_positions.return_value["data"][0]["positionId"]
    order = make_order(side=Order.OrderSide.SELL)
    order.reduce_only = True
    assert broker._submit_order(order).status == Order.OrderStatus.ERROR
    request.assert_not_called()


def test_exchange_short_position_stays_short_through_close(submission):
    broker, client, request = submission
    client.get_positions.return_value["data"] = [client.get_positions.return_value["data"][1]]
    position = broker._pull_positions(None)[0]
    assert position.quantity < 0
    broker.get_tracked_position = MagicMock(return_value=position)
    broker.submit_order = broker._submit_order
    assert broker.close_position("test-strategy", position.asset).status == Order.OrderStatus.SUBMITTED
    body = json.loads(request.call_args.kwargs["data"])
    assert body["side"] == "SELL"
    assert body["positionId"] == "test-short"
    assert body["tradeSide"] == "CLOSE"
