from types import SimpleNamespace

import pytest

from lumibot.brokers.broker import LumibotBrokerAPIError
from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order


class _Response:
    status_code = 200
    text = "OK"

    def __init__(self, positions):
        self._positions = positions

    def json(self):
        return {
            "securitiesAccount": {
                "positions": self._positions,
            },
        }


class _Client:
    Account = SimpleNamespace(Fields=SimpleNamespace(POSITIONS="positions"))

    def __init__(self, positions):
        self._positions = positions

    def get_account(self, hash_value, fields):
        return _Response(self._positions)


class _CancelResponse:
    def __init__(self, status_code=204, text=""):
        self.status_code = status_code
        self.text = text


class _CancelClient:
    def __init__(self, response=None, exc=None):
        self.response = response if response is not None else _CancelResponse()
        self.exc = exc
        self.cancel_calls = []

    def cancel_order(self, order_id, account_hash):
        self.cancel_calls.append((order_id, account_hash))
        if self.exc:
            raise self.exc
        return self.response


class _Stream:
    def __init__(self):
        self.dispatched = []

    def dispatch(self, event, wait_until_complete=False, **payload):
        self.dispatched.append((event, wait_until_complete, payload))


def _position(asset_type, symbol, quantity=1, **instrument):
    return {
        "instrument": {
            "assetType": asset_type,
            "symbol": symbol,
            **instrument,
        },
        "longQuantity": quantity,
        "shortQuantity": 0,
        "averagePrice": 1.0,
        "marketValue": float(quantity),
    }


def _broker_with_positions(positions):
    broker = Schwab.__new__(Schwab)
    broker._broker_fully_ready = True
    broker.schwab_authorization_error = False
    broker.client = _Client(positions)
    broker.hash_value = "account-hash"
    return broker


def _broker_for_cancel(client=None, stream=None):
    broker = Schwab.__new__(Schwab)
    broker.schwab_authorization_error = False
    broker.client = client if client is not None else _CancelClient()
    broker.hash_value = "account-hash"
    broker.stream = stream
    return broker


def _order(status=Order.OrderStatus.SUBMITTED, identifier="order-123"):
    return Order(
        strategy="unit-test",
        asset=Asset("LW"),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        identifier=identifier,
        status=status,
    )


def test_schwab_pull_positions_skips_unsupported_mutual_funds():
    broker = _broker_with_positions(
        [
            _position("MUTUAL_FUND", "SWVXX", quantity=10),
            _position("EQUITY", "SPY", quantity=3),
        ]
    )

    positions = broker._pull_positions(SimpleNamespace(name="unit-test"))

    assert len(positions) == 1
    assert positions[0].asset.symbol == "SPY"
    assert positions[0].asset.asset_type == Asset.AssetType.STOCK
    assert positions[0].quantity == 3


def test_schwab_pull_positions_skips_unsupported_and_unknown_asset_types_without_losing_supported_assets():
    broker = _broker_with_positions(
        [
            _position("MUTUAL_FUND", "SWVXX", quantity=10),
            _position("BOND", "912797LG9", quantity=5),
            _position("UNRECOGNIZED_NEW_TYPE", "MYSTERY", quantity=7),
            _position("EQUITY", "SPY", quantity=3),
            _position("ETF", "QQQ", quantity=4),
            _position("COLLECTIVE_INVESTMENT", "CQQQ", quantity=2),
            _position("CASH", "USD", quantity=100),
            _position("MONEY_MARKET_FUND", "SWVXX", quantity=20),
            _position("CASH_EQUIVALENT", "CASH", quantity=30),
            _position("FUTURE", "/ES", quantity=1),
            _position("OPTION", "SPY   260116C00500000", quantity=1),
        ]
    )

    positions = broker._pull_positions(SimpleNamespace(name="unit-test"))

    by_symbol_and_type = {(position.asset.symbol, position.asset.asset_type): position for position in positions}
    assert ("SPY", Asset.AssetType.STOCK) in by_symbol_and_type
    assert ("QQQ", Asset.AssetType.STOCK) in by_symbol_and_type
    assert ("CQQQ", Asset.AssetType.STOCK) in by_symbol_and_type
    assert ("USD", Asset.AssetType.FOREX) in by_symbol_and_type
    assert ("SWVXX", Asset.AssetType.FOREX) in by_symbol_and_type
    assert ("CASH", Asset.AssetType.FOREX) in by_symbol_and_type
    assert ("/ES", Asset.AssetType.FUTURE) in by_symbol_and_type

    option_positions = [
        position
        for position in positions
        if position.asset.asset_type == Asset.AssetType.OPTION
    ]
    assert len(option_positions) == 1
    assert option_positions[0].asset.symbol == "SPY"
    assert option_positions[0].asset.strike == 500.0

    returned_symbols = {position.asset.symbol for position in positions}
    assert "912797LG9" not in returned_symbols
    assert "MYSTERY" not in returned_symbols
    # Mutual funds are intentionally not tracked as stock-like assets.
    assert ("SWVXX", Asset.AssetType.STOCK) not in by_symbol_and_type


def test_schwab_parse_simple_order_skips_unsupported_order_leg_types_without_dropping_supported_legs():
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "12345",
        "enteredTime": "2026-05-22T15:30:00+0000",
        "orderType": "MARKET",
        "status": "FILLED",
        "orderLegCollection": [
            {
                "instruction": "SELL",
                "quantity": 10,
                "orderLegType": "MUTUAL_FUND",
                "instrument": {"symbol": "SWVXX"},
            },
            {
                "instruction": "BUY",
                "quantity": 3,
                "orderLegType": "EQUITY",
                "instrument": {"symbol": "SPY"},
            },
        ],
    }

    parsed = broker._parse_simple_order(order, strategy_name="unit-test")

    assert len(parsed) == 1
    assert parsed[0].identifier == "12345"
    assert parsed[0].side == Order.OrderSide.BUY
    assert parsed[0].asset.symbol == "SPY"
    assert parsed[0].asset.asset_type == Asset.AssetType.STOCK


def test_schwab_parse_broker_order_skips_unsupported_only_order_history():
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "mutual-fund-activity",
        "enteredTime": "2026-05-22T15:30:00+0000",
        "orderType": "MARKET",
        "status": "FILLED",
        "orderLegCollection": [
            {
                "instruction": "SELL",
                "quantity": 10,
                "orderLegType": "MUTUAL_FUND",
                "instrument": {"symbol": "SWVXX"},
            },
        ],
    }

    assert broker._parse_broker_order(order, strategy_name="unit-test") is None


def test_schwab_parse_broker_order_skips_unsupported_exercise_order_history():
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "exercise-activity",
        "enteredTime": "2026-05-22T15:30:00+0000",
        "orderType": "EXERCISE",
        "status": "FILLED",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": 1,
                "orderLegType": "OPTION",
                "instrument": {"symbol": "SPY   260522C00500000"},
            },
        ],
    }

    assert broker._parse_broker_order(order, strategy_name="unit-test") is None


def test_schwab_cancel_order_calls_client_with_order_id_then_account_hash():
    client = _CancelClient()
    stream = _Stream()
    broker = _broker_for_cancel(client=client, stream=stream)
    order = _order()

    broker.cancel_order(order)

    assert client.cancel_calls == [("order-123", "account-hash")]
    assert stream.dispatched == [
        (broker.CANCELED_ORDER, True, {"order": order}),
    ]


def test_schwab_cancel_order_marks_canceled_without_stream_after_success():
    client = _CancelClient()
    broker = _broker_for_cancel(client=client, stream=None)
    order = _order()

    broker.cancel_order(order)

    assert client.cancel_calls == [("order-123", "account-hash")]
    assert order.status == broker.CANCELED_ORDER
    assert order.is_canceled()


def test_schwab_cancel_order_noops_for_terminal_orders():
    client = _CancelClient()
    broker = _broker_for_cancel(client=client)

    broker.cancel_order(_order(status=Order.OrderStatus.FILLED))
    broker.cancel_order(_order(status=Order.OrderStatus.CANCELED))

    assert client.cancel_calls == []


def test_schwab_cancel_order_requires_identifier():
    broker = _broker_for_cancel()
    order = _order()
    order.identifier = None

    with pytest.raises(ValueError, match="Order identifier is not set"):
        broker.cancel_order(order)


def test_schwab_cancel_order_raises_on_http_error_without_marking_canceled():
    client = _CancelClient(response=_CancelResponse(status_code=400, text="cannot cancel"))
    broker = _broker_for_cancel(client=client)
    order = _order()

    with pytest.raises(LumibotBrokerAPIError, match="HTTP 400"):
        broker.cancel_order(order)

    assert client.cancel_calls == [("order-123", "account-hash")]
    assert order.status == Order.OrderStatus.SUBMITTED
