import asyncio
import logging
import time
from datetime import date
from threading import Barrier, BrokenBarrierError, Event, RLock, Thread
from types import SimpleNamespace

import pytest

from lumibot.brokers.broker import LumibotBrokerAPIError
from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order, Position
from lumibot.trading_builtins import SafeList


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


class _OrderClient:
    def __init__(self, response=None):
        self.response = response if response is not None else _OrderResponse()
        self.get_order_calls = []

    def get_order(self, order_id, account_hash):
        self.get_order_calls.append((order_id, account_hash))
        return self.response


class _RateLimitedOrderClient:
    def __init__(self):
        self.get_order_calls = []

    def get_order(self, order_id, account_hash):
        self.get_order_calls.append((order_id, account_hash))
        return SimpleNamespace(
            status_code=429,
            headers={"Retry-After": "2"},
            text="too many requests",
        )


class _OrderResponse:
    status_code = 200
    text = "OK"

    def json(self):
        return {
            "orderId": "order-123",
            "orderType": "LIMIT",
            "status": "WORKING",
            "orderLegCollection": [
                {
                    "instruction": "BUY",
                    "quantity": 1,
                    "orderLegType": "EQUITY",
                    "instrument": {"symbol": "SPY"},
                },
            ],
        }


class _ReplaceResponse:
    status_code = 201
    text = ""

    def __init__(self, order_id="replacement-456"):
        self.headers = {"Location": f"https://api.schwabapi.com/trader/v1/accounts/hash/orders/{order_id}"}


class _ReplaceClient:
    def __init__(self, original_order=None, response=None):
        self.original_order = original_order if original_order is not None else _option_order_payload()
        self.response = response if response is not None else _ReplaceResponse()
        self.get_order_calls = []
        self.replace_calls = []

    def get_order(self, order_id, account_hash):
        self.get_order_calls.append((order_id, account_hash))
        return SimpleNamespace(status_code=200, json=lambda: self.original_order)

    def replace_order(self, account_hash, order_id, order_spec):
        self.replace_calls.append((account_hash, order_id, order_spec))
        return self.response


class _PlaceClient:
    def __init__(self):
        self.place_calls = []

    def place_order(self, account_hash, order_spec):
        self.place_calls.append((account_hash, order_spec))
        return SimpleNamespace(
            status_code=201,
            headers={
                "Location": "https://api.schwabapi.com/trader/v1/accounts/hash/orders/submitted-123",
            },
            text="",
        )


class _Stream:
    def __init__(self):
        self.dispatched = []

    def dispatch(self, event, wait_until_complete=False, **payload):
        self.dispatched.append((event, wait_until_complete, payload))


class _AccountActivityStreamClient:
    def __init__(self):
        self.calls = []
        self.handler = None

    def add_account_activity_handler(self, handler):
        self.calls.append("add_handler")
        self.handler = handler

    async def login(self):
        self.calls.append("login")

    async def account_activity_sub(self):
        self.calls.append("subscribe")


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


def _broker_for_order_pull(client=None):
    broker = Schwab.__new__(Schwab)
    broker.schwab_authorization_error = False
    broker.client = client if client is not None else _OrderClient()
    broker.hash_value = "account-hash"
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


def _observed_order(status, cumulative_filled=0, average_fill_price=None, identifier="order-123"):
    order = _order(status=status, identifier=identifier)
    order._schwab_cumulative_filled_quantity = float(cumulative_filled)
    order._schwab_average_fill_price = average_fill_price
    return order


def _broker_for_lifecycle(stored_order):
    broker = Schwab.__new__(Schwab)
    broker._schwab_observed_fill_quantities = {}
    broker._schwab_terminal_observations = set()
    broker.get_tracked_order = lambda identifier: stored_order if identifier == stored_order.identifier else None
    broker.get_all_orders = lambda: [stored_order]
    broker._process_new_order = lambda order: order
    broker._lifecycle_events = []

    def process_trade_event(order, event, **payload):
        broker._lifecycle_events.append((event, payload))
        if event == broker.PARTIALLY_FILLED_ORDER:
            order.status = Order.OrderStatus.PARTIALLY_FILLED
        elif event == broker.FILLED_ORDER:
            order.status = Order.OrderStatus.FILLED
        elif event == broker.CANCELED_ORDER:
            order.status = Order.OrderStatus.CANCELED
        elif event == broker.ERROR_ORDER:
            order.status = Order.OrderStatus.ERROR

    broker._process_trade_event = process_trade_event
    return broker


def _option_asset():
    return Asset(
        "LW",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 5, 29),
        strike=38.0,
        right="CALL",
    )


def _option_order_payload(order_id="order-123", status="WORKING"):
    return {
        "orderId": order_id,
        "orderType": "LIMIT",
        "status": status,
        "orderLegCollection": [
            {
                "instruction": "BUY_TO_OPEN",
                "quantity": 1,
                "orderLegType": "OPTION",
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": "LW    260529C00038000",
                    "putCall": "CALL",
                    "underlyingSymbol": "LW",
                },
            },
        ],
    }


def _option_order(identifier="order-123", limit_price=4.84):
    return Order(
        strategy="unit-test",
        asset=_option_asset(),
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=limit_price,
        identifier=identifier,
        status=Order.OrderStatus.SUBMITTED,
    )


def _stock_market_order(side=Order.OrderSide.SELL_SHORT, quantity=100):
    return Order(
        strategy="unit-test",
        asset=Asset("LW", asset_type=Asset.AssetType.STOCK),
        quantity=quantity,
        side=side,
        order_type=Order.OrderType.MARKET,
    )


def _stock_limit_order(side=Order.OrderSide.SELL, quantity=1, limit_price=20.0):
    return Order(
        strategy="unit-test",
        asset=Asset("TSLL", asset_type=Asset.AssetType.STOCK),
        quantity=quantity,
        side=side,
        order_type=Order.OrderType.LIMIT,
        limit_price=limit_price,
    )


def _stock_stop_order(side=Order.OrderSide.SELL, quantity=1, stop_price=5.0):
    return Order(
        strategy="unit-test",
        asset=Asset("TSLL", asset_type=Asset.AssetType.STOCK),
        quantity=quantity,
        side=side,
        order_type=Order.OrderType.STOP,
        stop_price=stop_price,
    )


def test_schwab_pull_positions_preserves_mutual_funds_as_unknown_or_cash_like_records():
    broker = _broker_with_positions(
        [
            _position("MUTUAL_FUND", "SWVXX", quantity=10),
            _position("EQUITY", "SPY", quantity=3),
        ]
    )

    positions = broker._pull_positions(SimpleNamespace(name="unit-test"))

    by_symbol = {position.asset.symbol: position for position in positions}
    assert by_symbol["SPY"].asset.asset_type == Asset.AssetType.STOCK
    assert by_symbol["SPY"].quantity == 3
    assert by_symbol["SWVXX"].asset.asset_type == Asset.AssetType.UNKNOWN
    assert by_symbol["SWVXX"].raw_asset_type == "MUTUAL_FUND"
    assert by_symbol["SWVXX"].broker_parse_warning is None
    assert by_symbol["SWVXX"].broker_parse_degraded is False


def test_schwab_pull_positions_preserves_unknown_asset_types_without_losing_supported_assets():
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
    assert ("912797LG9", Asset.AssetType.UNKNOWN) in by_symbol_and_type
    assert ("MYSTERY", Asset.AssetType.UNKNOWN) in by_symbol_and_type

    option_positions = [
        position
        for position in positions
        if position.asset.asset_type == Asset.AssetType.OPTION
    ]
    assert len(option_positions) == 1
    assert option_positions[0].asset.symbol == "SPY"
    assert option_positions[0].asset.strike == 500.0

    assert by_symbol_and_type[("MYSTERY", Asset.AssetType.UNKNOWN)].raw_asset_type == "UNRECOGNIZED_NEW_TYPE"


def test_schwab_pull_positions_skips_malformed_quantity_without_losing_supported_assets(caplog):
    broker = _broker_with_positions(
        [
            {
                "instrument": {
                    "assetType": "EQUITY",
                    "symbol": "BROKEN",
                },
                "longQuantity": "not-a-number",
                "shortQuantity": 0,
                "averagePrice": 1.0,
                "marketValue": 1.0,
            },
            _position("EQUITY", "SPY", quantity=3),
        ]
    )

    with caplog.at_level(logging.WARNING):
        positions = broker._pull_positions(SimpleNamespace(name="unit-test"))

    assert len(positions) == 1
    assert positions[0].asset.symbol == "SPY"
    assert positions[0].quantity == 3
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_schwab_parse_simple_order_preserves_unknown_legs_without_dropping_supported_legs():
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

    assert len(parsed) == 2
    by_symbol = {order.asset.symbol: order for order in parsed}
    assert by_symbol["SWVXX"].asset.asset_type == Asset.AssetType.UNKNOWN
    assert by_symbol["SWVXX"].raw_asset_type == "MUTUAL_FUND"
    assert by_symbol["SPY"].identifier == "12345"
    assert by_symbol["SPY"].side == Order.OrderSide.BUY
    assert by_symbol["SPY"].asset.asset_type == Asset.AssetType.STOCK


def test_schwab_option_replacement_uses_option_builder_and_updates_identifier():
    client = _ReplaceClient()
    broker = Schwab.__new__(Schwab)
    broker.schwab_authorization_error = False
    broker.client = client
    broker.hash_value = "account-hash"
    order = _option_order()

    broker._modify_order(order, limit_price=4.75)

    assert client.get_order_calls == [("order-123", "account-hash")]
    assert len(client.replace_calls) == 1
    account_hash, original_id, replacement_spec = client.replace_calls[0]
    assert account_hash == "account-hash"
    assert original_id == "order-123"
    assert replacement_spec["orderType"] == "LIMIT"
    assert replacement_spec["price"] == "4.75"
    assert replacement_spec["duration"] == "DAY"
    assert replacement_spec["session"] == "NORMAL"
    assert replacement_spec["orderLegCollection"][0]["instruction"] == "BUY_TO_OPEN"
    assert replacement_spec["orderLegCollection"][0]["instrument"]["assetType"] == "OPTION"
    assert replacement_spec["orderLegCollection"][0]["instrument"]["symbol"] == "LW    260529C00038000"
    assert order.previous_identifiers == ["order-123"]
    assert order.identifier == "replacement-456"
    assert order.limit_price == 4.75


def test_schwab_stock_market_submit_uses_normal_session():
    client = _PlaceClient()
    stream = _Stream()
    broker = Schwab.__new__(Schwab)
    broker.name = "Schwab"
    broker.schwab_authorization_error = False
    broker.client = client
    broker.hash_value = "account-hash"
    broker.stream = stream
    broker._unprocessed_orders = SafeList(RLock())
    order = _stock_market_order(side=Order.OrderSide.BUY, quantity=12)

    submitted = broker._submit_order(order)

    assert submitted is order
    assert client.place_calls[0][0] == "account-hash"
    order_spec = client.place_calls[0][1]
    assert order_spec["session"] == "NORMAL"
    assert order_spec["duration"] == "DAY"
    assert order_spec["orderType"] == "MARKET"
    assert order.identifier == "submitted-123"
    assert stream.dispatched[-1][0] == broker.NEW_ORDER


def test_schwab_stock_limit_submit_uses_seamless_session():
    client = _PlaceClient()
    stream = _Stream()
    broker = Schwab.__new__(Schwab)
    broker.name = "Schwab"
    broker.schwab_authorization_error = False
    broker.client = client
    broker.hash_value = "account-hash"
    broker.stream = stream
    broker._unprocessed_orders = SafeList(RLock())
    order = _stock_limit_order(side=Order.OrderSide.BUY, limit_price=10.0)

    submitted = broker._submit_order(order)

    assert submitted is order
    assert client.place_calls[0][0] == "account-hash"
    order_spec = client.place_calls[0][1]
    assert order_spec["session"] == "SEAMLESS"
    assert order_spec["duration"] == "DAY"
    assert order.identifier == "submitted-123"
    assert stream.dispatched[-1][0] == broker.NEW_ORDER


def test_schwab_stock_market_replacement_spec_uses_normal_session():
    broker = Schwab.__new__(Schwab)
    broker.name = "Schwab"
    order = _stock_market_order(side=Order.OrderSide.SELL, quantity=1)

    order_spec = broker._prepare_stock_order_spec(order)

    assert order_spec["session"] == "NORMAL"
    assert order_spec["duration"] == "DAY"
    assert order_spec["orderType"] == "MARKET"


def test_schwab_stock_submit_succeeds_without_stream():
    client = _PlaceClient()
    broker = Schwab.__new__(Schwab)
    broker.name = "Schwab"
    broker.schwab_authorization_error = False
    broker.client = client
    broker.hash_value = "account-hash"
    broker.stream = None
    broker._unprocessed_orders = SafeList(RLock())
    order = _stock_limit_order(side=Order.OrderSide.BUY, limit_price=10.0)

    submitted = broker._submit_order(order)

    assert submitted is order
    assert order.identifier == "submitted-123"
    assert order.status == Order.OrderStatus.SUBMITTED


def test_schwab_stock_limit_replacement_spec_uses_seamless_session():
    broker = Schwab.__new__(Schwab)
    broker.name = "Schwab"
    order = _stock_limit_order(side=Order.OrderSide.SELL, limit_price=20.0)

    order_spec = broker._prepare_stock_order_spec(order, limit_price=19.5)

    assert order_spec["session"] == "SEAMLESS"
    assert order_spec["duration"] == "DAY"
    assert order_spec["price"] in {"19.50", "19.5000"}


def test_schwab_prepare_oto_order_builder_builds_trigger_with_cross_asset_child():
    broker = Schwab.__new__(Schwab)
    parent = Order(
        strategy="unit-test",
        asset=_option_asset(),
        quantity=1,
        side=Order.OrderSide.SELL_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
        limit_price=0.05,
        order_class=Order.OrderClass.OTO,
        child_orders=[_stock_market_order()],
    )

    order_builder = broker._prepare_oto_order_builder(parent)
    order_spec = broker._build_order_spec_from_builder(order_builder, parent.time_in_force)

    assert order_spec["orderStrategyType"] == "TRIGGER"
    assert order_spec["orderType"] == "LIMIT"
    assert order_spec["price"] in {"0.05", "0.0500"}
    assert order_spec["orderLegCollection"][0]["instruction"] == "SELL_TO_OPEN"
    assert order_spec["orderLegCollection"][0]["instrument"]["assetType"] == "OPTION"
    assert order_spec["orderLegCollection"][0]["instrument"]["symbol"] == "LW    260529C00038000"

    assert len(order_spec["childOrderStrategies"]) == 1
    child_spec = order_spec["childOrderStrategies"][0]
    assert child_spec["orderStrategyType"] == "SINGLE"
    assert child_spec["orderType"] == "MARKET"
    assert child_spec["orderLegCollection"][0]["instruction"] == "SELL_SHORT"
    assert child_spec["orderLegCollection"][0]["instrument"]["assetType"] == "EQUITY"
    assert child_spec["orderLegCollection"][0]["instrument"]["symbol"] == "LW"


def test_schwab_prepare_oco_order_builder_builds_oco_exit_pair():
    broker = Schwab.__new__(Schwab)
    order = Order(
        strategy="unit-test",
        asset=Asset("TSLL", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.SELL,
        order_class=Order.OrderClass.OCO,
        child_orders=[
            _stock_limit_order(limit_price=20.0),
            _stock_stop_order(stop_price=5.0),
        ],
    )

    order_builder = broker._prepare_oco_order_builder(order)
    order_spec = broker._build_order_spec_from_builder(order_builder, order.time_in_force, apply_defaults=False)

    assert order_spec["orderStrategyType"] == "OCO"
    assert "orderLegCollection" not in order_spec
    assert len(order_spec["childOrderStrategies"]) == 2
    assert order_spec["childOrderStrategies"][0]["orderType"] == "LIMIT"
    assert order_spec["childOrderStrategies"][0]["price"] in {"20.00", "20.0000"}
    assert order_spec["childOrderStrategies"][1]["orderType"] == "STOP"
    assert order_spec["childOrderStrategies"][1]["stopPrice"] == "5.0"


def test_schwab_prepare_bracket_order_builder_builds_trigger_with_oco_children():
    broker = Schwab.__new__(Schwab)
    order = Order(
        strategy="unit-test",
        asset=Asset("TSLL", asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=Order.OrderType.LIMIT,
        limit_price=10.0,
        order_class=Order.OrderClass.BRACKET,
        child_orders=[
            _stock_limit_order(limit_price=20.0),
            _stock_stop_order(stop_price=5.0),
        ],
    )

    order_builder = broker._prepare_bracket_order_builder(order)
    order_spec = broker._build_order_spec_from_builder(order_builder, order.time_in_force)

    assert order_spec["orderStrategyType"] == "TRIGGER"
    assert order_spec["orderType"] == "LIMIT"
    assert order_spec["price"] in {"10.00", "10.0000"}
    assert order_spec["orderLegCollection"][0]["instruction"] == "BUY"
    assert len(order_spec["childOrderStrategies"]) == 1

    oco_spec = order_spec["childOrderStrategies"][0]
    assert oco_spec["orderStrategyType"] == "OCO"
    assert len(oco_spec["childOrderStrategies"]) == 2
    assert oco_spec["childOrderStrategies"][0]["orderType"] == "LIMIT"
    assert oco_spec["childOrderStrategies"][1]["orderType"] == "STOP"


def test_schwab_prepare_option_replacement_spec_restores_order_on_failure(monkeypatch):
    broker = Schwab.__new__(Schwab)
    order = _option_order(limit_price=4.84)

    monkeypatch.setattr(broker, "_build_order_spec_from_builder", lambda *_args, **_kwargs: None)

    assert broker._prepare_option_order_spec(order, limit_price=4.75, tag="replacement") is None
    assert order.limit_price == 4.84
    assert order.tag == ""


def test_schwab_parse_broker_order_preserves_unknown_only_order_history():
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

    parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed is not None
    assert parsed.identifier == "mutual-fund-activity"
    assert parsed.asset.symbol == "SWVXX"
    assert parsed.asset.asset_type == Asset.AssetType.UNKNOWN
    assert parsed.status == Order.OrderStatus.FILLED


def test_schwab_parse_broker_order_preserves_known_exercise_order_history_without_warning(caplog):
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

    with caplog.at_level(logging.WARNING):
        parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed is not None
    assert parsed.identifier == "exercise-activity"
    assert parsed.order_type == Order.OrderType.UNKNOWN
    assert parsed.raw_order_type == "EXERCISE"
    assert parsed.status == Order.OrderStatus.FILLED
    assert "Unknown Schwab order type 'EXERCISE'" not in caplog.text


def test_schwab_parse_simple_order_preserves_unknown_order_type_without_error_logs(caplog):
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "unknown-history",
        "enteredTime": "2026-05-22T15:30:00+0000",
        "orderType": "SOMETHING_SCHWAB_ADDED",
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

    with caplog.at_level(logging.WARNING):
        parsed = broker._parse_simple_order(order, strategy_name="unit-test")

    assert len(parsed) == 1
    assert parsed[0].order_type == Order.OrderType.UNKNOWN
    assert parsed[0].raw_order_type == "SOMETHING_SCHWAB_ADDED"
    assert parsed[0].status == Order.OrderStatus.FILLED
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_schwab_parse_broker_order_preserves_child_exercise_without_error_logs(caplog):
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "parent-history",
        "orderStrategyType": "TRIGGER",
        "childOrderStrategies": [
            {
                "orderId": "child-exercise",
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
            },
        ],
    }

    with caplog.at_level(logging.WARNING):
        parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed is not None
    assert parsed.identifier == "child-exercise"
    assert parsed.order_type == Order.OrderType.UNKNOWN
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_schwab_parse_trigger_order_preserves_parent_and_child_orders():
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "parent-trigger",
        "orderStrategyType": "TRIGGER",
        "enteredTime": "2026-05-22T15:30:00+0000",
        "orderType": "LIMIT",
        "price": 0.05,
        "status": "WORKING",
        "orderLegCollection": [
            {
                "instruction": "SELL_TO_OPEN",
                "quantity": 1,
                "orderLegType": "OPTION",
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": "LW    260529C00038000",
                    "putCall": "CALL",
                    "underlyingSymbol": "LW",
                },
            },
        ],
        "childOrderStrategies": [
            {
                "orderId": "child-hedge",
                "enteredTime": "2026-05-22T15:30:01+0000",
                "orderType": "MARKET",
                "status": "PENDING_ACTIVATION",
                "orderLegCollection": [
                    {
                        "instruction": "SELL_SHORT",
                        "quantity": 100,
                        "orderLegType": "EQUITY",
                        "instrument": {"assetType": "EQUITY", "symbol": "LW"},
                    },
                ],
            },
        ],
    }

    parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed is not None
    assert parsed.identifier == "parent-trigger"
    assert parsed.order_class == Order.OrderClass.OTO
    assert parsed.asset.asset_type == Asset.AssetType.OPTION
    assert parsed.asset.symbol == "LW"
    assert len(parsed.child_orders) == 1
    assert parsed.child_orders[0].identifier == "child-hedge"
    assert parsed.child_orders[0].asset.asset_type == Asset.AssetType.STOCK
    assert parsed.child_orders[0].side == Order.OrderSide.SELL_SHORT


def test_schwab_parse_trigger_oco_order_preserves_bracket_children():
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "parent-bracket",
        "orderStrategyType": "TRIGGER",
        "enteredTime": "2026-05-22T15:30:00+0000",
        "orderType": "LIMIT",
        "price": 10.0,
        "status": "WORKING",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": 1,
                "orderLegType": "EQUITY",
                "instrument": {"assetType": "EQUITY", "symbol": "TSLL"},
            },
        ],
        "childOrderStrategies": [
            {
                "orderStrategyType": "OCO",
                "childOrderStrategies": [
                    {
                        "orderId": "take-profit",
                        "enteredTime": "2026-05-22T15:31:00+0000",
                        "orderType": "LIMIT",
                        "status": "PENDING_ACTIVATION",
                        "price": 20.0,
                        "orderLegCollection": [
                            {
                                "instruction": "SELL",
                                "quantity": 1,
                                "orderLegType": "EQUITY",
                                "instrument": {"assetType": "EQUITY", "symbol": "TSLL"},
                            },
                        ],
                    },
                    {
                        "orderId": "stop-loss",
                        "enteredTime": "2026-05-22T15:31:00+0000",
                        "orderType": "STOP",
                        "status": "PENDING_ACTIVATION",
                        "stopPrice": 5.0,
                        "orderLegCollection": [
                            {
                                "instruction": "SELL",
                                "quantity": 1,
                                "orderLegType": "EQUITY",
                                "instrument": {"assetType": "EQUITY", "symbol": "TSLL"},
                            },
                        ],
                    },
                ],
            },
        ],
    }

    parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed is not None
    assert parsed.identifier == "parent-bracket"
    assert parsed.order_class == Order.OrderClass.BRACKET
    assert len(parsed.child_orders) == 2
    assert parsed.child_orders[0].identifier == "take-profit"
    assert parsed.child_orders[1].identifier == "stop-loss"


def test_schwab_parse_broker_order_keeps_supported_child_when_sibling_is_unknown_history():
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "parent-mixed",
        "orderStrategyType": "TRIGGER",
        "childOrderStrategies": [
            {
                "orderId": "child-unknown-history",
                "enteredTime": "2026-05-22T15:30:00+0000",
                "orderType": "SOMETHING_SCHWAB_ADDED",
                "status": "FILLED",
                "orderLegCollection": [
                    {
                        "instruction": "BUY",
                        "quantity": 1,
                        "orderLegType": "OPTION",
                        "instrument": {"symbol": "SPY   260522C00500000"},
                    },
                ],
            },
            {
                "orderId": "child-supported",
                "enteredTime": "2026-05-22T15:30:00+0000",
                "orderType": "LIMIT",
                "status": "WORKING",
                "price": 500.0,
                "orderLegCollection": [
                    {
                        "instruction": "BUY",
                        "quantity": 1,
                        "orderLegType": "EQUITY",
                        "instrument": {"symbol": "SPY"},
                    },
                ],
            },
        ],
    }

    parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed is not None
    assert parsed.identifier == "child-supported"
    assert parsed.asset.symbol == "SPY"


def test_schwab_parse_broker_order_preserves_unsupported_mutual_fund_without_error_logs(caplog):
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

    with caplog.at_level(logging.WARNING):
        parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed is not None
    assert parsed.asset.symbol == "SWVXX"
    assert parsed.asset.asset_type == Asset.AssetType.UNKNOWN
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]


def test_schwab_unknown_active_order_type_with_working_status_matches_active_filter():
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "unknown-active",
        "enteredTime": "2026-05-22T15:30:00+0000",
        "orderType": "FUTURE_SCHWAB_TYPE",
        "status": "WORKING",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": 1,
                "orderLegType": "FUTURE_SCHWAB_ASSET",
                "instrument": {"symbol": "MYSTERY"},
            },
        ],
    }

    parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed.status == Order.OrderStatus.NEW
    assert parsed.order_type == Order.OrderType.UNKNOWN
    assert parsed.asset.asset_type == Asset.AssetType.UNKNOWN
    assert Order.OrderStatus(parsed.status) in Order.ACTIVE_STATUSES


def test_schwab_unknown_status_is_returned_but_not_active():
    broker = Schwab.__new__(Schwab)
    order = {
        "orderId": "unknown-status",
        "enteredTime": "2026-05-22T15:30:00+0000",
        "orderType": "LIMIT",
        "status": "SCHWAB_FUTURE_STATUS",
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": 1,
                "orderLegType": "EQUITY",
                "instrument": {"symbol": "SPY"},
            },
        ],
    }

    parsed = broker._parse_broker_order(order, strategy_name="unit-test")

    assert parsed is not None
    assert parsed.status == Order.OrderStatus.UNKNOWN
    assert not parsed.is_active()


def test_schwab_degraded_position_sync_does_not_remove_missing_tracked_positions():
    broker = _broker_with_positions(
        [
            {
                "instrument": {"assetType": "EQUITY", "symbol": "BROKEN"},
                "longQuantity": "not-a-number",
                "shortQuantity": 0,
            },
            _position("EQUITY", "SPY", quantity=3),
        ]
    )
    broker._lock = RLock()
    broker._filled_positions = SafeList(broker._lock)
    broker._tracked_positions_cache = {}
    broker.quote_assets = []
    stale_asset = Asset("OLD", asset_type=Asset.AssetType.STOCK)
    broker._filled_positions.append(Position("unit-test", stale_asset, 5))

    broker.sync_positions(SimpleNamespace(name="unit-test"))

    symbols = {position.asset.symbol for position in broker._filled_positions.get_list()}
    assert "OLD" in symbols
    assert "SPY" in symbols


def test_schwab_complete_position_sync_removes_missing_tracked_positions():
    broker = _broker_with_positions([_position("EQUITY", "SPY", quantity=3)])
    broker._lock = RLock()
    broker._filled_positions = SafeList(broker._lock)
    broker._tracked_positions_cache = {}
    broker.quote_assets = []
    stale_asset = Asset("OLD", asset_type=Asset.AssetType.STOCK)
    broker._filled_positions.append(Position("unit-test", stale_asset, 5))

    broker.sync_positions(SimpleNamespace(name="unit-test"))

    symbols = {position.asset.symbol for position in broker._filled_positions.get_list()}
    assert "OLD" not in symbols
    assert "SPY" in symbols


def test_schwab_cancel_order_calls_client_with_order_id_then_account_hash():
    client = _CancelClient()
    stream = _Stream()
    broker = _broker_for_cancel(client=client, stream=stream)
    order = _order()

    broker.cancel_order(order)

    assert client.cancel_calls == [("order-123", "account-hash")]
    # A successful DELETE only accepts the request. Terminal cancellation must
    # arrive later from a broker-observed order transition so a late fill cannot
    # be hidden behind a premature on_canceled_order callback.
    assert stream.dispatched == []
    assert order.status == Order.OrderStatus.CANCELLING


def test_schwab_cancel_lifecycle_telemetry_uses_opaque_order_reference(caplog):
    caplog.set_level(logging.INFO, logger="lumibot.brokers.schwab")
    client = _CancelClient()
    broker = _broker_for_cancel(client=client, stream=None)
    order = _order(identifier="sensitive-order-123")

    broker.cancel_order(order)

    lifecycle_rows = [record.message for record in caplog.records if "[SchwabLifecycle]" in record.message]
    assert any("event=order.cancel.request" in row for row in lifecycle_rows)
    assert any("event=order.cancel.response" in row and "elapsed_ms=" in row for row in lifecycle_rows)
    assert all("sensitive-order-123" not in row for row in lifecycle_rows)
    assert all("body=" not in row for row in lifecycle_rows)


def test_schwab_cancel_order_calls_client_even_when_local_status_is_cancelling():
    client = _CancelClient()
    broker = _broker_for_cancel(client=client)
    order = _order(status=Order.OrderStatus.SUBMITTED)

    # Strategy.cancel_order sets local status to CANCELLING before calling the
    # broker. Schwab must still receive the cancel request in that exact path.
    order.status = Order.OrderStatus.CANCELLING
    broker.cancel_order(order)

    assert client.cancel_calls == [("order-123", "account-hash")]


def test_schwab_pull_broker_order_calls_client_with_order_id_then_account_hash():
    client = _OrderClient()
    broker = _broker_for_order_pull(client=client)

    raw = broker._pull_broker_order("order-123")

    assert client.get_order_calls == [("order-123", "account-hash")]
    assert raw["orderId"] == "order-123"


def test_schwab_exact_order_read_honors_retry_after_without_inventing_state():
    client = _RateLimitedOrderClient()
    broker = _broker_for_order_pull(client=client)
    now = {"value": 100.0}
    broker._schwab_now = lambda: now["value"]

    assert broker._pull_broker_order("order-123") is None
    assert broker._pull_broker_order("order-123") is None
    assert client.get_order_calls == [("order-123", "account-hash")]

    now["value"] = 103.0
    assert broker._pull_broker_order("order-123") is None
    assert client.get_order_calls == [
        ("order-123", "account-hash"),
        ("order-123", "account-hash"),
    ]


def test_schwab_cancel_order_acceptance_is_non_terminal_without_stream():
    client = _CancelClient()
    broker = _broker_for_cancel(client=client, stream=None)
    order = _order()

    broker.cancel_order(order)

    assert client.cancel_calls == [("order-123", "account-hash")]
    assert order.status == Order.OrderStatus.CANCELLING
    assert not order.is_canceled()


def test_schwab_cancel_order_does_not_mark_advanced_order_children_terminal_on_acceptance():
    client = _CancelClient()
    stream = _Stream()
    broker = _broker_for_cancel(client=client, stream=stream)
    order = Order(
        strategy="unit-test",
        asset=Asset("LW"),
        quantity=1,
        side=Order.OrderSide.SELL,
        limit_price=999,
        stop_price=1,
        order_class=Order.OrderClass.OCO,
        identifier="oco-parent",
        status=Order.OrderStatus.SUBMITTED,
    )

    assert order.is_active()

    broker.cancel_order(order)

    assert client.cancel_calls == [("oco-parent", "account-hash")]
    assert not order.is_canceled()
    assert all(not child.is_canceled() for child in order.child_orders)
    assert order.is_active()


def test_schwab_status_maps_partial_fill_without_degrading_to_unknown():
    broker = Schwab.__new__(Schwab)

    assert broker._schwab_status_to_lumibot("PARTIALLY_FILLED") == Order.OrderStatus.PARTIALLY_FILLED


def test_schwab_healing_poll_stays_slow_when_account_activity_stream_is_primary():
    broker = Schwab.__new__(Schwab)

    stream = broker._get_stream_object()

    assert stream.polling_interval == 30.0


def test_schwab_account_activity_handler_is_registered_before_login_and_subscription():
    broker = Schwab.__new__(Schwab)
    broker._schwab_activity_wakeup = Event()
    client = _AccountActivityStreamClient()

    asyncio.run(broker._configure_schwab_account_activity_stream(client))

    assert client.calls == ["add_handler", "login", "subscribe"]
    assert callable(client.handler)
    assert broker._schwab_activity_wakeup.is_set()


def test_schwab_account_activity_callback_only_wakes_bounded_reconciliation():
    broker = Schwab.__new__(Schwab)
    broker._schwab_activity_wakeup = Event()

    broker._handle_schwab_account_activity(
        {"service": "ACCT_ACTIVITY", "content": [{"MESSAGE_TYPE": "OrderFill"}]}
    )

    assert broker._schwab_activity_wakeup.is_set()


def test_schwab_account_activity_log_sanitizes_provider_message_type(caplog):
    broker = Schwab.__new__(Schwab)
    broker._schwab_activity_wakeup = Event()
    caplog.set_level(logging.INFO, logger="lumibot.brokers.schwab")

    broker._handle_schwab_account_activity(
        {"content": [{"MESSAGE_TYPE": "OrderFill\nraw-account-data"}]}
    )

    rows = [record.message for record in caplog.records if "account_activity_received" in record.message]
    assert rows == ["[SchwabLifecycle] account_activity_received type=OrderFill_raw-account-data"]
    assert all("\n" not in row for row in rows)


def test_schwab_execution_activity_parses_cumulative_quantity_and_vwap():
    broker = Schwab.__new__(Schwab)
    payload = {
        "orderId": "order-123",
        "orderType": "LIMIT",
        "status": "PARTIALLY_FILLED",
        "price": 11,
        "orderLegCollection": [
            {
                "legId": 1,
                "instruction": "BUY",
                "quantity": 3,
                "orderLegType": "EQUITY",
                "instrument": {"symbol": "SPY", "assetType": "EQUITY"},
            }
        ],
        "orderActivityCollection": [
            {
                "activityType": "EXECUTION",
                "executionLegs": [
                    {"legId": 1, "quantity": 1, "price": 10},
                    {"legId": 1, "quantity": 1, "price": 11},
                ],
            }
        ],
    }

    observed = broker._parse_broker_order(payload, "unit-test")

    assert observed.status == Order.OrderStatus.PARTIALLY_FILLED
    assert observed._schwab_cumulative_filled_quantity == 2
    assert observed._schwab_average_fill_price == 10.5


def test_schwab_account_activity_reconciliation_reads_only_active_tracked_orders():
    active = _order(status=Order.OrderStatus.CANCELLING, identifier="active-1")
    broker = Schwab.__new__(Schwab)
    broker.get_active_tracked_orders = lambda: [active]
    broker._pull_broker_order_calls = []
    broker._pull_broker_order = (
        lambda identifier: broker._pull_broker_order_calls.append(identifier) or {"orderId": identifier}
    )
    observed = _observed_order(Order.OrderStatus.CANCELED, identifier="active-1")
    broker._parse_broker_order = lambda raw, strategy: observed
    broker._processed_snapshots = []
    broker._process_schwab_order_snapshot = broker._processed_snapshots.append

    broker._reconcile_active_schwab_orders()

    assert broker._pull_broker_order_calls == ["active-1"]
    assert broker._processed_snapshots == [observed]


def test_schwab_snapshot_reducer_emits_incremental_partial_and_final_fill_once():
    stored = _order(status=Order.OrderStatus.SUBMITTED)
    broker = _broker_for_lifecycle(stored)

    observations = [
        _observed_order(Order.OrderStatus.PARTIALLY_FILLED, 1, 10.0),
        _observed_order(Order.OrderStatus.PARTIALLY_FILLED, 1, 10.0),
        _observed_order(Order.OrderStatus.PARTIALLY_FILLED, 2, 10.5),
        _observed_order(Order.OrderStatus.PARTIALLY_FILLED, 1, 10.0),
        _observed_order(Order.OrderStatus.FILLED, 3, 11.0),
        _observed_order(Order.OrderStatus.FILLED, 3, 11.0),
    ]
    for observation in observations:
        broker._process_schwab_order_snapshot(observation)

    assert broker._lifecycle_events == [
        (broker.PARTIALLY_FILLED_ORDER, {"price": 10.0, "filled_quantity": 1.0, "multiplier": 1}),
        (broker.PARTIALLY_FILLED_ORDER, {"price": 10.5, "filled_quantity": 1.0, "multiplier": 1}),
        (broker.FILLED_ORDER, {"price": 11.0, "filled_quantity": 1.0, "multiplier": 1}),
    ]


def test_schwab_snapshot_reducer_serializes_duplicate_stream_and_rest_observations():
    stored = _order(status=Order.OrderStatus.SUBMITTED)
    broker = _broker_for_lifecycle(stored)
    simultaneous_gets = Barrier(2)

    class _RacingHighWater(dict):
        def get(self, key, default=None):
            value = super().get(key, default)
            try:
                simultaneous_gets.wait(timeout=0.1)
            except BrokenBarrierError:
                pass
            return value

    broker._schwab_observed_fill_quantities = _RacingHighWater()
    observation = _observed_order(Order.OrderStatus.PARTIALLY_FILLED, 1, 10.0)
    workers = [Thread(target=broker._process_schwab_order_snapshot, args=(observation,)) for _ in range(2)]

    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=2)

    assert broker._lifecycle_events == [
        (broker.PARTIALLY_FILLED_ORDER, {"price": 10.0, "filled_quantity": 1.0, "multiplier": 1}),
    ]


def test_schwab_snapshot_reducer_waits_for_terminal_cancel_and_dispatches_once():
    stored = _order(status=Order.OrderStatus.CANCELLING)
    broker = _broker_for_lifecycle(stored)

    broker._process_schwab_order_snapshot(_observed_order(Order.OrderStatus.CANCELLING))
    broker._process_schwab_order_snapshot(_observed_order(Order.OrderStatus.CANCELED))
    broker._process_schwab_order_snapshot(_observed_order(Order.OrderStatus.CANCELED))

    assert broker._lifecycle_events == [(broker.CANCELED_ORDER, {})]


def test_schwab_snapshot_reducer_prefers_late_fill_over_local_cancel_pending():
    stored = _order(status=Order.OrderStatus.CANCELLING)
    broker = _broker_for_lifecycle(stored)

    broker._process_schwab_order_snapshot(_observed_order(Order.OrderStatus.FILLED, 1, 25.0))

    assert broker._lifecycle_events == [
        (broker.FILLED_ORDER, {"price": 25.0, "filled_quantity": 1.0, "multiplier": 1}),
    ]


@pytest.mark.parametrize("terminal_status", [Order.OrderStatus.ERROR, Order.OrderStatus.EXPIRED])
def test_schwab_snapshot_reducer_emits_terminal_error_once(terminal_status):
    stored = _order(status=Order.OrderStatus.SUBMITTED)
    broker = _broker_for_lifecycle(stored)

    broker._process_schwab_order_snapshot(_observed_order(terminal_status))
    broker._process_schwab_order_snapshot(_observed_order(terminal_status))

    assert len(broker._lifecycle_events) == 1
    assert broker._lifecycle_events[0][0] == broker.ERROR_ORDER


@pytest.mark.parametrize(
    "status",
    [
        Order.OrderStatus.CANCELED,
        Order.OrderStatus.FILLED,
        Order.OrderStatus.ERROR,
        Order.OrderStatus.EXPIRED,
    ],
)
def test_schwab_cancel_order_still_calls_broker_for_local_terminal_statuses(status):
    client = _CancelClient()
    broker = _broker_for_cancel(client=client)

    broker.cancel_order(_order(status=status))

    assert client.cancel_calls == [("order-123", "account-hash")]


@pytest.mark.parametrize(
    "status",
    [
        Order.OrderStatus.CANCELLING,
        Order.OrderStatus.CANCELED,
        Order.OrderStatus.FILLED,
        Order.OrderStatus.ERROR,
        Order.OrderStatus.EXPIRED,
    ],
)
def test_schwab_modify_order_still_calls_broker_for_local_statuses(status):
    client = _ReplaceClient()
    broker = Schwab.__new__(Schwab)
    broker.schwab_authorization_error = False
    broker.client = client
    broker.hash_value = "account-hash"
    order = _option_order()
    order.status = status

    broker._modify_order(order, limit_price=4.75)

    assert client.get_order_calls == [("order-123", "account-hash")]
    assert len(client.replace_calls) == 1


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


def test_schwab_run_stream_without_stream_returns_without_traceback(caplog):
    caplog.set_level(logging.WARNING, logger="lumibot.brokers.schwab")
    broker = Schwab.__new__(Schwab)
    broker.stream = None

    broker._run_stream()

    assert "skipping stream runner" in caplog.text
    assert "Traceback" not in caplog.text


class _DirectReadClient(_CancelClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_order_calls = []

    def get_order(self, order_id, account_hash):
        self.get_order_calls.append((order_id, account_hash))
        return SimpleNamespace(status_code=200, text="OK", json=lambda: {"status": "CANCELED"})


def test_schwab_cancel_order_skips_diagnostic_direct_read_by_default(monkeypatch):
    # The post-cancel direct read doubles round trips on the cancel hot path;
    # it must only run when SCHWAB_CANCEL_DIAGNOSTICS is enabled.
    monkeypatch.delenv("SCHWAB_CANCEL_DIAGNOSTICS", raising=False)
    client = _DirectReadClient()
    broker = _broker_for_cancel(client=client)

    broker.cancel_order(_order())

    assert client.cancel_calls == [("order-123", "account-hash")]
    assert client.get_order_calls == []


def test_schwab_cancel_order_runs_diagnostic_direct_read_when_enabled(monkeypatch):
    monkeypatch.setenv("SCHWAB_CANCEL_DIAGNOSTICS", "1")
    client = _DirectReadClient()
    broker = _broker_for_cancel(client=client)

    broker.cancel_order(_order())

    assert client.cancel_calls == [("order-123", "account-hash")]
    assert client.get_order_calls == [("order-123", "account-hash")]


def test_schwab_oauth_session_requests_get_default_timeout():
    from lumibot.brokers.schwab import _apply_default_request_timeout

    captured = {}

    class _Session:
        def request(self, *args, **kwargs):
            captured.update(kwargs)
            return "ok"

    session = _Session()
    _apply_default_request_timeout(session, timeout_seconds=42.0)

    assert session.request("GET", "https://example.com") == "ok"
    assert captured["timeout"] == 42.0

    session.request("GET", "https://example.com", timeout=5.0)
    assert captured["timeout"] == 5.0


def test_schwab_proactive_token_refresh_rotates_before_expiry():
    from lumibot.brokers.schwab import _start_schwab_proactive_token_refresh

    token = {"refresh_token": "refresh-1", "expires_at": time.time() - 10}
    refresh_calls = []

    class _FakeOAuthSession:
        auto_refresh_url = "https://api.schwabapi.com/v1/oauth/token"

        def refresh_token(self, url, refresh_token=None, **kwargs):
            refresh_calls.append({"url": url, "refresh_token": refresh_token})
            return {"access_token": "access-2", "refresh_token": "refresh-2"}

    updated = {}

    def _update_token(updated_token):
        updated.update(updated_token)

    stop = _start_schwab_proactive_token_refresh(_FakeOAuthSession(), token, {}, _update_token)
    try:
        deadline = time.time() + 5
        while not updated and time.time() < deadline:
            time.sleep(0.01)
    finally:
        stop.set()

    assert len(refresh_calls) >= 1
    assert refresh_calls[0]["refresh_token"] == "refresh-1"
    assert updated.get("access_token") == "access-2"


def test_schwab_cleanup_stops_proactive_token_refresh():
    broker = Schwab.__new__(Schwab)
    refresh_stop = Event()
    broker._schwab_token_refresh_stop = refresh_stop
    broker.stream = None

    broker.cleanup_streams()

    assert refresh_stop.is_set()
    assert broker._schwab_token_refresh_stop is None
