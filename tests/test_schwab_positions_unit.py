import logging
from datetime import date
from threading import RLock
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
    assert stream.dispatched == [
        (broker.CANCELED_ORDER, True, {"order": order}),
    ]


def test_schwab_pull_broker_order_calls_client_with_order_id_then_account_hash():
    client = _OrderClient()
    broker = _broker_for_order_pull(client=client)

    raw = broker._pull_broker_order("order-123")

    assert client.get_order_calls == [("order-123", "account-hash")]
    assert raw["orderId"] == "order-123"


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


def test_schwab_run_stream_without_stream_returns_without_traceback(caplog):
    broker = Schwab.__new__(Schwab)
    broker.stream = None

    broker._run_stream()

    assert "skipping stream runner" in caplog.text
    assert "Traceback" not in caplog.text
