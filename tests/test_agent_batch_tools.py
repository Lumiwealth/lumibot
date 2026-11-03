"""Agent batch bindings preserve the native quote and independent-order APIs."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.agents import AgentManager, BuiltinTools
from lumibot.components.agents.runtime import _attempt_touched_broker, _wrap_tool_callable
from lumibot.entities import Asset, Order, Quote


REASON = (
    "Establish the requested exposure using the inspected account and current market evidence. "
    "The selected quantities respect the mandate and available cash. Reconcile each independent "
    "order and its actual remaining position before considering another submission."
)


class BatchStrategy:
    def __init__(self):
        self.vars = {}
        self.quote_calls = []
        self.submissions = []
        self.memory = None

    def get_datetime(self):
        return datetime(2026, 9, 1, 15, 0, tzinfo=timezone.utc)

    def get_quotes(self, assets, **kwargs):
        self.quote_calls.append((assets, kwargs))
        return {
            asset: None if asset.symbol == "MISSING" else Quote(
                asset, bid=2.1, ask=2.4, price=2.2, bid_size=4, ask_size=7,
                timestamp=self.get_datetime(), quote_time=self.get_datetime(),
            ) for asset in assets
        }

    def create_order(self, asset, quantity, side, **kwargs):
        return Order("batch-test", asset, quantity, side, **kwargs)

    def submit_order(self, orders, **kwargs):
        self.submissions.append((orders, kwargs))
        return orders

    def get_positions(self, **kwargs):
        return []


def tool(name, strategy=None, context=None):
    strategy = strategy or BatchStrategy()
    definition = next(item for item in BuiltinTools.all() if item.name == name)
    bound = definition.binder(strategy, AgentManager(strategy))
    return _wrap_tool_callable(bound, context or {})


def test_batch_quotes_preserve_exact_contracts_and_missing_siblings():
    strategy = BatchStrategy()
    assets = [
        {"symbol": "XYZ", "asset_type": "option", "expiration": expiry,
         "strike": 100, "right": "call"}
        for expiry in ["2026-09-18", "2026-10-16"]
    ] + [{"symbol": "MISSING"}]
    result = tool("market_quotes", strategy)(assets_json=json.dumps(assets))
    assert len(strategy.quote_calls) == 1
    assert len(set(strategy.quote_calls[0][0])) == 3
    assert [row["available"] for row in result["quotes"]] == [True, True, False]
    first, second, missing = result["quotes"]
    assert first["asset"] != second["asset"]
    assert first["bid"] == 2.1 and first["ask"] == 2.4
    assert first["bid_size"] == 4 and first["ask_size"] == 7
    assert first["quote_time"] == strategy.get_datetime().isoformat()
    assert missing["bid"] is None and missing["ask"] is None


def test_batch_quotes_do_not_fabricate_spread_or_retry_a_native_failure():
    strategy = BatchStrategy()
    strategy.get_quotes = lambda assets, **kwargs: {asset: Quote(asset, price=2.2) for asset in assets}
    result = tool("market_quotes", strategy)(assets_json='[{"symbol":"XYZ"}]')
    assert result["quotes"][0]["mid_price"] is None
    assert result["quotes"][0]["price"] == 2.2
    calls = []

    def unavailable(assets, **kwargs):
        calls.append(assets)
        raise ConnectionError("native quote endpoint unavailable")

    strategy.get_quotes = unavailable
    result = tool("market_quotes", strategy)(assets_json='[{"symbol":"XYZ"},{"symbol":"ABC"}]')
    assert result["tool_error"] is True
    assert len(calls) == 1


def test_batch_quotes_group_distinct_quote_currencies_without_losing_asset_identity():
    strategy = BatchStrategy()
    tool("market_quotes", strategy)(assets_json=json.dumps([
        {"symbol": "BTC", "asset_type": "crypto", "quote_symbol": "USD"},
        {"symbol": "ETH", "asset_type": "crypto", "quote_symbol": "USDT"},
    ]))
    assert {call[1]["quote"].symbol for call in strategy.quote_calls} == {"USD", "USDT"}


def test_independent_batch_submits_once_and_preserves_selected_quantities_and_limits():
    strategy = BatchStrategy()
    result = tool("orders_submit_orders", strategy)(orders_json=json.dumps([
        {"symbol": "XYZ", "quantity": 3, "side": "buy", "order_type": "limit", "limit_price": 100},
        {"symbol": "ABC", "quantity": 7, "side": "sell", "order_type": "limit", "limit_price": 50},
    ]), reason=REASON)
    assert len(strategy.submissions) == 1
    orders, kwargs = strategy.submissions[0]
    assert [(order.asset.symbol, order.quantity, order.limit_price) for order in orders] == [
        ("XYZ", 3, 100), ("ABC", 7, 50),
    ]
    assert kwargs["is_multileg"] is False
    assert kwargs["reason"] == REASON
    assert len(result["submitted"]) == 2
    assert result["atomic"] is False


def test_independent_option_orders_do_not_become_an_implicit_package():
    strategy = BatchStrategy()
    tool("orders_submit_orders", strategy)(orders_json=json.dumps([
        {"symbol": "XYZ", "asset_type": "option", "expiration": "2026-09-18", "strike": 100,
         "right": "call", "quantity": 2, "side": "buy_to_open", "order_type": "limit", "limit_price": 2},
        {"symbol": "ABC", "asset_type": "option", "expiration": "2026-10-16", "strike": 50,
         "right": "put", "quantity": 1, "side": "buy_to_open", "order_type": "limit", "limit_price": 1},
    ]), reason=REASON)
    assert strategy.submissions[0][1]["is_multileg"] is False


@pytest.mark.parametrize("bad", [
    {"symbol": "ABC", "quantity": -2, "side": "buy"},
    {"symbol": "ABC", "quantity": 2, "side": "buy", "order_type": "limit"},
])
def test_batch_validates_every_item_before_any_submission(bad):
    strategy = BatchStrategy()
    result = tool("orders_submit_orders", strategy)(orders_json=json.dumps([
        {"symbol": "XYZ", "quantity": 1, "side": "buy"}, bad,
    ]), reason=REASON)
    assert result["tool_error"] is True
    assert strategy.submissions == []


def test_batch_submission_unknown_outcome_is_not_retried():
    strategy = BatchStrategy()

    def uncertain(orders, **kwargs):
        strategy.submissions.append((orders, kwargs))
        raise TimeoutError("submission acknowledgment unavailable")

    strategy.submit_order = uncertain
    result = tool("orders_submit_orders", strategy)(
        orders_json='[{"symbol":"XYZ","quantity":1,"side":"buy"}]', reason=REASON)
    assert len(strategy.submissions) == 1
    assert result["tool_error"] is True
    assert result["execution_outcome"]["broker_state_certainty"] == "unknown"


def test_batch_submission_is_a_trading_mutation_for_permissions_and_replay():
    definition = next(item for item in BuiltinTools.all() if item.name == "orders_submit_orders")
    assert definition.metadata["mutates_trading"] is True
    assert _attempt_touched_broker(SimpleNamespace(_attempt_tool_calls=[{"tool_name": definition.name}]))


def test_batch_partial_result_preserves_returned_orders_and_unknown_remainder():
    strategy = BatchStrategy()
    strategy.submit_order = lambda orders, **kwargs: [orders[0], None]
    result = tool("orders_submit_orders", strategy)(orders_json=json.dumps([
        {"symbol": "XYZ", "quantity": 3, "side": "buy"},
        {"symbol": "ABC", "quantity": 7, "side": "buy"},
    ]), reason=REASON)
    assert len(result["submitted"]) == 1
    assert len(result["requested"]) == 2
    assert result["execution_outcome"]["broker_state_certainty"] == "unknown"


def test_batch_tool_declarations_use_the_actual_adk_schema():
    from google.adk.tools.function_tool import FunctionTool
    from lumibot.components.agents.runtime import _function_tools_with_name_aliases

    strategy = BatchStrategy()
    for name in ("market_quotes", "orders_submit_orders"):
        definition = next(item for item in BuiltinTools.all() if item.name == name)
        bound = definition.binder(strategy, AgentManager(strategy))
        declared = _function_tools_with_name_aliases(FunctionTool, [bound], {})[0]._get_declaration()
        assert declared.name == name
        assert declared.parameters_json_schema or declared.parameters


def test_batch_mutation_requires_fresh_account_reads_before_a_second_submission():
    strategy = BatchStrategy()
    context = {"enforce_order_readiness": True, "tool_calls": [
        {"tool_name": "market_last_price", "arguments": {"symbol": "XYZ"}, "ok": True},
    ], "account_snapshot": {"as_of": strategy.get_datetime().isoformat(), "account_complete": True,
                             "positions_complete": True, "open_orders_complete": True}}
    first = tool("orders_submit_orders", strategy, context)(
        orders_json='[{"symbol":"XYZ","quantity":1,"side":"buy"}]', reason=REASON)
    assert len(first["submitted"]) == 1
    second = tool("orders_submit_order", strategy, context)(symbol="XYZ", quantity=1, side="buy", reason=REASON)
    assert second["tool_error"] is True
    assert "account_portfolio" in second["error"]["message"]
    assert len(strategy.submissions) == 1


def test_batch_readiness_checks_every_symbol_before_submission():
    strategy = BatchStrategy()
    context = {"enforce_order_readiness": True, "tool_calls": []}
    result = tool("orders_submit_orders", strategy, context)(
        orders_json='[{"symbol":"XYZ","quantity":1,"side":"buy"}]', reason=REASON)
    assert result["tool_error"] is True
    assert "ORDER_READINESS_REQUIRED" in result["error"]["message"]
    assert strategy.submissions == []
