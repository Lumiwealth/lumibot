import json
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from lumibot.components.agents import AgentManager, AgentRunResult, AgentTraceEvent, BuiltinTools
from lumibot.components.agents.builtins import _order_to_dict, _position_to_dict
from lumibot.components.agents.runtime import _wrap_tool_callable
from lumibot.entities import Asset, Order, Position


class _Vars(dict):
    def set(self, key, value):
        self[key] = value


class _AccountStrategy:
    is_backtesting = True
    parameters = {}

    def __init__(self, positions=None, orders=None):
        self.vars = _Vars()
        self._positions = list(positions or [])
        self._orders = list(orders or [])
        self.submitted_orders = []

    def get_datetime(self):
        return datetime(2026, 9, 2, tzinfo=timezone.utc)

    def get_cash(self):
        return 100_000.0

    def get_portfolio_value(self):
        return 125_000.0

    def get_positions(self, include_cash_positions=True):
        return list(self._positions)

    def get_orders(self):
        return list(self._orders)

    def get_last_price(self, asset, quote=None, exchange=None):
        return 480.0

    def create_order(self, asset, quantity, side, **kwargs):
        return SimpleNamespace(
            identifier="new-order",
            status="new",
            side=side,
            asset=asset,
            quote=kwargs.get("quote"),
            quantity=quantity,
            order_type=kwargs.get("order_type", "market"),
            time_in_force=kwargs.get("time_in_force", "day"),
            limit_price=kwargs.get("limit_price"),
            stop_price=kwargs.get("stop_price"),
            child_orders=[],
        )

    def submit_order(self, order):
        self.submitted_orders.append(order)
        return order

    def log_message(self, *args, **kwargs):
        return None


class _CaptureRuntime:
    def __init__(self):
        self.requests = []

    def run(self, request):
        self.requests.append(request)
        return AgentRunResult(
            summary="Captured.",
            model=request.model,
            events=[AgentTraceEvent(kind="text", text="Captured.")],
        )


def _option_position(strike, quantity, **pricing):
    asset = Asset(
        "QQQ",
        asset_type=Asset.AssetType.OPTION,
        expiration=date(2026, 10, 16),
        strike=strike,
        right="PUT",
        multiplier=100,
    )
    position = Position("Test", asset, Decimal(str(quantity)))
    for key, value in pricing.items():
        setattr(position, key, value)
    return position


def _wrapped_tools(strategy, definitions, *, snapshot=None):
    manager = AgentManager(strategy)
    context = {
        "agent_name": "trader",
        "model_call_id": "account-context-test",
        "enforce_order_readiness": True,
        "account_snapshot": snapshot,
        "tool_calls": [],
    }
    tools = [definition.binder(strategy, manager) for definition in definitions]
    return {tool.name: _wrap_tool_callable(tool, context) for tool in tools}, context


def test_position_agent_projection_is_compact_and_omits_unknown_or_internal_fields():
    position = _option_position(
        480,
        -3,
        avg_fill_price=2.15,
        current_price=1.62,
        market_value=-486,
        pnl=159,
    )
    position._raw = {"broker": "x" * 100_000}
    position._bars = [object()] * 100

    payload = _position_to_dict(position)

    assert payload == {
        "asset": {
            "symbol": "QQQ",
            "type": "option",
            "strike": 480.0,
            "exp": "2026-10-16",
            "right": "PUT",
            "mult": 100,
        },
        "quantity": -3.0,
        "position_side": "short",
        "closing_side": "buy_to_close",
        "closing_quantity": 3.0,
        "avg_fill_price": 2.15,
        "current_price": 1.62,
        "market_value": -486,
        "pnl": 159,
    }
    assert len(json.dumps(payload, separators=(",", ":"))) < 450


def test_position_agent_projection_does_not_turn_missing_values_into_zeroes():
    payload = _position_to_dict(_option_position(475, 3))

    assert payload["quantity"] == 3.0
    assert payload["position_side"] == "long"
    assert payload["closing_side"] == "sell_to_close"
    assert not {"avg_fill_price", "current_price", "market_value", "pnl"}.intersection(payload)


def test_order_agent_projection_is_compact_and_preserves_quote_and_multileg_children():
    short_put = _option_position(480, -1).asset
    long_put = _option_position(475, 1).asset
    short_leg = Order("Test", short_put, 1, "sell_to_open", order_type="limit", limit_price=2.15)
    long_leg = Order("Test", long_put, 1, "buy_to_open", order_type="limit", limit_price=1.10)
    parent = Order(
        "Test",
        Asset("QQQ", asset_type=Asset.AssetType.MULTILEG),
        1,
        "buy",
        order_type="limit",
        limit_price=-1.05,
        child_orders=[short_leg, long_leg],
    )
    parent._raw = {"broker": "x" * 100_000}

    payload = _order_to_dict(parent)

    assert set(payload) == {
        "identifier",
        "asset",
        "side",
        "quantity",
        "order_type",
        "status",
        "time_in_force",
        "limit_price",
        "legs",
    }
    assert payload["asset"] == {"symbol": "QQQ", "type": "multileg"}
    assert [leg["asset"]["strike"] for leg in payload["legs"]] == [480.0, 475.0]
    assert all("legs" not in leg for leg in payload["legs"])
    assert len(json.dumps(payload, separators=(",", ":"), default=str)) < 900


def test_agent_projection_covers_stock_future_and_crypto_quote_without_expansion():
    stock = Position("Test", Asset("AAPL"), 10)
    future = Position(
        "Test",
        Asset("ES", asset_type=Asset.AssetType.FUTURE, expiration=date(2026, 12, 18), multiplier=50),
        2,
    )
    crypto_order = Order(
        "Test",
        Asset("BTC", asset_type=Asset.AssetType.CRYPTO),
        Decimal("0.25"),
        "buy",
        quote=Asset("USD", asset_type=Asset.AssetType.FOREX),
    )

    assert _position_to_dict(stock)["asset"] == {"symbol": "AAPL", "type": "stock"}
    assert _position_to_dict(future)["asset"] == {
        "symbol": "ES",
        "type": "future",
        "exp": "2026-12-18",
        "mult": 50,
    }
    assert _order_to_dict(crypto_order)["quote"] == {"symbol": "USD", "type": "forex"}


def test_zero_and_one_position_snapshots_report_exact_completeness():
    for positions, expected_total in (([], 0), ([Position("Test", Asset("AAPL"), 2)], 1)):
        strategy = _AccountStrategy(positions=positions)
        strategy.is_backtesting = False
        runtime = _CaptureRuntime()
        agent = AgentManager(strategy).create(
            name=f"snapshot_{expected_total}",
            tools=[],
            include_builtin_tools=False,
            _runtime=runtime,
        )

        agent.run(task_prompt="Inspect the account.")

        snapshot = runtime.requests[0].runtime_context["account_snapshot"]
        assert snapshot["positions_total"] == expected_total
        assert snapshot["positions_included"] == expected_total
        assert snapshot["positions_omitted"] == 0
        assert snapshot["positions_complete"] is True


def test_500_position_snapshot_stays_bounded_and_explicitly_incomplete():
    positions = [Position("Test", Asset(f"SYM{index:03d}"), 1) for index in range(500)]
    strategy = _AccountStrategy(positions=positions)
    strategy.is_backtesting = False
    runtime = _CaptureRuntime()
    agent = AgentManager(strategy).create(
        name="large_snapshot",
        tools=[],
        include_builtin_tools=False,
        _runtime=runtime,
    )

    agent.run(task_prompt="Inspect the large account.")

    context = runtime.requests[0].runtime_context
    assert len(context["positions"]) == 50
    assert context["account_snapshot"]["positions_total"] == 500
    assert context["account_snapshot"]["positions_omitted"] == 450
    assert context["account_snapshot"]["positions_complete"] is False
    assert len(json.dumps(context["positions"], separators=(",", ":"), default=str)) < 10_000


def test_runtime_snapshot_includes_50_deterministically_and_reports_omissions():
    positions = [_option_position(500 - index, 1) for index in range(63)]
    runtime = _CaptureRuntime()
    strategy = _AccountStrategy(positions=positions)
    strategy.is_backtesting = False
    agent = AgentManager(strategy).create(
        name="snapshot",
        tools=[],
        include_builtin_tools=False,
        _runtime=runtime,
    )

    agent.run(task_prompt="Inspect the account.")

    context = runtime.requests[0].runtime_context
    assert len(context["positions"]) == 50
    assert [row["asset"]["strike"] for row in context["positions"][:3]] == [438.0, 439.0, 440.0]
    assert context["account_snapshot"] == {
        "as_of": "2026-09-02T00:00:00+00:00",
        "account_complete": True,
        "positions_total": 63,
        "positions_included": 50,
        "positions_omitted": 13,
        "positions_complete": False,
        "open_orders_total": 0,
        "open_orders_included": 0,
        "open_orders_omitted": 0,
        "open_orders_complete": True,
    }
    assert "13 positions are omitted" in runtime.requests[0].system_prompt


def test_account_positions_supports_pagination_and_exact_option_filtering():
    positions = [_option_position(400 + index, 1) for index in range(63)]
    strategy = _AccountStrategy(positions=positions)
    tools, _ = _wrapped_tools(strategy, [BuiltinTools.account.positions()])

    first = tools["account_positions"]()
    second = tools["account_positions"](offset=50)
    exact = tools["account_positions"](
        symbol="QQQ",
        asset_type="option",
        expiration="2026-10-16",
        strike=462,
        right="put",
    )

    assert first["total"] == 63
    assert first["returned"] == 50
    assert first["complete"] is False
    assert first["next_offset"] == 50
    assert second["returned"] == 13
    assert second["complete"] is True
    assert second["next_offset"] is None
    assert exact["matched"] == 1
    assert exact["positions"][0]["asset"]["strike"] == 462.0
    assert exact["filters"] == {
        "symbol": "QQQ",
        "asset_type": "option",
        "expiration": "2026-10-16",
        "strike": 462.0,
        "right": "PUT",
    }


def test_account_positions_rejects_invalid_page_arguments():
    tools, _ = _wrapped_tools(_AccountStrategy(), [BuiltinTools.account.positions()])

    assert tools["account_positions"](offset=-1)["tool_error"] is True
    assert tools["account_positions"](limit=0)["tool_error"] is True
    assert tools["account_positions"](limit=101)["tool_error"] is True


def test_account_and_open_order_tool_schemas_are_gemini_compatible():
    from google.adk.tools.function_tool import FunctionTool

    strategy = _AccountStrategy()
    manager = AgentManager(strategy)
    for definition in (BuiltinTools.account.positions(), BuiltinTools.orders.open_orders()):
        bound = definition.binder(strategy, manager)
        declaration = FunctionTool(_wrap_tool_callable(bound))._get_declaration().model_dump(exclude_none=True)
        assert declaration["name"] == definition.name
        assert declaration["parameters_json_schema"]["properties"]["limit"]["type"] == "integer"


def test_open_orders_are_filtered_paginated_and_compact():
    orders = []
    for index in range(55):
        order = Order("Test", Asset(f"SYM{54 - index:02d}"), 1, "buy", order_type="limit", limit_price=10 + index)
        order.status = "new"
        orders.append(order)
    filled = Order("Test", Asset("DONE"), 1, "buy")
    filled.status = "fill"
    orders.append(filled)
    strategy = _AccountStrategy(orders=orders)
    tools, _ = _wrapped_tools(strategy, [BuiltinTools.orders.open_orders()])

    first = tools["orders_open_orders"]()
    second = tools["orders_open_orders"](offset=50)
    exact = tools["orders_open_orders"](symbol="SYM54")

    assert first["total"] == 55
    assert first["returned"] == 50
    assert first["next_offset"] == 50
    assert second["returned"] == 5
    assert second["complete"] is True
    assert exact["matched"] == 1
    assert exact["orders"][0]["asset"] == {"symbol": "SYM54", "type": "stock"}


def test_open_order_filter_matches_an_exact_multileg_child_contract():
    short_put = _option_position(480, -1).asset
    long_put = _option_position(475, 1).asset
    parent = Order(
        "Test",
        Asset("QQQ", asset_type=Asset.AssetType.MULTILEG),
        1,
        "buy",
        status="new",
        child_orders=[
            Order("Test", short_put, 1, "sell_to_open", status="new"),
            Order("Test", long_put, 1, "buy_to_open", status="new"),
        ],
    )
    strategy = _AccountStrategy(orders=[parent])
    tools, _context = _wrapped_tools(strategy, [BuiltinTools.orders.open_orders()])

    result = tools["orders_open_orders"](
        symbol="QQQ",
        asset_type="option",
        expiration="2026-10-16",
        strike=475,
        right="PUT",
    )

    assert result["matched"] == 1
    assert result["orders"][0]["asset"] == {"symbol": "QQQ", "type": "multileg"}


def test_truncated_snapshot_and_incomplete_position_page_do_not_satisfy_order_readiness():
    positions = [_option_position(400 + index, 1) for index in range(63)]
    strategy = _AccountStrategy(positions=positions)
    tools, context = _wrapped_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.orders.open_orders(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
        snapshot={
            "as_of": "2026-09-02T00:00:00+00:00",
            "account_complete": True,
            "positions_complete": False,
            "open_orders_complete": True,
        },
    )
    tools["account_portfolio"]()
    tools["account_positions"]()
    tools["orders_open_orders"]()
    tools["market_last_price"](symbol="QQQ", asset_type="stock")

    blocked = tools["orders_submit_order"](
        symbol="QQQ",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert blocked["tool_error"] is True
    assert "complete account_positions pagination" in blocked["error"]["message"]
    assert strategy.submitted_orders == []
    assert context["tool_calls"][-1]["ok"] is False


def test_contiguous_position_pages_and_open_orders_satisfy_order_readiness():
    positions = [_option_position(400 + index, 1) for index in range(63)]
    strategy = _AccountStrategy(positions=positions)
    tools, _ = _wrapped_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.orders.open_orders(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
    )
    tools["account_portfolio"]()
    tools["account_positions"]()
    tools["account_positions"](offset=50)
    tools["orders_open_orders"]()
    tools["market_last_price"](symbol="QQQ", asset_type="stock")

    submitted = tools["orders_submit_order"](
        symbol="QQQ",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert "tool_error" not in submitted
    assert len(strategy.submitted_orders) == 1


def test_changed_position_membership_with_same_count_invalidates_paginated_readiness():
    positions = [_option_position(400 + index, 1) for index in range(63)]
    strategy = _AccountStrategy(positions=positions)
    tools, _ = _wrapped_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.orders.open_orders(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
    )
    tools["account_portfolio"]()
    first_page = tools["account_positions"]()
    strategy._positions = [_option_position(401 + index, 1) for index in range(63)]
    second_page = tools["account_positions"](offset=50)
    tools["orders_open_orders"]()
    tools["market_last_price"](symbol="QQQ", asset_type="stock")

    blocked = tools["orders_submit_order"](
        symbol="QQQ",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert first_page["snapshot_id"] != second_page["snapshot_id"]
    assert blocked["tool_error"] is True
    assert "complete account_positions pagination" in blocked["error"]["message"]
    assert strategy.submitted_orders == []
