import json
from datetime import datetime, timezone
from types import SimpleNamespace

from lumibot.components.agents import AgentManager, BuiltinTools
from lumibot.components.agents.runtime import _wrap_tool_callable
from lumibot.entities import Order
from lumibot.entities.chains import Chains


class _OptionsStrategy:
    is_backtesting = True
    parameters = {}

    def __init__(self):
        self.submissions = []

    def get_datetime(self):
        return datetime(2026, 8, 3, tzinfo=timezone.utc)

    def log_message(self, *args, **kwargs):
        return None

    def get_positions(self, include_cash_positions=True):
        return []

    def get_cash(self):
        return 100_000.0

    def get_portfolio_value(self):
        return 100_000.0

    def get_last_price(self, asset, quote=None, exchange=None):
        return 630.0

    def get_chains(self, asset):
        return Chains(
            {
                "Multiplier": 100,
                "Exchange": "SMART",
                "Chains": {
                    "CALL": {"2026-09-18": [640.0, 645.0, 650.0]},
                    "PUT": {"2026-09-18": [610.0, 615.0, 620.0]},
                },
            }
        )

    def get_greeks(self, asset, **kwargs):
        return {"delta": 0.16 if str(asset.right).upper() == "CALL" else -0.16, "theta": -0.04}

    def get_quote(self, asset, quote=None, exchange=None):
        if float(asset.strike) in {610.0, 650.0}:
            return SimpleNamespace(bid=0.45, ask=0.55)
        return SimpleNamespace(bid=1.45, ask=1.55)

    def create_order(self, asset, quantity, side, **kwargs):
        return Order(
            strategy="agent-options-test",
            asset=asset,
            quantity=quantity,
            side=side,
            time_in_force=kwargs.get("time_in_force", "day"),
        )

    def submit_order(self, orders, **kwargs):
        self.submissions.append((orders, kwargs))
        return orders


def _wrapped_tools(strategy, definitions):
    manager = AgentManager(strategy)
    context = {
        "agent_name": "options-agent",
        "model_call_id": "options-test-call",
        "enforce_order_readiness": True,
        "tool_calls": [],
    }
    return {
        definition.name: _wrap_tool_callable(definition.binder(strategy, manager), context)
        for definition in definitions
    }


def _iron_condor_legs():
    return [
        {"symbol": "SPY", "expiration": "2026-09-18", "strike": 610, "right": "put", "quantity": 1, "side": "buy_to_open"},
        {"symbol": "SPY", "expiration": "2026-09-18", "strike": 615, "right": "put", "quantity": 1, "side": "sell_to_open"},
        {"symbol": "SPY", "expiration": "2026-09-18", "strike": 645, "right": "call", "quantity": 1, "side": "sell_to_open"},
        {"symbol": "SPY", "expiration": "2026-09-18", "strike": 650, "right": "call", "quantity": 1, "side": "buy_to_open"},
    ]


def test_default_agent_tools_expose_generic_option_discovery_and_multileg_execution():
    names = {definition.name for definition in BuiltinTools.all()}

    assert {
        "options_get_chain",
        "options_get_strikes",
        "options_get_greeks",
        "options_find_strike_for_delta",
        "options_evaluate_market",
        "options_calculate_multileg_price",
        "orders_submit_multileg",
    }.issubset(names)
    assert not any("condor" in name for name in names)


def test_option_chain_and_contract_tools_return_exact_listed_contract_data():
    strategy = _OptionsStrategy()
    tools = _wrapped_tools(
        strategy,
        [
            BuiltinTools.options.get_chain(),
            BuiltinTools.options.get_strikes(),
            BuiltinTools.options.get_greeks(),
        ],
    )

    chain = tools["options_get_chain"](symbol="SPY")
    strikes = tools["options_get_strikes"](
        symbol="SPY",
        expiration="2026-09-18",
        right="put",
    )
    greeks = tools["options_get_greeks"](
        symbol="SPY",
        expiration="2026-09-18",
        strike=615,
        right="put",
    )

    assert chain["call_expirations"] == ["2026-09-18"]
    assert chain["calls"]["2026-09-18"]["strike_count"] == 3
    assert strikes["strikes"] == [610.0, 615.0, 620.0]
    assert greeks["greeks"]["delta"] == -0.16


def test_multileg_price_preserves_opening_side_direction():
    strategy = _OptionsStrategy()
    tool = BuiltinTools.options.calculate_multileg_price().binder(strategy, AgentManager(strategy))

    result = tool.function(legs_json=json.dumps(_iron_condor_legs()), price_style="mid")

    assert result["available"] is True
    assert result["net_limit_price"] == -2.0
    assert result["order_type"] == "credit"


def test_multileg_submit_creates_one_atomic_four_leg_order_after_normal_readiness_checks():
    strategy = _OptionsStrategy()
    tools = _wrapped_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit_multileg(),
        ],
    )
    tools["account_portfolio"]()
    tools["account_positions"]()
    tools["market_last_price"](symbol="SPY")

    result = tools["orders_submit_multileg"](
        legs_json=json.dumps(_iron_condor_legs()),
        price_style="mid",
    )

    assert result["order_type"] == "credit"
    assert len(result["submitted"]) == 4
    assert len(strategy.submissions) == 1
    orders, kwargs = strategy.submissions[0]
    assert [str(order.side) for order in orders] == [
        "buy_to_open",
        "sell_to_open",
        "sell_to_open",
        "buy_to_open",
    ]
    assert kwargs["is_multileg"] is True
    assert kwargs["order_type"] == "credit"
    assert kwargs["price"] == 2.0


def test_generic_option_tool_schemas_are_gemini_function_declaration_compatible():
    from google.adk.tools.function_tool import FunctionTool

    strategy = _OptionsStrategy()
    manager = AgentManager(strategy)
    for definition in [
        BuiltinTools.options.get_chain(),
        BuiltinTools.options.get_strikes(),
        BuiltinTools.options.get_greeks(),
        BuiltinTools.options.find_strike_for_delta(),
        BuiltinTools.options.evaluate_market(),
        BuiltinTools.options.calculate_multileg_price(),
        BuiltinTools.orders.submit_multileg(),
    ]:
        bound = definition.binder(strategy, manager)
        declaration = FunctionTool(_wrap_tool_callable(bound))._get_declaration().model_dump(exclude_none=True)
        assert "additional_properties" not in str(declaration)
