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
        symbol = getattr(asset, "symbol", asset)
        prices = {"SPY": 630.0, "QQQ": 480.0, "AAPL": 210.0}
        return prices.get(str(symbol).upper(), 100.0)

    def get_last_prices(self, assets, quote=None, exchange=None):
        result = {}
        for asset in assets:
            symbol = getattr(asset, "symbol", asset)
            result[str(symbol).upper()] = self.get_last_price(asset, quote=quote, exchange=exchange)
        return result

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
        "market_last_prices",
        "options_get_chain",
        "options_get_strikes",
        "options_get_greeks",
        "options_find_strike_for_delta",
        "options_find_expiration",
        "options_evaluate_market",
        "options_calculate_multileg_price",
        "options_check_spread_profit",
        "orders_submit_multileg",
        "orders_get_status",
        "orders_wait_for_terminal",
    }.issubset(names)
    assert not any("condor" in name for name in names)
    assert len(names) == len(BuiltinTools.all())


def test_market_last_prices_returns_batch_prices_and_satisfies_order_readiness():
    strategy = _OptionsStrategy()
    tools = _wrapped_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.market.last_prices(),
            BuiltinTools.orders.submit(),
        ],
    )

    batch = tools["market_last_prices"](symbols_json='["SPY","QQQ","AAPL"]')
    assert batch["count_requested"] == 3
    assert batch["prices"]["SPY"] == 630.0
    assert batch["prices"]["QQQ"] == 480.0
    assert "AAPL" in batch["symbols_available"]

    tools["account_portfolio"]()
    tools["account_positions"]()
    # Batch price tool must satisfy readiness for a symbol included in the scan.
    submitted = tools["orders_submit_order"](
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )
    assert submitted["order"]["symbol"] == "SPY" or submitted["order"]["asset"]["symbol"] == "SPY"


def test_orb_prompt_requires_multi_ticker_scan_with_market_last_prices():
    from lumibot.example_strategies.ai_opening_range_breakout import (
        build_orb_system_prompt,
        _parse_universe,
        _DEFAULT_ORB_UNIVERSE,
    )

    universe = _parse_universe(_DEFAULT_ORB_UNIVERSE)
    assert len(universe) >= 90
    prompt = build_orb_system_prompt(
        {
            "universe": universe,
            "opening_range_minutes": 15,
            "max_positions": 1,
        }
    )
    assert "market_last_prices" in prompt
    assert "09:30" in prompt
    assert str(len(universe)) in prompt
    assert "SPY" in prompt and "AAPL" in prompt


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
        BuiltinTools.options.find_expiration(),
        BuiltinTools.options.evaluate_market(),
        BuiltinTools.options.calculate_multileg_price(),
        BuiltinTools.options.check_spread_profit(),
        BuiltinTools.orders.submit_multileg(),
        BuiltinTools.orders.get_status(),
        BuiltinTools.orders.wait_for_terminal(),
    ]:
        bound = definition.binder(strategy, manager)
        declaration = FunctionTool(_wrap_tool_callable(bound))._get_declaration().model_dump(exclude_none=True)
        assert "additional_properties" not in str(declaration)


def test_options_find_expiration_uses_min_days_target():
    strategy = _OptionsStrategy()
    tool = BuiltinTools.options.find_expiration().binder(strategy, AgentManager(strategy))

    result = tool.function(symbol="SPY", min_days=30, right="put")

    assert result["available"] is True
    assert result["expiration"] == "2026-09-18"
    assert result["requested_target_date"] == "2026-09-02"
    assert result["days_to_expiration"] == 46


def test_options_check_spread_profit_returns_percentage_for_credit_spread():
    strategy = _OptionsStrategy()
    tool = BuiltinTools.options.check_spread_profit().binder(strategy, AgentManager(strategy))
    legs = [
        {"symbol": "SPY", "expiration": "2026-09-18", "strike": 615, "right": "put", "quantity": 1, "side": "sell_to_open"},
        {"symbol": "SPY", "expiration": "2026-09-18", "strike": 610, "right": "put", "quantity": 1, "side": "buy_to_open"},
    ]

    result = tool.function(legs_json=json.dumps(legs), initial_cost=-100.0)

    assert result["available"] is True
    assert result["profit_pct"] is not None


def test_orders_get_status_reports_missing_and_known_identifiers():
    from lumibot.entities import Asset

    filled = Order(
        strategy="agent-options-test",
        asset=Asset("SPY"),
        quantity=1,
        side="buy",
    )
    filled.identifier = "bt_filled"
    filled.status = "filled"

    class _OrderAwareStrategy(_OptionsStrategy):
        def get_order(self, identifier, broker_refresh=True, broker_refresh_ttl_seconds=0.0):
            if identifier == "bt_filled":
                return filled
            return None

        def sleep(self, sleeptime, process_pending_orders=True):
            return None

    strategy = _OrderAwareStrategy()
    tools = _wrapped_tools(
        strategy,
        [
            BuiltinTools.orders.get_status(),
            BuiltinTools.orders.wait_for_terminal(),
        ],
    )

    status = tools["orders_get_status"](identifiers_json='["bt_filled","bt_missing"]')
    assert status["orders"][0]["is_filled"] is True
    assert status["orders"][0]["is_terminal"] is True
    assert status["orders"][1]["available"] is False
    assert status["missing_identifiers"] == ["bt_missing"]

    waited = tools["orders_wait_for_terminal"](identifier="bt_filled", timeout_seconds=1, poll_interval_seconds=0.25)
    assert waited["all_filled"] is True
    assert waited["timed_out"] is False
    assert waited["polls"] >= 1


def test_iron_condor_prompt_includes_parameterized_wing_and_delta():
    from lumibot.example_strategies.ai_iron_condor import build_iron_condor_system_prompt

    prompt = build_iron_condor_system_prompt(
        {
            "underlying": "QQQ",
            "wing_width": 7.0,
            "target_delta": 0.18,
            "delta_band": 0.03,
            "min_dte": 28,
            "max_dte": 40,
            "preferred_dte": 33,
            "profit_take_fraction": 0.4,
            "loss_multiple": 1.8,
            "time_stop_dte": 18,
            "max_risk_pct": 0.015,
            "max_contracts": 4,
        }
    )

    assert "underlying: QQQ" in prompt
    assert "wing_width: 7.0" in prompt
    assert "target_delta: 0.18" in prompt
    assert "0.15 through 0.21" in prompt
    assert "orders_get_status" in prompt
    assert "options_find_expiration" in prompt
