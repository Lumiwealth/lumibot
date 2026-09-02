import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.components.agents import AgentManager, BuiltinTools
from lumibot.components.agents.builtins import _position_to_dict, _validate_option_closing_orders
from lumibot.components.agents.runtime import _wrap_tool_callable
from lumibot.entities import Asset, Order
from lumibot.entities.chains import Chains


class _FakeBars:
    def __init__(self, closes):
        index = pd.date_range("2026-08-01", periods=len(closes), freq="min", tz="UTC")
        self.pandas_df = pd.DataFrame(
            {
                "open": closes,
                "high": [value + 1 for value in closes],
                "low": [value - 1 for value in closes],
                "close": closes,
                "volume": [1_000 + index_offset for index_offset, _ in enumerate(closes)],
            },
            index=index,
        )
        self.df = self.pandas_df


class _OptionsStrategy:
    is_backtesting = True
    parameters = {}

    def __init__(self):
        self.submissions = []
        self.historical_batch_calls = []
        self.historical_single_calls = []

    def get_datetime(self):
        return datetime(2026, 8, 3, tzinfo=timezone.utc)

    def log_message(self, *args, **kwargs):
        return None

    def get_positions(self, include_cash_positions=True):
        return []

    def get_orders(self):
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

    def get_historical_prices_for_assets(self, assets, length, timestep="day", **kwargs):
        self.historical_batch_calls.append(
            {"assets": list(assets), "length": length, "timestep": timestep, "kwargs": kwargs}
        )
        closes = {
            "SPY": [100.0 + index for index in range(length)],
            "QQQ": [200.0 + index for index in range(length)],
            "AAPL": [300.0 + index for index in range(length)],
        }
        result = {}
        for asset in assets:
            symbol = str(getattr(asset, "symbol", asset)).upper()
            if symbol not in closes:
                continue
            result[symbol] = _FakeBars(closes[symbol])
        return result

    def get_historical_prices(self, asset, length, timestep="day", **kwargs):
        self.historical_single_calls.append(
            {"asset": asset, "length": length, "timestep": timestep, "kwargs": kwargs}
        )
        symbol = str(getattr(asset, "symbol", asset)).upper()
        base = {"SPY": 100.0, "QQQ": 200.0, "AAPL": 300.0}.get(symbol)
        if base is None:
            return None
        return _FakeBars([base + index for index in range(length)])

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
        "market_historical_prices",
        "risk_calculate_stock_quantity",
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


def test_stock_quantity_calculator_respects_notional_and_cash_caps():
    strategy = _OptionsStrategy()
    tool = BuiltinTools.risk.calculate_stock_quantity().binder(strategy, AgentManager(strategy))

    result = tool.function(maximum_notional=10_000, price=230, available_cash=100_000)
    cash_limited = tool.function(maximum_notional=10_000, price=230, available_cash=5_000)

    assert result["quantity"] == 43
    assert result["notional"] == 9_890
    assert result["within_maximum_notional"] is True
    assert result["within_available_cash"] is True
    assert cash_limited["quantity"] == 21
    assert cash_limited["notional"] == 4_830


def test_stock_quantity_calculator_handles_zero_cash_and_rejects_invalid_inputs():
    strategy = _OptionsStrategy()
    tool = BuiltinTools.risk.calculate_stock_quantity().binder(strategy, AgentManager(strategy))

    no_cash = tool.function(maximum_notional=10_000, price=230, available_cash=0)

    assert no_cash["quantity"] == 0
    assert no_cash["notional"] == 0
    assert no_cash["within_available_cash"] is True
    with pytest.raises(ValueError, match="available_cash must be a finite number"):
        tool.function(maximum_notional=10_000, price=230, available_cash=-1)
    with pytest.raises(ValueError, match="price must be a finite number greater than 0"):
        tool.function(maximum_notional=10_000, price=0, available_cash=10_000)


def test_option_position_payload_exposes_unambiguous_closing_metadata():
    short_option = SimpleNamespace(asset_type="option", symbol="SPY")
    long_option = SimpleNamespace(asset_type="option", symbol="SPY")

    short_payload = _position_to_dict(SimpleNamespace(asset=short_option, quantity=-3))
    long_payload = _position_to_dict(SimpleNamespace(asset=long_option, quantity=3))

    assert short_payload["position_side"] == "short"
    assert short_payload["closing_side"] == "buy_to_close"
    assert short_payload["closing_quantity"] == 3
    assert long_payload["position_side"] == "long"
    assert long_payload["closing_side"] == "sell_to_close"
    assert long_payload["closing_quantity"] == 3


def test_market_last_prices_returns_batch_prices_and_satisfies_order_readiness():
    strategy = _OptionsStrategy()
    tools = _wrapped_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.orders.open_orders(),
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
    tools["orders_open_orders"]()
    # Batch price tool must satisfy readiness for a symbol included in the scan.
    submitted = tools["orders_submit_order"](
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )
    order_payload = submitted["order"]
    assert order_payload.get("symbol") == "SPY" or order_payload.get("asset", {}).get("symbol") == "SPY"


def test_market_historical_prices_uses_batch_strategy_api_once():
    strategy = _OptionsStrategy()
    tools = _wrapped_tools(strategy, [BuiltinTools.market.historical_prices()])

    batch = tools["market_historical_prices"](
        symbols_json='["SPY","QQQ","AAPL"]',
        length=3,
        timestep="minute",
    )

    assert len(strategy.historical_batch_calls) == 1
    assert strategy.historical_single_calls == []
    assert batch["count_requested"] == 3
    assert batch["count_available"] == 3
    assert batch["timestep"] == "minute"
    assert [row["close"] for row in batch["bars_by_symbol"]["SPY"]] == [100.0, 101.0, 102.0]
    assert [row["close"] for row in batch["bars_by_symbol"]["QQQ"]] == [200.0, 201.0, 202.0]
    assert batch["bars_by_symbol"]["AAPL"][0]["datetime"]
    assert batch["symbols_missing"] == []


def test_market_historical_prices_preserves_string_order_and_caps_parallelism():
    strategy = _OptionsStrategy()
    tools = _wrapped_tools(strategy, [BuiltinTools.market.historical_prices()])

    batch = tools["market_historical_prices"](
        symbols="QQQ,SPY,AAPL",
        length=2,
        chunk_size=10_000,
        max_workers=10_000,
    )

    call = strategy.historical_batch_calls[0]
    assert call["assets"] == ["QQQ", "SPY", "AAPL"]
    assert call["kwargs"]["chunk_size"] == 150
    assert call["kwargs"]["max_workers"] == 32
    assert batch["symbols_requested"] == ["QQQ", "SPY", "AAPL"]


def test_market_historical_prices_falls_back_per_symbol_when_batch_missing():
    strategy = _OptionsStrategy()
    strategy.get_historical_prices_for_assets = None
    tools = _wrapped_tools(strategy, [BuiltinTools.market.historical_prices()])

    batch = tools["market_historical_prices"](symbols=["SPY", "MSFT"], length=2, timestep="day")

    assert len(strategy.historical_single_calls) == 2
    assert batch["bars_by_symbol"]["SPY"][1]["close"] == 101.0
    assert batch["bars_by_symbol"]["MSFT"] == []
    assert "MSFT" in batch["symbols_missing"]


def test_orb_prompt_keeps_strategy_policy_without_repeating_tool_instructions():
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
    assert "Scan the full provided universe" in prompt
    assert "market_last_prices" not in prompt
    assert "market_historical_prices" not in prompt
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


def test_option_close_validation_rejects_reversed_side_and_oversized_quantity():
    strategy = _OptionsStrategy()
    long_put = Asset(
        symbol="SPY",
        asset_type="option",
        expiration=date(2026, 9, 18),
        strike=610,
        right="put",
    )
    strategy.get_positions = lambda include_cash_positions=True: [
        SimpleNamespace(asset=long_put, quantity=3)
    ]

    with pytest.raises(ValueError, match="required_side='sell_to_close'"):
        _validate_option_closing_orders(
            strategy,
            [SimpleNamespace(asset=long_put, quantity=3, side="buy_to_close")],
        )

    with pytest.raises(ValueError, match="requested_quantity=4.0"):
        _validate_option_closing_orders(
            strategy,
            [SimpleNamespace(asset=long_put, quantity=4, side="sell_to_close")],
        )

    _validate_option_closing_orders(
        strategy,
        [SimpleNamespace(asset=long_put, quantity=3, side="sell_to_close")],
    )


def test_multileg_submit_creates_one_atomic_four_leg_order_after_normal_readiness_checks():
    strategy = _OptionsStrategy()
    tools = _wrapped_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.orders.open_orders(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit_multileg(),
        ],
    )
    tools["account_portfolio"]()
    tools["account_positions"]()
    tools["orders_open_orders"]()
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

    assert "QQQ iron-condor" in prompt
    assert "7.0 points" in prompt
    assert "-0.18 delta" in prompt
    assert "0.03 of the target" in prompt
    assert "orders_get_status" not in prompt
    assert "options_find_expiration" not in prompt
