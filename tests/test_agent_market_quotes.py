"""Quote tools exercise the existing native data and broker path."""
import json
from datetime import date

import pytest

from lumibot.components.agents import BuiltinTools
from lumibot.entities import Asset
from lumibot.entities import Quote
from lumibot.components.agents.runtime import _wrap_tool_callable
from scripts.agent_eval_production_fixture import ProductionFixture
from scripts.run_agent_evals import build_fixture


@pytest.fixture
def native_market():
    fixture = ProductionFixture(build_fixture("flat_stock_account"))
    try:
        yield fixture
    finally:
        fixture.close()


def test_single_quote_preserves_native_prices_and_strategy_clock(native_market):
    tool = BuiltinTools.market.quote().binder(native_market.strategy, native_market.manager)
    expected = native_market.strategy.get_quote(Asset("AAPL"))
    result = tool.function(symbol="AAPL")
    assert result["asset"] == {"symbol": "AAPL", "type": "stock"}
    assert result["bid"] == expected.bid
    assert result["ask"] == expected.ask
    assert result["price"] == expected.price
    assert result["datetime"] == native_market.strategy.get_datetime().isoformat()
    assert result["bar_timestamp"] == expected.raw_data["bar_timestamp"].isoformat()
    assert result["bar_timestep"] == expected.raw_data["bar_timestep"]
    assert result["available"] is True
    assert "raw_data" not in result
    json.dumps(result, allow_nan=False)


def test_batch_quotes_preserve_exact_option_contracts_and_missing_assets(native_market):
    tool = BuiltinTools.market.quotes().binder(native_market.strategy, native_market.manager)
    result = tool.function(assets_json=json.dumps([
        {"symbol": "SPY", "asset_type": "option", "expiration": "2026-08-28", "strike": 602, "right": "call"},
        {"symbol": "SPY", "asset_type": "option", "expiration": "2026-08-28", "strike": 604, "right": "call"},
        {"symbol": "QZXMISSING", "asset_type": "stock"},
    ]))
    assert result["count_requested"] == 3
    assert result["count_available"] == 2
    rows = result["quotes"]
    assert [row["asset"].get("strike") for row in rows] == [602.0, 604.0, None]
    assert rows[0]["ask"] == 2.60
    assert rows[1]["ask"] == 1.70
    assert rows[0]["asset"]["mult"] == 100
    assert rows[2]["available"] is False
    assert rows[2]["price"] is None
    json.dumps(result, allow_nan=False)


def test_stock_batch_uses_existing_native_batch_method(native_market, monkeypatch):
    original = native_market.strategy.get_quotes
    observed = []

    def recorded(assets, **kwargs):
        observed.append((list(assets), kwargs))
        return original(assets, **kwargs)

    monkeypatch.setattr(native_market.strategy, "get_quotes", recorded)
    tool = BuiltinTools.market.quotes().binder(native_market.strategy, native_market.manager)
    result = tool.function(symbols_json='["AAPL","SPY"]', quote_symbol="USD", exchange="SMART")
    assert len(observed) == 1
    assert [asset.symbol for asset in observed[0][0]] == ["AAPL", "SPY"]
    assert observed[0][1] == {"quote": Asset("USD", asset_type="forex"), "exchange": "SMART"}
    assert result["count_available"] == 2


def test_quote_tools_are_in_the_normal_toolset():
    names = {definition.name for definition in BuiltinTools.all()}
    assert {"market_quote", "market_quotes"} <= names


def test_batch_failure_keeps_independent_group_available_without_retry(native_market, monkeypatch):
    original = native_market.strategy.get_quotes
    calls = []

    def provider(assets, **kwargs):
        calls.append(kwargs["exchange"])
        if kwargs["exchange"] == "UNAVAILABLE":
            raise RuntimeError("Recorded provider group is unavailable")
        return original(assets, **kwargs)

    monkeypatch.setattr(native_market.strategy, "get_quotes", provider)
    tool = BuiltinTools.market.quotes().binder(native_market.strategy, native_market.manager)
    result = tool.function(assets_json=json.dumps([
        {"symbol": "AAPL", "exchange": "UNAVAILABLE"}, {"symbol": "SPY", "exchange": "SMART"},
    ]))
    assert calls == ["UNAVAILABLE", "SMART"]
    assert result["quotes"][0]["available"] is False
    assert result["quotes"][0]["error"]["type"] == "RuntimeError"
    assert result["quotes"][1]["available"] is True


def test_quote_rejects_another_instrument_and_keeps_nonfinite_fields_unavailable(native_market, monkeypatch):
    monkeypatch.setattr(native_market.strategy, "get_quote", lambda *a, **k: Quote(Asset("QZXOTHER"), price=100))
    tool = BuiltinTools.market.quote().binder(native_market.strategy, native_market.manager)
    with pytest.raises(ValueError, match="different instrument"):
        tool.function(symbol="AAPL")
    monkeypatch.setattr(native_market.strategy, "get_quote", lambda *a, **k: Quote(Asset("AAPL"), price=float("nan")))
    result = tool.function(symbol="AAPL")
    assert result["price"] is None
    assert result["available"] is False
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("missing", [False, True])
def test_only_available_quote_evidence_satisfies_existing_order_readiness(native_market, missing):
    context = {"enforce_order_readiness": True, "tool_calls": []}
    definitions = [BuiltinTools.account.portfolio(), BuiltinTools.account.positions(),
                   BuiltinTools.orders.open_orders(), BuiltinTools.market.quotes(), BuiltinTools.orders.submit()]
    tools = {definition.name: _wrap_tool_callable(definition.binder(native_market.strategy, native_market.manager), context)
             for definition in definitions}
    tools["account_portfolio"]()
    tools["account_positions"]()
    tools["orders_open_orders"]()
    tools["market_quotes"](symbols_json='["AAPL","QZXMISSING"]')
    result = tools["orders_submit_order"](
        symbol="QZXMISSING" if missing else "AAPL", asset_type="stock", quantity=1, side="buy", order_type="market",
        reason="The task authorizes a single small stock purchase after the current account and exact instrument quote have been inspected. The observed quote is the execution reference; account cash covers this quantity. A missing quote or changed source identity invalidates this action.",
    )
    assert result.get("tool_error", False) is missing
    if missing:
        assert "ORDER_READINESS_REQUIRED" in str(result)
    else:
        native_market.settle()
        assert native_market.strategy.get_orders()[0].is_filled()


def test_quote_at_bar_start_preserves_native_no_lookahead(native_market):
    source = native_market.broker.data_source
    data = source._data_store[source.find_asset_in_data_store(Asset("AAPL"))]
    at = data.get_iter_count(source.get_datetime())
    data.datalines["close"].dataline[at] = 999.0
    tool = BuiltinTools.market.quote().binder(native_market.strategy, native_market.manager)
    result = tool.function(symbol="AAPL")
    assert result["price"] == data.datalines["open"].dataline[at]
    assert result["price"] != 999.0
    assert result["bid"] == 229.95
    assert result["ask"] == 230.05
