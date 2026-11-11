"""Exact-contract arithmetic through the native tool and existing helper."""
import json
from datetime import date
from types import SimpleNamespace

import pytest

from lumibot.components.agents import BuiltinTools
from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset
from scripts.agent_eval_production_fixture import ProductionFixture
from scripts.run_agent_evals import build_fixture


@pytest.fixture
def native_market():
    fixture = ProductionFixture(build_fixture('flat_options_account'))
    try:
        yield fixture
    finally:
        fixture.close()


def legs(strikes, quantities, right='call'):
    return [{'symbol': 'SPY', 'expiration': '2026-08-28', 'strike': strike,
             'right': right, 'quantity': abs(qty), 'side': 'buy_to_open' if qty > 0 else 'sell_to_open'}
            for strike, qty in zip(strikes, quantities)]


@pytest.mark.parametrize('strikes,quantities,delta,theta', [
    ([602, 604], [1, -1], .12, 0),
    ([602, 604], [2, -1], .48, -.05),
    ([602, 604, 606], [1, -2, 1], .03, 0),
])
def test_existing_greek_tool_accepts_exact_packages_and_labels_units(native_market, strikes, quantities, delta, theta):
    tool = BuiltinTools.options.get_greeks().binder(native_market.strategy, native_market.manager)
    result = tool.function(legs_json=json.dumps(legs(strikes, quantities)))
    assert result['available'] is True
    assert result['greeks']['delta'] == pytest.approx(delta)
    assert result['greeks']['theta'] == pytest.approx(theta)
    assert result['underlying_equivalent_greeks']['delta'] == pytest.approx(delta * 100)
    assert result['underlying_equivalent_greeks']['theta'] == pytest.approx(theta * 100)
    assert [row['asset']['strike'] for row in result['legs']] == list(map(float, strikes))
    assert [row['quantity'] for row in result['legs']] == quantities
    assert [row['multiplier'] for row in result['legs']] == [100] * len(strikes)
    assert [row['underlying_quantity'] for row in result['legs']] == [q * 100 for q in quantities]
    assert native_market.strategy.get_orders() == []
    json.dumps(result, allow_nan=False)


def test_package_put_greeks_preserve_raw_signs_and_signed_positions(native_market):
    result = BuiltinTools.options.get_greeks().binder(native_market.strategy, native_market.manager).function(
        legs_json=json.dumps(legs([592, 594], [1, -1], right='put')))
    assert [row['greeks']['delta'] for row in result['legs']] == [-.08, -.15]
    assert result['greeks']['delta'] == pytest.approx(.07)
    assert result['underlying_equivalent_greeks']['delta'] == pytest.approx(7)


def test_missing_field_remains_unknown_without_losing_other_greeks(native_market, monkeypatch):
    original = native_market.strategy.get_greeks
    def returned(option, **kwargs):
        result = original(option, **kwargs)
        if option.strike == 604:
            result = dict(result)
            result.pop('theta')
            result['vega'] = float('nan')
        return result
    monkeypatch.setattr(native_market.strategy, 'get_greeks', returned)
    result = BuiltinTools.options.get_greeks().binder(native_market.strategy, native_market.manager).function(
        legs_json=json.dumps(legs([602, 604], [1, -1])))
    assert result['available'] is False
    assert result['greeks']['delta'] == pytest.approx(.12)
    assert result['greeks']['theta'] is None
    assert result['underlying_equivalent_greeks']['vega'] is None
    assert result['legs'][1]['greeks']['theta'] is None
    json.dumps(result, allow_nan=False)


def test_existing_helper_preserves_legacy_units_and_reports_actual_multipliers(native_market):
    positions = [SimpleNamespace(asset=Asset('SPY', asset_type='option', expiration=date(2026, 8, 28),
        strike=strike, right='call', multiplier=multiplier), quantity=qty)
        for strike, qty, multiplier in [(602, 2, 50), (604, -1, 100)]]
    helper = OptionsHelper(native_market.strategy)
    legacy = helper.aggregate_portfolio_greeks(positions, Asset('SPY'))
    assert legacy == pytest.approx({'delta': .48, 'gamma': .012, 'theta': -.05, 'vega': .10})
    detail = helper.aggregate_portfolio_greeks(positions, Asset('SPY'), include_details=True)
    assert detail['greeks'] == pytest.approx(legacy)
    assert detail['underlying_equivalent_greeks']['delta'] == pytest.approx(12)
    assert [row['multiplier'] for row in detail['legs']] == [50, 100]


def test_package_query_preserves_explicit_data_arguments(native_market, monkeypatch):
    original = native_market.strategy.get_greeks
    seen = []
    def observed(asset, **kwargs):
        seen.append(kwargs)
        return original(asset, **kwargs)
    monkeypatch.setattr(native_market.strategy, 'get_greeks', observed)
    result = BuiltinTools.options.get_greeks().binder(native_market.strategy, native_market.manager).function(
        legs_json=json.dumps(legs([602, 604], [2, -1])), underlying_price=600,
        risk_free_rate=.06, query_greeks=True)
    assert result['available']
    assert seen == [{'underlying_price': 600, 'risk_free_rate': .06, 'query_greeks': True}] * 2


def test_multi_underlying_package_keeps_input_order(native_market):
    selected = legs([602, 604, 606], [1, -2, 1])
    selected[1]['symbol'] = 'AAPL'
    result = BuiltinTools.options.get_greeks().binder(native_market.strategy, native_market.manager).function(
        legs_json=json.dumps(selected))
    assert [row['asset']['symbol'] for row in result['legs']] == ['SPY', 'AAPL', 'SPY']
    assert [row['input_index'] for row in result['legs']] == [0, 1, 2]

