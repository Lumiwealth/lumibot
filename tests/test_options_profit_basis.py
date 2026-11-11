"""Signed position value and cash-basis regression contracts; no model or network."""
from datetime import date
from types import SimpleNamespace

import pytest

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset, Order


def option(multiplier=100):
    return Asset("XYZ", asset_type="option", expiration=date(2027, 1, 15),
                 strike=100, right="put", multiplier=multiplier)


def helper(prices):
    strategy = SimpleNamespace(log_message=lambda *a, **k: None,
                               get_last_price=lambda asset: prices.get(asset))
    result = OptionsHelper(strategy)
    result._get_option_mark_from_quote = lambda asset: (prices.get(asset), None, None)
    return result


@pytest.mark.parametrize("side,basis,expected", [
    ("sell_to_open", -395, 54.50949367088608),
    ("buy_to_open", 395, -54.50949367088608),
])
def test_signed_option_profit_uses_absolute_cash_basis(side, basis, expected):
    asset = option()
    orders = [Order("neutral", asset, 1, side)]
    assert helper({asset: 1.796875}).check_spread_profit(basis, orders) == pytest.approx(expected)


def test_credit_package_nets_buy_and_sell_legs():
    short, long = option(), Asset("XYZ", asset_type="option", expiration=date(2027, 1, 15), strike=95, right="put")
    orders = [Order("neutral", short, 1, "sell_to_open"), Order("neutral", long, 1, "buy_to_open")]
    assert helper({short: 4., long: 1.}).check_spread_profit(-400., orders) == pytest.approx(25.)


def test_each_asset_keeps_its_actual_multiplier():
    contract, stock = option(10), Asset("XYZ")
    orders = [Order("neutral", contract, 2, "sell_to_open"), Order("neutral", stock, 3, "buy")]
    assert helper({contract: 3., stock: 20.}).check_spread_profit(-50., orders) == pytest.approx(100.)


def test_explicit_multiplier_remains_a_compatible_override():
    asset = option(10)
    assert helper({asset: 1.}).check_spread_profit(-200., [Order("neutral", asset, 1, "sell_to_open")], contract_multiplier=100) == pytest.approx(50.)


def test_zero_basis_has_no_defined_percentage():
    asset = option()
    assert helper({asset: 1.}).check_spread_profit(0., [Order("neutral", asset, 1, "buy_to_open")]) is None


def test_missing_mark_is_unavailable_instead_of_zero_profit():
    asset = option()
    assert helper({}).check_spread_profit(-200., [Order("neutral", asset, 1, "sell_to_open")]) is None
