"""Package prices must not silently omit an unpriced leg."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset, Order


def package(second_quote):
    from datetime import date
    assets = [Asset("QQQ", asset_type="option", expiration=date(2026, 9, 18), strike=k, right="put")
              for k in (520, 525)]
    orders = [Order("synthetic", assets[0], 1, "buy_to_open"),
              Order("synthetic", assets[1], 1, "sell_to_open")]
    strategy = SimpleNamespace(log_message=Mock(), get_quote=Mock(side_effect=[
        SimpleNamespace(bid=.4, ask=.6), second_quote]))
    return OptionsHelper(strategy), orders


@pytest.mark.parametrize("bad_quote", [None, SimpleNamespace(bid=None, ask=1),
    SimpleNamespace(bid=float("nan"), ask=1), SimpleNamespace(bid=2, ask=1),
    SimpleNamespace(bid=-1, ask=1), SimpleNamespace(bid=1, ask=float("inf")),
    RuntimeError("fixture quote unavailable")])
def test_missing_invalid_or_failed_leg_never_returns_a_partial_package_price(bad_quote):
    helper, orders = package(bad_quote)
    assert helper.calculate_multileg_limit_price(orders, "mid") is None


@pytest.mark.parametrize("style,expected", [("mid", -.5), ("best", -.7), ("fastest", -.3)])
def test_complete_two_leg_price_uses_both_quotes(style, expected):
    helper, orders = package(SimpleNamespace(bid=.9, ask=1.1))
    assert helper.calculate_multileg_limit_price(orders, style) == pytest.approx(expected)


def test_invalid_price_style_fails_visibly():
    helper, orders = package(SimpleNamespace(bid=.9, ask=1.1))
    with pytest.raises(ValueError, match="limit_type"):
        helper.calculate_multileg_limit_price(orders, "unknown")
