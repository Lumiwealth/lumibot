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


@pytest.mark.parametrize("style,prices", [("mid", [.5, 1.0]), ("best", [.4, 1.1]), ("fastest", [.6, .9])])
def test_package_price_exposes_the_same_quote_observation_without_extra_reads(style, prices):
    helper, orders = package(SimpleNamespace(bid=.9, ask=1.1))
    details = []
    net = helper.calculate_multileg_limit_price(orders, style, price_details=details)
    assert helper.strategy.get_quote.call_count == 2
    assert [item["price"] for item in details] == pytest.approx(prices)
    assert [item["bid"] for item in details] == [.4, .9]
    assert [item["ask"] for item in details] == [.6, 1.1]
    assert sum(item["signed_price"] for item in details) == pytest.approx(net)


def test_unpriced_package_does_not_expose_partial_price_details():
    helper, orders = package(None)
    details = []
    assert helper.calculate_multileg_limit_price(orders, "mid", price_details=details) is None
    assert details == []
