"""Backtest SMART_LIMIT packages whose legs have no bid and ask.

When a package cannot be priced from quotes the simulator fills it market-style. That
fallback must behave like a market package: every leg is priced for the current bar (its
own bar open, else for options the labeled estimate), and either every leg fills or none.
No network: data and execution boundaries are replaced, the fallback owner is real.
"""
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd

from lumibot.backtesting import BacktestingBroker
from lumibot.entities import Asset, Order, SmartLimitConfig


def _package(strikes=(100, 110)):
    usd = Asset("USD", asset_type="forex")
    children = [
        Order(
            "synthetic",
            Asset("XYZ", asset_type="option", expiration=date(2030, 6, 21), strike=strike, right="call"),
            1,
            "buy_to_open" if index % 2 == 0 else "sell_to_open",
            quote=usd,
        )
        for index, strike in enumerate(strikes)
    ]
    return Order(
        "synthetic",
        Asset("XYZ", asset_type="multileg"),
        1,
        "buy",
        order_class=Order.OrderClass.MULTILEG,
        child_orders=children,
        smart_limit=SmartLimitConfig(),
    )


def _broker(opens, estimates):
    """opens: {strike: bar open or None}; estimates: {strike: (price, source)}."""
    broker = object.__new__(BacktestingBroker)
    broker._audit_enabled = lambda: False
    broker._execute_filled_order = Mock()
    broker._record_fill_price_source = Mock()

    def history(asset, **_kwargs):
        value = opens.get(asset.strike)
        if value is None:
            return None
        return SimpleNamespace(df=pd.DataFrame({"open": [value]}), empty=False)

    broker.data_source = SimpleNamespace(_timestep="minute", get_historical_prices=history)
    broker._option_fill_price_at_bar = lambda leg: estimates.get(leg.asset.strike, (None, None))
    return broker


def _fills(broker):
    return {
        call.kwargs["order"].asset.strike: call.kwargs["price"]
        for call in broker._execute_filled_order.call_args_list
    }


def test_leg_without_a_bar_fills_at_its_labeled_estimate():
    parent = _package()
    broker = _broker({100: 5.0, 110: None}, {110: (2.25, "estimated")})

    assert broker._fill_multileg_children_at_market_open(parent, None) is True

    assert _fills(broker) == {100: 5.0, 110: 2.25}
    by_strike = {leg.asset.strike: leg for leg in parent.child_orders}
    assert by_strike[100]._price_source == "market"
    assert by_strike[110]._price_source == "estimated"
    broker._record_fill_price_source.assert_called_once_with("estimated")


def test_no_leg_fills_when_one_leg_cannot_be_priced():
    parent = _package()
    broker = _broker({100: 5.0, 110: None}, {})

    assert broker._fill_multileg_children_at_market_open(parent, None) is False

    broker._execute_filled_order.assert_not_called()
    assert all(not hasattr(leg, "_price_source") for leg in parent.child_orders)


def test_legs_with_bars_still_fill_at_their_own_open():
    parent = _package()
    broker = _broker({100: 5.0, 110: 2.0}, {100: (9.0, "estimated"), 110: (9.0, "estimated")})

    assert broker._fill_multileg_children_at_market_open(parent, None) is True

    assert _fills(broker) == {100: 5.0, 110: 2.0}
    assert all(leg._price_source == "market" for leg in parent.child_orders)
    broker._record_fill_price_source.assert_not_called()
