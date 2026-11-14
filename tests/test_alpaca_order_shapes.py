"""Alpaca advanced-order request shapes; no network.

Alpaca documents bracket/OTO as an ordinary entry order plus nested
``take_profit`` {limit_price} and ``stop_loss`` {stop_price, optional limit_price}
legs, and OCO with ``type`` "limit" (https://docs.alpaca.markets/docs/orders-at-alpaca).
There is no trailing field on a leg. LumiBot must send the entry type the strategy
asked for and must refuse a trailing leg instead of sending it as a fixed stop.
"""

from unittest.mock import MagicMock

import pytest

from lumibot.brokers.alpaca import Alpaca
from lumibot.entities import Asset, Order


def _broker():
    broker = Alpaca.__new__(Alpaca)
    broker.api = MagicMock()
    broker.api.submit_order.return_value = MagicMock(id="alpaca-1", status="new")
    broker._unprocessed_orders = []
    return broker


def _fields(broker):
    return broker.api.submit_order.call_args.kwargs["order_data"].to_request_fields()


def _order(**kwargs):
    return Order("strategy", Asset("SPY"), 10, side="buy", **kwargs)


def test_bracket_limit_entry_is_sent_as_limit():
    broker = _broker()
    broker._submit_order(
        _order(
            limit_price=500.25,
            order_class=Order.OrderClass.BRACKET,
            secondary_limit_price=520.0,
            secondary_stop_price=490.0,
        )
    )
    fields = _fields(broker)
    assert fields["type"] == Order.OrderType.LIMIT
    assert fields["limit_price"] == "500.25"
    assert fields["take_profit"] == {"limit_price": 520.0}
    assert fields["stop_loss"] == {"stop_price": 490.0}


def test_oto_limit_entry_is_sent_as_limit():
    broker = _broker()
    broker._submit_order(
        _order(limit_price=500.25, order_class=Order.OrderClass.OTO, secondary_stop_price=490.0)
    )
    assert _fields(broker)["type"] == Order.OrderType.LIMIT


def test_bracket_market_entry_still_market():
    broker = _broker()
    broker._submit_order(
        _order(order_class=Order.OrderClass.BRACKET, secondary_limit_price=520.0, secondary_stop_price=490.0)
    )
    fields = _fields(broker)
    assert fields["type"] == Order.OrderType.MARKET
    assert "limit_price" not in fields


def test_bracket_trailing_leg_is_refused_not_sent_as_fixed_stop():
    broker = _broker()
    order = _order(order_class=Order.OrderClass.OTO, secondary_stop_price=490.0, secondary_trail_percent=2.0)
    with pytest.raises(ValueError, match="not supported by Alpaca"):
        broker._submit_order(order)
    broker.api.submit_order.assert_not_called()


def test_bracket_stop_entry_is_refused_not_sent_as_market():
    broker = _broker()
    order = _order(
        stop_price=505.0,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=520.0,
        secondary_stop_price=490.0,
    )
    with pytest.raises(ValueError, match="not supported by Alpaca"):
        broker._submit_order(order)
    broker.api.submit_order.assert_not_called()
