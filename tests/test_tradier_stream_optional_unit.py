from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from lumibot.brokers.tradier import Tradier
from lumibot.entities import Asset, Order


def test_tradier_submit_multileg_without_stream_does_not_crash(mocker):
    # Avoid Tradier.__init__ (no network / credentials).
    broker = Tradier.__new__(Tradier)
    broker.tradier = SimpleNamespace(orders=SimpleNamespace(multileg_order=mocker.Mock(return_value={"id": "oid"})))
    broker._unprocessed_orders = []
    broker.name = "Tradier"
    broker.NEW_ORDER = "new"
    # Ensure the attribute is absent to reproduce the historical AttributeError.
    if hasattr(broker, "stream"):
        delattr(broker, "stream")

    underlying = "SPY"
    exp = date(2026, 1, 16)
    leg1 = Order(
        "unit",
        asset=Asset(underlying, asset_type=Asset.AssetType.OPTION, expiration=exp, strike=500.0, right="call"),
        quantity=1,
        side=Order.OrderSide.BUY_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
    )
    leg2 = Order(
        "unit",
        asset=Asset(underlying, asset_type=Asset.AssetType.OPTION, expiration=exp, strike=505.0, right="call"),
        quantity=1,
        side=Order.OrderSide.SELL_TO_OPEN,
        order_type=Order.OrderType.LIMIT,
    )

    parent = broker._submit_multileg_order([leg1, leg2], order_type="debit", duration="day", price=0.1, tag="t")  # noqa: SLF001
    assert parent.identifier == "oid"
