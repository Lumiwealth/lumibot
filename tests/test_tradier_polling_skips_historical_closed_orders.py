from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pandas as pd

from lumibot.brokers.tradier import Tradier
from lumibot.entities import Asset, Order


class _MockDataSource:
    def get_datetime(self):
        return datetime(2026, 1, 10)

    def get_last_prices(self, *args, **kwargs):
        return {}

    def get_quote(self, *args, **kwargs):
        return None


def _make_order(identifier: str, status: str) -> Order:
    o = Order(strategy="s", asset=Asset("SPY"), side="buy", quantity=1)
    o.identifier = identifier
    o.status = status
    o.symbol = "SPY"
    o.exchange = None
    o.time_in_force = "day"
    o.order_type = "market"
    o.trade_cost = None
    o.asset = Asset("SPY")
    o.child_orders = []
    o.order_class = Order.OrderClass.SIMPLE
    o.avg_fill_price = 1.0
    o.broker_create_date = None
    o.broker_update_date = None
    return o


def test_first_poll_does_not_track_mass_historical_closed_orders(monkeypatch):
    broker = Tradier(
        access_token="fake",
        account_number="fake",
        paper=True,
        connect_stream=False,
        data_source=_MockDataSource(),
    )
    broker.sync_positions = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    broker._safe_stream_dispatch = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

    # Simulate a large historical order list returned by Tradier (closed orders).
    rows = 5000
    df = pd.DataFrame(
        [{"id": str(i), "status": "filled", "avg_fill_price": 1.0, "exec_quantity": 1.0} for i in range(rows)]
    )
    broker.tradier = SimpleNamespace(orders=SimpleNamespace(get_orders=lambda: df))

    monkeypatch.setattr(
        broker,
        "_parse_broker_order_dict",
        lambda order_row, strategy_name=None: _make_order(str(order_row["id"]), "fill"),
    )

    broker._first_iteration = True
    broker.do_polling()

    # We should NOT have tracked thousands of closed orders after first poll.
    assert len(broker.get_all_orders()) == 0
    assert broker._first_iteration is False
