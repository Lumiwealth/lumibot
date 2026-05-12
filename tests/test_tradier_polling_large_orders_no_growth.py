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
    o.avg_fill_price = None
    o.broker_create_date = None
    o.broker_update_date = None
    return o


def test_tradier_polling_large_orders_does_not_accumulate_orders_or_events(monkeypatch):
    # Create a Tradier broker without touching the network.
    broker = Tradier(
        access_token="fake",
        account_number="fake",
        paper=True,
        connect_stream=False,
        data_source=_MockDataSource(),
    )

    # Disable position syncing and any real dispatching.
    broker.sync_positions = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    dispatch_calls: list[str] = []
    broker._safe_stream_dispatch = lambda event, **kwargs: dispatch_calls.append(event)  # type: ignore[method-assign]

    # Seed a single stored order whose status is "new".
    stored = _make_order("123", "new")
    broker._new_orders.append(stored)
    broker._first_iteration = False

    # Create a "large" Tradier orders DataFrame where the same order appears as "submitted".
    # This previously could trigger repeated NEW events under polling.
    df = pd.DataFrame([{"id": "123", "status": "submitted"} for _ in range(2000)])

    # Patch the underlying tradier client to return our synthetic DataFrame.
    broker.tradier = SimpleNamespace(orders=SimpleNamespace(get_orders=lambda: df))

    # Patch parsing to keep it lightweight and deterministic.
    monkeypatch.setattr(
        broker,
        "_parse_broker_order_dict",
        lambda order_row, strategy_name=None: _make_order(str(order_row["id"]), str(order_row["status"])),
    )

    # Poll repeatedly; we should not dispatch NEW repeatedly and should not accumulate unbounded state.
    for _ in range(25):
        broker.do_polling()

    # No status-change dispatches expected once "submitted" is treated as equivalent to "new/open".
    assert dispatch_calls == []

    # Ensure we didn't accidentally create thousands of tracked orders from the huge raw list.
    assert len(broker.get_all_orders()) == 1

    # Trade-event history in live is bounded; should not grow here anyway since no dispatch.
    assert len(broker._trade_event_log_rows) == 0
