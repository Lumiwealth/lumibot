"""A filled multi-leg parent gets one final row in the trade-event log.

The legs carry the fills; the parent row used to stop at "new", so anything reading the
file's last row per order (progress views, order lists) showed every combo as pending
forever. The final row uses the status "legs_filled" with no filled quantity, so tools that
count trades from this file (BotSpot Node counts rows whose status is fill/filled/
partially_filled/partial_fill in status/event/event_type/type, or whose filled_quantity is
nonzero) count exactly what they counted before: the leg fills.
"""

import math
from decimal import Decimal

import pandas as pd

from lumibot.entities.order import Order
from tests.test_backtesting_multileg_unit import _build_stock_strategy, _make_child_order

NODE_FILL_STATUSES = {"fill", "filled", "partially_filled", "partial_fill"}
NODE_STATUS_COLUMNS = ("status", "event", "event_type", "type")
NODE_QUANTITY_COLUMNS = ("filled_quantity", "filled_qty")


def node_trade_count(frame: pd.DataFrame) -> int:
    """A copy of BotSpot Node's countTradeEvents rule (backtest.service.ts, filled_events mode)."""
    lower = {c.lower(): c for c in frame.columns}
    status_cols = [lower[c] for c in NODE_STATUS_COLUMNS if c in lower]
    qty_cols = [lower[c] for c in NODE_QUANTITY_COLUMNS if c in lower]
    count = 0
    for _, row in frame.iterrows():
        def norm(v):
            return str("" if v is None or (isinstance(v, float) and math.isnan(v)) else v).strip().lower().replace("-", "_").replace(" ", "_")
        hit = any(norm(row[c]) in NODE_FILL_STATUSES for c in status_cols)
        if not hit:
            for c in qty_cols:
                try:
                    q = float(row[c])
                except (TypeError, ValueError):
                    continue
                if math.isfinite(q) and q != 0:
                    hit = True
        count += hit
    return count


def _run_combo():
    strategy, broker, asset, quote = _build_stock_strategy()
    buy = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.BUY)
    sell = _make_child_order(strategy, asset, quote, "10", Order.OrderSide.SELL)
    parent = Order(strategy=strategy, asset=asset, quantity=Decimal("20"), side=Order.OrderSide.BUY,
                   order_type=Order.OrderType.MARKET, order_class=Order.OrderClass.MULTILEG,
                   child_orders=[buy, sell])
    strategy.submit_order(parent)
    broker._execute_filled_order(buy, price=50.0, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()
    broker._execute_filled_order(sell, price=50.0, filled_quantity=Decimal("10"), strategy=strategy)
    strategy._executor.process_queue()
    broker.process_pending_orders(strategy)
    strategy._executor.process_queue()
    return broker, parent, (buy, sell)


def test_filled_parent_ends_with_one_legs_filled_row():
    broker, parent, legs = _run_combo()
    assert parent.is_filled()
    events = broker._trade_event_log_df
    rows = events[events["identifier"] == parent.identifier]
    assert list(rows["status"]) == ["new", "legs_filled"]
    final = rows.iloc[-1]
    assert final["filled_quantity"] is None or pd.isna(final["filled_quantity"])
    for leg in legs:
        assert list(events[events["identifier"] == leg.identifier]["status"])[-1] == "fill"


def test_node_trade_count_is_unchanged_by_the_parent_row():
    broker, parent, legs = _run_combo()
    events = broker._trade_event_log_df
    without_parent_final = events[~((events["identifier"] == parent.identifier) & (events["status"] == "legs_filled"))]
    assert node_trade_count(events) == node_trade_count(without_parent_final) == 2
