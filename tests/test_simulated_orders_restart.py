"""What LumiBot-simulated orders do across a stop and restart of the strategy process.

Nothing watches the market while the process is stopped, so anything LumiBot simulates
is inactive then; only orders resting at the broker protect a position. These tests pin
what the strategy finds when it starts again (docsrc/brokers.order_support.rst, "If your
strategy is not running all the time").

Also covered elsewhere and cited in the docs:
- ProjectX: a bracket exit that filled while stopped gets its sibling cancelled on the next
  start (tests/test_projectx_bracket_lifecycle_unit.py::test_offline_child_fill_cancels_restored_live_sibling).
- ProjectX: exits placed before the stop are linked again without new orders
  (test_normal_startup_recovers_accepted_protection_without_resubmission).
- Broker-held stops, brackets and OCOs come back linked (tests/test_order_restart_protective_orders.py).
"""

from types import SimpleNamespace
from unittest.mock import Mock

from lumibot.entities import Asset, Order
from lumibot.entities.smart_limit import SmartLimitConfig
from tests.test_projectx_bracket_lifecycle_unit import broker, mes  # noqa: F401  (pytest fixtures)


def test_projectx_entry_filled_while_stopped_gets_no_exits_after_restart(broker, mes):  # noqa: F811
    """The stop and target prices live only in the running process. If the entry fills
    while the strategy is stopped, a restart finds the filled entry and no exits, and
    does not place any: the position is unprotected until the strategy acts."""
    common = {"accountId": 1, "contractId": "CON.F.US.MES.Z25", "size": 1,
              "creationTimestamp": "2026-11-01T23:58:23.786264+00:00"}
    rows = [{**common, "id": 101, "status": 2, "type": 2, "side": 0, "fillVolume": 1,
             "filledPrice": 7642.25, "customTag": "BRK_ENTRY_Strat-0123456789abcdef0123456789abcdef"}]
    broker.client.order_search = Mock(return_value=rows)
    broker._search_recent_trades = Mock(return_value=[])
    broker._get_asset_from_contract_id_cached = Mock(return_value=mes)
    broker._first_iteration = True

    broker.sync_orders(SimpleNamespace(name="Strat"))

    assert broker.client.placed == []
    entry = broker.get_tracked_order("101")
    assert entry is not None and entry.is_filled()
    assert "101" not in getattr(broker, "_bracket_meta", {})


def test_smart_limit_restored_from_saved_orders_restarts_its_steps():
    """A saved smart limit keeps its settings but not its step timer, so re-pricing
    starts over from the first step after a restart."""
    order = Order("s", Asset("SPY"), 1, side="buy", order_type="smart_limit",
                  smart_limit=SmartLimitConfig(), limit_price=10)
    order._smart_limit_state = {"created_at": 1.0, "step_index": 3, "steps": 5, "step_seconds": 10,
                                "final_hold_seconds": 30}

    restored = Order.from_dict(order.to_dict())

    assert restored.order_type == Order.OrderType.SMART_LIMIT
    assert restored.smart_limit == order.smart_limit
    assert getattr(restored, "_smart_limit_state", None) is None


def test_simulated_orders_are_labelled_so_callers_can_check_first(broker):  # noqa: F811
    support = broker.supports_order(order_type="market", order_class="bracket")
    assert support.simulated and "while the strategy is running" in support.reason
