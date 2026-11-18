"""A strategy-built bracket must leave no phantom exits behind on ProjectX.

``Order(order_class=BRACKET, secondary_limit_price=..., secondary_stop_price=...)``
builds two local child placeholders. ProjectX simulates brackets: after the entry
fills, the broker places its own take-profit and stop orders. The placeholders are
never sent to the venue, so they must never be tracked as live orders. Otherwise
order sync keeps looking them up forever and the strategy thinks protective exits
are still working after the real exits have filled or been cancelled.
"""

from lumibot.entities import Order

from tests.test_projectx_bracket_lifecycle_unit import broker, mes  # noqa: F401


def _strategy_bracket(asset):
    return Order(
        strategy="Strat",
        asset=asset,
        quantity=1,
        side="buy",
        order_type="limit",
        limit_price=5000.0,
        order_class=Order.OrderClass.BRACKET,
        secondary_limit_price=5050.0,
        secondary_stop_price=4975.0,
    )


def _active_unsent(broker):
    return [
        order
        for order in broker.get_all_orders()
        if order.is_active() and not getattr(order, "_projectx_provider_id", None)
    ]


def test_native_bracket_placeholders_are_never_tracked_as_live_orders(broker, mes):
    parent = _strategy_bracket(mes)
    assert len(parent.child_orders) == 2  # placeholders built by Order itself

    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000, filled_quantity=1, multiplier=5)

    children = parent._synthetic_bracket["children"]
    assert set(children) == {"tp", "sl"}
    placeholder_ids = {child.identifier for child in parent.child_orders} - set(children.values())
    tracked_ids = {order.identifier for order in broker.get_all_orders()}
    assert not placeholder_ids & tracked_ids


def test_finished_bracket_leaves_no_active_unsent_orders(broker, mes):
    parent = _strategy_bracket(mes)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000, filled_quantity=1, multiplier=5)
    children = parent._synthetic_bracket["children"]
    tp = broker.get_tracked_order(children["tp"])
    sl = broker.get_tracked_order(children["sl"])

    broker._process_trade_event(sl, broker.FILLED_ORDER, price=4975, filled_quantity=1, multiplier=5)
    broker._process_trade_event(tp, broker.CANCELED_ORDER)

    assert _active_unsent(broker) == []
    # The real exits name the entry that opened them.
    assert tp.parent_identifier == parent.identifier
    assert sl.parent_identifier == parent.identifier
    assert not any(
        order.is_active() for order in broker.get_tracked_orders("Strat", mes)
    ), [str(o) for o in broker.get_tracked_orders("Strat", mes) if o.is_active()]


def test_canceled_unfilled_entry_is_finished_and_tracked_once(broker, mes):
    parent = _strategy_bracket(mes)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.CANCELED_ORDER)
    broker._process_trade_event(parent, broker.CANCELED_ORDER)  # a repeated cancel event

    assert not parent.is_active()
    matches = [o for o in broker.get_all_orders() if o.identifier == parent.identifier]
    assert len(matches) == 1
