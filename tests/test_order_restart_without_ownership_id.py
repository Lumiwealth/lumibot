"""Restart recovery for a strategy whose launcher sets no ``broker.order_ownership_id``.

A strategy restarted on its own restores only the orders it placed, from the checkpoint it
saved in its own variable backup (same broker, same account, same strategy name). It never
adopts other open orders it can see on the account: a manual order or another bot's order is
not its own, even though the broker reports it under the running strategy's name. Orders
that are open at the broker but not restored are named once in a warning; they stay live at
the broker. With an ownership id set, the stricter instance-bound behavior is unchanged.
"""

import importlib.util
import json
import logging
from pathlib import Path

import pytest

from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy
from lumibot.strategies._strategy import Vars

_SPEC = importlib.util.spec_from_file_location(
    "restart_transport", Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py"
)
_TRANSPORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_TRANSPORT)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError("Restart tests cannot send HTTP")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def place(broker, name, quantity=1):
    order = Order(name, Asset("SYN"), quantity, "buy", order_type="limit", limit_price=10)
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    return order


def strategy_for(broker, saved_state=None):
    strategy = object.__new__(Strategy)
    strategy._name = "own-bot"
    strategy.vars = Vars()
    if saved_state is not None:
        strategy.vars.set("_lumibot_order_state", json.dumps(saved_state))
    strategy.broker = broker
    strategy.logger = logging.getLogger("restart-recovery")
    return strategy


def restart(transport, saved_state=None, ownership_id=None):
    """A new process: fresh broker, variables restored, then the startup account sync."""
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport, name="own-bot")
    if ownership_id is not None:
        broker.order_ownership_id = ownership_id
    strategy_for(broker, saved_state)._restore_order_ownership()
    broker._first_iteration = True
    broker.sync_orders("own-bot")
    return broker, recipient


def fill(transport, broker, order):
    transport.queue_order_reads(order.identifier, {"status": "FILLED", "filledQuantity": 1, "averagePrice": 10})
    broker._reconcile_active_schwab_orders()


def callbacks(recipient):
    events = []
    while not recipient.priority_queue.empty():
        events.append(recipient.priority_queue.get_nowait())
    while not recipient.queue.empty():
        events.append(recipient.queue.get_nowait())
    return events


def test_own_open_order_with_a_checkpoint_is_restored_and_its_fill_reaches_the_strategy():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    before, _ = _TRANSPORT.data_only_schwab_broker(transport, name="own-bot")
    own = place(before, "own-bot")
    state = json.loads(json.dumps(strategy_checkpoint(before)))

    after, recipient = restart(transport, state)
    assert after.get_tracked_order(own.identifier) is not None
    assert after.order_belongs_to_local_strategy(own, "own-bot")
    fill(transport, after, own)
    events = callbacks(recipient)
    assert [event for event, _ in events].count(after.FILLED_ORDER) == 1


def test_manual_and_other_bot_orders_on_the_same_account_are_never_adopted(caplog):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    before, _ = _TRANSPORT.data_only_schwab_broker(transport, name="own-bot")
    own = place(before, "own-bot")
    other_bot, _ = _TRANSPORT.data_only_schwab_broker(transport, name="other-bot")
    theirs = place(other_bot, "other-bot")
    by_hand, _ = _TRANSPORT.data_only_schwab_broker(transport, name="own-bot")
    manual = place(by_hand, "own-bot")  # same display name, never in this bot's checkpoint
    state = strategy_checkpoint(before)

    with caplog.at_level(logging.WARNING):
        after, recipient = restart(transport, state)
    assert after.get_tracked_order(own.identifier) is not None
    assert after.get_tracked_order(theirs.identifier) is None
    assert after.get_tracked_order(manual.identifier) is None
    for order in (theirs, manual):
        fill(transport, after, order)
    assert callbacks(recipient) == []
    warnings = [r.getMessage() for r in caplog.records if "not managing" in r.getMessage().lower()]
    assert len(warnings) == 1
    assert theirs.identifier in warnings[0] and manual.identifier in warnings[0]
    assert own.identifier not in warnings[0]


def test_without_a_checkpoint_nothing_is_adopted_and_one_warning_names_the_open_orders(caplog):
    # For example orders opened before upgrading, or a strategy without variable backup.
    transport = _TRANSPORT.SchwabLifecycleTransport()
    before, _ = _TRANSPORT.data_only_schwab_broker(transport, name="own-bot")
    first, second = place(before, "own-bot"), place(before, "own-bot")

    with caplog.at_level(logging.WARNING):
        after, recipient = restart(transport, None)
        after.sync_orders("own-bot")  # a later sync does not repeat the warning
    assert after.get_all_orders() == []
    fill(transport, after, first)
    assert callbacks(recipient) == []
    warnings = [r.getMessage() for r in caplog.records if "not managing" in r.getMessage().lower()]
    assert len(warnings) == 1
    message = warnings[0]
    assert first.identifier in message and second.identifier in message
    assert "stay live at the broker" in message
    assert "cancel or manage them at the broker" in message.lower()


def test_with_an_ownership_id_recovery_stays_bound_to_that_instance():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    before, _ = _TRANSPORT.data_only_schwab_broker(transport, name="own-bot")
    before.order_ownership_id = "instance-one"
    own = place(before, "own-bot")
    state = strategy_checkpoint(before)

    same, _ = restart(transport, state, ownership_id="instance-one")
    assert same.get_tracked_order(own.identifier) is not None
    other, _ = restart(transport, state, ownership_id="instance-two")
    assert other.get_tracked_order(own.identifier) is None
    unbound, _ = restart(transport, state, ownership_id=None)
    assert unbound.get_tracked_order(own.identifier) is None, "an instance-bound checkpoint needs its id"


def test_a_checkpoint_from_another_account_or_strategy_is_never_restored():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    before, _ = _TRANSPORT.data_only_schwab_broker(transport, name="own-bot")
    own = place(before, "own-bot")
    state = strategy_checkpoint(before)
    foreign_account = {**state, "account": "another-account"}
    after, _ = restart(transport, foreign_account)
    assert after.get_tracked_order(own.identifier) is None
    renamed = {**state, "orders": [{**row, "strategy": "another-bot"} for row in state["orders"]]}
    after, _ = restart(transport, renamed)
    assert after.get_tracked_order(own.identifier) is None


def strategy_checkpoint(broker):
    return broker.export_order_state("own-bot")
