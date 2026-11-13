"""Native ownership boundaries: account visibility is not submission ownership."""

import importlib.util
import json
import logging
from pathlib import Path
from queue import Empty
from threading import Event
from types import SimpleNamespace

import pytest

from lumibot.entities import Asset, Order, Position
from lumibot.strategies import Strategy
from lumibot.strategies._strategy import Vars
from lumibot.strategies.strategy_executor import StrategyExecutor

_SPEC = importlib.util.spec_from_file_location(
    "ownership_transport", Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py"
)
_TRANSPORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_TRANSPORT)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError("Ownership contract tests cannot send HTTP")

    monkeypatch.setattr("httpx.Client.send", reject)
    monkeypatch.setattr("requests.sessions.Session.send", reject)


def submitted(broker):
    order = Order("same-name", Asset("SYN"), 1, "buy", order_type="limit", limit_price=10)
    broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    return order


@pytest.mark.parametrize("first_iteration", [True, False])
def test_account_sync_does_not_adopt_unsubmitted_same_name_orders(first_iteration):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    first, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    second, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own, other = submitted(first), submitted(second)
    first._first_iteration = first_iteration

    # The native account read contains both orders. The parser uses the local
    # display name for both, which cannot prove the second order is ours.
    visible = first._pull_all_orders("same-name", None)
    assert {o.identifier for o in visible} == {own.identifier, other.identifier}
    first.sync_orders("same-name")

    assert first.get_tracked_order(own.identifier) is own
    assert first.get_tracked_order(other.identifier) is None
    assert second.get_tracked_order(own.identifier) is None
    assert second.get_tracked_order(other.identifier) is other


def test_same_name_tag_cannot_adopt_an_unknown_order():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    observed = Order("same-name", Asset("SYN"), 1, "buy", identifier="manual", tag="same-name")
    observed.status = Order.OrderStatus.NEW
    broker._process_schwab_order_snapshot(observed)
    assert broker.get_tracked_order("manual") is None


@pytest.mark.parametrize("callback", ["_on_new_order", "_on_canceled_order", "_on_partially_filled_order", "_on_filled_order"])
def test_unknown_same_name_order_cannot_reach_strategy_callbacks(callback):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    unknown = Order("same-name", Asset("SYN"), 1, "buy", identifier="manual")
    args = (unknown,) if callback in {"_on_new_order", "_on_canceled_order"} else (None, unknown, 10, 1, 1)
    getattr(broker, callback)(*args)
    assert recipient.queue.empty()
    assert recipient.priority_queue.empty()


def test_submitted_order_still_delivers_real_fill_once():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own = submitted(broker)
    # Drain the creation notification before checking real fill delivery.
    while True:
        try:
            recipient.queue.get_nowait()
        except Empty:
            break
    transport.queue_order_reads(own.identifier, {"status": "FILLED", "filledQuantity": 1, "averagePrice": 10})
    for _ in range(2):
        broker._reconcile_active_schwab_orders()
    fills = []
    while True:
        try:
            event, data = recipient.priority_queue.get_nowait()
        except Empty:
            break
        if event == broker.FILLED_ORDER:
            fills.append(data)
    assert len(fills) == 1
    assert fills[0]["order"] is own


def test_restart_checkpoint_preserves_prior_fills_without_another_callback():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own = Order("same-name", Asset("SYN"), 100, "buy", order_type="limit", limit_price=10)
    broker.submit_order(own)
    _TRANSPORT.drain_stream_events(broker)
    transport.queue_order_reads(own.identifier, {"status": "PARTIALLY_FILLED", "filledQuantity": 40, "averagePrice": 10})
    broker._reconcile_active_schwab_orders()
    state = json.loads(json.dumps(broker.export_order_state("same-name")))

    recovered, recipient = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    recovered.restore_order_state(state, "same-name")
    recovered._reconcile_active_schwab_orders()
    assert recipient.priority_queue.empty()
    restored = recovered.get_tracked_order(own.identifier)
    assert sum(t.quantity for t in restored.transactions) == 40

    transport.queue_order_reads(own.identifier, {"status": "FILLED", "filledQuantity": 100, "averagePrice": 10})
    recovered._reconcile_active_schwab_orders()
    event, data = recipient.priority_queue.get_nowait()
    assert event == recovered.FILLED_ORDER
    assert data["quantity"] == 60
    assert sum(t.quantity for t in restored.transactions) == 100


def test_checkpoint_from_another_account_cannot_claim_overlapping_order_ids():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    broker.account_number = "synthetic-account-one"
    submitted(broker)
    state = broker.export_order_state("same-name")
    recovered, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    recovered.account_number = "synthetic-account-two"
    with pytest.raises(ValueError, match="different account"):
        recovered.restore_order_state(state, "same-name")
    assert recovered.get_all_orders() == []


@pytest.mark.parametrize("read_path", ["account", "direct"])
def test_only_a_known_parent_can_establish_child_order_ownership(read_path):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own = submitted(broker)
    child = Order("same-name", Asset("SYN"), 1, "sell", identifier="broker-child", status="new")
    parent = Order.from_dict(own.to_dict())
    parent.child_orders = [child]
    unknown_child = Order("same-name", Asset("SYN"), 1, "sell", identifier="unrelated-child", status="new")
    unknown_parent = Order("same-name", Asset("SYN"), 1, "buy", identifier="unrelated-parent", status="new")
    unknown_parent.child_orders = [unknown_child]
    broker._pull_all_orders = lambda *args: [parent, unknown_parent]
    if read_path == "account":
        broker.sync_orders("same-name")
    else:
        broker._process_schwab_order_snapshot(parent)
        broker._process_schwab_order_snapshot(unknown_parent)
    assert broker.get_tracked_order(child.identifier) is not None
    assert broker.get_tracked_order(child.identifier) in own.child_orders
    assert broker.get_tracked_order(child.identifier).parent_identifier == own.identifier
    assert broker.get_tracked_order(unknown_child.identifier) is None
    assert broker.get_tracked_order(unknown_parent.identifier) is None


@pytest.mark.parametrize("backend", ["scheduled", "database"])
def test_normal_variable_backup_restores_local_order_ownership(backend, tmp_path, monkeypatch):
    from sqlalchemy import create_engine, text

    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE variables (id TEXT, last_updated TEXT, variables TEXT, strategy_id TEXT)"))
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", str(backend == "scheduled").lower())
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(tmp_path / "state.json"))

    def make_strategy(broker):
        strategy = object.__new__(Strategy)
        broker.order_ownership_id = "stable-instance"
        strategy._name = "same-name"
        strategy.is_backtesting = False
        strategy.vars = Vars()
        strategy.logger = logging.getLogger("ownership-backup")
        strategy._last_backup_state = None
        strategy.db_connection_str = "sqlite:///:memory:"
        strategy.db_engine = engine
        strategy.backup_table_name = "variables"
        strategy.should_backup_variables_to_database = True
        strategy.broker = broker
        return strategy

    try:
        transport = _TRANSPORT.SchwabLifecycleTransport()
        broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
        own = submitted(broker)
        original = make_strategy(broker)
        original.vars.set("unrelated_setting", 37)
        original.backup_variables_to_db()
        recovered, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
        restarted = make_strategy(recovered)
        restarted.load_variables_from_db()
        assert restarted.vars.unrelated_setting == 37
        assert recovered.get_tracked_order(own.identifier) is not None
        assert recovered.order_belongs_to_local_strategy(own, "same-name")
    finally:
        engine.dispose()


def test_checkpoint_keeps_child_fill_ledger_and_omits_raw_responses():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own = submitted(broker)
    child = Order("same-name", Asset("SYN"), 10, "sell", identifier="broker-child", status="partially_filled")
    child.raw_broker_payload = {"accountNumber": "synthetic-private-value"}
    child.add_transaction(10, 4)
    own.child_orders = [child]
    state = broker.export_order_state("same-name")
    assert "raw_broker_payload" not in json.dumps(state)
    recovered, recipient = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    recovered.restore_order_state(json.loads(json.dumps(state)), "same-name")
    restored = recovered.get_tracked_order(child.identifier)
    assert restored is not None
    assert restored in recovered.get_tracked_order(own.identifier).child_orders
    assert sum(t.quantity for t in restored.transactions) == 4
    assert recipient.priority_queue.empty()


@pytest.mark.parametrize("callback", ["_on_new_order", "_on_canceled_order", "_on_partially_filled_order", "_on_filled_order"])
def test_executor_rechecks_ownership_before_invoking_strategy(callback):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own = submitted(broker)
    unknown = Order("same-name", Asset("SYN"), 1, "buy", identifier="manual")
    calls = []
    strategy = SimpleNamespace(name="same-name", logger=logging.getLogger("ownership-executor"),
                               portfolio_value=1000, hide_trades=False, send_discord_message=lambda *a, **k: None)
    setattr(strategy, callback[1:], lambda *args: calls.append(args))
    executor = object.__new__(StrategyExecutor)
    executor.broker = broker
    executor.strategy = strategy
    executor.stop_event = Event()
    for order in (unknown, own):
        args = (order,) if callback in {"_on_new_order", "_on_canceled_order"} else (Position("same-name", order.asset, 1), order, 10, 1, 1)
        getattr(executor, callback)(*args)
    assert len(calls) == 1
    assert own in calls[0]
    assert broker._get_order_health().snapshot()["fill_callback_entries"] == int(callback in {"_on_partially_filled_order", "_on_filled_order"})


def test_final_health_line_matches_native_callback_counts(caplog):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own = submitted(broker)
    unknown = Order("same-name", Asset("SYN"), 1, "buy", identifier="manual")
    broker._on_filled_order(None, unknown, 10, 1, 1)
    transport.queue_order_reads(own.identifier, {"status": "FILLED", "filledQuantity": 1, "averagePrice": 10})
    broker._reconcile_active_schwab_orders()
    assert recipient.priority_queue.get_nowait()[0] == broker.FILLED_ORDER
    with caplog.at_level("INFO"):
        broker.cleanup_streams()
        broker.cleanup_streams()
    rows = [r.message for r in caplog.records if "[OrderRunHealth]" in r.message]
    assert len(rows) == 1
    summary = json.loads(rows[0].split("[OrderRunHealth] ", 1)[1])
    assert summary["fill_callbacks_enqueued"] == 1
    assert summary["fill_callback_entries"] == 0  # This control inspected the queue, not the executor.
    assert summary["foreign_callback_attempts"] == 1
    assert summary["contradictions"]["unowned_order_callback"] == 1


def test_checkpoint_from_same_named_instance_cannot_reassign_order_ownership():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    first, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    second, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    first.order_ownership_id = "instance-one"
    second.order_ownership_id = "instance-two"
    submitted(first)
    state = first.export_order_state("same-name")
    with pytest.raises(ValueError, match="different instance"):
        second.restore_order_state(state, "same-name")
    assert second.get_all_orders() == []


def test_automatic_restore_never_lets_a_name_alone_claim_an_instance_bound_checkpoint():
    # Without an ownership id a plain restart restores its own checkpoint
    # (tests/test_order_restart_without_ownership_id.py); a checkpoint written by an
    # id-bound instance still needs that id.
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    broker.order_ownership_id = "instance-one"
    own = submitted(broker)
    second, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    strategy = object.__new__(Strategy)
    strategy._name = "same-name"
    strategy.vars = Vars()
    strategy.vars.set("_lumibot_order_state", json.dumps(broker.export_order_state("same-name")))
    strategy.broker = second
    strategy.logger = logging.getLogger("ownership-recovery")
    strategy._restore_order_ownership()
    assert second.get_tracked_order(own.identifier) is None


def test_hash_only_accounts_cannot_restore_each_others_checkpoint():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    first, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    second, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    first.hash_value = "synthetic-account-hash-one"
    second.hash_value = "synthetic-account-hash-two"
    submitted(first)
    with pytest.raises(ValueError, match="different account"):
        second.restore_order_state(first.export_order_state("same-name"), "same-name")
    assert second.get_all_orders() == []


@pytest.mark.parametrize("recover", [False, True])
def test_owned_submission_with_an_arbitrary_tag_still_receives_its_fill(recover):
    transport = _TRANSPORT.SchwabLifecycleTransport()
    broker, recipient = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own = Order("same-name", Asset("SYN"), 1, "buy", order_type="limit", limit_price=10, tag="operation-42")
    broker.submit_order(own)
    _TRANSPORT.drain_stream_events(broker)
    if recover:
        state = broker.export_order_state("same-name")
        broker, recipient = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
        broker.restore_order_state(state, "same-name")
    transport.queue_order_reads(own.identifier, {"status": "FILLED", "filledQuantity": 1, "averagePrice": 10})
    broker._reconcile_active_schwab_orders()
    assert recipient.priority_queue.get_nowait()[0] == broker.FILLED_ORDER


def test_automatic_restore_requires_adapter_account_identity():
    transport = _TRANSPORT.SchwabLifecycleTransport()
    first, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    second, _ = _TRANSPORT.data_only_schwab_broker(transport, name="same-name")
    own = submitted(first)
    first.order_ownership_id = second.order_ownership_id = "stable-instance"
    # Exercise the generic default used by adapters without an account scope.
    first._order_checkpoint_account_identity = second._order_checkpoint_account_identity = lambda: None
    strategy = object.__new__(Strategy)
    strategy._name = "same-name"
    strategy.vars = Vars()
    strategy.vars.set("_lumibot_order_state", json.dumps(first.export_order_state("same-name")))
    strategy.broker = second
    strategy.logger = logging.getLogger("ownership-recovery")
    strategy._restore_order_ownership()
    assert second.get_tracked_order(own.identifier) is None
