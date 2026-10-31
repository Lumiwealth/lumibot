"""Native monitoring contracts. These are unit tests, not model evals."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier, Event

import pytest

from lumibot.components.agents.attention import AttentionMonitor


class Store(dict):
    def set(self, key, value):
        self[key] = value


class Strategy:
    def __init__(self, store=None):
        self.vars = store if store is not None else Store()
        self.now = datetime(2026, 10, 1, 14, tzinfo=timezone.utc)
        self.model_calls = 0

    def get_datetime(self):
        return self.now


def watch(monitor, name="boundary", threshold=100, **kwargs):
    return monitor.watch(owner="trader", watch_id=name, operation_id="position-a",
                         metric="price", asset={"symbol": "XYZ"}, comparison="gte",
                         value=threshold, reason="Review changed exposure", **kwargs)


def poll(monitor, value):
    return monitor.poll(observations={"price:XYZ:stock::::": value})


def test_quiet_minutes_and_sustained_warning_do_not_request_inference():
    strategy = Strategy()
    monitor = AttentionMonitor(strategy)
    watch(monitor)
    for _ in range(390):
        assert poll(monitor, 99) == []
    first = poll(monitor, 101)
    assert len(first) == 1
    monitor.acknowledge([first[0]["id"]])
    for _ in range(390):
        assert poll(monitor, 103) == []
    assert strategy.model_calls == 0
    assert poll(monitor, 99) == []
    assert poll(monitor, 101)[0]["id"] != first[0]["id"]


def test_simultaneous_conditions_form_one_packet_and_new_severity_is_not_muted():
    monitor = AttentionMonitor(Strategy())
    watch(monitor)
    watch(monitor, "severe", 110)
    assert len(poll(monitor, 111)) == 2
    packet = monitor.pending()
    monitor.acknowledge([event["id"] for event in packet])
    watch(monitor, "critical", 120, order_ids=["exit-a"])
    packet = poll(monitor, 121)
    assert len(packet) == 1
    assert packet[0]["order_ids"] == ["exit-a"]


def test_restart_retains_undelivered_warning_and_acknowledged_episode():
    strategy = Strategy()
    monitor = AttentionMonitor(strategy)
    watch(monitor)
    packet = poll(monitor, 101)
    restored = AttentionMonitor(Strategy(strategy.vars))
    assert restored.pending() == packet
    restored.acknowledge([packet[0]["id"]])
    assert poll(AttentionMonitor(Strategy(strategy.vars)), 102) == []


def test_actual_scheduled_state_file_restores_attention_identity_and_episode(tmp_path, monkeypatch):
    import logging
    from lumibot.strategies._strategy import Vars, _Strategy

    state_file = tmp_path / "scheduled-state.json"
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(state_file))
    strategy = Strategy()
    strategy.vars = Vars()
    monitor = AttentionMonitor(strategy)
    watch(monitor, order_ids=["working-close-parent", "working-close-child"])
    packet = poll(monitor, 101)

    def persistence_owner(variables):
        owner = object.__new__(_Strategy)
        owner.vars = variables
        owner.logger = logging.getLogger("native-attention-state-roundtrip")
        owner._last_backup_state = ""
        return owner

    persistence_owner(strategy.vars)._backup_variables_to_scheduled_state_file()
    restored_owner = persistence_owner(Vars())
    restored_owner._load_variables_from_scheduled_state_file()
    restored_strategy = Strategy(restored_owner.vars)
    restored = AttentionMonitor(restored_strategy)
    assert restored.pending() == packet
    assert restored.pending()[0]["operation_id"] == "position-a"
    assert restored.pending()[0]["order_ids"] == ["working-close-parent", "working-close-child"]
    restored.acknowledge([packet[0]["id"]])
    restored_owner._backup_variables_to_scheduled_state_file()
    again_owner = persistence_owner(Vars())
    again_owner._load_variables_from_scheduled_state_file()
    again = AttentionMonitor(Strategy(again_owner.vars))
    assert poll(again, 102) == []
    assert poll(again, 99) == []
    assert len(poll(again, 101)) == 1


def test_order_status_watches_match_the_real_order_alias_contract():
    from lumibot.entities import Asset, Order

    strategy = Strategy()
    order = Order('synthetic', Asset('XYZ'), 2, 'sell', identifier='working-close')
    order.status = 'partially_filled'
    assert order.status == 'partial_fill'
    strategy.get_orders = lambda: [order]
    monitor = AttentionMonitor(strategy)
    monitor.watch(owner='trader', watch_id='order', operation_id='position-a',
        metric='order_status', comparison='in', value=['partially_filled', 'cancelled', 'filled'],
        order_ids=['working-close'], reason='Review partial or terminal execution')
    first = monitor.poll()
    assert len(first) == 1
    assert first[0]['observed'] == ['partial_fill']
    monitor.acknowledge([first[0]['id']])
    order.status = 'cancelled'
    assert len(monitor.poll()) == 1


@pytest.mark.parametrize("operation", ["watch", "remove", "acknowledge", "poll"])
def test_normal_vars_backup_snapshot_is_stable_while_attention_changes(operation):
    from lumibot.strategies._strategy import Vars, _Strategy

    strategy = Strategy()
    strategy.vars = Vars()
    monitor = AttentionMonitor(strategy)
    watch(monitor)
    packet = poll(monitor, 101)
    snapshot = strategy.vars.all()
    original = _Strategy._serialize_variables_for_backup(snapshot)
    if operation == "watch":
        watch(monitor, "new-risk", 110)
    elif operation == "remove":
        monitor.remove(owner="trader", watch_id="boundary")
    elif operation == "acknowledge":
        monitor.acknowledge([packet[0]["id"]])
    else:
        poll(monitor, 99)
    assert _Strategy._serialize_variables_for_backup(snapshot) == original
    assert _Strategy._serialize_variables_for_backup(strategy.vars.all()) != original


def test_actual_backup_serializer_can_overlap_new_watch_registration(monkeypatch):
    from lumibot.strategies._strategy import Vars, _Strategy

    strategy = Strategy()
    strategy.vars = Vars()
    monitor = AttentionMonitor(strategy)
    watch(monitor)
    snapshot = strategy.vars.all()
    definition = next(iter(snapshot["_agent_attention"]["watches"].values()))["definition"]
    entered, release = Event(), Event()
    original = _Strategy._encode_variable_for_backup

    def pause_inside_watch_iteration(value):
        if value is definition:
            entered.set()
            release.wait()
        return original(value)

    monkeypatch.setattr(_Strategy, "_encode_variable_for_backup", staticmethod(pause_inside_watch_iteration))
    with ThreadPoolExecutor(max_workers=1) as executor:
        serialized = executor.submit(_Strategy._serialize_variables_for_backup, snapshot)
        entered.wait()
        try:
            watch(monitor, "new-risk", 110)
        finally:
            release.set()
        restored = _Strategy._deserialize_variables_from_backup(serialized.result())
    assert len(restored["_agent_attention"]["watches"]) == 1
    assert len(monitor.snapshot(owner="trader")["watches"]) == 2


def test_an_alias_spelling_change_cannot_rearm_an_order_warning():
    from lumibot.entities import Asset, Order

    strategy = Strategy()
    order = Order('synthetic', Asset('XYZ'), 2, 'sell', identifier='working-close')
    order.status = 'fill'
    strategy.get_orders = lambda: [order]
    monitor = AttentionMonitor(strategy)
    settings = dict(owner='trader', watch_id='order', operation_id='position-a',
        metric='order_status', comparison='in', order_ids=['working-close'], reason='Review completion')
    monitor.watch(value=['fill'], **settings)
    packet = monitor.poll()
    monitor.acknowledge([packet[0]['id']])
    monitor.watch(value=['filled'], **settings)
    assert monitor.poll() == []
    assert monitor.snapshot(owner='trader')['watches'][0]['value'] == ['fill']


def test_identical_registration_cannot_rearm_a_sustained_breach():
    monitor = AttentionMonitor(Strategy())
    watch(monitor)
    packet = poll(monitor, 101)
    monitor.acknowledge([packet[0]["id"]])
    watch(monitor)
    assert poll(monitor, 101) == []
    watch(monitor, threshold=105)
    assert poll(monitor, 106)


def test_updating_reason_or_related_order_does_not_rearm_the_same_price_condition():
    monitor = AttentionMonitor(Strategy())
    watch(monitor)
    monitor.acknowledge([e["id"] for e in poll(monitor,101)])
    monitor.watch(owner="trader",watch_id="boundary",operation_id="position-a",metric="price",
                  asset={"symbol":"XYZ"},comparison="gte",value=100,
                  reason="Reconciled the exit; keep watching this same boundary",order_ids=["close-a"])
    assert poll(monitor,101) == []


def test_poll_during_decision_preserves_new_warnings_until_next_cycle():
    monitor = AttentionMonitor(Strategy())
    watch(monitor)
    first = poll(monitor, 101)
    with monitor.decision() as packet:
        assert packet == first
        watch(monitor, "worse", 110)
        assert len(poll(monitor, 111)) == 2  # original is still unacknowledged
        with monitor.decision() as concurrent:
            assert concurrent is None
    assert [e["watch_id"] for e in monitor.pending()] == ["worse"]


def test_failed_decision_remains_pending_and_only_one_thread_can_decide():
    monitor = AttentionMonitor(Strategy())
    watch(monitor)
    poll(monitor, 101)
    with pytest.raises(RuntimeError):
        with monitor.decision():
            raise RuntimeError("provider interrupted")
    barrier = Barrier(2)

    def run():
        with monitor.decision() as packet:
            barrier.wait()
            return packet

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: run(), range(2)))
    assert sum(packet is not None for packet in results) == 1
    assert monitor.pending() == []


def test_owner_cannot_replace_or_remove_another_agents_watch():
    monitor = AttentionMonitor(Strategy())
    watch(monitor)
    monitor.remove(owner="researcher", watch_id="boundary")
    assert len(poll(monitor, 101)) == 1
    monitor.remove(owner="trader", watch_id="boundary")
    assert monitor.pending() == []


def test_time_watch_uses_simulation_clock_once_until_agent_reschedules():
    strategy = Strategy()
    monitor = AttentionMonitor(strategy)
    monitor.watch(owner="researcher", watch_id="review", operation_id="entry-review",
                  metric="time", comparison="gte", value=(strategy.now + timedelta(minutes=5)).isoformat(),
                  reason="Scheduled setup review")
    assert monitor.poll(observations={}) == []
    strategy.now += timedelta(minutes=5)
    with monitor.decision(monitor.poll(observations={})) as packet:
        assert len(packet) == 1
    assert monitor.poll(observations={}) == []


def test_nonfinite_observation_is_a_latched_data_warning_not_a_false_clear():
    monitor = AttentionMonitor(Strategy())
    watch(monitor)
    packet = poll(monitor, float("nan"))
    assert packet[0]["kind"] == "observation_unavailable"
    monitor.acknowledge([packet[0]["id"]])
    assert poll(monitor, None) == []
    assert poll(monitor, 101)[0]["kind"] == "condition_met"


def test_builtin_registration_uses_actual_agent_identity_and_survives_manager_restart():
    from lumibot.components.agents import AgentManager, BuiltinTools
    from lumibot.components.agents.tool_context import agent_tool_context

    strategy = Strategy()
    manager = AgentManager(strategy)
    bound = BuiltinTools.attention.watch().binder(strategy, manager)
    with agent_tool_context({"agent_name": "trader"}):
        bound.function(watch_id="boundary", operation_id="position-a", metric="price",
                       comparison="gte", value_json="100", reason="Review exposure",
                       asset_json='{"symbol":"XYZ"}')
    assert len(poll(manager.attention, 101)) == 1
    restored = AgentManager(strategy)
    assert restored.attention.pending() == manager.attention.pending()


def test_live_readers_keep_partial_position_and_exact_order_identity_visible():
    from types import SimpleNamespace
    from lumibot.entities import Asset

    strategy = Strategy()
    asset = Asset("XYZ")
    strategy.get_positions = lambda: [SimpleNamespace(asset=asset, quantity=2)]
    strategy.get_orders = lambda: [SimpleNamespace(identifier="close-a", status="partially_filled"),
                                   SimpleNamespace(identifier="unrelated", status="new")]
    strategy.get_last_price = lambda asset: 101
    monitor = AttentionMonitor(strategy)
    watch(monitor, order_ids=["close-a"])
    monitor.watch(owner="trader", watch_id="remaining", operation_id="position-a",
                  metric="position_quantity", asset={"symbol": "XYZ"}, comparison="gte", value=1,
                  reason="Review residual quantity", order_ids=["close-a"])
    monitor.watch(owner="trader", watch_id="order", operation_id="position-a", metric="order_status",
                  comparison="in", value=["partially_filled", "error", "canceled"],
                  order_ids=["close-a"], reason="Reconcile this operation")
    packet = monitor.poll()
    assert len(packet) == 3
    assert next(e for e in packet if e["metric"] == "position_quantity")["observed"] == 2
    assert all(e["order_ids"] == ["close-a"] for e in packet)
    monitor.acknowledge([e["id"] for e in packet])
    strategy.get_positions = lambda: []
    assert monitor.poll() == []


def test_data_call_failure_for_one_watch_does_not_silence_another():
    strategy = Strategy()
    strategy.get_last_price = lambda asset: (_ for _ in ()).throw(ConnectionError())
    monitor = AttentionMonitor(strategy)
    watch(monitor)
    monitor.watch(owner="trader", watch_id="due", operation_id="position-b", metric="time",
                  comparison="gte", value=strategy.now.isoformat(), reason="Review now")
    packet = monitor.poll()
    assert {e["kind"] for e in packet} == {"observation_unavailable", "condition_met"}


def test_tracked_order_status_transition_is_new_evidence_without_a_new_price_crossing():
    from types import SimpleNamespace
    strategy = Strategy()
    status = ["partially_filled"]
    strategy.get_orders = lambda: [SimpleNamespace(identifier="close-a", status=status[0])]
    monitor = AttentionMonitor(strategy)
    monitor.watch(owner="trader", watch_id="order", operation_id="position-a", metric="order_status",
                  comparison="in", value=["partially_filled", "error", "canceled"],
                  order_ids=["close-a"], reason="Review changed order state")
    first = monitor.poll()
    monitor.acknowledge([e["id"] for e in first])
    assert monitor.poll() == []
    status[0] = "canceled"
    changed = monitor.poll()
    assert len(changed) == 1 and changed[0]["observed"] == ["canceled"]


def test_watch_identity_keeps_owners_distinct_even_with_punctuation():
    monitor = AttentionMonitor(Strategy())
    for owner, name in [("one:two", "three"), ("one", "two:three")]:
        monitor.watch(owner=owner, watch_id=name, operation_id="a", metric="price",
                      asset={"symbol": "XYZ"}, comparison="gte", value=100, reason="Review")
    assert len(poll(monitor, 101)) == 2


def test_agent_selected_basket_observes_signed_ratios_and_multiplier_without_trading():
    from lumibot.entities import Asset
    strategy = Strategy()
    prices = {Asset('XYZ'): 12, Asset('ABC'): 7}
    calls = []
    def price(asset):
        calls.append(asset)
        return prices[asset]
    strategy.get_last_price = price
    monitor = AttentionMonitor(strategy)
    monitor.watch(owner='trader', watch_id='package', operation_id='package-a',
                  metric='basket_value', comparison='gte', value=1700,
                  reason='Review marked package gain', legs=[
                      {'asset': {'symbol': 'XYZ'}, 'quantity': 2, 'multiplier': 100},
                      {'asset': {'symbol': 'ABC'}, 'quantity': -1, 'multiplier': 100}])
    packet = monitor.poll()
    assert packet[0]['observed'] == 1700
    assert len(calls) == 2
    monitor.acknowledge([e['id'] for e in packet])
    assert monitor.poll() == []
    assert strategy.model_calls == 0


def test_position_change_watch_preserves_remaining_exposure_on_partial_fill():
    from types import SimpleNamespace
    from lumibot.entities import Asset
    strategy = Strategy()
    remaining = [3]
    strategy.get_positions = lambda: [SimpleNamespace(asset=Asset('XYZ'), quantity=remaining[0])]
    monitor = AttentionMonitor(strategy)
    monitor.watch(owner='trader', watch_id='quantity', operation_id='a', metric='position_quantity',
                  comparison='ne', value=3, asset={'symbol': 'XYZ'}, reason='Reconcile changed quantity')
    assert monitor.poll() == []
    remaining[0] = 2
    assert monitor.poll()[0]['observed'] == 2


def test_live_dispatch_keeps_polling_responsive_and_serializes_team_work():
    strategy = Strategy()
    strategy.is_backtesting = False
    monitor = AttentionMonitor(strategy)
    watch(monitor)
    first = poll(monitor, 101)
    entered, release = Event(), Event()
    calls = []
    def decide(packet):
        calls.append(packet)
        entered.set()
        release.wait()
    try:
        assert monitor.dispatch(decide) is True
        entered.wait()
        watch(monitor, 'worse', 110)
        assert len(poll(monitor,111)) == 2
        assert monitor.dispatch(decide) is False
    finally:
        release.set()
        monitor.join()
    assert calls == [first]
    assert [e['watch_id'] for e in monitor.pending()] == ['worse']


def test_historical_dispatch_blocks_clock_and_failed_live_work_surfaces_without_retry():
    strategy = Strategy()
    strategy.is_backtesting = True
    monitor = AttentionMonitor(strategy)
    watch(monitor)
    packet = poll(monitor,101)
    seen=[]
    assert monitor.dispatch(lambda current: seen.append((current, strategy.now))) is True
    assert seen == [(packet, strategy.now)]
    assert monitor.pending() == []
    strategy.is_backtesting = False
    watch(monitor,'worse',110)
    poll(monitor,111)
    def broken(packet):
        raise RuntimeError('inference interrupted')
    monitor.dispatch(broken)
    with pytest.raises(RuntimeError,match='inference interrupted'):
        monitor.join()
    assert len(monitor.pending()) == 1


def test_cached_agent_answer_replays_watch_registration_and_removal():
    from lumibot.components.agents import AgentManager, BuiltinTools, AgentRunResult, AgentTraceEvent
    strategy = Strategy()
    manager = AgentManager(strategy)
    handle = manager.create(name="trader", _runtime=object(), allow_trading=False,
                            include_builtin_skills=False, include_builtin_tools=False,
                            tools=[BuiltinTools.attention.watch(), BuiltinTools.attention.remove()])
    result = AgentRunResult(summary="Saved", model="test", events=[AgentTraceEvent(
        kind="tool_call", tool_name="attention_watch", payload={"watch_id":"risk", "operation_id":"a",
        "metric":"price", "asset_json":'{"symbol":"XYZ"}', "comparison":"gte", "value_json":"100", "reason":"Review"})])
    handle._replay_cached_side_effects(result)
    assert len(poll(manager.attention,101)) == 1
    result.events = [AgentTraceEvent(kind="tool_call",tool_name="attention_remove",payload={"watch_id":"risk"})]
    handle._replay_cached_side_effects(result)
    assert manager.attention.snapshot(owner="trader") == {"watches":[],"pending":[]}
