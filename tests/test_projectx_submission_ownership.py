"""Local handoff ownership is retained even when provider acknowledgement is lost."""
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.entities import Order
from tests.test_projectx_bracket_lifecycle_unit import broker, mes, _make_bracket_entry  # noqa: F401


def ordinary(asset, **kwargs):
    return Order(strategy='Strat', asset=asset, quantity=1, side='buy', **kwargs)


def receipt(broker, order, **overrides):
    return {'id': 9000, 'accountId': 1, 'customTag': order.tag,
            'contractId': broker.data_source._get_contract_id_from_asset(order.asset),
            'side': 0, 'size': 1, 'type': 2, 'status': 1, 'fillVolume': 0, **overrides}


@pytest.mark.parametrize('failure', ['pending', 'timeout'])
def test_uncertain_public_submit_remains_owned_and_blocks_replay(broker, mes, failure):
    broker.client.order_place = Mock(side_effect=TimeoutError('lost response')) if failure == 'timeout' else Mock(
        return_value={'success': False, 'errorCode': 6})
    order = ordinary(mes)
    assert broker.submit_order(order) is order
    assert order.status == 'submitted'
    assert order.submission_outcome['broker_state_certainty'] == 'unknown'
    assert broker.get_active_tracked_orders('Strat', mes) == [order]
    assert not order.transactions and not order._new_event.is_set()
    assert broker.submit_order(order) is order
    assert broker.submit_order(ordinary(mes, tag=order.tag)) is order
    assert broker.client.order_place.call_count == 1
    broker._pull_all_orders = Mock(return_value=[])
    broker.sync_orders('Strat')
    assert order.status == 'submitted' and broker.get_active_tracked_orders('Strat', mes) == [order]


def test_reused_identity_changed_terms_refused_distinct_intent_allowed(broker, mes):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    order = ordinary(mes)
    broker.submit_order(order)
    other = ordinary(mes, tag=order.tag)
    other.quantity = 2
    with pytest.raises(ValueError, match='terms'):
        broker.submit_order(other)
    order.quantity = 2
    with pytest.raises(ValueError, match='terms'):
        broker.submit_order(order)
    assert broker.client.order_place.call_count == 1
    broker.submit_order(ordinary(mes))
    assert broker.client.order_place.call_count == 2


@pytest.mark.parametrize('channel', ['stream', 'poll'])
def test_uncertain_entry_receipt_reuses_original_and_spawns_exits_once(broker, mes, channel):
    original_place = broker.client.order_place
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    broker.submit_order(parent)
    metadata = parent._synthetic_bracket
    old_id = parent.identifier
    broker.client.order_place = original_place
    row = receipt(broker, parent, status=2, fillVolume=1, filledPrice=5000)
    if channel == 'stream':
        broker._handle_order_update(row)
        broker._handle_order_update(row)
    else:
        broker.client.order_search = Mock(return_value=[row])
        broker._search_recent_trades = Mock(return_value=[])
        broker.sync_orders('Strat')
        broker.sync_orders('Strat')
    assert broker.get_tracked_order('9000') is parent
    assert broker.get_tracked_order(old_id) is None
    assert parent._synthetic_bracket is metadata
    assert sum(t.quantity for t in parent.transactions) == 1
    assert parent.submission_outcome['broker_state_certainty'] == 'confirmed_submitted'
    assert len(broker.client.placed) == 2


@pytest.mark.parametrize('reply', [{'success': False, 'errorCode': 6}, {'success': False, 'errorCode': 2}, {'success': True, 'orderId': 9999}])
def test_stream_before_rest_ack_keeps_original_fill_and_no_duplicate_exits(broker, mes, reply):
    original_place = broker.client.order_place
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    def place(**kwargs):
        if kwargs['type'] == 2:
            broker._handle_order_update(receipt(broker, parent, status=2, fillVolume=1, filledPrice=5000))
            return reply
        return original_place(**kwargs)
    broker.client.order_place = place
    broker.submit_order(parent)
    assert broker.get_tracked_order('9000') is parent
    assert parent.is_filled() and sum(t.quantity for t in parent.transactions) == 1
    assert len(broker.client.placed) == 2


@pytest.mark.parametrize('mismatch', [{'accountId': 2}, {'accountId': None}, {'size': 2}, {'side': 1}, {'contractId': 'OTHER'}, {'side': False}, {'id': True}, {'type': '2'}])
def test_conflicting_exact_tag_does_not_adopt_receipt(broker, mes, mismatch):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    order = ordinary(mes)
    broker.submit_order(order)
    broker._handle_order_update(receipt(broker, order, **mismatch))
    assert order.status == 'submitted'
    assert broker.get_tracked_order('9000') is None
    assert broker.get_active_tracked_orders('Strat', mes) == [order]


def test_unrecognized_external_unknown_stays_nonactive(mes):
    order = ordinary(mes)
    order.status = 'unknown'
    assert not order.is_active()


@pytest.mark.parametrize('batch', [False, True])
def test_generic_submission_tools_do_not_confirm_uncertain_return(monkeypatch, mes, batch):
    from lumibot.components.agents import builtins
    order = ordinary(mes)
    order.status = 'submitted'
    order.submission_outcome = {'broker_state_certainty': 'unknown', 'reconciliation_required': True}
    monkeypatch.setattr(builtins, '_prepare_agent_order', lambda *a, **kw: (order, kw['reason']))
    strategy = SimpleNamespace(submit_order=lambda *a, **kw: order,
                               submit_orders=lambda *a, **kw: [order])
    reason = 'The requested order uses observed market evidence and bounded portfolio exposure. ' * 4
    tool = (builtins._bind_submit_orders if batch else builtins._bind_submit_order)(strategy, None)
    result = tool.function(**({'orders_json': json.dumps([{'symbol': 'MES', 'side': 'buy', 'quantity': 1}]), 'reason': reason}
                             if batch else {'symbol': 'MES', 'side': 'buy', 'quantity': 1, 'reason': reason}))
    assert result['execution_outcome']['broker_state_certainty'] == 'unknown'
    assert result['execution_outcome']['retryability'] == 'non_retryable'
    assert not result['ok']
    projected = result['orders'][0] if batch else result['order']
    assert projected['submission_outcome'] == order.submission_outcome


def test_same_local_id_and_changed_tag_cannot_repost(broker, mes):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    item = ordinary(mes)
    broker.submit_order(item)
    duplicate = ordinary(mes, identifier=item.identifier)
    with pytest.raises(ValueError, match='terms'):
        broker.submit_order(duplicate)
    item.tag = 'changed-tag'
    with pytest.raises(ValueError, match='terms'):
        broker.submit_order(item)
    assert broker.client.order_place.call_count == 1


def test_concurrent_same_tag_has_one_handoff(broker, mes):
    import threading
    entered, release = threading.Event(), threading.Event()
    results = []
    def post(**kwargs):
        entered.set()
        assert release.wait(5)
        return {'success': False, 'errorCode': 6}
    broker.client.order_place = Mock(side_effect=post)
    item = ordinary(mes, tag='concurrent-intent')
    worker = threading.Thread(target=lambda: results.append(broker.submit_order(item)))
    worker.start()
    try:
        assert entered.wait(5)
        assert broker.submit_order(ordinary(mes, tag=item.tag)) is item
        assert broker.get_active_tracked_orders('Strat', mes) == [item]
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive() and results == [item]
    assert broker.client.order_place.call_count == 1


@pytest.mark.parametrize('channel', ['stream', 'poll'])
def test_exact_rejection_receipt_resolves_without_reposting(broker, mes, channel):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    item = ordinary(mes)
    broker.submit_order(item)
    row = receipt(broker, item, status=5)
    if channel == 'stream':
        broker._handle_order_update(row)
    else:
        broker.client.order_search = Mock(return_value=[row])
        broker._search_recent_trades = Mock(return_value=[])
        broker.sync_orders('Strat')
    assert broker.get_tracked_order('9000') is item
    assert item.is_canceled()
    assert item.submission_outcome['broker_state_certainty'] == 'confirmed_rejected'
    assert broker.submit_order(item) is item
    assert broker.client.order_place.call_count == 1


def test_bound_provider_id_conflict_holds_without_rekey(broker, mes):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    item = ordinary(mes)
    broker.submit_order(item)
    broker._handle_order_update(receipt(broker, item))
    broker._handle_order_update(receipt(broker, item, id=9001))
    assert broker.get_tracked_order('9000') is item
    assert broker.get_tracked_order('9001') is None
    assert item.submission_outcome['reconciliation_required']
    assert broker.submit_order(item) is item
    assert broker.client.order_place.call_count == 1


def test_pending_local_numeric_id_cannot_be_used_for_cancel_or_modify(broker, mes):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    broker.client.order_cancel = Mock()
    broker.client.order_modify = Mock()
    item = ordinary(mes, identifier='314')
    broker.submit_order(item)
    assert broker.cancel_order(item) is False
    assert broker._modify_order(item, limit_price=4900) is False
    broker.client.order_cancel.assert_not_called()
    broker.client.order_modify.assert_not_called()


def test_original_asset_mutation_does_not_change_reserved_identity(broker, mes):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    item = ordinary(mes)
    broker.submit_order(item)
    item.asset.symbol = 'DIFFERENT'
    with pytest.raises(ValueError, match='terms'):
        broker.submit_order(item)
    assert broker.client.order_place.call_count == 1


def test_normal_rest_rejection_is_not_active_or_reported_accepted(broker, mes):
    from lumibot.components.agents.builtins import _submission_certainty
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 2})
    item = ordinary(mes)
    assert broker.submit_order(item) is item
    assert _submission_certainty(item) == 'confirmed_rejected'
    assert broker.get_active_tracked_orders('Strat', mes) == []
    assert broker.get_tracked_order(item.identifier) is item
    assert broker.submit_order(item) is item
    assert broker.client.order_place.call_count == 1


@pytest.mark.parametrize('source', ['rest', 'stream'])
def test_mutated_local_terms_cannot_become_false_accepted_order(broker, mes, source):
    item = ordinary(mes)
    local_id = item.identifier
    def post(**kwargs):
        item.quantity = 2
        if source == 'stream':
            broker._handle_order_update(receipt(broker, item))
            return {'success': False, 'errorCode': 6}
        return {'success': True, 'orderId': 9000}
    broker.client.order_place = Mock(side_effect=post)
    broker.submit_order(item)
    assert broker.get_tracked_order(local_id) is item
    assert broker.get_tracked_order('9000') is None
    assert item.status == 'submitted'
    assert item.submission_outcome['broker_state_certainty'] == 'unknown'
    assert item.submission_outcome['reconciliation_required'] is True
    assert item._raw
    assert not item._new_event.is_set()
    assert broker.client.order_place.call_count == 1


def test_accepted_id_collision_retains_unknown_active_handoff(broker, mes):
    existing = ordinary(mes, identifier='9000')
    broker._process_trade_event(existing, broker.NEW_ORDER)
    item = ordinary(mes)
    broker.client.order_place = Mock(return_value={'success': True, 'orderId': 9000})
    broker.submit_order(item)
    assert broker.get_tracked_order('9000') is existing
    assert broker.get_tracked_order(item.identifier) is item
    assert item.status == 'submitted'
    assert item.submission_outcome['broker_state_certainty'] == 'unknown'
    assert item._raw['orderId'] == 9000
    assert broker.submit_order(item) is item
    assert broker.client.order_place.call_count == 1


@pytest.mark.parametrize('metadata', [None, 'invalid', [], {'broker_state_certainty': []}, {'broker_state_certainty': 'surprise'},
    {'broker_state_certainty': 'confirmed_submitted', 'reconciliation_required': 'false'}])
def test_explicit_malformed_certainty_never_defaults_to_legacy_acceptance(mes, metadata):
    from lumibot.components.agents.builtins import _submission_certainty, _order_to_dict
    item = ordinary(mes)
    assert _submission_certainty(item) == 'confirmed_submitted'
    item.submission_outcome = metadata
    assert _submission_certainty(item) == 'unknown'
    assert _order_to_dict(item)['submission_outcome']['reconciliation_required'] is True


def test_late_matching_rest_ack_does_not_clear_conflicting_stream_identity(broker, mes):
    item = ordinary(mes)
    def post(**kwargs):
        broker._handle_order_update(receipt(broker, item))
        broker._handle_order_update(receipt(broker, item, id=9001))
        return {'success': True, 'orderId': 9000}
    broker.client.order_place = Mock(side_effect=post)
    broker.submit_order(item)
    assert broker.get_tracked_order('9000') is item
    assert item.submission_outcome['reconciliation_required'] is True
    assert item._projectx_reconciliation_receipt['id'] == 9001
    assert broker.client.order_place.call_count == 1


@pytest.mark.parametrize('metadata', ['roundtrip', None, 'invalid', {'broker_state_certainty': []},
    {'broker_state_certainty': 'confirmed_submitted', 'reconciliation_required': True}])
def test_restored_unresolved_or_malformed_marker_cannot_repost(broker, mes, metadata):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    item = ordinary(mes)
    broker.submit_order(item)
    restored = Order.from_dict(item.to_dict())
    if metadata != 'roundtrip':
        restored.submission_outcome = metadata
    original_marker = restored.submission_outcome
    original_identifier, original_tag = restored.identifier, restored.tag
    # A fresh broker has no process-local receipt/identity registry.
    from tests.test_projectx_bracket_lifecycle_unit import broker as broker_fixture
    fresh = broker_fixture.__wrapped__()
    fresh.client = Mock()
    with pytest.raises(ValueError, match='reconcil'):
        fresh.submit_order(restored)
    assert restored.submission_outcome == original_marker
    assert (restored.identifier, restored.tag) == (original_identifier, original_tag)
    fresh.client.order_place.assert_not_called()
    assert broker.client.order_place.call_count == 1


@pytest.mark.parametrize('mismatch', [{'accountId': None}, {'accountId': 2}, {'side': 1},
    {'contractId': 'OTHER'}, {'type': 1}])
def test_bound_receipt_still_requires_immutable_venue_identity(broker, mes, mismatch):
    item = ordinary(mes)
    broker.submit_order(item)
    identifier = item.identifier
    broker._handle_order_update(receipt(broker, item, id=int(identifier), status=2,
                                         fillVolume=1, filledPrice=5000, **mismatch))
    assert broker.get_tracked_order(identifier) is item
    assert not item.transactions
    assert not item.is_filled()
    # Wrong-account stream messages are discarded before looking up any order.
    if mismatch != {'accountId': 2}:
        assert item.submission_outcome['reconciliation_required'] is True


def test_bound_receipt_permits_confirmed_quantity_and_price_modification(broker, mes):
    item = ordinary(mes, order_type='limit', limit_price=5000)
    broker.submit_order(item)
    broker.client.order_modify = Mock(return_value={'success': True})
    item.quantity = 2
    assert broker._modify_order(item, limit_price=4999)
    row = receipt(broker, item, id=int(item.identifier), size=2, type=1,
                  limitPrice=4999, status=2, fillVolume=2, filledPrice=4999)
    broker._handle_order_update(row)
    assert item.is_filled()
    assert sum(t.quantity for t in item.transactions) == 2
    assert item.submission_outcome['reconciliation_required'] is False


@pytest.mark.parametrize('tag', [None, 'unrelated-tag'])
def test_local_numeric_id_collision_cannot_adopt_foreign_order(broker, mes, tag):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    item = ordinary(mes, identifier='9000')
    broker.submit_order(item)
    broker._handle_order_update(receipt(broker, item, customTag=tag, status=2,
                                       fillVolume=1, filledPrice=5000))
    assert broker.get_tracked_order('9000') is item
    assert item.status == 'submitted' and not item.transactions
    assert item.submission_outcome['reconciliation_required'] is True
    # Exact tag and terms establish provider identity even when numeric IDs coincide.
    broker._handle_order_update(receipt(broker, item, status=2, fillVolume=1, filledPrice=5000))
    assert item.is_filled() and sum(t.quantity for t in item.transactions) == 1
    assert broker.client.order_place.call_count == 1


def test_trade_order_id_cannot_resolve_a_local_unbound_identifier(broker, mes):
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    broker._update_positions_cache = Mock()
    item = ordinary(mes, identifier='9000')
    broker.submit_order(item)
    trade = {'accountId': 1, 'orderId': 9000, 'id': 42, 'size': 1, 'price': 5000}
    broker._handle_trade_update(trade)
    assert item.status == 'submitted' and not item.transactions
    broker._handle_order_update(receipt(broker, item))
    broker._handle_trade_update(trade)
    broker._handle_trade_update(trade)
    assert item.is_filled() and sum(t.quantity for t in item.transactions) == 1
    assert broker.client.order_place.call_count == 1


def test_rejection_branch_cannot_overwrite_concurrent_exact_fill(broker, mes):
    item = ordinary(mes)
    class RacingRejection(dict):
        def get(self, key, default=None):
            if key == 'errorMessage':
                broker._handle_order_update(receipt(broker, item, status=2, fillVolume=1, filledPrice=5000))
            return super().get(key, default)
    broker.client.order_place = Mock(return_value=RacingRejection(success=False, errorCode=2))
    broker.submit_order(item)
    assert broker.get_tracked_order('9000') is item
    assert item.is_filled() and sum(t.quantity for t in item.transactions) == 1
    assert item.submission_outcome['broker_state_certainty'] == 'confirmed_submitted'
    assert broker.submit_order(item) is item
    assert broker.client.order_place.call_count == 1


def test_exact_partial_receipt_can_supersede_earlier_unbound_rest_rejection(broker, mes):
    item = ordinary(mes)
    item.quantity = 2
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 2})
    broker.submit_order(item)
    assert item.is_canceled()
    row = receipt(broker, item, size=2, status=1, fillVolume=1, filledPrice=5000)
    broker._handle_order_update(row)
    broker._handle_order_update(row)
    assert broker.get_tracked_order('9000') is item
    assert item.status == 'partial_fill' and sum(t.quantity for t in item.transactions) == 1
    assert broker.get_active_tracked_orders('Strat', mes) == [item]
    assert item.submission_outcome['broker_state_certainty'] == 'confirmed_submitted'


@pytest.mark.parametrize('channel', ['stream', 'poll'])
def test_first_partial_receipt_and_trade_stream_converge_once(broker, mes, channel):
    item = ordinary(mes)
    item.quantity = 2
    broker.client.order_place = Mock(return_value={'success': False, 'errorCode': 6})
    broker._update_positions_cache = Mock()
    broker._process_trade_event = Mock(wraps=broker._process_trade_event)
    broker.submit_order(item)
    row = receipt(broker, item, size=2, fillVolume=1, filledPrice=5000)
    if channel == 'stream':
        broker._handle_order_update(row)
        broker._handle_order_update(row)
    else:
        broker.client.order_search = Mock(return_value=[row])
        broker._search_recent_trades = Mock(return_value=[])
        broker.sync_orders('Strat')
        broker.sync_orders('Strat')
    trade = {'accountId': 1, 'orderId': 9000, 'id': 42, 'size': 1, 'price': 5000}
    broker._handle_trade_update(trade)
    broker._handle_trade_update(trade)
    assert broker.get_active_tracked_orders('Strat', mes) == [item]
    assert sum(t.quantity for t in item.transactions) == 1
    partials = [call for call in broker._process_trade_event.call_args_list
                if call.args[1] == broker.PARTIALLY_FILLED_ORDER]
    assert len(partials) == 1
    assert broker.client.order_place.call_count == 1


@pytest.mark.parametrize('tag', [None, 'another-tag'])
@pytest.mark.parametrize('mismatch', [{'contractId': 'OTHER'}, {'side': 1}, {'type': 1}])
def test_bound_provider_identity_cannot_bypass_validation_through_tag(broker, mes, tag, mismatch):
    item = ordinary(mes)
    broker.submit_order(item)
    broker._handle_order_update(receipt(broker, item, id=int(item.identifier), customTag=tag,
        status=2, fillVolume=1, filledPrice=5000, **mismatch))
    assert not item.transactions and not item.is_filled()
    assert item.submission_outcome['reconciliation_required'] is True


def test_bound_valid_no_tag_snapshot_still_reconciles(broker, mes):
    item = ordinary(mes)
    broker.submit_order(item)
    broker._handle_order_update(receipt(broker, item, id=int(item.identifier), customTag=None,
        status=2, fillVolume=1, filledPrice=5000))
    assert item.is_filled() and sum(t.quantity for t in item.transactions) == 1
    assert item.submission_outcome['reconciliation_required'] is False


def test_tag_and_bound_id_resolving_different_intents_hold_both(broker, mes):
    first, second = ordinary(mes), ordinary(mes)
    broker.submit_order(first)
    broker.submit_order(second)
    broker._handle_order_update(receipt(broker, first, id=int(first.identifier), customTag=second.tag,
        status=2, fillVolume=1, filledPrice=5000))
    assert not first.transactions and not second.transactions
    assert first.submission_outcome['reconciliation_required'] is True
    assert second.submission_outcome['reconciliation_required'] is True


@pytest.mark.parametrize('mismatch', [{'contractId': 'OTHER'}, {'side': 1}, {'side': False}])
def test_bound_trade_contradictions_do_not_consume_execution_identity(broker, mes, mismatch):
    item = ordinary(mes)
    broker.submit_order(item)
    broker._update_positions_cache = Mock()
    trade = {'accountId': 1, 'orderId': int(item.identifier), 'id': 42, 'size': 1, 'price': 5000}
    broker._handle_trade_update({**trade, **mismatch})
    assert not item.transactions
    assert item.submission_outcome['reconciliation_required'] is True
    # The malformed row must not consume this trade ID; sparse valid rows remain supported.
    broker._handle_trade_update(trade)
    broker._handle_trade_update(trade)
    assert item.is_filled() and sum(t.quantity for t in item.transactions) == 1


@pytest.mark.parametrize('filled', [1, 2])
def test_trade_driven_partial_and_full_protection_release_observation_lock(broker, mes, filled):
    import threading
    parent = _make_bracket_entry(mes, qty=2, tp=5050, sl=4975)
    original = broker.client.order_place
    availability = []
    def post(**kwargs):
        if kwargs['type'] in (1, 4):
            def probe():
                acquired = broker._order_update_lock.acquire(timeout=0.5)
                availability.append(acquired)
                if acquired:
                    broker._order_update_lock.release()
            worker = threading.Thread(target=probe)
            worker.start()
            worker.join(2)
            assert not worker.is_alive()
        return original(**kwargs)
    broker.client.order_place = post
    broker._update_positions_cache = Mock()
    broker.submit_order(parent)
    trade = {'accountId': 1, 'orderId': int(parent.identifier), 'id': 42,
             'size': filled, 'price': 5000}
    broker._handle_trade_update(trade)
    broker._handle_trade_update(trade)
    assert availability == [True, True]
    assert len(broker.client.placed) == 3
    assert sum(t.quantity for t in parent.transactions) == filled
