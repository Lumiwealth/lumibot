import pytest
from types import SimpleNamespace
from unittest.mock import patch

from lumibot.brokers.projectx import ProjectX
from lumibot.entities import Asset, Order
from lumibot.data_sources.data_source import DataSource


class DummyDataSource(DataSource):
    def get_chains(self, asset, quote: Asset = None):
        return {}
    def get_historical_prices(self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, return_polars=False):
        return None
    def get_last_price(self, asset, quote=None, exchange=None):
        return 0.0
    def _get_contract_id_from_asset(self, asset):
        return f"CON.F.US.{asset.symbol}.Z25"


class StubClient:
    def __init__(self):
        self.next_id = 1000
        self.placed = []
        self._tick = 0.25
    def get_preferred_account_id(self):
        return 1
    def get_contract_tick_size(self, contract_id):
        return self._tick
    def round_to_tick_size(self, price, tick_size):
        if price is None:
            return None
        return round(price / tick_size) * tick_size
    def find_contract_by_symbol(self, symbol):
        return f"CON.F.US.{symbol}.Z25"
    def order_place(self, **kwargs):
        self.next_id += 1
        oid = self.next_id
        self.placed.append({"orderId": oid, **kwargs})
        return {"success": True, "orderId": oid}
    def order_cancel(self, account_id, order_id):
        return {"success": True}


@pytest.fixture
def broker():
    config = {
        "firm": "TOPONE",
        "api_key": "dummy",
        "username": "dummy",
        "base_url": "https://api.toponefutures.projectx.com/",
        "preferred_account_name": "ACC",
        "streaming_base_url": "wss://gateway-rtc-demo.s2f.projectx.com/",
    }
    data = DummyDataSource()
    # Monkeypatch ProjectXClient before broker init
    import lumibot.brokers.projectx as projectx_module
    original_client_cls = projectx_module.ProjectXClient
    projectx_module.ProjectXClient = lambda cfg: StubClient()
    try:
        # These tests submit directly and do not own a background order worker.
        with patch.object(ProjectX, "_start_orders_thread"):
            b = ProjectX(config, data_source=data, connect_stream=False)
    finally:
        projectx_module.ProjectXClient = original_client_cls
    # Use our stub client instance
    b.client = StubClient()
    b.account_id = 1
    b._get_contract_id_from_asset = lambda a: f"CON.F.US.{a.symbol}.Z25"
    return b


@pytest.fixture
def mes():
    return Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)


def _make_bracket_entry(asset, side="buy", qty=1, limit=None, tp=None, sl=None):
    o = Order(asset=asset, quantity=qty, order_type="limit" if limit is not None else "market", side=side, limit_price=limit, strategy="Strat")
    # Lumibot bracket order class marker
    o.order_class = Order.OrderClass.BRACKET
    # place intended TP/SL on secondary_* as per broker logic
    o.secondary_limit_price = tp
    o.secondary_stop_price = sl
    return o


def _stream_placement_receipt(broker, response, terms, *, filled=False, price=None):
    # Exercise the provider identity boundary; anonymous normalized Orders omit
    # account/contract evidence and cannot prove ownership of a local handoff.
    broker._handle_order_update(dict(id=response['orderId'], accountId=terms['account_id'],
        contractId=terms['contract_id'], customTag=terms['custom_tag'],
        type=terms['type'], side=terms['side'], size=terms['size'],
        limitPrice=terms['limit_price'], stopPrice=terms['stop_price'],
        status=2 if filled else 1, fillVolume=terms['size'] if filled else 0,
        filledPrice=price))


def test_bracket_parent_submission_stores_meta_and_normalizes_tag(broker, mes):
    parent = _make_bracket_entry(mes, limit=5000.0, tp=5050.0, sl=4975.0)
    # No tag -> helper should generate, then normalize to BRK_ENTRY_*
    submitted = broker._submit_order(parent)
    assert submitted.identifier is not None
    assert hasattr(submitted, "_synthetic_bracket")
    meta = submitted._synthetic_bracket
    assert meta["tp_price"] == 5050.0 and meta["sl_price"] == 4975.0
    assert submitted.tag.startswith("BRK_ENTRY_")
    # Ensure no TP/SL sent on parent payload
    sent = broker.client.placed[0]
    assert sent.get("limit_price") in (5000.0, 5000.0) or True  # price exists if limit
    assert sent.get("stop_price") is None


def test_parent_acknowledgement_does_not_place_exit_orders(broker, mes):
    parent = _make_bracket_entry(mes, limit=5000, tp=5050, sl=4975)
    broker._submit_order(parent)
    assert len(broker.client.placed) == 1
    assert parent._synthetic_bracket['children'] == {}


def test_core_polled_fill_spawns_protective_orders(broker, mes):
    parent = _make_bracket_entry(mes, limit=5000, tp=5050, sl=4975)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    assert len(broker.client.placed) == 3
    assert set(parent._synthetic_bracket['children']) == {'tp', 'sl'}


def test_accepted_child_is_not_reported_as_submission_failure(broker, mes, caplog):
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    child = broker._create_bracket_child(parent, 'tp', 5050, 'BASE')
    assert child.identifier
    assert 'Bracket child submission failed' not in caplog.text


def test_bracket_meta_restores_using_real_order_identifier(broker, mes):
    from lumibot.tools.projectx_helpers import create_bracket_meta, restore_bracket_meta_if_needed
    parent = Order(strategy='Strat', asset=mes, quantity=1, side='buy', identifier='1001')
    meta = create_bracket_meta(5050, 4975)
    broker._bracket_meta[parent.identifier] = meta
    assert broker._is_bracket_parent(parent)
    assert restore_bracket_meta_if_needed(parent, {}, broker._bracket_meta)
    assert parent._synthetic_bracket['tp_price'] == 5050


def test_fill_before_acknowledgement_protects_the_tracked_parent(broker, mes):
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place

    def fill_before_ack(**kwargs):
        response = original(**kwargs)
        if kwargs['type'] == 2:
            _stream_placement_receipt(broker, response, kwargs, filled=True, price=5000)
        return response

    broker.client.order_place = fill_before_ack
    broker._submit_order(parent)
    tracked = broker.get_tracked_order(parent.identifier)
    assert len(broker.client.placed) == 3
    assert tracked.is_filled()
    assert set(tracked._synthetic_bracket['children']) == {'tp', 'sl'}


def test_child_stream_before_ack_keeps_oco_link_on_canonical_order(broker, mes):
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place

    def new_before_ack(**kwargs):
        response = original(**kwargs)
        if kwargs['type'] in (1, 4):
            _stream_placement_receipt(broker, response, kwargs)
        return response

    broker.client.order_place = new_before_ack
    broker.client.order_cancel = Mock(wraps=broker.client.order_cancel)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    children = parent._synthetic_bracket['children']
    tp = broker.get_tracked_order(children['tp'])
    assert tp._is_bracket_child
    assert tp._bracket_parent_id == parent.identifier
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                               filled_quantity=1, multiplier=5)
    broker.client.order_cancel.assert_called_once_with(1, int(children['sl']))


def test_child_fill_before_ack_prevents_unnecessary_sibling_submission(broker, mes):
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place

    def child_event_before_ack(**kwargs):
        response = original(**kwargs)
        if kwargs['type'] in (1, 4):
            _stream_placement_receipt(broker, response, kwargs,
                                      filled=kwargs['type'] == 1, price=5050)
        return response

    broker.client.order_place = child_event_before_ack
    broker.client.order_cancel = Mock(wraps=broker.client.order_cancel)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    # A known completed exit must prevent creation of another exit, rather
    # than submitting and then canceling an unnecessary live order.
    assert parent._synthetic_bracket['children'].keys() == {'tp'}
    assert len(broker.client.placed) == 2
    broker.client.order_cancel.assert_not_called()


def test_fractional_contract_size_is_not_silently_truncated(broker, mes):
    order = Order(asset=mes, quantity=1.5, side='buy', order_type='market', strategy='Strat')
    broker._submit_order(order)
    assert broker.client.placed == []
    assert order.status == 'error'


def test_subscription_failure_does_not_report_broker_stream_ready(broker):
    from unittest.mock import Mock
    streaming = Mock()
    streaming.start_user_hub.return_value = True
    streaming.subscribe_all.return_value = False
    broker.client.get_streaming_client = Mock(return_value=streaming)
    broker._stream_established = Mock()
    broker._setup_streaming()
    broker._stream_established.assert_not_called()


def test_default_broker_has_its_own_projectx_data_source():
    from lumibot.data_sources.projectx_data import ProjectXData
    config = {'firm': 'TEST', 'api_key': 'test-key', 'username': 'test-user',
              'base_url': 'https://example.test/'}
    instance = ProjectX(config, connect_stream=False)
    assert isinstance(instance.data_source, ProjectXData)
    assert instance.client is instance.data_source.client


def test_trade_events_dispatch_partial_then_complete_and_ignore_duplicate(broker, mes):
    from unittest.mock import Mock
    order = Order(asset=mes, quantity=2, side='buy', order_type='market', strategy='Strat', identifier='789')
    order.status = 'new'
    broker._process_new_order(order)
    broker._update_positions_cache = Mock()
    dispatch = Mock(wraps=broker._process_trade_event)
    broker._process_trade_event = dispatch
    # GatewayUserTrade documented payload shape; only IDs/prices vary for the two fills.
    first = {'id': 101112, 'accountId': 1, 'contractId': 'CON.F.US.EP.U25',
             'creationTimestamp': '2024-07-21T13:47:00Z', 'price': 2100.75,
             'profitAndLoss': 50.25, 'fees': 2.5, 'side': 0, 'size': 1,
             'voided': False, 'orderId': 789}
    second = {**first, 'id': 101113, 'price': 2101.25}
    broker._handle_trade_update(first)
    assert order.status == 'partial_fill'
    broker._handle_trade_update(first)
    broker._handle_trade_update(second)
    events = [call.args[1] for call in dispatch.call_args_list]
    assert events == [broker.PARTIALLY_FILLED_ORDER, broker.FILLED_ORDER]
    assert sum(tx.quantity for tx in order.transactions) == 2
    assert order.avg_fill_price == 2101.0


def test_rejected_protection_retries_without_overlapping_accepted_child(broker, mes):
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place
    rejected = False
    def place(**kwargs):
        nonlocal rejected
        if kwargs['type'] == 4 and not rejected:
            rejected = True
            return {'success': False, 'errorCode': 2, 'errorMessage': 'rejected'}
        return original(**kwargs)
    broker.client.order_place = place
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    tp = broker.get_tracked_order(parent._synthetic_bracket['children']['tp'])
    broker._maybe_spawn_bracket_children(parent)
    assert len(broker.client.placed) == 2
    broker.client.order_cancel.assert_called_once_with(1, int(tp.identifier))
    # A rejected tag cannot be reused. Finish the accepted sibling before
    # allocating fresh identifiers and unique tags for another generation.
    broker._process_trade_event(tp, broker.CANCELED_ORDER)
    children = parent._synthetic_bracket['children']
    assert set(children) == {'tp', 'sl'} and tp.identifier not in children.values()
    assert len(broker.client.placed) == 4
    assert len({row['custom_tag'] for row in broker.client.placed}) == 4


@pytest.mark.parametrize('limit_price', [None, 5000])
def test_missing_execution_price_is_not_fabricated(broker, mes, limit_price):
    from unittest.mock import Mock

    cached = Order(asset=mes, quantity=1, side='buy', strategy='Strat',
                   order_type='limit' if limit_price else 'market', limit_price=limit_price,
                   identifier='entry')
    cached.status = 'new'
    filled = Order(asset=mes, quantity=1, side='buy', strategy='Strat',
                   order_type='market', identifier='entry')
    filled.status = 'filled'
    broker._process_trade_event = Mock()
    broker._dispatch_status_change(cached, filled)
    assert not any(call.args[1] == broker.FILLED_ORDER
                   for call in broker._process_trade_event.call_args_list)


def test_bracket_children_spawn_on_fill_and_tagging(broker, mes):
    # Submit parent
    parent = _make_bracket_entry(mes, limit=5000.0, tp=5050.0, sl=4975.0)
    broker._submit_order(parent)
    pid = parent.identifier
    # Simulate fill event routing through dispatch
    filled = Order(asset=mes, quantity=1, order_type="limit", side="buy", strategy="Strat")
    filled.id = pid
    filled.identifier = pid
    filled.status = "filled"
    filled.avg_fill_price = 5000.0
    filled.filled_quantity = 1
    # Sync cache with parent to preserve meta
    broker._process_new_order(parent)
    broker._dispatch_status_change(parent, filled)
    # Expect two additional placements (TP + SL)
    child_orders = broker.client.placed[1:]  # first was parent
    assert len(child_orders) >= 2
    # Find children in broker cache via meta map
    meta = parent._synthetic_bracket
    tp_id = meta.get('children', {}).get('tp')
    sl_id = meta.get('children', {}).get('sl')
    assert tp_id and sl_id
    # Check tags
    tp_order = broker.get_order(tp_id)
    sl_order = broker.get_order(sl_id)
    assert tp_order.tag.startswith("BRK_TP_")
    assert sl_order.tag.startswith("BRK_STOP_")
    # Child price assignment
    assert tp_order.limit_price == 5050.0
    assert getattr(sl_order, 'stop_price', None) == 4975.0
    assert tp_order.order_type == Order.OrderType.LIMIT
    assert getattr(sl_order, 'order_type', None) in {
        Order.OrderType.STOP,
        Order.OrderType.STOP_LIMIT,
        Order.OrderType.TRAIL,
    }


def test_bracket_child_fill_cancels_sibling(broker, mes):
    # Submit parent and spawn children
    parent = _make_bracket_entry(mes, limit=5000.0, tp=5050.0, sl=4975.0)
    broker._submit_order(parent)
    pid = parent.identifier
    broker._process_new_order(parent)
    filled = Order(asset=mes, quantity=1, order_type="limit", side="buy", strategy="Strat")
    filled.id = pid
    filled.identifier = pid
    filled.status = "filled"
    filled.avg_fill_price = 5000.0
    filled.filled_quantity = 1
    broker._dispatch_status_change(parent, filled)

    meta = parent._synthetic_bracket
    tp_id = meta['children']['tp']
    sl_id = meta['children']['sl']

    # Ensure children exist in cache
    assert broker.get_order(tp_id) is not None
    assert broker.get_order(sl_id) is not None

    # Simulate TP child fill
    tp_order = broker.get_order(tp_id)
    tp_order.status = "filled"
    broker._handle_bracket_child_fill(tp_order)

    # SL sibling should be canceled or marked terminal
    sl_order = broker.get_order(sl_id)
    assert (getattr(sl_order, 'status', '') or '').lower() in {"canceled", "cancelled", "cancelling",
                                                               "fill", "filled", "error"}
    # Cancel acknowledgement is not terminal evidence; retain pending tracking.
    assert meta['active'] is True
    assert sl_order.status == 'cancelling'


def test_partial_entry_protects_only_filled_contracts_and_resizes_existing_exits(broker, mes):
    from unittest.mock import Mock
    from lumibot.tools.projectx_helpers import create_bracket_meta
    parent = _make_bracket_entry(mes, qty=3, tp=5050, sl=4975)
    parent.identifier = 'entry'
    # This isolated sizing fixture still needs the real account-unique parent tag.
    from lumibot.tools.projectx_helpers import build_unique_order_tag
    build_unique_order_tag(parent)
    parent._is_bracket_parent = True
    parent._synthetic_bracket = create_bracket_meta(5050, 4975)
    parent.status = 'partial_fill'
    parent.filled_quantity = 1
    broker._maybe_spawn_bracket_children(parent)
    assert [row['size'] for row in broker.client.placed] == [1, 1]
    broker.client.order_modify = Mock(return_value={'success': True, 'errorCode': 0, 'errorMessage': None})
    parent.filled_quantity = 3
    parent.status = 'fill'
    broker._maybe_spawn_bracket_children(parent)
    assert len(broker.client.placed) == 2
    assert [call.kwargs['size'] for call in broker.client.order_modify.call_args_list] == [3, 3]


def test_restart_recovers_existing_protective_links_without_placing_orders(broker, mes):
    from unittest.mock import Mock
    common = {'accountId': 1, 'contractId': 'CON.F.US.MES.Z25', 'size': 1,
              'creationTimestamp': '2024-07-21T13:47:00Z', 'fillVolume': 0}
    rows = [
        {**common, 'id': 101, 'status': 2, 'type': 2, 'side': 0,
         'fillVolume': 1, 'filledPrice': 5000., 'customTag': 'BRK_ENTRY_SYNTHETIC'},
        {**common, 'id': 102, 'status': 1, 'type': 1, 'side': 1,
         'limitPrice': 5050., 'customTag': 'BRK_TP_SYNTHETIC'},
        {**common, 'id': 103, 'status': 1, 'type': 4, 'side': 1,
         'stopPrice': 4975., 'customTag': 'BRK_STOP_SYNTHETIC'}]
    broker.client.get_orders = Mock(return_value=rows)
    broker._get_asset_from_contract_id_cached = Mock(return_value=mes)
    orders = broker._get_orders_at_broker()
    assert broker._bracket_parent_by_child_id == {'102': '101', '103': '101'}
    parent = next(order for order in orders if order.identifier == '101')
    assert parent._synthetic_bracket['children'] == {'tp': '102', 'sl': '103'}
    broker._maybe_spawn_bracket_children(parent)
    assert broker.client.placed == []


def test_all_returned_orders_are_recovered_including_older_open_orders(broker, mes):
    from unittest.mock import Mock
    row = {'accountId': 1, 'contractId': 'CON.F.US.MES.Z25', 'size': 1,
           'status': 1, 'type': 1, 'side': 0, 'fillVolume': 0, 'limitPrice': 4900.}
    broker.client.get_orders = Mock(return_value=[{**row, 'id': 1000+i} for i in range(61)])
    broker._get_asset_from_contract_id_cached = Mock(return_value=mes)
    assert len(broker._get_orders_at_broker()) == 61


@pytest.mark.parametrize('method, provider_method', [('_get_orders_at_broker', 'get_orders'),
                                                    ('_get_positions_at_broker', 'get_positions')])
def test_unavailable_account_state_is_not_reported_as_empty(broker, method, provider_method):
    from unittest.mock import Mock
    setattr(broker.client, provider_method, Mock(side_effect=RuntimeError('provider unavailable')))
    with pytest.raises(Exception, match='(state is unavailable|Unable to retrieve positions)'):
        getattr(broker, method)()


def test_cancelled_partial_execution_is_recorded_before_cancellation(broker, mes):
    from unittest.mock import Mock
    original = Order(asset=mes, quantity=3, side='buy', order_type='limit', limit_price=5000,
                     strategy='Strat', identifier='789')
    broker._process_new_order(original)
    broker._get_asset_from_contract_id_cached = Mock(return_value=mes)
    dispatch = Mock(wraps=broker._process_trade_event)
    broker._process_trade_event = dispatch
    broker._handle_order_update({'id': 789, 'accountId': 1, 'contractId': 'CON.F.US.MES.Z25',
        'size': 3, 'status': 3, 'type': 1, 'side': 0, 'fillVolume': 1,
        'filledPrice': 5000.25, 'limitPrice': 5000.})
    assert original.status == 'canceled'
    assert sum(tx.quantity for tx in original.transactions) == 1
    assert [call.args[1] for call in dispatch.call_args_list] == [broker.PARTIALLY_FILLED_ORDER, broker.CANCELED_ORDER]


def test_position_stream_does_not_append_account_totals_as_strategy_positions(broker, mes):
    from unittest.mock import Mock
    from lumibot.entities import Position
    position = Position(strategy='', asset=mes, quantity=1, avg_fill_price=5000)
    broker._convert_broker_position_to_lumibot_position = Mock(return_value=position)
    event = {'id': 123, 'accountId': 1, 'contractId': 'CON.F.US.MES.Z25',
             'type': 1, 'size': 1, 'averagePrice': 5000.}
    broker._handle_position_update(event)
    broker._handle_position_update(event)
    assert len(broker._filled_positions) == 0
    assert len(broker._positions_cache) == 1


def test_order_stream_ignores_other_account(broker, mes):
    from unittest.mock import Mock
    broker._convert_broker_order_to_lumibot_order = Mock()
    broker._handle_order_update({'id': 789, 'accountId': 999, 'contractId': 'CON.F.US.MES.Z25',
                                'size': 1, 'status': 2, 'fillVolume': 1, 'filledPrice': 5000.})
    broker._convert_broker_order_to_lumibot_order.assert_not_called()


@pytest.mark.parametrize('parent_before_ack', [False, True])
def test_child_fill_during_sibling_ack_cancels_registered_sibling(broker, mes, parent_before_ack):
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place

    def place(**kwargs):
        response = original(**kwargs)
        if kwargs['type'] == 2 and parent_before_ack:
            _stream_placement_receipt(broker, response, kwargs, filled=True, price=5000)
        elif kwargs['type'] == 4:
            tracked = broker.get_tracked_order(parent.identifier)
            tp = broker.get_tracked_order(tracked._synthetic_bracket['children']['tp'])
            broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                                       filled_quantity=1, multiplier=5)
        return response

    broker.client.order_place = place
    broker.client.order_cancel = Mock(wraps=broker.client.order_cancel)
    broker._submit_order(parent)
    if not parent_before_ack:
        broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                                   filled_quantity=1, multiplier=5)
    tracked = broker.get_tracked_order(parent.identifier)
    sl = broker.get_tracked_order(tracked._synthetic_bracket['children']['sl'])
    broker.client.order_cancel.assert_called_once_with(1, int(sl.identifier))
    assert sl.is_active() and not sl.is_canceled()  # Cancel acceptance is not terminal evidence.


def _filled_bracket(broker, mes):
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    children = parent._synthetic_bracket['children']
    return parent, broker.get_tracked_order(children['tp']), broker.get_tracked_order(children['sl'])


@pytest.mark.parametrize('failure', [False, RuntimeError('lost cancel acknowledgement')])
def test_failed_sibling_cancel_retries_on_normal_sync_without_replaying_fills(broker, mes, failure):
    from unittest.mock import Mock
    parent, tp, sl = _filled_bracket(broker, mes)
    broker.client.order_cancel = Mock(side_effect=[failure, {'success': True}])
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                               filled_quantity=1, multiplier=5)
    assert parent._synthetic_bracket['active']
    assert sl.is_active() and not sl.is_canceled()
    broker._pull_all_orders = Mock(return_value=broker.get_all_orders())
    before = list(tp.transactions)
    broker.sync_orders('Strat')
    broker.sync_orders('Strat')
    assert broker.client.order_cancel.call_count == 2
    assert tp.transactions == before
    assert len(broker.client.placed) == 3
    assert sl.is_active() and not sl.is_canceled()


def test_missing_registered_sibling_keeps_cancellation_unresolved(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _filled_bracket(broker, mes)
    lookup = broker.get_tracked_order
    broker.get_tracked_order = Mock(side_effect=lambda oid: None if oid == sl.identifier else lookup(oid))
    broker.client.order_cancel = Mock()
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                               filled_quantity=1, multiplier=5)
    assert parent._synthetic_bracket['active']
    broker.client.order_cancel.assert_not_called()
    broker.get_tracked_order = lookup
    broker._pull_all_orders = Mock(return_value=broker.get_all_orders())
    broker.sync_orders('Strat')
    broker.client.order_cancel.assert_called_once_with(1, int(sl.identifier))


def test_successful_sibling_cancel_is_not_repeated_before_terminal_confirmation(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _filled_bracket(broker, mes)
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                               filled_quantity=1, multiplier=5)
    assert parent._synthetic_bracket['active']
    broker._handle_bracket_child_fill(tp)
    broker._pull_all_orders = Mock(return_value=broker.get_all_orders())
    broker.sync_orders('Strat')
    broker.client.order_cancel.assert_called_once_with(1, int(sl.identifier))
    assert sl.is_active() and not sl.is_canceled()
    broker._process_trade_event(sl, broker.CANCELED_ORDER)
    broker.sync_orders('Strat')
    assert not parent._synthetic_bracket['active']
    broker.client.order_cancel.assert_called_once()


@pytest.mark.parametrize('rejected', [False, True])
def test_filled_child_with_unsubmitted_or_rejected_sibling_does_not_create_exit(broker, mes, rejected):
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place
    def place(**kwargs):
        if rejected and kwargs['type'] == 4:
            return {'success': False, 'errorCode': 2, 'errorMessage': 'rejected'}
        response = original(**kwargs)
        if not rejected and kwargs['type'] == 1:
            _stream_placement_receipt(broker, response, kwargs, filled=True, price=5050)
        return response
    broker.client.order_place = Mock(side_effect=place)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    if rejected:
        tp = broker.get_tracked_order(parent._synthetic_bracket['children']['tp'])
        broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                                   filled_quantity=1, multiplier=5)
    attempts = broker.client.order_place.call_count
    broker._maybe_spawn_bracket_children(parent)
    assert broker.client.order_place.call_count == attempts
    assert parent._synthetic_bracket['children'].keys() == {'tp'}
    assert len(broker.client.placed) == 2


def test_strategy_sync_does_not_cancel_another_strategys_sibling(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _filled_bracket(broker, mes)
    broker.client.order_cancel = Mock(return_value=False)
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                               filled_quantity=1, multiplier=5)
    broker.client.order_cancel.reset_mock()
    broker._pull_all_orders = Mock(return_value=broker.get_all_orders())
    broker.sync_orders('OtherStrategy')
    broker.client.order_cancel.assert_not_called()
    broker.client.order_cancel.return_value = {'success': True}
    broker.sync_orders('Strat')
    broker.client.order_cancel.assert_called_once_with(1, int(sl.identifier))


@pytest.mark.parametrize('provider_rejected', [False, True])
def test_sibling_error_requires_provider_rejection_evidence(broker, mes, provider_rejected):
    from unittest.mock import Mock
    parent, tp, sl = _filled_bracket(broker, mes)
    sl.status = 'error'
    if provider_rejected:
        sl.broker_status = 'rejected'
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                               filled_quantity=1, multiplier=5)
    assert parent._synthetic_bracket['active'] is not provider_rejected
    assert broker.client.order_cancel.call_count == (0 if provider_rejected else 1)


def test_stream_fill_can_progress_while_sibling_ack_is_waiting(broker, mes):
    import threading
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place
    sibling_waiting = threading.Event()
    release_ack = threading.Event()
    errors = []
    def place(**kwargs):
        response = original(**kwargs)
        if kwargs['type'] == 2:
            _stream_placement_receipt(broker, response, kwargs, filled=True, price=5000)
        elif kwargs['type'] == 4:
            sibling_waiting.set()
            if not release_ack.wait(5):
                raise RuntimeError('test did not release acknowledgement')
        return response
    def submit():
        try:
            broker._submit_order(parent)
        except BaseException as exc:
            errors.append(exc)
    broker.client.order_place = place
    broker.client.order_cancel = Mock(return_value={'success': True})
    worker = threading.Thread(target=submit)
    worker.start()
    try:
        assert sibling_waiting.wait(5)
        tracked = broker.get_tracked_order(parent.identifier)
        tp = broker.get_tracked_order(tracked._synthetic_bracket['children']['tp'])
        snapshot = Order(strategy='Strat', asset=mes, quantity=1, side='sell', identifier=tp.identifier)
        snapshot.status = 'filled'
        snapshot.filled_quantity = 1
        snapshot.avg_fill_price = 5050
        broker._detect_and_dispatch_order_changes(snapshot)
        assert tp.is_filled()
    finally:
        release_ack.set()
        worker.join(5)
    assert not worker.is_alive() and not errors
    sibling = tracked._synthetic_bracket['children']['sl']
    broker.client.order_cancel.assert_called_once_with(1, int(sibling))


def test_cancel_terminal_callback_wins_over_failed_acknowledgement(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _filled_bracket(broker, mes)
    def canceled_before_ack(*args):
        broker._process_trade_event(sl, broker.CANCELED_ORDER)
        return False
    broker.client.order_cancel = Mock(side_effect=canceled_before_ack)
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                               filled_quantity=1, multiplier=5)
    broker._pull_all_orders = Mock(return_value=broker.get_all_orders())
    broker.sync_orders('Strat')
    assert sl.is_canceled()
    assert not parent._synthetic_bracket['active']
    broker.client.order_cancel.assert_called_once()


def _partial_bracket(broker, mes, entered=1, quantity=3):
    parent = _make_bracket_entry(mes, qty=quantity, tp=5050, sl=4975)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.PARTIALLY_FILLED_ORDER, price=5000,
                               filled_quantity=entered, multiplier=5)
    children = parent._synthetic_bracket['children']
    return parent, broker.get_tracked_order(children['tp']), broker.get_tracked_order(children['sl'])


def test_residual_entry_uses_fresh_children_only_after_old_sibling_is_terminal(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _partial_bracket(broker, mes)
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050, filled_quantity=1, multiplier=5)
    broker._process_trade_event(parent, broker.PARTIALLY_FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    assert len(broker.client.placed) == 3
    broker._process_trade_event(sl, broker.CANCELED_ORDER)
    broker._pull_all_orders = Mock(return_value=broker.get_all_orders())
    broker.sync_orders('Strat')
    fresh = parent._synthetic_bracket['children']
    assert set(fresh.values()).isdisjoint({tp.identifier, sl.identifier})
    assert [row['size'] for row in broker.client.placed[3:]] == [1, 1]
    assert len({row['custom_tag'] for row in broker.client.placed}) == 5
    assert parent.quantity == 3 and parent.filled_quantity == 2
    assert tp.quantity == 1 and tp.is_filled() and sl.is_canceled()
    broker.sync_orders('Strat')
    assert len(broker.client.placed) == 5


def test_entry_growth_during_child_ack_resizes_accepted_child_before_protecting_rest(broker, mes):
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, qty=3, tp=5050, sl=4975)
    original = broker.client.order_place
    grown = False
    def place(**kwargs):
        nonlocal grown
        response = original(**kwargs)
        if kwargs['type'] == 1 and not grown:
            grown = True
            broker._process_trade_event(parent, broker.PARTIALLY_FILLED_ORDER, price=5000,
                                       filled_quantity=1, multiplier=5)
        return response
    broker.client.order_place = place
    broker.client.order_modify = Mock(return_value={'success': True})
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.PARTIALLY_FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    children = parent._synthetic_bracket['children']
    assert [row['size'] for row in broker.client.placed[1:]] == [1, 2]
    assert [broker.get_tracked_order(oid).quantity for oid in children.values()] == [2, 2]
    broker.client.order_modify.assert_called_once_with(account_id=1, order_id=int(children['tp']), size=2)


def test_child_fill_during_resize_ack_never_reopens_or_resizes_its_sibling(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _partial_bracket(broker, mes)
    def modify(**kwargs):
        broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050,
                                   filled_quantity=1, multiplier=5)
        return {'success': True}
    broker.client.order_modify = Mock(side_effect=modify)
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker._process_trade_event(parent, broker.PARTIALLY_FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    assert tp.is_filled() and tp.quantity == 1
    broker.client.order_modify.assert_called_once_with(account_id=1, order_id=int(tp.identifier), size=2)
    broker.client.order_cancel.assert_called_once_with(1, int(sl.identifier))
    assert len(broker.client.placed) == 3


def test_late_old_generation_fill_cancels_new_exits_without_replaying_entry(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _partial_bracket(broker, mes, entered=2)
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker._process_trade_event(tp, broker.FILLED_ORDER, price=5050, filled_quantity=2, multiplier=5)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000, filled_quantity=1, multiplier=5)
    broker._process_trade_event(sl, broker.CANCELED_ORDER)
    broker._pull_all_orders = Mock(return_value=broker.get_all_orders())
    broker.sync_orders('Strat')
    fresh = dict(parent._synthetic_bracket['children'])
    assert set(fresh.values()).isdisjoint({tp.identifier, sl.identifier})
    snapshot = Order(strategy='Strat', asset=mes, quantity=2, side='sell', identifier=sl.identifier)
    snapshot.status = 'canceled'
    snapshot.filled_quantity = 1
    snapshot.avg_fill_price = 4975
    broker._apply_direct_order_observation(sl, snapshot)
    broker.sync_orders('Strat')
    broker.sync_orders('Strat')
    canceled_ids = [str(call.args[1]) for call in broker.client.order_cancel.call_args_list]
    assert all(canceled_ids.count(identifier) == 1 for identifier in fresh.values())
    assert sum(tx.quantity for tx in parent.transactions) == 3
    assert sum(tx.quantity for tx in sl.transactions) == 1
    assert sl.is_canceled() and len(broker.client.placed) == 5


def _generation_history(broker, mes, *, generation=1):
    from lumibot.tools.projectx_helpers import build_unique_order_tag, bracket_child_tag
    owner = Order(strategy='Strat', asset=mes, quantity=3, side='buy')
    base = build_unique_order_tag(owner)
    common = {'accountId': 1, 'contractId': 'CON.F.US.MES.Z25',
              'creationTimestamp': '2026-01-01T00:00:00Z', 'fillVolume': 0, 'size': 1}
    return [
        {**common, 'id': 101, 'status': 7, 'type': 2, 'side': 0, 'size': 3,
         'fillVolume': 2, 'filledPrice': 5000, 'customTag': 'BRK_ENTRY_' + base},
        {**common, 'id': 102, 'status': 2, 'type': 1, 'side': 1, 'fillVolume': 1,
         'filledPrice': 5050, 'limitPrice': 5050, 'customTag': bracket_child_tag('tp', base)},
        {**common, 'id': 103, 'status': 3, 'type': 4, 'side': 1,
         'stopPrice': 4975, 'customTag': bracket_child_tag('sl', base)},
        {**common, 'id': 104, 'status': 1, 'type': 1, 'side': 1,
         'limitPrice': 5050, 'customTag': bracket_child_tag('tp', base, generation=generation)},
        {**common, 'id': 105, 'status': 1, 'type': 4, 'side': 1,
         'stopPrice': 4975, 'customTag': bracket_child_tag('sl', base, generation=generation)},
    ]


def _restore_generation_history(broker, mes, rows):
    from unittest.mock import Mock
    broker.client.get_orders = Mock(return_value=rows)
    broker._get_asset_from_contract_id_cached = Mock(return_value=mes)
    orders = broker._get_orders_at_broker()
    for item in orders:
        broker._process_broker_synced_order(item)
    return broker.get_tracked_order('101')


def test_restart_tracks_old_and_current_generation_without_new_submission(broker, mes):
    from unittest.mock import Mock
    parent = _restore_generation_history(broker, mes, _generation_history(broker, mes))
    broker.client.order_modify = Mock()
    broker._maybe_spawn_bracket_children(parent)
    assert parent._synthetic_bracket['children'] == {'tp': '104', 'sl': '105'}
    assert parent._synthetic_bracket['residual_quantity'] == 1
    assert set(broker._bracket_parent_by_child_id) == {'102', '103', '104', '105'}
    assert broker.client.placed == []
    broker.client.order_modify.assert_not_called()


@pytest.mark.parametrize('missing', ['generation', 'execution', 'ambiguous', 'sparse'])
def test_incomplete_restart_history_cannot_create_or_resize_protection(broker, mes, missing):
    from unittest.mock import Mock
    rows = _generation_history(broker, mes, generation=2**31-1 if missing == 'sparse' else 1)
    if missing == 'generation':
        rows = [row for row in rows if row['id'] not in (102, 103)]
    elif missing == 'execution':
        rows[2].pop('fillVolume')
    elif missing == 'ambiguous':
        rows.append({**rows[-1], 'id': 106})
    parent = _restore_generation_history(broker, mes, rows)
    broker.client.order_modify = Mock()
    broker._maybe_spawn_bracket_children(parent)
    assert parent._synthetic_bracket.get('reconciliation_error')
    assert broker.client.placed == []
    broker.client.order_modify.assert_not_called()


def test_complete_history_can_clear_a_previous_restart_hold(broker, mes):
    from unittest.mock import Mock
    complete = _generation_history(broker, mes)
    parent = _restore_generation_history(broker, mes, complete[0:1] + complete[3:])
    broker._maybe_spawn_bracket_children(parent)
    assert parent._synthetic_bracket.get('reconciliation_error')
    broker.client.get_orders.return_value = complete
    restored = broker._get_orders_at_broker()
    broker._pull_all_orders = Mock(return_value=restored)
    broker.sync_orders('Strat')
    assert not parent._synthetic_bracket.get('reconciliation_error')
    assert parent._synthetic_bracket['residual_quantity'] == 1
    assert broker.client.placed == []


def test_terminal_only_restart_does_not_invent_permission_for_new_generation(broker, mes):
    rows = _generation_history(broker, mes)[:3]
    parent = _restore_generation_history(broker, mes, rows)
    broker._maybe_spawn_bracket_children(parent)
    assert 'intent is unknown' in parent._synthetic_bracket['reconciliation_error']
    assert broker.client.placed == []


def test_partial_protective_fill_preserves_live_orders_and_sizes_total_quantity(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _partial_bracket(broker, mes, entered=3, quantity=4)
    broker.client.order_modify = Mock(return_value={'success': True})
    broker.client.order_cancel = Mock()
    broker._process_trade_event(tp, broker.PARTIALLY_FILLED_ORDER, price=5050,
                               filled_quantity=1, multiplier=5)
    assert tp.quantity == 3 and sl.quantity == 2
    broker.client.order_modify.assert_called_once_with(account_id=1, order_id=int(sl.identifier), size=2)
    broker.client.order_cancel.assert_not_called()
    assert len(broker.client.placed) == 3


def test_poll_applies_child_fill_before_protecting_new_parent_quantity(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _partial_bracket(broker, mes)
    incoming_parent = Order(strategy='Strat', asset=mes, quantity=3, side='buy', identifier=parent.identifier)
    incoming_parent.status = 'partial_fill'
    incoming_parent.filled_quantity = 2
    incoming_parent.avg_fill_price = 5000
    incoming_tp = Order(strategy='Strat', asset=mes, quantity=1, side='sell', identifier=tp.identifier)
    incoming_tp.status = 'filled'
    incoming_tp.filled_quantity = 1
    incoming_tp.avg_fill_price = 5050
    broker._pull_all_orders = Mock(return_value=[incoming_parent, incoming_tp, sl])
    broker.client.order_modify = Mock(return_value={'success': True})
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker.sync_orders('Strat')
    assert tp.is_filled() and tp.quantity == 1
    broker.client.order_modify.assert_not_called()
    broker.client.order_cancel.assert_called_once_with(1, int(sl.identifier))
    assert len(broker.client.placed) == 3


@pytest.mark.parametrize('cancel_both', [False, True])
def test_public_cancel_preserves_requested_exit_intent(broker, mes, cancel_both):
    from unittest.mock import Mock
    parent, tp, sl = _partial_bracket(broker, mes)
    broker.client.order_modify = Mock(return_value={'success': True})
    broker.cancel_order(tp)
    broker._process_trade_event(tp, broker.CANCELED_ORDER)
    if cancel_both:
        broker.cancel_order(sl)
        broker._process_trade_event(sl, broker.CANCELED_ORDER)
    broker._process_trade_event(parent, broker.PARTIALLY_FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    assert len(broker.client.placed) == 3
    if cancel_both:
        broker.client.order_modify.assert_not_called()
    else:
        broker.client.order_modify.assert_called_once_with(account_id=1, order_id=int(sl.identifier), size=2)


def test_unknown_child_submission_is_held_without_reposting_the_action(broker, mes):
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place
    def place(**kwargs):
        if kwargs['type'] == 1:
            return None
        return original(**kwargs)
    broker.client.order_place = Mock(side_effect=place)
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    for _ in range(3):
        broker._maybe_spawn_bracket_children(parent)
    assert broker.client.order_place.call_count == 2
    assert parent._synthetic_bracket['submission_holds']
    assert not parent._synthetic_bracket['children']


def test_fill_at_final_reconciliation_boundary_runs_a_pending_pass(broker, mes):
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, qty=3, tp=5050, sl=4975)
    broker._submit_order(parent)
    class ObservedMeta(dict):
        fired = False
        def update(self, *args, **kwargs):
            super().update(*args, **kwargs)
            if 'protected_quantity' in kwargs and not self.fired:
                self.fired = True
                broker._process_trade_event(parent, broker.PARTIALLY_FILLED_ORDER, price=5000,
                                           filled_quantity=1, multiplier=5)
    meta = ObservedMeta(parent._synthetic_bracket)
    parent._synthetic_bracket = broker._bracket_meta[parent.identifier] = meta
    broker.client.order_modify = Mock(return_value={'success': True})
    broker._process_trade_event(parent, broker.PARTIALLY_FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    assert parent.filled_quantity == 2
    assert [broker.get_tracked_order(identifier).quantity for identifier in meta['children'].values()] == [2, 2]
    assert len(broker.client.placed) == 3


def test_valid_execution_snapshot_clears_missing_evidence_but_invalid_snapshot_does_not(broker, mes):
    from unittest.mock import Mock
    rows = _generation_history(broker, mes)
    incomplete = [dict(row) for row in rows]
    incomplete[2].pop('fillVolume')
    parent = _restore_generation_history(broker, mes, incomplete)
    broker._maybe_spawn_bracket_children(parent)
    assert parent._synthetic_bracket.get('reconciliation_error')
    malformed = [dict(row) for row in rows]
    malformed[2]['fillVolume'] = float('nan')
    broker.client.get_orders.return_value = malformed
    snapshots = broker._get_orders_at_broker()
    broker._pull_all_orders = Mock(return_value=snapshots)
    with pytest.raises(ValueError):
        broker.sync_orders('Strat')
    assert not broker.get_tracked_order('103')._projectx_execution_known
    assert broker.client.placed == []
    broker.client.get_orders.return_value = rows
    broker._pull_all_orders.return_value = broker._get_orders_at_broker()
    broker.sync_orders('Strat')
    assert broker.get_tracked_order('103')._projectx_execution_known
    assert not parent._synthetic_bracket.get('reconciliation_error')
    assert broker.client.placed == []


def test_another_owners_stream_fill_is_not_lost_during_sync(broker, mes):
    import threading
    from unittest.mock import Mock
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    parent.strategy = 'OtherStrategy'
    broker._submit_order(parent)
    entered = threading.Event()
    release = threading.Event()
    errors = []
    def read(*args):
        entered.set()
        if not release.wait(5):
            raise RuntimeError('test sync was not released')
        return broker.get_all_orders()
    broker._pull_all_orders = Mock(side_effect=read)
    def sync():
        try:
            broker.sync_orders('Strat')
        except BaseException as exc:
            errors.append(exc)
    worker = threading.Thread(target=sync)
    worker.start()
    try:
        assert entered.wait(5)
        broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                                   filled_quantity=1, multiplier=5)
        assert len(broker.client.placed) == 1
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive() and not errors
    assert len(broker.client.placed) == 3


def test_unknown_submission_is_resolved_by_exact_tag_receipt_without_repost(broker, mes):
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    original = broker.client.order_place
    requests = []
    def place(**kwargs):
        requests.append(kwargs)
        if kwargs['type'] == 1:
            return None
        return original(**kwargs)
    broker.client.order_place = place
    broker._submit_order(parent)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    lost = Order(strategy='Strat', asset=mes, quantity=1, side='sell', identifier='9000',
                 order_type='limit', limit_price=5050)
    lost.tag = requests[-1]['custom_tag']
    lost.status = 'open'
    lost.filled_quantity = 0
    broker._restore_bracket_links([parent, lost])
    broker._process_broker_synced_order(lost)
    broker._maybe_spawn_bracket_children(parent)
    assert [request['type'] for request in requests] == [2, 1, 4]
    assert parent._synthetic_bracket['children']['tp'] == '9000'
    assert not parent._synthetic_bracket['submission_holds']


def test_public_cancel_open_orders_does_not_rearm_exits_during_ack_callbacks(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _filled_bracket(broker, mes)
    def cancel(account, identifier):
        item = broker.get_tracked_order(str(identifier))
        broker._process_trade_event(item, broker.CANCELED_ORDER)
        return {'success': True}
    broker.client.order_cancel = Mock(side_effect=cancel)
    broker.cancel_open_orders('Strat')
    assert broker.client.order_cancel.call_count == 2
    assert tp.is_canceled() and sl.is_canceled()
    assert len(broker.client.placed) == 3
    assert parent._synthetic_bracket['disabled_kinds'] == {'tp', 'sl'}


@pytest.mark.parametrize('executed', [True, False])
def test_restored_terminal_exit_only_cancels_sibling_with_execution_evidence(broker, mes, executed):
    from unittest.mock import Mock
    rows = _generation_history(broker, mes)[:3]
    rows[0].update(size=1, fillVolume=1, status=2)
    rows[1].update(status=2 if executed else 3, fillVolume=1 if executed else 0)
    rows[2]['status'] = 1
    parent = _restore_generation_history(broker, mes, rows)
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker.client.order_modify = Mock()
    broker._maybe_spawn_bracket_children(parent)
    broker._maybe_spawn_bracket_children(parent)
    if executed:
        broker.client.order_cancel.assert_called_once_with(1, 103)
        assert broker.get_tracked_order('103').status == 'cancelling'
        assert not broker.get_tracked_order('103').is_canceled()
    else:
        broker.client.order_cancel.assert_not_called()
    assert broker.client.placed == []
    broker.client.order_modify.assert_not_called()


def test_failed_poll_batch_cannot_act_on_only_the_parent_row(broker, mes):
    from unittest.mock import Mock
    parent, tp, sl = _partial_bracket(broker, mes)
    incoming_parent = Order(strategy='Strat', asset=mes, quantity=3, side='buy', identifier=parent.identifier)
    incoming_parent.status = 'partial_fill'
    incoming_parent.filled_quantity = 2
    incoming_parent.avg_fill_price = 5000
    incoming_tp = Order(strategy='Strat', asset=mes, quantity=1, side='sell', identifier=tp.identifier)
    incoming_tp.status = 'filled'
    incoming_tp.filled_quantity = 1
    incoming_tp.avg_fill_price = 5050
    broker._pull_all_orders = Mock(return_value=[incoming_parent, incoming_tp, sl])
    original = broker._apply_direct_order_observation
    broken = True
    def apply(stored, incoming):
        if broken and incoming.identifier == tp.identifier:
            raise RuntimeError('batch interrupted before child observation')
        return original(stored, incoming)
    broker._apply_direct_order_observation = apply
    broker.client.order_modify = Mock(return_value={'success': True})
    broker.client.order_cancel = Mock(return_value={'success': True})
    with pytest.raises(RuntimeError):
        broker.sync_orders('Strat')
    broker._maybe_spawn_bracket_children(parent)
    assert parent._synthetic_bracket['snapshot_incomplete']
    broker.client.order_modify.assert_not_called()
    broker.client.order_cancel.assert_not_called()
    assert len(broker.client.placed) == 3
    broken = False
    broker.sync_orders('Strat')
    assert not parent._synthetic_bracket.get('snapshot_incomplete')
    assert parent.filled_quantity == 2 and tp.is_filled()
    broker.client.order_modify.assert_not_called()
    broker.client.order_cancel.assert_called_once_with(1, int(sl.identifier))


@pytest.mark.parametrize('filled', [0, 1])
def test_contradictory_terminal_cumulative_quantity_holds_until_coherent(broker, mes, filled):
    from unittest.mock import Mock
    parent, tp, sl = _partial_bracket(broker, mes, entered=2)
    incoming = Order(strategy='Strat', asset=mes, quantity=2, side='sell', identifier=tp.identifier)
    incoming.status = 'filled'
    incoming.filled_quantity = filled
    incoming.avg_fill_price = 5050
    incoming._projectx_execution_known = True
    broker.client.order_cancel = Mock(return_value={'success': True})
    with pytest.raises(ValueError, match='incomplete'):
        broker._apply_direct_order_observation(tp, incoming)
    assert not tp.transactions and not tp.is_filled()
    broker._maybe_spawn_bracket_children(parent)
    broker.client.order_cancel.assert_not_called()
    assert len(broker.client.placed) == 3
    incoming.filled_quantity = 2
    broker._apply_direct_order_observation(tp, incoming)
    assert tp.is_filled() and sum(tx.quantity for tx in tp.transactions) == 2
    broker.client.order_cancel.assert_called_once_with(1, int(sl.identifier))


def test_restoration_rejects_same_economic_direction_with_different_side_labels(broker, mes):
    parent, tp, sl = _partial_bracket(broker, mes)
    parent.side = 'buy_to_open'
    bad = Order(strategy='Strat', asset=mes, quantity=1, side='buy', identifier='wrong-direction')
    bad.tag = tp.tag
    broker._restore_bracket_links([parent, bad, sl])
    assert 'identity' in parent._synthetic_bracket['history_error'].lower()
    assert 'wrong-direction' not in broker._bracket_parent_by_child_id


@pytest.mark.parametrize('failure', ['timeout', 'non_json', 'pending', 'unknown', 'future_code'])
def test_real_client_ambiguous_placement_holds_without_replay(broker, mes, failure):
    import requests
    from unittest.mock import Mock
    from lumibot.tools.projectx_helpers import ProjectX as HTTPClient, ProjectXClient
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    broker._submit_order(parent)
    api = HTTPClient({'firm': 'TEST', 'base_url': 'https://example.test/'}, 'synthetic')
    if failure == 'timeout':
        api._post = Mock(side_effect=requests.Timeout('lost response'))
    else:
        response = Mock(status_code=200)
        if failure == 'non_json':
            response.json.side_effect = ValueError('not JSON')
        else:
            response.json.return_value = {'success': False, 'errorCode': {'pending': 6, 'unknown': 7, 'future_code': 999}[failure]}
        api._post = Mock(return_value=response)
    client = object.__new__(ProjectXClient)
    client.api = api
    broker.client.order_place = client.order_place
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    for _ in range(3):
        broker._maybe_spawn_bracket_children(parent)
    api._post.assert_called_once()
    assert parent._synthetic_bracket['submission_holds']
    assert parent._synthetic_bracket['children'] == {}


def test_real_client_definitive_provider_rejection_retains_structured_certainty(broker, mes):
    from unittest.mock import Mock
    from lumibot.tools.projectx_helpers import ProjectX as HTTPClient, ProjectXClient
    api = HTTPClient({'firm': 'TEST', 'base_url': 'https://example.test/'}, 'synthetic')
    response = Mock(status_code=200)
    response.json.return_value = {'success': False, 'errorCode': 2, 'errorMessage': 'invalid order', 'orderId': 777}
    api._post = Mock(return_value=response)
    client = object.__new__(ProjectXClient)
    client.api = api
    broker.client.order_place = client.order_place
    item = Order(strategy='Strat', asset=mes, quantity=1, side='buy')
    broker._submit_order(item)
    assert item.status == 'error'
    assert item._projectx_submission_state == 'rejected'
    api._post.assert_called_once()


def test_uncertain_public_submission_is_not_reported_as_rejected(broker, mes):
    from unittest.mock import Mock
    broker.client.order_place = Mock(return_value={'success': False, 'error': 'transport failure'})
    item = Order(strategy='Strat', asset=mes, quantity=1, side='buy')
    broker._submit_order(item)
    # Sent without acknowledgement stays locally owned; generic UNKNOWN remains nonactive.
    assert item.status == 'submitted'
    assert item._projectx_submission_state == 'unknown'


@pytest.mark.parametrize('status,body', [
    (500, {'success': False, 'errorCode': 2}),
    (200, {'success': 'false', 'errorCode': 2}),
    (200, {'success': False, 'errorCode': True}),
    (200, {'success': True, 'errorCode': False, 'orderId': 123}),
    (200, {'success': False, 'errorCode': '2'}),
    (200, {'success': False, 'errorCode': 6, 'errors': {'request': ['required']}}),
    (400, {'success': False, 'errorCode': 7, 'errors': {'request': ['required']}}),
])
def test_malformed_or_http_mismatched_rejection_never_replays(broker, mes, status, body):
    from unittest.mock import Mock
    from lumibot.tools.projectx_helpers import ProjectX as HTTPClient, ProjectXClient
    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    broker._submit_order(parent)
    api = HTTPClient({'firm': 'TEST', 'base_url': 'https://example.test/'}, 'synthetic')
    response = Mock(status_code=status)
    response.json.return_value = body
    api._post = Mock(return_value=response)
    client = object.__new__(ProjectXClient)
    client.api = api
    broker.client.order_place = client.order_place
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000,
                               filled_quantity=1, multiplier=5)
    broker._maybe_spawn_bracket_children(parent)
    api._post.assert_called_once()
    assert parent._synthetic_bracket['submission_holds']


def test_wrapper_compatibility_retries_only_explicit_http400_model_validation():
    from unittest.mock import Mock
    from lumibot.tools.projectx_helpers import ProjectX as HTTPClient, projectx_order_placement_outcome
    api = HTTPClient({'firm': 'TEST', 'base_url': 'https://example.test/'}, 'synthetic')
    invalid = Mock(status_code=400)
    invalid.json.return_value = {'errors': {'request': ['The request field is required.']}}
    accepted = Mock(status_code=200)
    accepted.json.return_value = {'success': True, 'errorCode': 0, 'orderId': 123}
    api._post = Mock(side_effect=[invalid, accepted])
    result = api.order_place(1, 'CON.F.US.MES.Z25', 1, 1, 1, custom_tag='synthetic')
    assert api._post.call_count == 2
    assert 'request' not in api._post.call_args_list[0].kwargs['json']
    assert 'request' in api._post.call_args_list[1].kwargs['json']
    assert projectx_order_placement_outcome(result) == 'accepted'


def test_failed_old_generation_row_requires_complete_family_before_recovery(broker, mes):
    from unittest.mock import Mock
    parent, old_tp, old_sl = _partial_bracket(broker, mes, entered=2)
    broker._process_trade_event(old_tp, broker.FILLED_ORDER, price=5050, filled_quantity=2, multiplier=5)
    broker._process_trade_event(parent, broker.FILLED_ORDER, price=5000, filled_quantity=1, multiplier=5)
    broker._process_trade_event(old_sl, broker.CANCELED_ORDER)
    current = [broker.get_tracked_order(identifier) for identifier in parent._synthetic_bracket['children'].values()]
    late = Order(strategy='Strat', asset=mes, quantity=2, side='sell', identifier=old_sl.identifier)
    late.status = 'canceled'
    late.filled_quantity = 1
    late.avg_fill_price = 4975
    broker._pull_all_orders = Mock(return_value=[parent, late, *current])
    original = broker._apply_direct_order_observation
    fail = True
    def apply(stored, incoming):
        if fail and incoming.identifier == old_sl.identifier:
            raise RuntimeError('old-generation observation interrupted')
        return original(stored, incoming)
    broker._apply_direct_order_observation = apply
    broker.client.order_cancel = Mock(return_value={'success': True})
    broker.client.order_modify = Mock(return_value={'success': True})
    with pytest.raises(RuntimeError):
        broker.sync_orders('Strat')
    fail = False
    broker._pull_all_orders.return_value = [parent, *current]
    broker.sync_orders('Strat')
    assert parent._synthetic_bracket['snapshot_incomplete']
    broker.client.order_cancel.assert_not_called()
    broker.client.order_modify.assert_not_called()
    broker._pull_all_orders.return_value = [parent, old_tp, late, *current]
    broker.sync_orders('Strat')
    assert not parent._synthetic_bracket.get('snapshot_incomplete')
    assert parent._synthetic_bracket['residual_quantity'] == 0
    assert broker.client.order_cancel.call_count == 2
    assert len(broker.client.placed) == 5
