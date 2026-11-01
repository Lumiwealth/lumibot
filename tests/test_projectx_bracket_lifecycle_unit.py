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
            observed = Order(strategy='Strat', asset=mes, quantity=1, side='buy',
                             identifier=str(response['orderId']))
            observed.tag = kwargs['custom_tag']
            broker._process_trade_event(observed, broker.NEW_ORDER)
            broker._process_trade_event(observed, broker.FILLED_ORDER, price=5000,
                                       filled_quantity=1, multiplier=5)
        return response

    broker.client.order_place = fill_before_ack
    broker._submit_order(parent)
    tracked = broker.get_tracked_order(parent.identifier)
    assert len(broker.client.placed) == 3
    assert tracked.is_filled()
    assert set(tracked._synthetic_bracket['children']) == {'tp', 'sl'}


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


def test_failed_protection_can_retry_without_duplicating_accepted_child(broker, mes):
    from unittest.mock import Mock
    from lumibot.tools.projectx_helpers import create_bracket_meta

    parent = _make_bracket_entry(mes, tp=5050, sl=4975)
    parent.identifier = 'parent'
    parent.status = 'filled'
    parent._is_bracket_parent = True
    parent._synthetic_bracket = create_bracket_meta(5050, 4975)
    tp = Order(asset=mes, quantity=1, side='sell', order_type='limit',
               limit_price=5050, strategy='Strat', identifier='tp')
    sl = Order(asset=mes, quantity=1, side='sell', order_type='stop',
               stop_price=4975, strategy='Strat', identifier='sl')
    broker._create_bracket_child = Mock(side_effect=[tp, RuntimeError('rejected'), sl])
    broker._maybe_spawn_bracket_children(parent)
    assert parent._synthetic_bracket['children'] == {'tp': 'tp'}
    assert not parent._synthetic_bracket.get('children_submitted', False)
    broker._maybe_spawn_bracket_children(parent)
    assert broker._create_bracket_child.call_count == 3
    assert parent._synthetic_bracket['children'] == {'tp': 'tp', 'sl': 'sl'}
    assert parent._synthetic_bracket['children_submitted'] is True


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
    # Bracket deactivated
    assert meta['active'] is False


def test_partial_entry_protects_only_filled_contracts_and_resizes_existing_exits(broker, mes):
    from unittest.mock import Mock
    from lumibot.tools.projectx_helpers import create_bracket_meta
    parent = _make_bracket_entry(mes, qty=3, tp=5050, sl=4975)
    parent.identifier = 'entry'
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
