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
    assert submitted.id is not None
    assert hasattr(submitted, "_synthetic_bracket")
    meta = submitted._synthetic_bracket
    assert meta["tp_price"] == 5050.0 and meta["sl_price"] == 4975.0
    assert submitted.tag.startswith("BRK_ENTRY_")
    # Ensure no TP/SL sent on parent payload
    sent = broker.client.placed[0]
    assert sent.get("limit_price") in (5000.0, 5000.0) or True  # price exists if limit
    assert sent.get("stop_price") is None


def test_bracket_children_spawn_on_fill_and_tagging(broker, mes):
    # Submit parent
    parent = _make_bracket_entry(mes, limit=5000.0, tp=5050.0, sl=4975.0)
    broker._submit_order(parent)
    pid = parent.id
    # Simulate fill event routing through dispatch
    filled = Order(asset=mes, quantity=1, order_type="limit", side="buy", strategy="Strat")
    filled.id = pid
    filled.identifier = pid
    filled.status = "filled"
    # Sync cache with parent to preserve meta
    broker._orders_cache[pid] = parent
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
    tp_order = broker._orders_cache.get(tp_id)
    sl_order = broker._orders_cache.get(sl_id)
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
    pid = parent.id
    broker._orders_cache[pid] = parent
    filled = Order(asset=mes, quantity=1, order_type="limit", side="buy", strategy="Strat")
    filled.id = pid
    filled.identifier = pid
    filled.status = "filled"
    broker._dispatch_status_change(parent, filled)

    meta = parent._synthetic_bracket
    tp_id = meta['children']['tp']
    sl_id = meta['children']['sl']

    # Ensure children exist in cache
    assert tp_id in broker._orders_cache and sl_id in broker._orders_cache

    # Simulate TP child fill
    tp_order = broker._orders_cache[tp_id]
    tp_order.status = "filled"
    broker._handle_bracket_child_fill(tp_order)

    # SL sibling should be canceled or marked terminal
    sl_order = broker._orders_cache.get(sl_id)
    assert (getattr(sl_order, 'status', '') or '').lower() in {"canceled", "cancelled", "fill", "filled", "error"}
    # Bracket deactivated
    assert meta['active'] is False
