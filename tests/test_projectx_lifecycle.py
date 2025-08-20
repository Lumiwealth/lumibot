import types
import pytest
from lumibot.brokers.projectx import ProjectX
from lumibot.entities import Asset, Order
from lumibot.data_sources import DataSource


class DummyDataSource(DataSource):
    name = "dummy"
    def __init__(self):
        pass
    def get_last_price(self, asset):
        return 100.0
    def get_datetime(self):
        import datetime
        return datetime.datetime.utcnow()
    # Abstract methods required by base class
    def get_historical_prices(self, asset, length, timestep, **kwargs):
        import pandas as pd
        now = self.get_datetime()
        data = {
            "datetime": [now],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "volume": [0],
        }
        return pd.DataFrame(data).set_index("datetime")
    def get_chains(self, asset):
        return {"Chains": {"CALL": {}, "PUT": {}}}


class DummyClient:
    def __init__(self):
        self._orders = {}
    # Minimal attributes used by ProjectX broker init
    def get_contract_details(self, contract_id):
        return {"symbol": "MES"}


class MinimalProjectX(ProjectX):
    def __init__(self):
        # Provide minimal config
        config = {"firm": "demo", "api_key": "k", "username": "u", "base_url": "http://x", "preferred_account_name": "acct"}
        # Build a shim client first
        class _ShimClient:
            def get_contract_details(self_inner, cid):
                return {"symbol": "MES"}
            def get_account_balance(self_inner, account_id):
                return {"cash": 0, "equity": 0, "buying_power": 0, "account_value": 0}
        # Temporarily monkeypatch symbol inside projectx module namespace BEFORE super().__init__ uses it
        import lumibot.brokers.projectx as projectx_module
        original_client = getattr(projectx_module, "ProjectXClient")
        projectx_module.ProjectXClient = lambda cfg: _ShimClient()
        try:
            super().__init__(config=config, data_source=DummyDataSource(), connect_stream=False)
        finally:
            projectx_module.ProjectXClient = original_client
        self.client = _ShimClient()


def _make_order(strategy_name="strat", status="new", qty=1):
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
    o = Order(strategy=strategy_name, asset=asset, quantity=qty, side=Order.OrderSide.BUY, order_type=Order.OrderType.MARKET)
    o.id = str(id(o))
    o.identifier = o.id
    o.status = status
    return o


@pytest.fixture
def broker():
    return MinimalProjectX()


def test_apply_order_update_new_to_filled(broker):
    new_order = _make_order(status="new")
    broker._apply_order_update_tracking(new_order)
    assert any(o.identifier == new_order.identifier for o in broker._new_orders.get_list())

    # Simulate fill update
    new_order.status = "filled"
    new_order.avg_fill_price = 4321.0
    new_order.filled_quantity = new_order.quantity
    broker._apply_order_update_tracking(new_order)
    filled_ids = [o.identifier for o in broker._filled_orders.get_list()]
    statuses = {o.identifier: o.status for o in broker._filled_orders.get_list()}
    assert any(o.identifier == new_order.identifier for o in broker._filled_orders.get_list()), f"Order should transition to filled list. Filled IDs={filled_ids} statuses={statuses} new_order.status={new_order.status}"


def test_apply_order_update_partial_then_fill(broker):
    order = _make_order(status="submitted")
    broker._apply_order_update_tracking(order)
    assert any(o.identifier == order.identifier for o in broker._new_orders.get_list())

    # Partial fill (should move to partially filled list)
    order.status = "partially_filled"
    order.filled_quantity = 1
    order.avg_fill_price = 4000.0
    broker._apply_order_update_tracking(order)
    assert any(o.identifier == order.identifier for o in broker._partially_filled_orders.get_list())

    # Complete fill
    order.status = "filled"
    order.filled_quantity = order.quantity
    order.avg_fill_price = 4050.0
    broker._apply_order_update_tracking(order)
    assert any(o.identifier == order.identifier for o in broker._filled_orders.get_list()), "Order should move from partial to filled"


def test_apply_order_update_cancel(broker):
    order = _make_order(status="submitted")
    broker._apply_order_update_tracking(order)
    assert any(o.identifier == order.identifier for o in broker._new_orders.get_list())
    order.status = "cancelled"
    broker._apply_order_update_tracking(order)
    assert any(o.identifier == order.identifier for o in broker._canceled_orders.get_list())


def test_apply_order_update_error(broker):
    order = _make_order(status="submitted")
    broker._apply_order_update_tracking(order)
    order.status = "error"
    order.error = "Some failure"
    broker._apply_order_update_tracking(order)
    assert any(o.identifier == order.identifier for o in broker._error_orders.get_list())
