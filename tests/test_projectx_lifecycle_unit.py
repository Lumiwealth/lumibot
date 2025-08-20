import pytest
from lumibot.brokers.projectx import ProjectX
from lumibot.entities import Asset, Order
from lumibot.data_sources.data_source import DataSource

# --- Lightweight dummy data source to satisfy Broker requirement without real API calls ---
class DummyDataSource(DataSource):
    def get_chains(self, asset, quote: Asset = None):
        return {}
    def get_historical_prices(self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, return_polars=False):
        return None
    def get_last_price(self, asset, quote=None, exchange=None):
        return 0.0

# Dummy ProjectXClient to avoid real authentication during broker init
class DummyProjectXClient:
    def __init__(self, config):
        self.config = config
    # Methods potentially called indirectly (polling/connect). Provide safe defaults.
    def get_preferred_account_id(self):
        return 1
    def get_contract_tick_size(self, contract_id):
        return 0.25
    def round_to_tick_size(self, price, tick_size):
        return price
    def find_contract_by_symbol(self, symbol):
        return f"CON.F.US.{symbol}.Z25"
    # Placeholders for called methods (not used in these tests but referenced by broker code paths)
    def get_orders(self, *a, **k):
        return []
    def get_positions(self, *a, **k):
        return []
    def get_contract_details(self, contract_id):
        return {"symbol": contract_id.split(".")[3] if contract_id.startswith("CON.F.US.") else "MES"}

class StubClient:
    def __init__(self):
        self.placed = []
        self.canceled = []
    def order_place(self, **kwargs):
        # Simulate success with incrementing orderId
        oid = 100000 + len(self.placed) + 1
        self.placed.append(oid)
        return {"success": True, "orderId": oid}
    def order_cancel(self, account_id, order_id):
        self.canceled.append(order_id)
        return {"success": True}
    def get_contract_tick_size(self, contract_id):
        return 0.25
    def round_to_tick_size(self, price, tick_size):
        return price
    def find_contract_by_symbol(self, symbol):
        return f"CON.F.US.{symbol}.Z25"

@pytest.fixture
def projectx_broker():
    # Minimal config; values won't be used because we stub the client
    config = {
        "firm": "TOPONE",
        "api_key": "dummy",
        "username": "dummy",
        "base_url": "https://api.toponefutures.projectx.com/",
        "preferred_account_name": "ACC",
        "streaming_base_url": "wss://gateway-rtc-demo.s2f.projectx.com/",
    }
    data = DummyDataSource()
    # Monkeypatch ProjectXClient class used inside broker before instantiation
    import lumibot.brokers.projectx as projectx_module
    original_client_cls = projectx_module.ProjectXClient
    projectx_module.ProjectXClient = DummyProjectXClient
    try:
        broker = ProjectX(config, data_source=data, connect_stream=False)
    finally:
        # Restore original to avoid side-effects on other tests
        projectx_module.ProjectXClient = original_client_cls
    # Replace with our order placement stub for lifecycle testing
    broker.client = StubClient()
    broker.account_id = 1
    broker._get_contract_id_from_asset = lambda asset: "CON.F.US.MES.Z25"
    return broker

@pytest.fixture
def mes_asset():
    return Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)

def test_submit_partial_fill_full_fill_and_cancel(projectx_broker, mes_asset, caplog):
    order = Order(asset=mes_asset, quantity=3, order_type="limit", side="buy", limit_price=5000.0, strategy="Strat")
    # Submit
    projectx_broker._submit_order(order)
    assert order.id is not None
    # Force tracking application (status already 'submitted')
    projectx_broker._apply_order_update_tracking(order)
    # Simulate partial fill
    order_prev_qty = getattr(order, 'filled_quantity', 0) or 0
    order.filled_quantity = 1
    order.status = 'partially_filled'
    projectx_broker._apply_order_update_tracking(order)
    # Simulate full fill
    order.filled_quantity = order.quantity
    order.status = 'filled'
    order.avg_fill_price = 5000.25
    projectx_broker._apply_order_update_tracking(order)
    # Cancel after fill should no-op but test cancel path on fresh order
    cancel_order = Order(asset=mes_asset, quantity=1, order_type="limit", side="sell", limit_price=5050.0, strategy="Strat")
    projectx_broker._submit_order(cancel_order)
    projectx_broker._apply_order_update_tracking(cancel_order)
    projectx_broker.cancel_order(cancel_order)
    # Assertions (avoid brittle log dependency in CI)
    # Validate final states instead of relying on log capture which can vary with global logging config.
    assert order.status.lower() in ("fill", "filled"), f"Unexpected final status: {order.status}"
    assert cancel_order.status.lower() in ("canceled", "cancelled"), f"Unexpected cancel status: {cancel_order.status}"

def test_rejection_mapping_max_position(projectx_broker, mes_asset):
    # Monkeypatch order_place to simulate risk rejection
    def reject(**kwargs):
        return {"success": False, "errorMessage": "Maximum position exceeded for symbol"}
    projectx_broker.client.order_place = reject
    order = Order(asset=mes_asset, quantity=9999, order_type="limit", side="buy", limit_price=1.0, strategy="Strat")
    projectx_broker._submit_order(order)
    # STATUS_ALIAS_MAP maps 'rejected' to 'error'
    assert order.status == 'error'
    assert order.error == 'max_position_exceeded'

def test_rejection_generic(projectx_broker, mes_asset):
    def reject(**kwargs):
        return {"success": False, "errorMessage": "Some other error"}
    projectx_broker.client.order_place = reject
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=1.0, strategy="Strat")
    projectx_broker._submit_order(order)
    assert order.status == 'error'
    assert order.error == 'Some other error'
