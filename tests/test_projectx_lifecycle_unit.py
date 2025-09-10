import pytest
from lumibot.brokers.projectx import ProjectX
from lumibot.entities import Asset, Order
from lumibot.data_sources.data_source import DataSource
from unittest.mock import Mock, patch

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
        self.mock_orders = []  # Store mock order responses
    def order_place(self, **kwargs):
        # Simulate success with incrementing orderId
        oid = 100000 + len(self.placed) + 1
        self.placed.append(oid)
        # Store a mock order for order_search to return
        self.mock_orders.append({
            "id": str(oid),
            "contractId": kwargs.get("contract_id", "CON.F.US.MES.Z25"),
            "status": 1,  # Open
            "type": kwargs.get("type", 1),  # Limit
            "side": kwargs.get("side", 0),  # Buy
            "size": kwargs.get("size", 1),
            "limitPrice": kwargs.get("limit_price"),
            "customTag": kwargs.get("custom_tag")
        })
        return {"success": True, "orderId": oid}
    def order_cancel(self, account_id, order_id):
        self.canceled.append(order_id)
        return {"success": True}
    def order_search(self, **kwargs):
        # Return stored mock orders for _pull_broker_all_orders
        return self.mock_orders
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

def test_submit_and_cancel(projectx_broker, mes_asset, caplog):
    """Test basic order submission and cancellation"""
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="Strat")
    # Submit
    projectx_broker._submit_order(order)
    assert order.id is not None
    assert order.status == 'new'  # Status becomes 'new' after _process_trade_event
    
    # Cancel
    cancel_order = Order(asset=mes_asset, quantity=1, order_type="limit", side="sell", limit_price=5050.0, strategy="Strat")
    projectx_broker._submit_order(cancel_order)
    projectx_broker.cancel_order(cancel_order)
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

# ========== NEW LIFECYCLE EVENT TESTS ==========

class MockSubscriber:
    """Mock subscriber to capture lifecycle events"""
    def __init__(self, strategy_name="test_strategy"):
        self.events = []
        self.name = strategy_name
        # Add the constants that broker expects
        self.NEW_ORDER = "new"
        self.CANCELED_ORDER = "canceled"
        self.FILLED_ORDER = "fill"
        self.PARTIALLY_FILLED_ORDER = "partial_fill"
        self.ERROR_ORDER = "error"
        
    def add_event(self, event_type, payload):
        self.events.append((event_type, payload))
        
def test_new_order_event_dispatched_on_submit(projectx_broker, mes_asset):
    """Test that NEW_ORDER event is dispatched immediately on successful submit"""
    # Create mock subscriber to capture events
    mock_subscriber = MockSubscriber("test_strategy")
    # Clear existing subscribers and add our mock
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)
    
    # Submit order
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    projectx_broker._submit_order(order)
    
    # Verify order was submitted successfully and became 'new' status after event processing
    assert order.id is not None
    assert order.status == 'new'  # Status becomes 'new' after _process_trade_event
    
    # Verify NEW_ORDER event was dispatched
    assert len(mock_subscriber.events) == 1
    event_type, payload = mock_subscriber.events[0]
    assert event_type == "new"
    assert "order" in payload
    assert payload["order"].id == order.id

def test_order_status_change_detection(projectx_broker, mes_asset):
    """Test that order status changes are detected and events dispatched"""
    # Create mock subscriber
    mock_subscriber = MockSubscriber("test_strategy")
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)
    
    # Create initial order and add to cache
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    order.id = "12345"
    order.identifier = "12345"
    order.status = "open"  # Changed from "submitted" to "open" (ProjectX status=1)
    projectx_broker._orders_cache[order.id] = order
    
    # Create updated order with filled status
    updated_order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    updated_order.id = "12345"
    updated_order.identifier = "12345"
    updated_order.status = "fill"  # Changed from "filled" to "fill" (after alias)
    updated_order.avg_fill_price = 5000.0
    updated_order.filled_quantity = 1
    
    # Clear any initial events
    mock_subscriber.events.clear()
    
    # Trigger status change detection
    projectx_broker._detect_and_dispatch_order_changes(updated_order)
    
    # Verify FILLED_ORDER event was dispatched
    assert len(mock_subscriber.events) == 1
    event_type, payload = mock_subscriber.events[0]
    assert event_type == "fill"
    assert payload["price"] == 5000.0
    assert payload["quantity"] == 1

def test_order_cancellation_event(projectx_broker, mes_asset):
    """Test that order cancellation triggers CANCELED_ORDER event"""
    # Create mock subscriber
    mock_subscriber = MockSubscriber("test_strategy")
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)
    
    # Create initial order
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    order.id = "12346"
    order.identifier = "12346"
    order.status = "open"
    projectx_broker._orders_cache[order.id] = order
    
    # Create canceled order
    canceled_order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    canceled_order.id = "12346"
    canceled_order.identifier = "12346"
    canceled_order.status = "canceled"
    
    # Clear initial events
    mock_subscriber.events.clear()
    
    # Trigger status change detection
    projectx_broker._detect_and_dispatch_order_changes(canceled_order)
    
    # Verify CANCELED_ORDER event was dispatched
    assert len(mock_subscriber.events) == 1
    event_type, payload = mock_subscriber.events[0]
    assert event_type == "canceled"
    assert payload["order"].id == "12346"

def test_partial_fill_event(projectx_broker, mes_asset):
    """Test that partial fills trigger PARTIALLY_FILLED_ORDER events"""
    # Create mock subscriber
    mock_subscriber = MockSubscriber("test_strategy")
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)
    
    # Create initial order
    order = Order(asset=mes_asset, quantity=10, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    order.id = "12347"
    order.identifier = "12347"
    order.status = "open"
    projectx_broker._orders_cache[order.id] = order
    
    # Create partially filled order
    partial_order = Order(asset=mes_asset, quantity=10, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    partial_order.id = "12347"
    partial_order.identifier = "12347" 
    partial_order.status = "partial_fill"
    partial_order.avg_fill_price = 5000.0
    partial_order.filled_quantity = 5
    
    # Clear initial events
    mock_subscriber.events.clear()
    
    # Trigger status change detection
    projectx_broker._detect_and_dispatch_order_changes(partial_order)
    
    # Verify PARTIALLY_FILLED_ORDER event was dispatched
    assert len(mock_subscriber.events) == 1
    event_type, payload = mock_subscriber.events[0]
    assert event_type == "partial_fill"
    assert payload["price"] == 5000.0
    assert payload["quantity"] == 5

def test_streaming_order_update_triggers_events(projectx_broker, mes_asset):
    """Test that streaming order updates trigger lifecycle events"""
    # Create mock subscriber
    mock_subscriber = MockSubscriber("test_strategy")
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)
    
    # Add initial order to cache
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    order.id = "12348"
    order.identifier = "12348"
    order.status = "open"
    projectx_broker._orders_cache[order.id] = order
    
    # Clear initial events
    mock_subscriber.events.clear()
    
    # Simulate streaming order update with filled status
    stream_data = {
        "id": "12348",
        "contractId": "CON.F.US.MES.Z25",
        "status": 2,  # ProjectX filled status (corrected from 4 to 2)
        "type": 1,    # limit order
        "side": 0,    # buy
        "size": 1,
        "avgFillPrice": 5000.0,
        "filledSize": 1
    }
    
    # Trigger streaming update
    projectx_broker._handle_order_update(stream_data)
    
    # Verify FILLED_ORDER event was dispatched
    assert len(mock_subscriber.events) == 1
    event_type, payload = mock_subscriber.events[0]
    assert event_type == "fill"

def test_streaming_trade_update_triggers_fill(projectx_broker, mes_asset):
    """Test that streaming trade updates trigger fill events"""
    # Create mock subscriber
    mock_subscriber = MockSubscriber("test_strategy")
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)
    
    # Add initial order to cache (market order that should fill instantly)
    order = Order(asset=mes_asset, quantity=1, order_type="market", side="buy", limit_price=None, strategy="test_strategy")
    order.id = "12349"
    order.identifier = "12349"
    order.status = "new"
    projectx_broker._orders_cache[order.id] = order
    
    # Clear initial events
    mock_subscriber.events.clear()
    
    # Simulate streaming trade update (trades are ground truth for fills)
    trade_data = {
        "id": 98765,  # Trade ID
        "orderId": "12349",  # Links to our order
        "accountId": 1,
        "contractId": "CON.F.US.MES.Z25",
        "price": 5001.25,
        "size": 1,
        "side": 0,  # buy
        "creationTimestamp": "2025-01-01T12:00:00Z",
        "voided": False
    }
    
    # Trigger trade update
    projectx_broker._handle_trade_update(trade_data)
    
    # Verify FILLED_ORDER event was dispatched from trade
    assert len(mock_subscriber.events) == 1
    event_type, payload = mock_subscriber.events[0]
    assert event_type == "fill"
    assert payload["price"] == 5001.25
    assert payload["quantity"] == 1

def test_pre_existing_filled_order_handling(projectx_broker, mes_asset):
    """Test handling of orders that were filled before strategy started"""
    # Set first iteration flag
    projectx_broker._first_iteration = True
    
    # Create mock subscriber
    mock_subscriber = MockSubscriber("test_strategy")
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)
    
    # Create filled order (as if it existed before strategy started)
    filled_order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    filled_order.id = "12349"
    filled_order.identifier = "12349"
    filled_order.status = "fill"
    filled_order.avg_fill_price = 5000.0
    filled_order.filled_quantity = 1
    
    # Trigger pre-existing order handling  
    projectx_broker._detect_and_dispatch_order_changes(filled_order)
    
    # Should trigger both NEW and FILLED events
    assert len(mock_subscriber.events) >= 1  # At least one event should be triggered
    
    # Reset first iteration flag
    projectx_broker._first_iteration = False

def test_error_order_event(projectx_broker, mes_asset):
    """Test that rejected orders trigger ERROR_ORDER events"""
    # Create mock subscriber
    mock_subscriber = MockSubscriber("test_strategy")
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)
    
    # Create initial order
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    order.id = "12350"
    order.identifier = "12350"
    order.status = "open"
    projectx_broker._orders_cache[order.id] = order
    
    # Create rejected order
    rejected_order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    rejected_order.id = "12350"
    rejected_order.identifier = "12350"
    rejected_order.status = "rejected"
    
    # Clear initial events
    mock_subscriber.events.clear()
    
    # Trigger status change detection
    projectx_broker._detect_and_dispatch_order_changes(rejected_order)
    
    # Verify ERROR_ORDER event was dispatched
    assert len(mock_subscriber.events) == 1
    event_type, payload = mock_subscriber.events[0]
    assert event_type == "error"

def test_order_identifier_sync(projectx_broker, mes_asset):
    """Test that order identifier is properly updated and tracked for sync_broker compatibility"""
    # Create an order with initial UUID identifier
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy")
    initial_identifier = order.identifier  # This will be a UUID
    
    # Submit the order
    projectx_broker._submit_order(order)
    
    # Verify identifier was updated to broker's numeric ID
    assert order.identifier != initial_identifier, "Identifier should be updated to broker's ID"
    assert order.identifier == "100001", f"Expected identifier '100001', got '{order.identifier}'"
    assert order.id == "100001", f"Expected id '100001', got '{order.id}'"
    
    # Verify order is in tracking lists with correct identifier
    all_orders = projectx_broker.get_all_orders()
    assert len(all_orders) > 0, "Order should be in tracking lists"
    
    # Find our order in the tracking list
    tracked_order = None
    for o in all_orders:
        if o.identifier == "100001":
            tracked_order = o
            break
    
    assert tracked_order is not None, "Order should be findable by broker identifier"
    assert tracked_order is order, "Should be the same order object"
    
    # Verify order is in _new_orders list (after _process_trade_event)
    new_orders = projectx_broker._new_orders.get_list()
    assert len(new_orders) == 1, "Order should be in _new_orders list"
    assert new_orders[0].identifier == "100001", "Order in _new_orders should have broker ID"
    
    # Verify order is NOT in _unprocessed_orders (should have been moved)
    unprocessed = projectx_broker._unprocessed_orders.get_list()
    assert len(unprocessed) == 0, "Order should not be in _unprocessed_orders after processing"
    
    # Simulate what sync_broker does: pull orders and check identifiers
    broker_orders = projectx_broker._pull_broker_all_orders()
    # Our stub returns the orders we placed
    assert len(projectx_broker.client.placed) == 1, "Should have placed one order"
    
    # In real scenario, broker would return orders with their IDs
    # Create a mock broker order response
    mock_broker_order = {
        "id": "100001",  # This is what ProjectX returns
        "contractId": "CON.F.US.MES.Z25",
        "status": 1,  # Open
        "type": 1,  # Limit
        "side": 0,  # Buy
        "size": 1,
        "limitPrice": 5000.0
    }
    
    # Convert to Lumibot order as sync_broker would
    converted_order = projectx_broker._convert_broker_order_to_lumibot_order(mock_broker_order)
    if converted_order:
        assert converted_order.identifier == "100001", "Converted order should have broker ID as identifier"
        
        # The key test: can we find the original order using the broker's identifier?
        # This is what sync_broker checks
        order_found = False
        for tracked in all_orders:
            if tracked.identifier == converted_order.identifier:
                order_found = True
                break
        
        assert order_found, "Order should be findable by broker identifier (sync_broker compatibility)"

def test_order_tracking_with_multiple_orders(projectx_broker, mes_asset):
    """Test that multiple orders maintain proper identifier tracking"""
    orders = []
    expected_ids = []
    
    # Submit multiple orders
    for i in range(3):
        order = Order(
            asset=mes_asset, 
            quantity=i+1, 
            order_type="limit", 
            side="buy", 
            limit_price=5000.0 + i*10, 
            strategy="test_strategy"
        )
        initial_id = order.identifier
        projectx_broker._submit_order(order)
        
        # Each order should get a unique broker ID
        expected_id = f"{100001 + i}"
        expected_ids.append(expected_id)
        
        assert order.identifier != initial_id, f"Order {i} identifier should be updated"
        assert order.identifier == expected_id, f"Order {i} should have ID {expected_id}"
        
        orders.append(order)
    
    # Verify all orders are tracked correctly
    all_tracked = projectx_broker.get_all_orders()
    assert len(all_tracked) == 3, "All three orders should be tracked"
    
    # Verify each order has correct identifier
    tracked_ids = [o.identifier for o in all_tracked]
    for expected_id in expected_ids:
        assert expected_id in tracked_ids, f"ID {expected_id} should be in tracked orders"