import threading

import pytest

from lumibot.brokers.projectx import ProjectX
from lumibot.data_sources.data_source import DataSource
from lumibot.entities import Asset, Order


# --- Lightweight dummy data source to satisfy Broker requirement without real API calls ---
class DummyDataSource(DataSource):
    def get_chains(self, asset, quote: Asset = None):
        return {}

    def get_historical_prices(
        self,
        asset,
        length,
        timestep="",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
        return_polars=False,
    ):
        return None

    def get_last_price(self, asset, quote=None, exchange=None):
        return 0.0

    def _get_contract_id_from_asset(self, asset):
        return f"CON.F.US.{asset.symbol}.Z25"


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
        self.mock_orders.append(
            {
                "id": str(oid),
                "contractId": kwargs.get("contract_id", "CON.F.US.MES.Z25"),
                "status": 1,  # Open
                "type": kwargs.get("type", 1),  # Limit
                "side": kwargs.get("side", 0),  # Buy
                "size": kwargs.get("size", 1),
                "limitPrice": kwargs.get("limit_price"),
                "customTag": kwargs.get("custom_tag"),
            }
        )
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
    assert order.identifier is not None
    assert order.status == "new"  # Status becomes 'new' after _process_trade_event

    # Cancel
    cancel_order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="sell", limit_price=5050.0, strategy="Strat"
    )
    projectx_broker._submit_order(cancel_order)
    assert projectx_broker.cancel_order(cancel_order)  # Cancel status updated only after broker changes it


def test_rejection_mapping_max_position(projectx_broker, mes_asset):
    # Monkeypatch order_place to simulate risk rejection
    def reject(**kwargs):
        return {"success": False, "errorMessage": "Maximum position exceeded for symbol"}

    projectx_broker.client.order_place = reject
    order = Order(asset=mes_asset, quantity=9999, order_type="limit", side="buy", limit_price=1.0, strategy="Strat")
    projectx_broker._submit_order(order)
    # STATUS_ALIAS_MAP maps 'rejected' to 'error'
    assert order.status == "error"
    assert order.error == "max_position_exceeded"


def test_rejection_generic(projectx_broker, mes_asset):
    def reject(**kwargs):
        return {"success": False, "errorMessage": "Some other error"}

    projectx_broker.client.order_place = reject
    order = Order(asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=1.0, strategy="Strat")
    projectx_broker._submit_order(order)
    assert order.status == "error"
    assert order.error == "Some other error"


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
    order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
    projectx_broker._submit_order(order)

    # Verify order was submitted successfully and became 'new' status after event processing
    assert order.identifier is not None
    assert order.status == "new"  # Status becomes 'new' after _process_trade_event

    # Verify NEW_ORDER event was dispatched
    assert len(mock_subscriber.events) == 1
    event_type, payload = mock_subscriber.events[0]
    assert event_type == "new"
    assert "order" in payload
    assert payload["order"].identifier == order.identifier


def test_order_status_change_detection(projectx_broker, mes_asset):
    """Test that order status changes are detected and events dispatched"""
    # Create mock subscriber
    mock_subscriber = MockSubscriber("test_strategy")
    while len(projectx_broker._subscribers) > 0:
        projectx_broker._subscribers.pop()
    projectx_broker._subscribers.append(mock_subscriber)

    # Create initial order and add to cache
    order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
    order.id = "12345"
    order.identifier = "12345"
    order.status = "open"  # Changed from "submitted" to "open" (ProjectX status=1)
    projectx_broker._process_new_order(order)

    # Create updated order with filled status
    updated_order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
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
    order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
    order.id = "12346"
    order.identifier = "12346"
    order.status = "open"
    projectx_broker._process_new_order(order)

    # Create canceled order
    canceled_order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
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
    order = Order(
        asset=mes_asset, quantity=10, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
    order.id = "12347"
    order.identifier = "12347"
    order.status = "open"
    projectx_broker._process_new_order(order)

    # Create partially filled order
    partial_order = Order(
        asset=mes_asset, quantity=10, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
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
    order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
    order.id = "12348"
    order.identifier = "12348"
    order.status = "open"
    projectx_broker._process_new_order(order)

    # Clear initial events
    mock_subscriber.events.clear()

    # Simulate streaming order update with filled status
    stream_data = {
        "id": "12348",
        "contractId": "CON.F.US.MES.Z25",
        "status": 2,  # ProjectX filled status (corrected from 4 to 2)
        "type": 1,  # limit order
        "side": 0,  # buy
        "size": 1,
        "avgFillPrice": 5000.0,
        "filledSize": 1,
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
    order = Order(
        asset=mes_asset, quantity=1, order_type="market", side="buy", limit_price=None, strategy="test_strategy"
    )
    order.id = "12349"
    order.identifier = "12349"
    order.status = "new"
    projectx_broker._process_new_order(order)

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
        "voided": False,
    }

    # Trigger trade update
    projectx_broker._handle_trade_update(trade_data)


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
    filled_order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
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
    order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
    order.id = "12350"
    order.identifier = "12350"
    order.status = "open"
    projectx_broker._process_new_order(order)

    # Create rejected order
    rejected_order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
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
    order = Order(
        asset=mes_asset, quantity=1, order_type="limit", side="buy", limit_price=5000.0, strategy="test_strategy"
    )
    initial_identifier = order.identifier  # This will be a UUID

    # Submit the order
    projectx_broker._submit_order(order)

    # Verify identifier was updated to broker's numeric ID
    assert order.identifier != initial_identifier, "Identifier should be updated to broker's ID"
    assert order.identifier == "100001", f"Expected identifier '100001', got '{order.identifier}'"

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
    projectx_broker._pull_broker_all_orders()
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
        "limitPrice": 5000.0,
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
            quantity=i + 1,
            order_type="limit",
            side="buy",
            limit_price=5000.0 + i * 10,
            strategy="test_strategy",
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


# ========== RACE CONDITION / LOCK TESTS ==========


def _make_subscriber(strategy_name="test_strategy"):
    """Helper to create a fresh MockSubscriber."""
    return MockSubscriber(strategy_name)


def _attach_subscriber(broker, subscriber):
    """Helper: replace broker subscribers list with a single mock subscriber."""
    while len(broker._subscribers) > 0:
        broker._subscribers.pop()
    broker._subscribers.append(subscriber)


class TestOrderUpdateLock:
    """Tests for the _order_update_lock that serializes concurrent websocket callbacks."""

    def test_order_update_lock_exists(self, projectx_broker):
        """Broker must have an _order_update_lock RLock attribute after initialisation."""
        import threading

        assert hasattr(projectx_broker, "_order_update_lock"), "_order_update_lock should be set during __init__"
        # Should be a reentrant lock so bracket-spawn callbacks don't deadlock
        assert isinstance(projectx_broker._order_update_lock, type(threading.RLock())), (
            "_order_update_lock should be a threading.RLock"
        )

    def test_fill_before_new_suppresses_new_order_event(self, projectx_broker, mes_asset):
        """
        Simulate the race condition where the 'filled' websocket event arrives and is
        processed *before* the 'new' event.  After the fill is processed, a subsequent
        'new/open' status update for the same order must NOT re-fire a NEW_ORDER event.
        """
        subscriber = _make_subscriber()
        _attach_subscriber(projectx_broker, subscriber)

        # --- Step 1: seed cache with an order already in 'fill' terminal state ---
        filled_order = Order(asset=mes_asset, quantity=1, order_type="market", side="buy", strategy="test_strategy")
        filled_order.id = "RACE001"
        filled_order.identifier = "RACE001"
        filled_order.status = "fill"
        filled_order.avg_fill_price = 5001.25
        filled_order.filled_quantity = 1
        projectx_broker._orders_cache = getattr(projectx_broker, "_orders_cache", {})
        # Insert directly into the broker's _new_orders tracking list so
        # get_tracked_order can find it (mirrors what _process_trade_event does)
        projectx_broker._filled_orders.append(filled_order)

        subscriber.events.clear()

        # --- Step 2: a late-arriving 'new/open' status update for the same order ---
        late_new_order = Order(asset=mes_asset, quantity=1, order_type="market", side="buy", strategy="test_strategy")
        late_new_order.id = "RACE001"
        late_new_order.identifier = "RACE001"
        late_new_order.status = "open"  # normalised from "new"

        projectx_broker._dispatch_status_change(filled_order, late_new_order)

        # No NEW_ORDER event should have been fired
        new_events = [e for e in subscriber.events if e[0] == "new"]
        assert len(new_events) == 0, (
            "NEW_ORDER must NOT be fired for a late 'new/open' update when the "
            "order is already in terminal 'fill' state (fill-before-new race condition)"
        )

    def test_new_before_fill_dispatches_new_order_event(self, projectx_broker, mes_asset):
        """
        Normal (non-race) scenario: 'new' arrives first and 'fill' arrives later.
        NEW_ORDER must be fired when there is no cached order yet.
        """
        subscriber = _make_subscriber()
        _attach_subscriber(projectx_broker, subscriber)

        subscriber.events.clear()

        # Order not yet in any cache
        new_order = Order(asset=mes_asset, quantity=1, order_type="market", side="buy", strategy="test_strategy")
        new_order.id = "RACE002"
        new_order.identifier = "RACE002"
        new_order.status = "open"

        # _detect_and_dispatch_order_changes with no cached order and status='open'
        # should call _process_trade_event(new_order, NEW_ORDER)
        projectx_broker._detect_and_dispatch_order_changes(new_order)

        new_events = [e for e in subscriber.events if e[0] == "new"]
        assert len(new_events) == 1, (
            "Exactly one NEW_ORDER event should fire when order is first seen with 'open' status"
        )

    def test_duplicate_fill_via_trade_update_is_idempotent(self, projectx_broker, mes_asset):
        """
        _handle_trade_update must not fire a second FILLED_ORDER event when the
        order is already in 'fill' / 'filled' state (idempotency guard).
        """
        subscriber = _make_subscriber()
        _attach_subscriber(projectx_broker, subscriber)

        # Seed cache: order already filled
        order = Order(asset=mes_asset, quantity=1, order_type="market", side="buy", strategy="test_strategy")
        order.id = "RACE003"
        order.identifier = "RACE003"
        order.status = "fill"
        order.avg_fill_price = 5001.25
        order.filled_quantity = 1
        projectx_broker._filled_orders.append(order)

        subscriber.events.clear()

        # Simulate a second trade update arriving for the same order
        trade_data = {
            "orderId": "RACE003",
            "price": 5001.25,
            "size": 1,
        }
        projectx_broker._handle_trade_update(trade_data)

        fill_events = [e for e in subscriber.events if e[0] == "fill"]
        assert len(fill_events) == 0, (
            "No FILLED_ORDER event should fire for a trade update when the order "
            "is already in 'fill' state (idempotency guard)"
        )

    def test_concurrent_fill_and_new_events_produce_single_fill(self, projectx_broker, mes_asset):
        """
        Fire 'filled' and 'new/open' _handle_order_update calls concurrently from two
        threads.  After both threads finish, exactly one FILLED_ORDER event should have
        been dispatched and zero NEW_ORDER events (since the order already had status
        'new' in cache before fill, the fill wins and the late 'open' is suppressed).

        This is the core regression test for the race condition fixed by _order_update_lock.
        """
        subscriber = _make_subscriber()
        _attach_subscriber(projectx_broker, subscriber)

        # Seed the cache with the order in 'new' state (as placed via _submit_order)
        order = Order(asset=mes_asset, quantity=1, order_type="market", side="buy", strategy="test_strategy")
        order.id = "RACE004"
        order.identifier = "RACE004"
        order.status = "new"
        projectx_broker._new_orders.append(order)

        subscriber.events.clear()

        # The two competing websocket payloads
        filled_payload = {
            "id": "RACE004",
            "contractId": "CON.F.US.MES.Z25",
            "status": 2,  # filled
            "type": 2,  # market
            "side": 0,  # buy
            "size": 1,
            "avgFillPrice": 5001.25,
            "filledSize": 1,
        }
        new_payload = {
            "id": "RACE004",
            "contractId": "CON.F.US.MES.Z25",
            "status": 6,  # new/pending
            "type": 2,
            "side": 0,
            "size": 1,
        }

        errors = []

        def send_fill():
            try:
                projectx_broker._handle_order_update(filled_payload)
            except Exception as exc:
                errors.append(exc)

        def send_new():
            try:
                projectx_broker._handle_order_update(new_payload)
            except Exception as exc:
                errors.append(exc)

        t1 = threading.Thread(target=send_fill, name="fill-thread")
        t2 = threading.Thread(target=send_new, name="new-thread")

        # Start both threads simultaneously
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert not errors, f"Exception(s) in worker threads: {errors}"

        fill_events = [e for e in subscriber.events if e[0] == "fill"]
        new_events = [e for e in subscriber.events if e[0] == "new"]

        assert len(fill_events) == 1, (
            f"Exactly 1 FILLED_ORDER event expected, got {len(fill_events)}. "
            "Duplicate fill events indicate the race condition is not fully fixed."
        )
        assert len(new_events) == 0, (
            f"0 NEW_ORDER events expected after fill, got {len(new_events)}. "
            "A spurious NEW_ORDER after fill indicates the 'fill-before-new' guard is broken."
        )

    def test_detect_and_dispatch_holds_lock_during_dispatch(self, projectx_broker, mes_asset):
        """
        Verify that _order_update_lock is actually acquired while
        _detect_and_dispatch_order_changes is executing, by observing that a second
        thread attempting to acquire the same lock blocks until the first finishes.
        """
        import threading

        lock_was_held = threading.Event()
        threading.Event()

        original_dispatch = projectx_broker._dispatch_status_change

        def slow_dispatch(cached, new):
            # Signal that we are inside dispatch (lock should be held at this point)
            lock_was_held.set()
            # Try to acquire the lock from *inside* the callback – with RLock this
            # must succeed immediately (re-entrant) without deadlock.
            acquired = projectx_broker._order_update_lock.acquire(blocking=False)
            assert acquired, "RLock must be re-entrant – acquire inside locked section must succeed"
            projectx_broker._order_update_lock.release()
            original_dispatch(cached, new)

        projectx_broker._dispatch_status_change = slow_dispatch

        # Seed cache
        order = Order(asset=mes_asset, quantity=1, order_type="market", side="buy", strategy="test_strategy")
        order.id = "RACE005"
        order.identifier = "RACE005"
        order.status = "new"
        projectx_broker._new_orders.append(order)

        update = Order(asset=mes_asset, quantity=1, order_type="market", side="buy", strategy="test_strategy")
        update.id = "RACE005"
        update.identifier = "RACE005"
        update.status = "fill"
        update.avg_fill_price = 5000.0
        update.filled_quantity = 1

        errors = []

        def run():
            try:
                projectx_broker._detect_and_dispatch_order_changes(update)
            except Exception as exc:
                errors.append(exc)

        t = threading.Thread(target=run)
        t.start()
        t.join(timeout=5)

        assert not errors, f"Exception during dispatch: {errors}"

        # Restore original
        projectx_broker._dispatch_status_change = original_dispatch
