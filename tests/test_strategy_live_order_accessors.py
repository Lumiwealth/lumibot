from datetime import datetime
from threading import Event, RLock, Thread

import pytest

from lumibot.brokers import Broker
from lumibot.entities import Asset, Order, Position
from lumibot.strategies.strategy import Strategy
from lumibot.trading_builtins import SafeList


class _MockDataSource:
    def get_datetime(self, adjust_for_delay=False):
        return None

    def get_last_price(self, asset, quote=None, exchange=None):
        return 100.0

    def get_last_prices(self, assets, quote=None, exchange=None):
        return {}

    def get_yesterday_dividends(self, assets, quote=None):
        return None


class _LiveBroker(Broker):
    IS_BACKTESTING_BROKER = False

    def __init__(self):
        super().__init__(name="LiveBroker", connect_stream=False, data_source=_MockDataSource())
        self.market = "NYSE"
        self._lock = RLock()
        self._unprocessed_orders = SafeList(self._lock)
        self._placeholder_orders = SafeList(self._lock)
        self._new_orders = SafeList(self._lock)
        self._canceled_orders = SafeList(self._lock)
        self._partially_filled_orders = SafeList(self._lock)
        self._filled_orders = SafeList(self._lock)
        self._error_orders = SafeList(self._lock)
        self._filled_positions = SafeList(self._lock)
        self._subscribers = SafeList(self._lock)
        self._first_iteration = False
        self.broker_orders = []
        self.direct_orders = {}
        self.broker_positions = []
        self.order_pull_count = 0
        self.position_pull_count = 0
        self.balance_pull_count = 0
        self.broker_balances = (100000.0, {}, 100000.0)

    def is_market_open(self):
        return True

    def should_continue(self):
        return False

    def cancel_order(self, order):
        return None

    def _modify_order(self, order, limit_price=None, stop_price=None):
        return None

    def _submit_order(self, order):
        return order

    def _get_balances_at_broker(self, quote_asset, strategy):
        self.balance_pull_count += 1
        return self.broker_balances

    def get_historical_account_value(self):
        return {}

    def _get_stream_object(self):
        return None

    def _register_stream_events(self):
        return None

    def _run_stream(self):
        return None

    def _pull_positions(self, strategy):
        self.position_pull_count += 1
        return self.broker_positions

    def _pull_position(self, strategy, asset):
        return next((position for position in self.broker_positions if position.asset == asset), None)

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        return response

    def _pull_broker_order(self, identifier):
        if identifier in self.direct_orders:
            return self.direct_orders[identifier]
        return next((order for order in self.broker_orders if order.identifier == identifier), None)

    def _pull_broker_all_orders(self):
        self.order_pull_count += 1
        return self.broker_orders


class _AccessorStrategy(Strategy):
    def initialize(self, parameters=None):
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        return None


def _strategy():
    broker = _LiveBroker()
    strategy = _AccessorStrategy(broker=broker, budget=100_000.0, analyze_backtest=False, parameters={})
    strategy._first_iteration = False
    broker.order_pull_count = 0
    broker.position_pull_count = 0
    return strategy, broker


def test_live_strategy_synchronizes_broker_state_on_start_by_default():
    broker = _LiveBroker()

    _AccessorStrategy(broker=broker, budget=100_000.0, analyze_backtest=False, parameters={})

    # Default remains safe for trading strategies: balances and positions are ready at startup.
    assert broker.balance_pull_count == 1
    assert broker.position_pull_count == 1


def test_read_only_live_strategy_can_defer_startup_sync_until_requested():
    broker = _LiveBroker()
    strategy = _AccessorStrategy(
        broker=broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={},
        synchronize_broker_on_start=False,
    )

    # Read-only one-shot clients must avoid unrelated network calls at construction.
    assert broker.balance_pull_count == 0
    assert broker.position_pull_count == 0

    strategy.get_positions()

    # Requested data remains fresh and incurs exactly one provider refresh.
    assert broker.balance_pull_count == 0
    assert broker.position_pull_count == 1


def _order(strategy_name, identifier, status, symbol="SPY", order_type=Order.OrderType.LIMIT):
    return Order(
        strategy=strategy_name,
        asset=Asset(symbol, asset_type=Asset.AssetType.STOCK),
        quantity=1,
        side=Order.OrderSide.BUY,
        order_type=order_type,
        identifier=identifier,
        status=status,
    )


def _age_past_market_order_grace(order):
    order.created_at = datetime(2000, 1, 1)
    return order


def test_get_orders_filters_by_order_status_enum_and_identifiers():
    strategy, broker = _strategy()
    broker._new_orders.append(_order(strategy.name, "open-1", Order.OrderStatus.OPEN))
    broker._filled_orders.append(_order(strategy.name, "filled-1", Order.OrderStatus.FILLED))
    broker._canceled_orders.append(_order(strategy.name, "canceled-1", Order.OrderStatus.CANCELED))

    active_orders = strategy.get_orders(
        identifiers=["open-1", "filled-1"],
        statuses=Order.ACTIVE_STATUSES,
        broker_refresh=False,
    )

    assert [order.identifier for order in active_orders] == ["open-1"]


def test_get_orders_active_statuses_include_cancel_pending_orders():
    strategy, broker = _strategy()
    cancel_pending = _order(strategy.name, "cancel-pending-1", Order.OrderStatus.CANCELLING)
    broker._new_orders.append(cancel_pending)

    active_orders = strategy.get_orders(statuses=Order.ACTIVE_STATUSES, broker_refresh=False)

    assert [order.identifier for order in active_orders] == ["cancel-pending-1"]
    assert cancel_pending.is_active() is True


def test_get_orders_accepts_single_enum_status():
    strategy, broker = _strategy()
    broker._new_orders.append(_order(strategy.name, "open-1", Order.OrderStatus.OPEN))
    broker._filled_orders.append(_order(strategy.name, "filled-1", Order.OrderStatus.FILLED))

    open_orders = strategy.get_orders(statuses=Order.OrderStatus.OPEN, broker_refresh=False)

    assert [order.identifier for order in open_orders] == ["open-1"]


def test_get_orders_rejects_raw_string_statuses():
    strategy, _broker = _strategy()

    with pytest.raises(TypeError, match="Order.OrderStatus"):
        strategy.get_orders(statuses="open", broker_refresh=False)

    with pytest.raises(TypeError, match="Order.OrderStatus"):
        strategy.get_orders(statuses=["open"], broker_refresh=False)


def test_partially_filled_alias_maps_to_partial_fill_status():
    order = _order("unit-test", "partial-1", "partially_filled")

    assert order.status == Order.OrderStatus.PARTIALLY_FILLED


def test_live_get_orders_refreshes_every_call_by_default_and_ignores_stale_terminal_history():
    strategy, broker = _strategy()
    broker.broker_orders = [
        _order(strategy.name, "old-filled", Order.OrderStatus.FILLED),
        _order(strategy.name, "live-open", Order.OrderStatus.OPEN),
    ]

    first = strategy.get_orders(statuses=Order.ACTIVE_STATUSES)
    second = strategy.get_orders(statuses=Order.ACTIVE_STATUSES)

    assert [order.identifier for order in first] == ["live-open"]
    assert [order.identifier for order in second] == ["live-open"]
    assert broker.order_pull_count == 2


def test_live_get_orders_can_still_use_explicit_ttl():
    strategy, broker = _strategy()
    broker.broker_orders = [_order(strategy.name, "live-open", Order.OrderStatus.OPEN)]

    strategy.get_orders(statuses=Order.ACTIVE_STATUSES, broker_refresh_ttl_seconds=1.0)
    strategy.get_orders(statuses=Order.ACTIVE_STATUSES, broker_refresh_ttl_seconds=1.0)

    assert broker.order_pull_count == 1


def test_live_get_order_refreshes_existing_order_status():
    strategy, broker = _strategy()
    tracked = _order(strategy.name, "order-1", Order.OrderStatus.OPEN)
    broker._new_orders.append(tracked)
    broker.broker_orders = [_order(strategy.name, "order-1", Order.OrderStatus.FILLED)]

    refreshed = strategy.get_order("order-1")

    assert refreshed is tracked
    assert refreshed.status == Order.OrderStatus.FILLED
    assert strategy.get_orders(statuses=Order.ACTIVE_STATUSES) == []


def test_live_get_order_survives_submit_callback_duplicate_then_terminal_sync():
    """A fast broker callback must not make a just-submitted identifier disappear.

    The submit response and the broker callback can race, briefly leaving the same
    identifier in the unprocessed and new buckets.  A following broker refresh may
    already report the order as filled.  Reconciliation must collapse the duplicate
    into one terminal record instead of deleting both local copies.
    """
    strategy, broker = _strategy()
    submitted = _order(strategy.name, "fast-fill-1", Order.OrderStatus.SUBMITTED)
    callback_copy = _order(strategy.name, "fast-fill-1", Order.OrderStatus.OPEN)
    broker._unprocessed_orders.append(submitted)
    broker._new_orders.append(callback_copy)
    broker.broker_orders = [
        _order(strategy.name, "fast-fill-1", Order.OrderStatus.FILLED),
    ]

    refreshed = strategy.get_order("fast-fill-1")

    assert refreshed is not None
    assert refreshed.identifier == "fast-fill-1"
    assert refreshed.status == Order.OrderStatus.FILLED
    assert [order.identifier for order in broker.get_all_orders()] == ["fast-fill-1"]


def test_fresh_process_imports_terminal_broker_order_for_durable_reconciliation():
    """A later scheduled process must recover an order that filled after prior exit."""
    strategy, broker = _strategy()
    broker._first_iteration = True
    broker.broker_orders = [
        _order(strategy.name, "filled-after-exit-1", Order.OrderStatus.FILLED),
    ]

    broker.sync_orders(strategy)

    recovered = broker.get_tracked_order("filled-after-exit-1")
    assert recovered is not None
    assert recovered.status == Order.OrderStatus.FILLED
    assert broker._filled_orders.get_list() == [recovered]


@pytest.mark.parametrize(
    ("broker_status", "expected_bucket"),
    [
        (Order.OrderStatus.OPEN, "_new_orders"),
        (Order.OrderStatus.PARTIALLY_FILLED, "_partially_filled_orders"),
        (Order.OrderStatus.FILLED, "_filled_orders"),
        (Order.OrderStatus.CASH_SETTLED, "_filled_orders"),
        (Order.OrderStatus.CANCELED, "_canceled_orders"),
        (Order.OrderStatus.EXPIRED, "_canceled_orders"),
        (Order.OrderStatus.ERROR, "_error_orders"),
    ],
)
def test_clean_order_trackers_collapses_duplicates_without_losing_lifecycle_or_provenance(
    broker_status,
    expected_bucket,
):
    strategy, broker = _strategy()
    submitted = _order(strategy.name, "duplicate-1", Order.OrderStatus.SUBMITTED)
    submitted.decision_provenance = {"agent": "trader", "cycle": 7}
    callback_copy = _order(strategy.name, "duplicate-1", Order.OrderStatus.OPEN)
    authoritative = _order(strategy.name, "duplicate-1", broker_status)
    authoritative.limit_price = 99.25
    broker._unprocessed_orders.append(submitted)
    broker._new_orders.append(callback_copy)

    survivor = broker._clean_order_trackers(authoritative)

    assert survivor is submitted
    assert survivor.status == broker_status
    assert survivor.limit_price == 99.25
    assert survivor.decision_provenance == {"agent": "trader", "cycle": 7}
    assert getattr(broker, expected_bucket).get_list() == [submitted]
    assert [order.identifier for order in broker.get_all_orders()] == ["duplicate-1"]


def test_tracker_transition_is_atomic_for_concurrent_identifier_lookup(monkeypatch):
    strategy, broker = _strategy()
    submitted = _order(strategy.name, "atomic-1", Order.OrderStatus.SUBMITTED)
    callback_copy = _order(strategy.name, "atomic-1", Order.OrderStatus.OPEN)
    authoritative = _order(strategy.name, "atomic-1", Order.OrderStatus.FILLED)
    broker._unprocessed_orders.append(submitted)
    broker._new_orders.append(callback_copy)

    append_entered = Event()
    allow_append = Event()
    original_append = broker._filled_orders.append

    def paused_append(order):
        append_entered.set()
        assert allow_append.wait(timeout=2)
        original_append(order)

    monkeypatch.setattr(broker._filled_orders, "append", paused_append)
    cleanup = Thread(target=broker._clean_order_trackers, args=(authoritative,))
    cleanup.start()
    assert append_entered.wait(timeout=2)

    observed = []
    lookup_done = Event()

    def lookup():
        observed.append(broker.get_tracked_order("atomic-1"))
        lookup_done.set()

    reader = Thread(target=lookup)
    reader.start()
    assert lookup_done.wait(timeout=0.05) is False
    allow_append.set()
    cleanup.join(timeout=2)
    reader.join(timeout=2)

    assert cleanup.is_alive() is False
    assert reader.is_alive() is False
    assert observed == [submitted]
    assert submitted.status == Order.OrderStatus.FILLED


def test_live_order_list_miss_uses_direct_lookup_before_terminal_update():
    strategy, broker = _strategy()
    tracked = _order(strategy.name, "order-1", Order.OrderStatus.OPEN)
    broker._new_orders.append(tracked)
    broker.broker_orders = [_order(strategy.name, "other-order", Order.OrderStatus.OPEN)]
    broker.direct_orders["order-1"] = _order(strategy.name, "order-1", Order.OrderStatus.CANCELED)

    refreshed = strategy.get_order("order-1")

    assert refreshed is tracked
    assert refreshed.status == Order.OrderStatus.CANCELED


def test_live_order_list_miss_without_direct_match_does_not_fake_cancel():
    strategy, broker = _strategy()
    tracked = _order(strategy.name, "order-1", Order.OrderStatus.OPEN)
    broker._new_orders.append(tracked)
    broker.broker_orders = [_order(strategy.name, "other-order", Order.OrderStatus.OPEN)]

    refreshed = strategy.get_order("order-1")

    assert refreshed is tracked
    assert refreshed.status == Order.OrderStatus.OPEN


def test_live_market_order_list_miss_uses_direct_lookup_before_terminal_update():
    strategy, broker = _strategy()
    tracked = _age_past_market_order_grace(
        _order(
            strategy.name,
            "market-1",
            Order.OrderStatus.OPEN,
            order_type=Order.OrderType.MARKET,
        )
    )
    broker._new_orders.append(tracked)
    broker.broker_orders = [_order(strategy.name, "other-order", Order.OrderStatus.OPEN)]
    broker.direct_orders["market-1"] = _order(strategy.name, "market-1", Order.OrderStatus.FILLED)

    active_orders = strategy.get_orders(statuses=Order.ACTIVE_STATUSES)

    assert "market-1" not in {order.identifier for order in active_orders}
    assert tracked.status == Order.OrderStatus.FILLED


def test_live_market_order_list_miss_without_direct_match_becomes_non_active_unknown():
    strategy, broker = _strategy()
    tracked = _age_past_market_order_grace(
        _order(
            strategy.name,
            "market-1",
            Order.OrderStatus.OPEN,
            order_type=Order.OrderType.MARKET,
        )
    )
    broker._new_orders.append(tracked)
    broker.broker_orders = [_order(strategy.name, "other-order", Order.OrderStatus.OPEN)]

    active_orders = strategy.get_orders(statuses=Order.ACTIVE_STATUSES)

    assert "market-1" not in {order.identifier for order in active_orders}
    assert tracked.status == Order.OrderStatus.UNKNOWN


def test_live_market_order_list_miss_with_empty_broad_list_is_reconciled():
    strategy, broker = _strategy()
    tracked = _age_past_market_order_grace(
        _order(
            strategy.name,
            "market-1",
            Order.OrderStatus.OPEN,
            order_type=Order.OrderType.MARKET,
        )
    )
    broker._new_orders.append(tracked)
    broker.broker_orders = []

    active_orders = strategy.get_orders(statuses=Order.ACTIVE_STATUSES)

    assert active_orders == []
    assert tracked.status == Order.OrderStatus.UNKNOWN


def test_live_get_positions_refreshes_every_call_by_default():
    strategy, broker = _strategy()
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    broker.broker_positions = [Position(strategy.name, asset, 3)]

    position = strategy.get_position(asset)
    positions = strategy.get_positions()

    assert position.quantity == 3
    assert [item.asset for item in positions] == [asset]
    assert broker.position_pull_count == 2


def test_live_get_positions_can_still_use_explicit_ttl():
    strategy, broker = _strategy()
    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    broker.broker_positions = [Position(strategy.name, asset, 3)]

    strategy.get_position(asset, broker_refresh_ttl_seconds=1.0)
    strategy.get_positions(broker_refresh_ttl_seconds=1.0)

    assert broker.position_pull_count == 1


def test_get_cash_and_portfolio_value_force_fresh_balance_reads():
    strategy, broker = _strategy()
    broker.broker_balances = (1234.0, 100.0, 1334.0)

    broker.balance_pull_count = 0
    assert strategy.get_cash() == 1234.0
    after_cash = broker.balance_pull_count
    assert strategy.get_portfolio_value() == 1334.0
    assert after_cash >= 1
    assert broker.balance_pull_count > after_cash


def test_get_cash_and_portfolio_value_return_none_without_overwriting_cached_values():
    strategy, broker = _strategy()
    broker.broker_balances = (5000.0, 100.0, 5100.0)
    assert strategy.get_cash() == 5000.0
    assert strategy.get_portfolio_value() == 5100.0

    broker.broker_balances = None
    assert strategy.get_cash() is None
    assert strategy.get_portfolio_value() is None
    assert strategy.cash == 5000.0
    assert strategy.portfolio_value == 5100.0
