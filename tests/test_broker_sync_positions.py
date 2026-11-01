from lumibot.brokers.broker import Broker
from lumibot.entities import Asset, Position


class _BrokerForSyncTest(Broker):
    def __init__(self, broker_positions):
        super().__init__(name="sync-test", data_source=object())
        self._broker_positions = broker_positions

    def cancel_order(self, order):
        pass

    def _modify_order(self, order, limit_price=None, stop_price=None):
        pass

    def _submit_order(self, order):
        return order

    def _get_balances_at_broker(self, quote_asset, strategy):
        return (1000.0, 0.0, 1000.0)

    def get_historical_account_value(self):
        return {}

    def _get_stream_object(self):
        return None

    def _register_stream_events(self):
        pass

    def _run_stream(self):
        pass

    def _pull_positions(self, strategy):
        return self._broker_positions

    def _pull_position(self, strategy, asset):
        return None

    def _pull_broker_order(self, identifier):
        return None

    def _pull_broker_all_orders(self):
        return []

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        return None


class _Strategy:
    name = "strategy"


def _stock_position(quantity, current_price, market_value, pnl):
    asset = Asset("NVDA")
    position = Position("strategy", asset, quantity, avg_fill_price=219.78)
    position.current_price = current_price
    position.market_value = market_value
    position.pnl = pnl
    return position


def test_sync_positions_refreshes_broker_mark_fields_for_existing_position():
    existing = _stock_position(quantity=744, current_price=219.6201, market_value=439.2402, pnl=-0.3198)
    broker_fresh = _stock_position(quantity=744, current_price=219.6201, market_value=163397.3544, pnl=-118.9656)
    broker = _BrokerForSyncTest([broker_fresh])
    broker._filled_positions.append(existing)
    revision_before = broker._filled_positions.revision

    broker.sync_positions(_Strategy())

    synced = broker._filled_positions.get_list()[0]
    assert synced.quantity == 744
    assert synced.current_price == 219.6201
    assert synced.market_value == 163397.3544
    assert synced.pnl == -118.9656
    assert broker._filled_positions.revision > revision_before


def test_sync_positions_clears_stale_mark_fields_missing_from_broker_position():
    existing = _stock_position(quantity=10, current_price=150.0, market_value=1500.0, pnl=25.0)
    broker_fresh = Position("strategy", Asset("NVDA"), 10, avg_fill_price=149.0)
    broker = _BrokerForSyncTest([broker_fresh])
    broker._filled_positions.append(existing)

    broker.sync_positions(_Strategy())

    synced = broker._filled_positions.get_list()[0]
    assert synced.quantity == 10
    assert not hasattr(synced, "current_price")
    assert not hasattr(synced, "market_value")
    assert not hasattr(synced, "pnl")


def test_sync_positions_does_not_prune_position_added_after_snapshot_started():
    broker = _BrokerForSyncTest([])
    new_fill = _stock_position(quantity=1, current_price=220.0, market_value=220.0, pnl=0.0)

    def pull_positions_with_interleaved_fill(strategy):
        broker._filled_positions.append(new_fill)
        return []

    broker._pull_positions = pull_positions_with_interleaved_fill

    broker.sync_positions(_Strategy())

    assert broker._filled_positions.get_list() == [new_fill]


def test_sync_positions_preserves_fields_on_position_added_during_read():
    """A streamed fill takes precedence over a same-asset in-flight snapshot."""
    broker = _BrokerForSyncTest([])
    new_fill = _stock_position(quantity=2, current_price=220.0, market_value=440.0, pnl=1.0)
    new_fill.strategy = "fill-owner"
    stale_snapshot = _stock_position(quantity=1, current_price=210.0, market_value=210.0, pnl=-10.0)

    def pull_positions_with_interleaved_fill(strategy):
        broker._filled_positions.append(new_fill)
        return [stale_snapshot]

    broker._pull_positions = pull_positions_with_interleaved_fill
    broker.sync_positions(_Strategy())

    assert broker._filled_positions.get_list() == [new_fill]
    assert new_fill.quantity == 2
    assert new_fill.current_price == 220.0
    assert new_fill.market_value == 440.0
    assert new_fill.pnl == 1.0
    assert new_fill.strategy == "fill-owner"

    # The next fresh snapshot may reconcile that now-existing position normally.
    broker._pull_positions = lambda strategy: [stale_snapshot]
    broker.sync_positions(_Strategy())
    assert new_fill.quantity == 1
    assert new_fill.current_price == 210.0
    assert new_fill.strategy == _Strategy.name


from decimal import Decimal
from lumibot.entities import Order
import pytest


@pytest.mark.parametrize("filled", [Decimal("2"), Decimal("0.125")])
def test_partial_order_hydration_uses_observed_execution_quantity(filled):
    broker = _BrokerForSyncTest([])
    cash = Position("strategy", Asset("USD", asset_type="forex"), 1000)
    broker._filled_positions.append(cash)
    order = Order("strategy", Asset("SYN", asset_type="crypto"), 10, "buy",
                  quote=Asset("USD", asset_type="forex"), avg_fill_price=100)
    order.status = "partially_filled"
    order.filled_quantity = filled
    broker._process_broker_synced_order(order)
    assert sum(t.quantity for t in order.transactions) == filled
    assert cash.quantity == 1000


def test_partial_order_without_execution_quantity_does_not_invent_fill():
    broker = _BrokerForSyncTest([])
    order = Order("strategy", Asset("SYN"), 10, "buy", avg_fill_price=100)
    order.status = "partially_filled"
    broker._process_broker_synced_order(order)
    assert order.status == "partial_fill"
    assert order.transactions == []


@pytest.mark.parametrize("raw", ["pending_cancel", "pending_replace", "PendingCancel"])
def test_pending_cancellation_or_replacement_remains_active(raw):
    order = Order("strategy", Asset("SYN"), 10, "buy")
    order.status = raw
    assert order.is_active()
    assert not order.is_canceled()


def test_partial_order_hydration_is_cumulative_and_duplicate_safe():
    broker = _BrokerForSyncTest([])
    cash = Position("strategy", Asset("USD", asset_type="forex"), 1000)
    broker._filled_positions.append(cash)
    order = Order("strategy", Asset("SYN", asset_type="crypto"), 10, "buy",
                  quote=Asset("USD", asset_type="forex"), avg_fill_price=100)
    for filled, average in [(2, 100), (5, 102), (5, 102), (2, 100)]:
        order.status = "partial_fill"
        order.filled_quantity = filled
        order.avg_fill_price = average
        broker._process_broker_synced_order(order)
    assert sum(t.quantity for t in order.transactions) == 5
    assert cash.quantity == 690

@pytest.mark.parametrize('raw,canonical,active', [
    ('filled', 'fill', False), ('partially_filled', 'partial_fill', True),
    ('cancelled', 'canceled', False), ('canceled', 'canceled', False),
    ('rejected', 'error', False), ('expired', 'expired', False),
    ('pending_cancel', 'cancelling', True), ('pending_replace', 'pending_replace', True),
    ('done_for_day', 'done_for_day', True), ('suspended', 'suspended', True),
    ('stopped', 'stopped', True), ('replaced', 'replaced', False),
    ('assignment', 'assigned', False), ('exercise', 'exercised', False),
])
def test_broker_lifecycle_normalization_preserves_execution_state(raw, canonical, active):
    order = Order('strategy', Asset('SYN'), 10, 'buy')
    order.status = raw
    assert order.status == canonical
    assert order.is_active() == active

@pytest.mark.parametrize('terminal', ['canceled', 'replaced', 'expired', 'error'])
def test_terminal_hydration_retains_executions_without_replaying_cash(terminal):
    broker = _BrokerForSyncTest([])
    cash = Position('strategy', Asset('USD', asset_type='forex'), 1000)
    broker._filled_positions.append(cash)
    order = Order('strategy', Asset('SYN', asset_type='crypto'), 10, 'buy',
                  quote=Asset('USD', asset_type='forex'), avg_fill_price=100)
    order.status = terminal
    order.filled_quantity = Decimal('2')
    broker._process_broker_synced_order(order)
    assert order.status == terminal
    assert sum(t.quantity for t in order.transactions) == 2
    assert cash.quantity == 1000
    broker._process_broker_synced_order(order)
    assert sum(t.quantity for t in order.transactions) == 2
    assert cash.quantity == 1000


def test_reparsed_duplicate_observations_share_one_execution_ledger():
    broker = _BrokerForSyncTest([])
    cash = Position('strategy', Asset('USD', asset_type='forex'), 1000)
    broker._filled_positions.append(cash)
    for quantity in (2, 5, 5, 2):
        order = Order('strategy', Asset('SYN', asset_type='crypto'), 10, 'buy',
                      identifier='synthetic-same-order', quote=Asset('USD', asset_type='forex'), avg_fill_price=100)
        order.status = 'partial_fill'
        order.filled_quantity = quantity
        broker._process_broker_synced_order(order)
    tracked = broker.get_tracked_order('synthetic-same-order')
    assert sum(t.quantity for t in tracked.transactions) == 5
    assert cash.quantity == 700

@pytest.mark.parametrize('status', ['unknown', 'cash_settled', 'assigned', 'exercised'])
def test_non_working_snapshots_remain_visible_without_synthetic_executions(status):
    broker = _BrokerForSyncTest([])
    order = Order('strategy', Asset('SYN'), 10, 'buy', identifier=f'synthetic-{status}')
    order.status = status
    broker._process_broker_synced_order(order)
    assert order.status == status
    assert order in broker.get_all_orders()
    assert order.transactions == []


def test_a_late_cancel_snapshot_does_not_erase_a_completed_execution():
    broker = _BrokerForSyncTest([])
    order = Order('strategy', Asset('SYN'), 10, 'buy', identifier='synthetic-race', avg_fill_price=100)
    order.status = 'fill'
    order.filled_quantity = 10
    broker._process_broker_synced_order(order)
    late = Order('strategy', Asset('SYN'), 10, 'buy', identifier=order.identifier, avg_fill_price=100)
    late.status = 'canceled'
    late.filled_quantity = 2
    broker._process_broker_synced_order(late)
    assert order.status == 'fill'
    assert sum(t.quantity for t in order.transactions) == 10


def test_historical_crypto_execution_does_not_recharge_reported_cash_or_replace_net_holdings():
    cash = Position('strategy', Asset('USD', asset_type='forex'), 9800)
    coin = Position('strategy', Asset('SYN', asset_type='crypto'), Decimal('1.997'))
    broker = _BrokerForSyncTest([cash, coin])
    broker.sync_positions(_Strategy())
    order = Order('strategy', coin.asset, 10, 'buy', identifier='historical-fee-fill',
                  quote=cash.asset, avg_fill_price=100)
    order.status = 'partial_fill'
    order.filled_quantity = 2  # Gross execution; the broker position is net of its base-unit fee.
    broker._process_broker_synced_order(order)
    assert sum(transaction.quantity for transaction in order.transactions) == 2
    assert cash.quantity == 9800
    assert Decimal(str(coin.quantity)) == Decimal('1.997')
    broker._process_broker_synced_order(order)
    assert cash.quantity == 9800
    assert Decimal(str(coin.quantity)) == Decimal('1.997')


@pytest.mark.parametrize("quantity", [0.3, 0.7, Decimal("0.123456789123456789")])
def test_fractional_cumulative_fill_uses_decimal_requested_quantity(quantity):
    broker = _BrokerForSyncTest([])
    order = Order("strategy", Asset("SYN", asset_type="crypto"), quantity, "buy",
                  identifier="fractional-history", quote=Asset("USD", asset_type="forex"), avg_fill_price=100)
    order.status = "fill"
    order.filled_quantity = Decimal(str(quantity))
    broker._process_broker_synced_order(order)
    assert order.status == "fill"
    assert sum(Decimal(str(tx.quantity)) for tx in order.transactions) == Decimal(str(float(quantity)))
    assert order._broker_synced_filled_quantity == Decimal(str(quantity))
    broker._process_broker_synced_order(order)
    assert len(order.transactions) == 1


@pytest.mark.parametrize("quantity", [Decimal("0.300000000000000001"), Decimal("-0.1"), Decimal("NaN")])
def test_fractional_cumulative_fill_still_rejects_invalid_executions(quantity):
    broker = _BrokerForSyncTest([])
    order = Order("strategy", Asset("SYN"), 0.3, "buy", avg_fill_price=100)
    order.status = "fill"
    order.filled_quantity = quantity
    with pytest.raises(ValueError, match="cumulative fill quantity"):
        broker._process_broker_synced_order(order)
    assert order.transactions == []


@pytest.mark.parametrize("status", ["partial_fill", "canceled", "replaced", "expired", "error"])
def test_warm_order_sync_keeps_cumulative_receipts_and_relations(status):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    broker = _BrokerForSyncTest([])
    broker.data_source = SimpleNamespace(get_datetime=lambda: datetime(2027, 1, 4, tzinfo=timezone.utc))
    order = Order("strategy", Asset("SYN"), 10, "buy", identifier="synthetic", avg_fill_price=100)
    order.status = "partial_fill"
    order.filled_quantity = Decimal("2")
    broker._process_broker_synced_order(order)
    observed = Order("strategy", Asset("SYN"), 10, "buy", identifier="synthetic", avg_fill_price=102)
    observed.status = status
    observed.filled_quantity = Decimal("5")
    observed.replaced_by = "replacement"
    observed.error_message = "provider reason"
    observed.broker_status = status
    from unittest.mock import MagicMock
    broker._on_canceled_order = MagicMock()
    broker._pull_all_orders = lambda *_: [observed]
    for _ in range(2):
        broker.sync_orders(_Strategy())
    assert sum(t.quantity for t in order.transactions) == 5
    assert sum(t.quantity * t.price for t in order.transactions) == 510
    assert order.filled_quantity == 5
    assert order.status == observed.status
    assert order.replaced_by == "replacement"
    assert order.broker_status == status
    if status in ("canceled", "replaced", "expired"):
        broker._on_canceled_order.assert_called_once_with(order)
    if status == "error":
        assert order.error_message == "provider reason"


@pytest.mark.parametrize("terminal", ["canceled", "replaced", "expired", "error"])
@pytest.mark.parametrize("later_filled", [5, 6, 10])
def test_late_working_snapshot_keeps_terminal_receipt_and_new_executions(terminal, later_filled):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    broker = _BrokerForSyncTest([])
    broker.data_source = SimpleNamespace(get_datetime=lambda: datetime(2027, 1, 4, tzinfo=timezone.utc))
    order = Order("strategy", Asset("SYN"), 10, "buy", identifier="late-working", avg_fill_price=100)
    order.status = terminal
    order.filled_quantity = 5
    broker._process_broker_synced_order(order)
    observed = Order("strategy", Asset("SYN"), 10, "buy", identifier=order.identifier, avg_fill_price=100)
    observed.status = "partial_fill" if later_filled < 10 else "fill"
    observed.filled_quantity = later_filled
    broker._pull_all_orders = lambda *_: [observed]
    broker.sync_orders(_Strategy())
    assert sum(t.quantity for t in order.transactions) == later_filled
    assert order.status == ("fill" if later_filled == 10 else terminal)
    assert len(broker.get_all_orders()) == 1
    assert not order.is_active()
