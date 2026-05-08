from types import SimpleNamespace

from lumibot.brokers.broker import Broker
from lumibot.entities import Asset, Order, Position


class _BrokerForPotentialTotalTest(Broker):
    def __init__(self):
        pass

    def _get_balances_at_broker(self, quote_asset, strategy):
        return 0, 0, 0

    def _get_stream_object(self):
        return None

    def _modify_order(self, order, limit_price=None, stop_price=None):
        return order

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        return None

    def _pull_broker_all_orders(self):
        return []

    def _pull_broker_order(self, identifier):
        return None

    def _pull_position(self, strategy, asset):
        return None

    def _pull_positions(self, strategy):
        return []

    def _register_stream_events(self):
        return None

    def _run_stream(self):
        return None

    def _submit_order(self, order):
        return order

    def cancel_order(self, order):
        return order

    def get_historical_account_value(self):
        return []


def test_asset_potential_total_invalidation_uses_simple_new_orders_revision():
    broker = _BrokerForPotentialTotalTest()
    broker._filled_positions = SimpleNamespace(revision=0)
    broker._unprocessed_orders = SimpleNamespace(revision=0)
    broker._new_orders = SimpleNamespace(revision=0)
    broker._partially_filled_orders = SimpleNamespace(revision=0)
    broker._placeholder_orders = SimpleNamespace(revision=0)
    broker._simple_new_orders_revision = 0
    broker._asset_potential_total_cache = {}

    asset = Asset("SPY", Asset.AssetType.STOCK)
    position = Position("strategy", asset, 1)
    order = Order("strategy", asset, 2, Order.OrderSide.BUY)
    order.status = Order.OrderStatus.NEW
    active_orders = [order]

    broker.get_tracked_position = lambda strategy, asset_arg: position
    broker.get_active_tracked_orders = lambda strategy, asset_arg: list(active_orders)

    assert broker.get_asset_potential_total("strategy", asset) == 3

    replacement = Order("strategy", asset, 5, Order.OrderSide.BUY)
    replacement.status = Order.OrderStatus.NEW
    active_orders[:] = [replacement]

    assert broker.get_asset_potential_total("strategy", asset) == 3

    broker._simple_new_orders_revision += 1

    assert broker.get_asset_potential_total("strategy", asset) == 6
