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
