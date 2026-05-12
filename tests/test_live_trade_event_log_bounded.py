from datetime import datetime

from lumibot.brokers.broker import Broker
from lumibot.entities import Asset, Order


class _MockDataSource:
    def get_datetime(self):
        return datetime(2026, 1, 10)


class _MockBroker(Broker):
    def cancel_order(self, order: Order) -> None:
        return

    def _modify_order(self, order: Order, limit_price=None, stop_price=None):
        return

    def _submit_order(self, order: Order) -> Order:
        return order

    def _get_balances_at_broker(self, quote_asset: Asset, strategy):
        return (0.0, 0.0, 0.0)

    def get_historical_account_value(self) -> dict:
        return {}

    def _get_stream_object(self):
        return None

    def _register_stream_events(self):
        return

    def _run_stream(self):
        return

    def _pull_positions(self, strategy):
        return []

    def _pull_position(self, strategy, asset: Asset):
        return None

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object=None):
        raise NotImplementedError

    def _pull_broker_order(self, identifier: str):
        return None

    def _pull_broker_all_orders(self):
        return []


def test_live_trade_event_log_is_bounded():
    broker = _MockBroker(name="mock", connect_stream=False, data_source=_MockDataSource())

    # Avoid subscriber/noise; we only care about trade-event log growth.
    broker._on_new_order = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

    asset = Asset("SPY")
    order = Order(strategy="s", asset=asset, side="buy", quantity=1)
    order.identifier = "1"

    for _ in range(6000):
        broker._process_trade_event(order, broker.NEW_ORDER)

    # Live brokers keep only the tail to prevent unbounded memory growth.
    assert len(broker._trade_event_log_rows) == 5000
