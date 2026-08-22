from unittest.mock import Mock

import pytest

from lumibot.brokers.broker import Broker
from lumibot.entities import Order


class UnsupportedAtomicBroker(Broker):
    def _get_balances_at_broker(self, *args, **kwargs):
        return None

    def _get_stream_object(self, *args, **kwargs):
        return None

    def _modify_order(self, *args, **kwargs):
        return None

    def _parse_broker_order(self, *args, **kwargs):
        return None

    def _pull_broker_all_orders(self, *args, **kwargs):
        return []

    def _pull_broker_order(self, *args, **kwargs):
        return None

    def _pull_position(self, *args, **kwargs):
        return None

    def _pull_positions(self, *args, **kwargs):
        return []

    def _register_stream_events(self, *args, **kwargs):
        return None

    def _run_stream(self, *args, **kwargs):
        return None

    def _submit_order(self, order):
        return self.submit_leg(order)

    def cancel_order(self, *args, **kwargs):
        return None

    def get_historical_account_value(self, *args, **kwargs):
        return None


@pytest.mark.parametrize(
    "order_type",
    [
        Order.OrderType.MARKET,
        Order.OrderType.LIMIT,
        "credit",
        "debit",
        "even",
    ],
)
def test_base_broker_never_falls_back_to_independent_multileg_submissions(order_type):
    broker = object.__new__(UnsupportedAtomicBroker)
    broker.name = "UnsupportedAtomicBroker"
    broker.max_workers = 2
    broker.submit_leg = Mock()
    legs = [Mock(spec=Order), Mock(spec=Order)]

    with pytest.raises(NotImplementedError, match="atomic multi-leg"):
        Broker.submit_orders(
            broker,
            legs,
            is_multileg=True,
            order_type=order_type,
            price=1.25,
        )

    broker.submit_leg.assert_not_called()
