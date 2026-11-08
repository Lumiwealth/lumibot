"""Schwab single-order prices are sent to schwab-py as strings.

schwab-py deprecated float prices and will remove them. Its float path also
truncates in binary, which sends a 1.15 limit as "1.14". These tests turn the
deprecation warning into an error so a float price cannot slip back in.
"""
from datetime import date
from threading import RLock

import pytest

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order
from lumibot.trading_builtins import SafeList

pytestmark = pytest.mark.filterwarnings("error:passing floats to set_price:UserWarning")


class _PlaceClient:
    def __init__(self):
        self.place_calls = []

    def place_order(self, account_hash, order_spec):
        self.place_calls.append(order_spec)
        return type("Response", (), {
            "status_code": 201,
            "headers": {"Location": "https://api.schwabapi.com/trader/v1/accounts/hash/orders/order-1"},
            "text": "",
        })()


def _broker():
    broker = Schwab.__new__(Schwab)
    broker.name = "Schwab"
    broker.schwab_authorization_error = False
    broker.client = _PlaceClient()
    broker.hash_value = "account-hash"
    broker.stream = None
    broker._unprocessed_orders = SafeList(RLock())
    return broker


def _option(strike=500, right="PUT"):
    return Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 6, 19), strike=strike, right=right)


@pytest.mark.parametrize(
    ("price", "expected"),
    [
        (500, "500.00"),
        (1.15, "1.15"),  # the float path sends "1.14"
        (4.35, "4.35"),  # the float path sends "4.34"
        (1.159, "1.15"),  # truncated toward zero, as schwab-py does
        (0.12345, "0.1234"),
        (0.1, "0.1000"),
        (0, "0.00"),
        (None, None),
    ],
)
def test_single_price_keeps_schwab_py_truncation_without_float_error(price, expected):
    assert Schwab._format_schwab_single_price(price) == expected


@pytest.mark.parametrize(
    "order",
    [
        Order("unit-test", Asset("SPY"), 10, Order.OrderSide.BUY, limit_price=1.15),
        Order("unit-test", Asset("SPY"), 10, Order.OrderSide.SELL, stop_price=1.20, stop_limit_price=1.15),
        Order("unit-test", _option(), 1, Order.OrderSide.BUY_TO_OPEN, limit_price=1.15),
        Order("unit-test", _option(), 1, Order.OrderSide.SELL_TO_CLOSE, limit_price=1.15),
    ],
    ids=["stock-limit", "stock-stop-limit", "option-buy-to-open", "option-sell-to-close"],
)
def test_single_limit_order_sends_string_price(order):
    broker = _broker()

    submitted = broker._submit_orders([order], is_multileg=False)

    assert submitted == [order]
    assert broker.client.place_calls[0]["price"] == "1.15"
    assert broker.client.place_calls[0]["tag"] == "unit-test"
