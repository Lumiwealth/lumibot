"""Exercise the real adapter and SDK with transport intercepted before any network IO."""

import json
from datetime import date
from decimal import Decimal
from threading import RLock
from types import SimpleNamespace

import httpx
import pytest
from schwab.client import Client

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order, Position
from lumibot.trading_builtins import SafeList


@pytest.fixture
def broker_transport():
    requests = []
    events = []
    response = {"status": 201}

    def handle(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"orderId": "original"})
        return httpx.Response(
            response["status"],
            headers={"Location": "https://api.schwabapi.com/trader/v1/accounts/synthetic/orders/accepted"},
            json={"message": "Fractional order is not eligible for this account"},
        )

    with httpx.Client(transport=httpx.MockTransport(handle)) as session:
        broker = Schwab.__new__(Schwab)
        broker.name = "Schwab"
        broker.schwab_authorization_error = False
        broker.client = Client("synthetic", session)
        broker.hash_value = "synthetic"
        broker.max_workers = 1
        broker.stream = SimpleNamespace(dispatch=lambda event, **payload: events.append((event, payload)))
        broker._lock = RLock()
        for bucket in (
            "_unprocessed_orders",
            "_new_orders",
            "_partially_filled_orders",
            "_filled_orders",
            "_error_orders",
        ):
            setattr(broker, bucket, SafeList(broker._lock))
        yield broker, requests, events, response


def stock_order(quantity, side="sell", order_type="market", **kwargs):
    prices = {
        "market": {},
        "limit": {"limit_price": 100},
        "stop": {"stop_price": 90},
        "stop_limit": {"stop_price": 90, "stop_limit_price": 89},
    }
    return Order("synthetic", Asset("SPY"), quantity, side, order_type=order_type, **prices[order_type], **kwargs)


@pytest.mark.parametrize("quantity", [Decimal("0.0436"), Decimal("1.0436"), 0.0436, 1.0436, 1, Decimal("2.0000")])
@pytest.mark.parametrize("side", ["buy", "sell", "sell_short", "buy_to_cover"])
def test_submit_preserves_requested_quantity(broker_transport, quantity, side):
    broker, requests, events, _ = broker_transport
    order = stock_order(quantity, side)
    original = order.quantity

    assert broker.submit_order(order) is order, "positive fractional quantity must reach the transport unchanged"
    payload = json.loads(requests[0].content, parse_float=Decimal)
    assert payload["orderLegCollection"][0]["quantity"] == Decimal(str(quantity)), "quantity silently changed"
    assert order.quantity == original
    assert order.status == Order.OrderStatus.SUBMITTED
    assert events[-1][0] == broker.NEW_ORDER


@pytest.mark.parametrize("order_type", ["market", "limit", "stop", "stop_limit"])
def test_replacement_preserves_fraction_and_restores_prices(broker_transport, order_type):
    broker, requests, _, _ = broker_transport
    order = stock_order(Decimal("1.0436"), order_type=order_type, identifier="original")
    broker._modify_order(order, limit_price=101)
    replacement = next(request for request in requests if request.method == "PUT")
    payload = json.loads(replacement.content, parse_float=Decimal)
    assert payload["orderLegCollection"][0]["quantity"] == Decimal("1.0436"), "replacement quantity silently changed"
    assert order.quantity == Decimal("1.0436")
    assert order.identifier == "accepted"


@pytest.mark.parametrize("order_class", ["oto", "oco", "bracket"])
def test_advanced_children_preserve_requested_quantities(broker_transport, order_class):
    broker, requests, _, _ = broker_transport
    children = [stock_order(Decimal("1.0436"), order_type="limit")]
    if order_class != "oto":
        children.append(stock_order(Decimal("1.0436"), order_type="stop"))
    order = stock_order(Decimal("1.0436"), side="buy", order_class=order_class, child_orders=children)
    assert broker.submit_order(order) is order

    def quantities(spec):
        found = [leg["quantity"] for leg in spec.get("orderLegCollection", [])]
        for child in spec.get("childOrderStrategies", []):
            found.extend(quantities(child))
        return found

    amounts = quantities(json.loads(requests[0].content, parse_float=Decimal))
    assert len(amounts) == (3 if order_class == "bracket" else 2)
    assert all(amount == Decimal("1.0436") for amount in amounts), "advanced quantity silently changed"


def test_cleanup_batch_reports_provider_rejection_without_changing_positions(broker_transport):
    broker, requests, events, response = broker_transport
    response["status"] = 400
    positions = [Position("synthetic", Asset("SPY"), quantity) for quantity in (0.0436, 1.0436)]
    originals = [position.quantity for position in positions]
    orders = [position.get_selling_order() for position in positions]

    assert broker.submit_orders(orders) == [None, None]
    assert len(requests) == 2, "positive cleanup quantities must reach provider validation"
    for order, (event, payload), request in zip(orders, events, requests):
        assert event == broker.ERROR_ORDER
        assert "Fractional order is not eligible for this account" in payload["error_msg"]
        # Position close orders retain floats; compare their decimal JSON value, not binary float bits.
        assert json.loads(request.content, parse_float=Decimal)["orderLegCollection"][0]["quantity"] == Decimal(
            str(order.quantity)
        )
        broker._process_error_order(order, payload["error_msg"])
        assert order.status == broker.ERROR_ORDER
        assert not order.is_filled()
    assert [position.quantity for position in positions] == originals
    assert not broker._unprocessed_orders.get_list()


@pytest.mark.parametrize(
    "quantity",
    [
        Decimal("0"),
        Decimal("-0.2"),
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("1E-1000"),
        Decimal("1.00000000000000001"),
    ],
)
def test_invalid_or_lossy_quantity_rejected_before_transport(broker_transport, quantity):
    broker, requests, events, _ = broker_transport
    order = stock_order(1)
    order.quantity = quantity
    assert broker.submit_order(order) is None
    assert requests == []
    assert events[-1][0] == broker.ERROR_ORDER
    assert "Schwab stock quantity" in events[-1][1]["error_msg"]


def test_integral_options_unchanged(broker_transport):
    broker, requests, _, _ = broker_transport
    asset = Asset("SPY", asset_type="option", expiration=date(2027, 1, 15), strike=100, right="CALL")
    order = Order("synthetic", asset, 2, "buy_to_open")
    assert broker._submit_order(order) is order
    leg = json.loads(requests[0].content)["orderLegCollection"][0]
    assert leg["quantity"] == 2
    assert isinstance(leg["quantity"], int)
    assert leg["instrument"]["assetType"] == "OPTION"
