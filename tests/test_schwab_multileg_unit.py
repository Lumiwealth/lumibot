import importlib.util
from datetime import date
from pathlib import Path
from queue import Empty
from threading import RLock

import pytest

from lumibot.brokers.schwab import Schwab
from lumibot.entities import Asset, Order, Position
from lumibot.trading_builtins import SafeList


class _PlaceClient:
    def __init__(self):
        self.place_calls = []

    def place_order(self, account_hash, order_spec):
        self.place_calls.append((account_hash, order_spec))
        return type("Response", (), {
            "status_code": 201,
            "headers": {
                "Location": "https://api.schwabapi.com/trader/v1/accounts/hash/orders/package-123",
            },
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


def _option_leg(strike, right, side, symbol="SPY", quantity=1):
    return Order(
        "unit-test",
        Asset(
            symbol,
            asset_type=Asset.AssetType.OPTION,
            expiration=date(2026, 6, 19),
            strike=strike,
            right=right,
        ),
        quantity,
        side,
    )


def _put_credit_children():
    return [
        _option_leg(500, "PUT", Order.OrderSide.SELL_TO_OPEN),
        _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN),
    ]


def _put_credit_parent():
    return Order(
        "unit-test",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        1,
        Order.OrderSide.SELL,
        limit_price=1.25,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=_put_credit_children(),
    )


def _spec(broker, order):
    builder = broker._prepare_multileg_option_order_builder(order)
    return broker._build_order_spec_from_builder(
        builder,
        order.time_in_force,
        session=broker._schwab_session_for_order(order),
    )


def test_put_credit_spread_builds_vertical_net_credit_and_submits_once():
    """A two-leg put credit is one VERTICAL NET_CREDIT order."""
    broker = _broker()
    order = _put_credit_parent()

    spec = _spec(broker, order)

    assert spec["complexOrderStrategyType"] == "VERTICAL"
    assert spec["orderType"] == "NET_CREDIT"
    assert spec["orderStrategyType"] == "SINGLE"
    assert spec["quantity"] == 1
    assert spec["price"] == "1.25"
    assert spec["session"] == "NORMAL"
    assert spec["duration"] == "DAY"
    assert [leg["instruction"] for leg in spec["orderLegCollection"]] == [
        "SELL_TO_OPEN",
        "BUY_TO_OPEN",
    ]
    assert [leg["instrument"]["symbol"] for leg in spec["orderLegCollection"]] == [
        "SPY   260619P00500000",
        "SPY   260619P00490000",
    ]
    assert all(leg["instrument"]["assetType"] == "OPTION" for leg in spec["orderLegCollection"])
    assert [leg["quantity"] for leg in spec["orderLegCollection"]] == [1, 1]

    # A custom tag that does not normalize to the strategy name: Schwab still
    # gets the strategy, so recovery after a restart can find the package.
    order.tag = "entry-42"
    submitted = broker._submit_order(order)

    assert submitted is order
    assert len(broker.client.place_calls) == 1
    assert broker.client.place_calls[0][1]["orderType"] == "NET_CREDIT"
    assert broker.client.place_calls[0][1]["tag"] == "unit-test"
    assert order.identifier == "package-123"
    assert order.status == Order.OrderStatus.SUBMITTED
    assert order.order_class == Order.OrderClass.MULTILEG
    assert order.child_orders[0].parent_identifier == "package-123"
    assert order.child_orders[1].asset.strike == 490


def test_submit_orders_credit_keyword_uses_one_package():
    broker = _broker()
    children = _put_credit_children()

    submitted = broker._submit_orders(children, is_multileg=True, order_type="credit", price=1.25)

    assert submitted is not None
    assert len(submitted) == 1
    assert submitted[0].order_class == Order.OrderClass.MULTILEG
    assert submitted[0].child_orders == children
    assert len(broker.client.place_calls) == 1
    spec = broker.client.place_calls[0][1]
    assert spec["complexOrderStrategyType"] == "VERTICAL"
    assert spec["orderType"] == "NET_CREDIT"
    assert spec["price"] == "1.25"


def test_four_leg_box_builds_custom_net_debit():
    broker = _broker()
    order = Order(
        "unit-test",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        1,
        Order.OrderSide.BUY,
        limit_price=0.40,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[
            _option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN),
            _option_leg(510, "CALL", Order.OrderSide.SELL_TO_OPEN),
            _option_leg(500, "PUT", Order.OrderSide.SELL_TO_OPEN),
            _option_leg(510, "PUT", Order.OrderSide.BUY_TO_OPEN),
        ],
    )

    spec = _spec(broker, order)

    assert spec["complexOrderStrategyType"] == "CUSTOM"
    assert spec["orderType"] == "NET_DEBIT"
    assert spec["price"] == "0.4000"
    assert spec["quantity"] == 1
    assert len(spec["orderLegCollection"]) == 4
    assert spec["orderLegCollection"][0]["instrument"]["symbol"] == "SPY   260619C00500000"
    assert spec["orderLegCollection"][3]["instruction"] == "BUY_TO_OPEN"

    submitted = broker._submit_order(order)
    assert submitted is order
    assert len(broker.client.place_calls) == 1
    assert broker.client.place_calls[0][1]["complexOrderStrategyType"] == "CUSTOM"


def test_invalid_multileg_shapes_are_rejected_without_placing_legs():
    """Five-leg, stock, mixed-underlying, empty, and market packages are refused before any request."""
    broker = _broker()
    five_legs = Order(
        "unit-test",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        1,
        Order.OrderSide.BUY,
        limit_price=0.10,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[_option_leg(500 + offset, "CALL", Order.OrderSide.BUY_TO_OPEN) for offset in range(5)],
    )
    stock_leg = Order(
        "unit-test",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        1,
        Order.OrderSide.BUY,
        limit_price=0.10,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[
            _option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN),
            Order("unit-test", Asset("SPY"), 100, Order.OrderSide.SELL),
        ],
    )
    two_underlyings = Order(
        "unit-test",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        1,
        Order.OrderSide.SELL,
        limit_price=1.00,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[
            _option_leg(500, "PUT", Order.OrderSide.SELL_TO_OPEN, symbol="SPY"),
            _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN, symbol="QQQ"),
        ],
    )
    empty_parent = Order(
        "unit-test",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        1,
        Order.OrderSide.SELL,
        limit_price=1.00,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[],
    )
    market_parent = Order(
        "unit-test",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        1,
        Order.OrderSide.SELL,
        order_type=Order.OrderType.MARKET,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=_put_credit_children(),
    )

    for order in (five_legs, stock_leg, two_underlyings, empty_parent, market_parent):
        assert broker._prepare_multileg_option_order_builder(order) is None
        assert broker._submit_order(order) is None

    assert broker._submit_orders(
        [_option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN) for _ in range(5)],
        is_multileg=True,
        order_type="debit",
        price=0.10,
    ) is None
    assert broker.client.place_calls == []


def test_non_multileg_submit_orders_still_submits_each_order():
    """Orders that are not a package are still submitted one at a time."""
    broker = _broker()
    orders = [
        Order("unit-test", Asset("SPY"), 10, Order.OrderSide.BUY, limit_price=500),
        Order("unit-test", Asset("QQQ"), 5, Order.OrderSide.BUY, limit_price=450),
    ]

    submitted = broker._submit_orders(orders, is_multileg=False)

    assert submitted == orders
    assert len(broker.client.place_calls) == 2
    assert [call[1]["orderLegCollection"][0]["instrument"]["symbol"] for call in broker.client.place_calls] == [
        "SPY",
        "QQQ",
    ]


def test_filled_vertical_snapshot_parses_as_multileg_parent():
    """A filled vertical snapshot becomes one multi-leg parent."""
    broker = _broker()
    snapshot = {
        "orderId": "spread-1",
        "orderType": "NET_CREDIT",
        "complexOrderStrategyType": "VERTICAL",
        "orderStrategyType": "SINGLE",
        "status": "FILLED",
        "quantity": 1,
        "filledQuantity": 1,
        "price": 1.25,
        "averagePrice": 1.20,
        "duration": "DAY",
        "enteredTime": "2026-06-01T14:30:00+0000",
        "closeTime": "2026-06-01T14:31:00+0000",
        "orderLegCollection": [
            {
                "instruction": "SELL_TO_OPEN",
                "quantity": 1,
                "orderLegType": "OPTION",
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": "SPY   260619P00500000",
                    "putCall": "PUT",
                    "underlyingSymbol": "SPY",
                },
            },
            {
                "instruction": "BUY_TO_OPEN",
                "quantity": 1,
                "orderLegType": "OPTION",
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": "SPY   260619P00490000",
                    "putCall": "PUT",
                    "underlyingSymbol": "SPY",
                },
            },
        ],
        "orderActivityCollection": [
            {
                "activityType": "EXECUTION",
                "executionLegs": [
                    {"legId": 1, "quantity": 1, "price": 2.00},
                    {"legId": 2, "quantity": 1, "price": 0.80},
                ],
            }
        ],
    }

    parsed = broker._parse_broker_order(snapshot, strategy_name="unit-test")

    assert parsed.order_class == Order.OrderClass.MULTILEG
    assert parsed.order_type == Order.OrderType.LIMIT
    assert parsed.raw_order_type == "NET_CREDIT"
    assert parsed.side == Order.OrderSide.SELL
    assert parsed.limit_price == 1.25
    assert parsed.asset.asset_type == Asset.AssetType.MULTILEG
    assert parsed.asset.symbol == "SPY"
    assert parsed.identifier == "spread-1"
    assert parsed.status == Order.OrderStatus.FILLED
    assert parsed._schwab_cumulative_filled_quantity == 1
    assert parsed.avg_fill_price == 1.20
    assert len(parsed.child_orders) == 2
    assert [child.identifier for child in parsed.child_orders] == ["spread-1-leg-1", "spread-1-leg-2"]
    assert [child.parent_identifier for child in parsed.child_orders] == ["spread-1", "spread-1"]
    assert parsed.child_orders[0].side == Order.OrderSide.SELL_TO_OPEN
    assert parsed.child_orders[0].asset.strike == 500
    assert parsed.child_orders[1].side == Order.OrderSide.BUY_TO_OPEN
    assert parsed.child_orders[1].asset.strike == 490
    assert parsed.child_orders[0] != parsed


def test_ratio_legs_send_gcd_package_quantity_with_absolute_leg_quantities():
    """A 2-by-4 ratio uses package quantity 2, keeps both leg sizes, and is CUSTOM."""
    broker = _broker()
    children = [
        _option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN, quantity=2),
        _option_leg(510, "CALL", Order.OrderSide.SELL_TO_OPEN, quantity=4),
    ]

    submitted = broker._submit_orders(children, is_multileg=True, order_type="debit", price=0.15)

    assert submitted[0].quantity == 2
    spec = broker.client.place_calls[0][1]
    assert spec["quantity"] == 2
    assert [leg["quantity"] for leg in spec["orderLegCollection"]] == [2, 4]
    # Unequal leg quantities are a ratio spread, not an ordinary vertical.
    assert spec["complexOrderStrategyType"] == "CUSTOM"


# The tests below run the native Schwab reducer and stream queue against the
# shared synthetic transport, so they prove the package round trip end to end.
_SPEC = importlib.util.spec_from_file_location(
    "schwab_lifecycle_transport",
    Path(__file__).parent / "fixtures/schwab_lifecycle_transport.py",
)
_TRANSPORT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_TRANSPORT)


class _ReplaceableTransport(_TRANSPORT.SchwabLifecycleTransport):
    """Adds a replace endpoint; replies are scripted, never inferred."""

    def __init__(self):
        super().__init__()
        self.replace_calls = []

    def replace_order(self, account_hash, identifier, order_spec):
        self._check_account(account_hash)
        self.replace_calls.append((identifier, order_spec))
        return type("Response", (), {
            "status_code": 201,
            "headers": {"Location": f"https://schwab-fixture.invalid/orders/{identifier}-r"},
            "text": "",
        })()


@pytest.fixture
def lifecycle(monkeypatch):
    monkeypatch.setenv("LUMIBOT_TELEMETRY", "false")
    transport = _ReplaceableTransport()
    broker, queues = _TRANSPORT.data_only_schwab_broker(transport)
    return transport, broker, queues


def _priority_events(queues):
    """Drain the priority queue and return the events waiting there."""
    events = []
    while True:
        try:
            events.append(queues.priority_queue.get_nowait())
        except Empty:
            return events


def _fixture_put_credit_legs():
    """Put-credit legs tagged with the lifecycle fixture strategy."""
    legs = _put_credit_children()
    for leg in legs:
        leg.strategy = "fixture"
    return legs


def test_package_fill_reaches_strategy_once_at_net_price_and_package_quantity(lifecycle):
    """One package fill reaches the strategy at the net price and the package quantity."""
    transport, broker, queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    _TRANSPORT.drain_stream_events(broker)
    assert broker.get_tracked_order(parent.identifier) is parent

    transport.queue_order_reads(parent.identifier, {"status": "FILLED", "filledQuantity": 1, "averagePrice": 1.20})
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    # A repeated snapshot must not deliver a second fill.
    broker._process_schwab_order_snapshot(observed)

    assert parent.is_filled()
    assert not parent.is_active()
    assert all(child.is_filled() and child._closed_event.is_set() for child in parent.child_orders)
    assert [(t.price, t.quantity) for t in parent.transactions] == [(1.20, 1)]
    fills = [payload for name, payload in _priority_events(queues) if name == broker.FILLED_ORDER]
    assert len(fills) == 1
    assert fills[0]["order"] is parent
    assert fills[0]["price"] == 1.20
    assert fills[0]["quantity"] == 1
    assert len(transport.attempted_orders) == 1


def test_modify_multileg_replaces_the_package_at_the_new_net_price(lifecycle):
    """Replacing a package sends one order at the new net credit price."""
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    original_id = parent.identifier

    broker.modify_order(parent, limit_price=1.15)

    assert len(transport.replace_calls) == 1
    replaced_id, spec = transport.replace_calls[0]
    assert replaced_id == original_id
    assert spec["orderType"] == "NET_CREDIT"
    assert spec["complexOrderStrategyType"] == "VERTICAL"
    assert spec["price"] == "1.15"
    assert spec["quantity"] == 1
    assert spec["tag"] == "fixture"
    assert [leg["instrument"]["symbol"] for leg in spec["orderLegCollection"]] == [
        "SPY   260619P00500000",
        "SPY   260619P00490000",
    ]
    assert parent.identifier == f"{original_id}-r"
    assert parent.limit_price == 1.15
    assert all(child.parent_identifier == parent.identifier for child in parent.child_orders)


def test_modify_broker_parsed_multileg_keeps_credit_type(lifecycle):
    """A package parsed from a credit snapshot is replaced as a credit."""
    transport, broker, _queues = lifecycle
    submitted = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    # A strategy restart only has the broker snapshot, not the submit-time effect.
    parsed = broker._parse_broker_order(broker._pull_broker_order(submitted.identifier), "fixture")
    # The snapshot keeps NET_CREDIT, so a restart can replace the package as a credit.
    assert parsed._multileg_effect == "credit"

    broker.modify_order(parsed, limit_price=1.10)

    _, spec = transport.replace_calls[0]
    assert spec["orderType"] == "NET_CREDIT"
    assert spec["price"] == "1.10"
    assert [leg["instruction"] for leg in spec["orderLegCollection"]] == ["SELL_TO_OPEN", "BUY_TO_OPEN"]


def _filled_credit_snapshot(executions, **fields):
    """Filled NET_CREDIT snapshot, with the caller's fields layered on top."""
    snapshot = {
        "orderId": "spread-2",
        "orderType": "NET_CREDIT",
        "complexOrderStrategyType": "VERTICAL",
        "orderStrategyType": "SINGLE",
        "status": "FILLED",
        "quantity": 2,
        "filledQuantity": 2,
        "price": 1.25,
        "duration": "DAY",
        "enteredTime": "2026-06-01T14:30:00+0000",
        "orderLegCollection": [
            {
                "legId": 1,
                "instruction": "SELL_TO_OPEN",
                "quantity": 2,
                "orderLegType": "OPTION",
                "instrument": {"assetType": "OPTION", "symbol": "SPY   260619P00500000", "putCall": "PUT"},
            },
            {
                "legId": 2,
                "instruction": "BUY_TO_OPEN",
                "quantity": 2,
                "orderLegType": "OPTION",
                "instrument": {"assetType": "OPTION", "symbol": "SPY   260619P00490000", "putCall": "PUT"},
            },
        ],
        "orderActivityCollection": [
            {"activityType": "EXECUTION", "executionLegs": list(rows)} for rows in executions
        ],
    }
    snapshot.update(fields)
    return snapshot


def test_package_fill_price_comes_from_leg_executions_not_the_limit():
    # Schwab order snapshots carry the limit in "price" and usually no averagePrice.
    # The package filled better than its 1.25 limit: two fills at 1.30 and 1.20 net.
    broker = _broker()
    snapshot = _filled_credit_snapshot([
        [{"legId": 1, "quantity": 1, "price": 2.10}, {"legId": 2, "quantity": 1, "price": 0.80}],
        [{"legId": 1, "quantity": 1, "price": 2.00}, {"legId": 2, "quantity": 1, "price": 0.80}],
    ])

    parsed = broker._parse_broker_order(snapshot, strategy_name="unit-test")

    assert parsed.limit_price == 1.25
    assert parsed._schwab_cumulative_filled_quantity == 2
    assert parsed._schwab_average_fill_price == 1.25
    assert parsed.avg_fill_price == 1.25

    better = _filled_credit_snapshot([
        [{"legId": 1, "quantity": 2, "price": 2.10}, {"legId": 2, "quantity": 2, "price": 0.80}],
    ])
    parsed = broker._parse_broker_order(better, strategy_name="unit-test")
    assert parsed._schwab_cumulative_filled_quantity == 2
    assert parsed.avg_fill_price == 1.30


def test_ratio_package_fill_price_weights_each_leg_by_its_ratio():
    broker = _broker()
    snapshot = _filled_credit_snapshot(
        [[{"legId": 1, "quantity": 2, "price": 1.00}, {"legId": 2, "quantity": 4, "price": 0.30}]],
        orderType="NET_DEBIT",
        complexOrderStrategyType="CUSTOM",
        price=0.30,
    )
    snapshot["orderLegCollection"][0]["instruction"] = "BUY_TO_OPEN"
    snapshot["orderLegCollection"][1]["instruction"] = "SELL_TO_OPEN"
    snapshot["orderLegCollection"][1]["quantity"] = 4

    parsed = broker._parse_broker_order(snapshot, strategy_name="unit-test")

    # One package = buy 1 at 1.00, sell 2 at 0.30: net debit 0.40.
    assert parsed._schwab_cumulative_filled_quantity == 2
    assert parsed.avg_fill_price == 0.40


def test_executions_without_leg_ids_fall_back_to_order_level_fill():
    # Unattributed rows would otherwise count toward every leg and net to zero.
    broker = _broker()
    snapshot = _filled_credit_snapshot(
        [[{"quantity": 2, "price": 2.05}, {"quantity": 2, "price": 0.80}]],
        averagePrice=1.25,
    )

    parsed = broker._parse_broker_order(snapshot, strategy_name="unit-test")

    assert parsed._schwab_cumulative_filled_quantity == 2
    assert parsed.avg_fill_price == 1.25


def test_unsendable_package_type_or_duration_is_rejected_before_any_request():
    """A market, stop-limit, or premarket package is refused, and GTC is sent as GOOD_TILL_CANCEL."""
    broker = _broker()

    for kwargs in (
        {"order_type": Order.OrderType.MARKET},
        {"order_type": "market"},
        {"order_type": "stop_limit", "price": 1.25},
        {"order_type": "credit", "price": 1.25, "duration": "pre"},
    ):
        assert broker._submit_orders(_put_credit_children(), is_multileg=True, **kwargs) is None

    assert broker.client.place_calls == []

    submitted = broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="credit", price=1.25, duration="GTC")
    assert submitted[0].time_in_force == "gtc"
    assert broker.client.place_calls[0][1]["duration"] == "GOOD_TILL_CANCEL"


# --- Coverage of every package shape and price path the broker accepts or rejects.

def _parent(children, side=Order.OrderSide.BUY, limit_price=0.50, quantity=1, **fields):
    return Order(
        "unit-test",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        quantity,
        side,
        limit_price=limit_price,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=children,
        **fields,
    )


def test_even_keyword_sends_net_zero_at_zero_price():
    broker = _broker()

    submitted = broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="even", price=None)

    spec = broker.client.place_calls[0][1]
    assert spec["orderType"] == "NET_ZERO"
    assert spec["price"] == "0.00"
    assert submitted[0].limit_price == 0.0
    assert submitted[0]._multileg_effect == "even"


@pytest.mark.parametrize(
    ("price", "order_type", "expected_type", "expected_price", "expected_side"),
    [
        (-1.25, Order.OrderType.LIMIT, "NET_CREDIT", "1.25", Order.OrderSide.SELL),
        (0.45, "limit", "NET_DEBIT", "0.4500", Order.OrderSide.BUY),
        (0, None, "NET_ZERO", "0.00", Order.OrderSide.BUY),
        (-0.30, "credit", "NET_CREDIT", "0.3000", Order.OrderSide.SELL),
        (2.10, "debit", "NET_DEBIT", "2.10", Order.OrderSide.BUY),
    ],
)
def test_broker_level_price_sign_and_keyword_choose_the_net_type(price, order_type, expected_type, expected_price, expected_side):
    # A direct broker call has no quotes: negative is a credit, positive a debit.
    broker = _broker()

    submitted = broker._submit_orders(_put_credit_children(), is_multileg=True, order_type=order_type, price=price)

    spec = broker.client.place_calls[0][1]
    assert spec["orderType"] == expected_type
    assert spec["price"] == expected_price
    assert submitted[0].side == expected_side


def test_time_in_force_alias_and_uppercase_parent_duration_reach_schwab():
    """A gtc alias and an uppercase GTC duration both reach Schwab as GOOD_TILL_CANCEL."""
    broker = _broker()

    broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="credit", price=1.25, time_in_force="gtc")
    # Order keeps "GTC" as given; Schwab must still receive GOOD_TILL_CANCEL.
    hand_built = _parent(_put_credit_children(), side=Order.OrderSide.SELL, limit_price=1.25, time_in_force="GTC")
    assert broker._submit_order(hand_built) is hand_built

    assert [call[1]["duration"] for call in broker.client.place_calls] == ["GOOD_TILL_CANCEL", "GOOD_TILL_CANCEL"]
    assert hand_built.time_in_force == "gtc"


@pytest.mark.parametrize(
    "legs",
    [
        # Calendar: same strike and right, different expirations.
        [
            _option_leg(500, "CALL", Order.OrderSide.SELL_TO_OPEN),
            Order(
                "unit-test",
                Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=date(2026, 7, 17), strike=500, right="CALL"),
                1,
                Order.OrderSide.BUY_TO_OPEN,
            ),
        ],
        # Risk reversal: different rights.
        [_option_leg(510, "CALL", Order.OrderSide.BUY_TO_OPEN), _option_leg(490, "PUT", Order.OrderSide.SELL_TO_OPEN)],
        # Straddle: same strike.
        [_option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN), _option_leg(500, "PUT", Order.OrderSide.BUY_TO_OPEN)],
        # Both legs bought.
        [_option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN), _option_leg(510, "CALL", Order.OrderSide.BUY_TO_OPEN)],
    ],
    ids=["calendar", "risk-reversal", "straddle", "both-bought"],
)
def test_two_leg_packages_that_are_not_verticals_are_custom(legs):
    broker = _broker()

    spec = _spec(broker, _parent(legs, limit_price=1.00))

    assert spec["complexOrderStrategyType"] == "CUSTOM"
    assert len(spec["orderLegCollection"]) == 2


@pytest.mark.parametrize(
    "order",
    [
        # Unsupported leg side for an option.
        _parent([_option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN), _option_leg(510, "CALL", Order.OrderSide.SELL_SHORT)]),
        # Fractional contracts.
        _parent([_option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN, quantity=1.5), _option_leg(510, "CALL", Order.OrderSide.SELL_TO_OPEN)]),
        # Parent quantity is not the GCD of the legs.
        _parent([_option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN), _option_leg(510, "CALL", Order.OrderSide.SELL_TO_OPEN)], quantity=3),
        # Credit with a zero price.
        _parent(_put_credit_children(), side=Order.OrderSide.SELL, limit_price=None),
        # Unsupported duration on a hand-built parent; without the check it is sent with no duration.
        _parent(_put_credit_children(), time_in_force="pre"),
    ],
    ids=["sell-short-side", "fractional-leg", "parent-quantity-mismatch", "missing-price", "pre-duration"],
)
def test_invalid_package_details_are_rejected_without_a_request(order):
    broker = _broker()

    assert broker._submit_order(order) is None
    assert broker.client.place_calls == []


def test_leg_without_right_or_credit_effect_without_price_is_rejected():
    broker = _broker()
    no_right = _option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN)
    no_right.asset.right = None

    assert broker._submit_order(_parent([no_right, _option_leg(510, "CALL", Order.OrderSide.SELL_TO_OPEN)])) is None
    assert broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="credit", price=None) is None
    assert broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="credit", price="abc") is None
    assert broker._submit_orders([], is_multileg=True, order_type="credit", price=1.0) is None
    fractional = [
        _option_leg(500, "PUT", Order.OrderSide.SELL_TO_OPEN, quantity=0.5),
        _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN, quantity=0.5),
    ]
    assert broker._submit_orders(fractional, is_multileg=True, order_type="credit", price=1.0) is None
    assert broker.client.place_calls == []


def test_spread_snapshot_with_plain_limit_type_parses_as_package_with_first_leg_side():
    # Spreads entered outside LumiBot can carry a plain LIMIT type.
    broker = _broker()
    snapshot = _filled_credit_snapshot([], orderType="LIMIT", status="WORKING", filledQuantity=0)

    parsed = broker._parse_broker_order(snapshot, strategy_name="unit-test")

    assert parsed.order_class == Order.OrderClass.MULTILEG
    assert parsed.order_type == Order.OrderType.LIMIT
    assert parsed.side == Order.OrderSide.SELL_TO_OPEN
    assert not hasattr(parsed, "_multileg_effect")


# --- Snapshot shapes that must not become a package, and fill fallbacks.

def _single_leg_snapshot():
    snapshot = _filled_credit_snapshot([[{"legId": 1, "quantity": 2, "price": 2.05}]], orderType="LIMIT", price=2.00)
    snapshot["orderLegCollection"] = snapshot["orderLegCollection"][:1]
    return snapshot


def test_single_leg_and_mixed_asset_snapshots_keep_the_one_order_parse():
    broker = _broker()

    single = broker._parse_broker_order(_single_leg_snapshot(), strategy_name="unit-test")
    assert single.order_class != Order.OrderClass.MULTILEG
    assert single.asset.asset_type == Asset.AssetType.OPTION
    assert single.identifier == "spread-2"

    buy_write = _filled_credit_snapshot([], orderType="NET_DEBIT")
    buy_write["orderLegCollection"][0] = {
        "legId": 1,
        "instruction": "BUY",
        "quantity": 200,
        "orderLegType": "EQUITY",
        "instrument": {"assetType": "EQUITY", "symbol": "SPY"},
    }
    mixed = broker._parse_broker_order(buy_write, strategy_name="unit-test")
    assert mixed.order_class != Order.OrderClass.MULTILEG


def test_net_zero_snapshot_without_price_parses_as_even_buy_at_zero():
    broker = _broker()
    snapshot = _filled_credit_snapshot([], orderType="NET_ZERO", status="WORKING", filledQuantity=0)
    del snapshot["price"]
    del snapshot["quantity"]

    parsed = broker._parse_broker_order(snapshot, strategy_name="unit-test")

    assert parsed.order_class == Order.OrderClass.MULTILEG
    assert parsed.side == Order.OrderSide.BUY
    assert parsed.limit_price == 0.0
    assert parsed.quantity == 2  # first leg quantity when the package quantity is missing
    assert parsed._schwab_cumulative_filled_quantity == 0
    assert parsed.avg_fill_price is None


def test_out_of_ratio_executions_and_missing_executions_use_order_level_fields():
    broker = _broker()
    out_of_ratio = _filled_credit_snapshot(
        [[{"legId": 1, "quantity": 2, "price": 2.05}, {"legId": 2, "quantity": 1, "price": 0.80}]],
    )
    no_average = _filled_credit_snapshot([])

    parsed = broker._parse_broker_order(out_of_ratio, strategy_name="unit-test")
    assert parsed._schwab_cumulative_filled_quantity == 2
    # No averagePrice: Schwab's own convention is the order price, as for single legs.
    assert parsed.avg_fill_price == 1.25

    parsed = broker._parse_broker_order(no_average, strategy_name="unit-test")
    assert parsed._schwab_cumulative_filled_quantity == 2
    assert parsed.avg_fill_price == 1.25


# --- Leg intent and the full lifecycle on the native reducer.

def test_generic_leg_sides_resolve_against_the_package_and_never_over_close(lifecycle):
    """Generic sell and buy legs open when nothing is held, and a second close of the same contract is refused."""
    transport, broker, _queues = lifecycle
    long_put = _option_leg(500, "PUT", Order.OrderSide.SELL).asset
    broker._filled_positions.append(Position("fixture", long_put, 1))

    opening = _parent(
        [_option_leg(510, "PUT", Order.OrderSide.SELL), _option_leg(490, "PUT", Order.OrderSide.BUY)],
        side=Order.OrderSide.SELL,
        limit_price=1.25,
    )
    opening.strategy = "fixture"
    for leg in opening.child_orders:
        leg.strategy = "fixture"
    assert broker.submit_order(opening) is opening
    assert [leg["instruction"] for leg in transport.attempted_orders[0]["order_spec"]["orderLegCollection"]] == [
        "SELL_TO_OPEN",
        "BUY_TO_OPEN",
    ]

    # Two sells of the one held contract: the second must not close a contract
    # the first already closes, so the package is refused before any request.
    over_close = _parent(
        [_option_leg(500, "PUT", Order.OrderSide.SELL), _option_leg(500, "PUT", Order.OrderSide.SELL)],
        side=Order.OrderSide.SELL,
        limit_price=1.00,
    )
    over_close.strategy = "fixture"
    for leg in over_close.child_orders:
        leg.strategy = "fixture"
    assert broker.submit_order(over_close) is None
    assert len(transport.attempted_orders) == 1


def test_partial_then_full_package_fill_delivers_package_deltas(lifecycle):
    """Each package fill reports only the new quantity, at that fill's own price."""
    transport, broker, queues = lifecycle
    legs = [
        _option_leg(500, "PUT", Order.OrderSide.SELL_TO_OPEN, quantity=2),
        _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN, quantity=2),
    ]
    for leg in legs:
        leg.strategy = "fixture"
    parent = broker.submit_orders(legs, is_multileg=True, order_type="credit", price=1.25)[0]
    first = {"activityType": "EXECUTION", "executionLegs": [
        {"legId": 1, "quantity": 1, "price": 2.10}, {"legId": 2, "quantity": 1, "price": 0.80},
    ]}
    second = {"activityType": "EXECUTION", "executionLegs": [
        {"legId": 1, "quantity": 1, "price": 2.00}, {"legId": 2, "quantity": 1, "price": 0.80},
    ]}
    transport.queue_order_reads(
        parent.identifier,
        {"status": "PARTIALLY_FILLED", "filledQuantity": 1, "orderActivityCollection": [first]},
        {"status": "FILLED", "filledQuantity": 2, "orderActivityCollection": [first, second]},
    )

    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert not parent.is_filled()
    assert parent.is_active()
    assert parent.transactions[0].price == 1.30
    assert not any(child.is_filled() for child in parent.child_orders)

    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)

    assert parent.is_filled()
    assert not parent.is_active()
    assert all(child.is_filled() for child in parent.child_orders)
    assert [t.quantity for t in parent.transactions] == [1, 1]
    assert parent.transactions[0].price == 1.30
    # The second fill is its own 1.20, not the cumulative 1.25 average.
    assert parent.transactions[1].price == 1.20
    assert parent.get_fill_price() == 1.25
    events = [(name, payload["quantity"]) for name, payload in _priority_events(queues)]
    assert events == [(broker.PARTIALLY_FILLED_ORDER, 1), (broker.FILLED_ORDER, 1)]


def test_package_cancel_and_http_rejection_reach_the_strategy(lifecycle):
    """A cancel and a rejected submit both reach the strategy, and the legs close with the parent."""
    transport, broker, queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    _TRANSPORT.drain_stream_events(broker)

    broker.cancel_order(parent)
    transport.queue_order_reads(parent.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert parent.is_canceled()
    assert not parent.is_active()
    assert all(child.is_canceled() and child._closed_event.is_set() for child in parent.child_orders)

    transport.queue_submission_response(400, "Invalid request data")
    assert broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25) is None
    _TRANSPORT.drain_stream_events(broker)
    names = [name for name, _payload in _priority_events(queues)]
    while True:
        try:
            names.append(queues.queue.get_nowait()[0])
        except Empty:
            break
    assert broker.CANCELED_ORDER in names
    assert broker.ERROR_ORDER in names
    assert len(transport.attempted_orders) == 2


def test_package_price_rounds_in_decimal_space():
    # 1.15 * 100 is 114.999... in binary. Truncating that float would send 1.14.
    assert Schwab._format_schwab_package_price(1.15) == "1.15"
    assert Schwab._format_schwab_package_price(1.259) == "1.26"
    assert Schwab._format_schwab_package_price(0.12349) == "0.1235"
    assert Schwab._format_schwab_package_price(0.9951) == "0.9951"
    assert Schwab._format_schwab_package_price(0.99999) == "1.00"
    assert Schwab._format_schwab_package_price(0) == "0.00"
    # A debit must not pay more, and a credit must not accept less.
    assert Schwab._format_schwab_package_price(1.255, effect="debit") == "1.25"
    assert Schwab._format_schwab_package_price(1.254, effect="credit") == "1.26"
    # Half-up keeps 0.00006, and debit rounding cuts it to zero.
    assert Schwab._format_schwab_package_price(0.00006) == "0.0001"
    assert Schwab._format_schwab_package_price(0.00006, effect="debit") == "0.00"


def test_closing_vertical_high_strike_and_three_leg_package():
    """A closing vertical stays VERTICAL, a butterfly is CUSTOM, and a wide strike keeps its price."""
    broker = _broker()
    closing = _parent(
        [
            _option_leg(500, "PUT", Order.OrderSide.BUY_TO_CLOSE),
            _option_leg(490, "PUT", Order.OrderSide.SELL_TO_CLOSE),
        ],
        side=Order.OrderSide.BUY,
        limit_price=0.85,
    )
    butterfly = _parent(
        [
            _option_leg(490, "CALL", Order.OrderSide.BUY_TO_OPEN),
            _option_leg(500, "CALL", Order.OrderSide.SELL_TO_OPEN, quantity=2),
            _option_leg(510, "CALL", Order.OrderSide.BUY_TO_OPEN),
        ],
        quantity=1,
        limit_price=0.20,
    )
    wide = _parent(
        [
            _option_leg(5040, "CALL", Order.OrderSide.BUY_TO_OPEN, symbol="SPX"),
            _option_leg(5050, "CALL", Order.OrderSide.SELL_TO_OPEN, symbol="SPX"),
        ],
        limit_price=1.15,
    )

    closing_spec = _spec(broker, closing)
    assert closing_spec["complexOrderStrategyType"] == "VERTICAL"
    assert [leg["instruction"] for leg in closing_spec["orderLegCollection"]] == [
        "BUY_TO_CLOSE",
        "SELL_TO_CLOSE",
    ]

    butterfly_spec = _spec(broker, butterfly)
    assert butterfly_spec["complexOrderStrategyType"] == "CUSTOM"
    assert butterfly_spec["quantity"] == 1
    assert [leg["quantity"] for leg in butterfly_spec["orderLegCollection"]] == [1, 2, 1]
    assert butterfly_spec["price"] == "0.2000"

    wide_spec = _spec(broker, wide)
    assert wide_spec["price"] == "1.15"
    assert wide_spec["orderLegCollection"][0]["instrument"]["symbol"] == "SPX   260619C05040000"


def test_unusable_package_inputs_are_rejected_without_a_request():
    broker = _broker()
    long_root = _option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN, symbol="TOOLONG")
    negative_strike = _option_leg(-5, "CALL", Order.OrderSide.BUY_TO_OPEN)
    zero_quantity = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_OPEN)
    zero_quantity.quantity = 0

    assert broker._submit_order(_parent([long_root, _option_leg(510, "CALL", Order.OrderSide.SELL_TO_OPEN)])) is None
    assert broker._submit_order(_parent([negative_strike, _option_leg(510, "CALL", Order.OrderSide.SELL_TO_OPEN)])) is None
    assert broker._submit_orders(
        [zero_quantity, _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN)],
        is_multileg=True,
        order_type="credit",
        price=1.0,
    ) is None
    assert broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="credit", price=0) is None
    assert broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="debit", price=float("nan")) is None
    assert broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="limit", price=float("inf")) is None
    assert broker.client.place_calls == []


def test_broken_option_package_snapshot_is_not_returned_as_one_leg():
    broker = _broker()
    snapshot = _filled_credit_snapshot([[{"legId": 1, "quantity": 2, "price": 2.05}]])
    snapshot["orderLegCollection"][1]["instrument"] = {"assetType": "OPTION"}

    assert broker._parse_broker_order(snapshot, strategy_name="unit-test") is None


def test_unknown_leg_side_uses_the_order_level_fill_price():
    broker = _broker()
    snapshot = _filled_credit_snapshot(
        [[{"legId": 1, "quantity": 2, "price": 2.05}, {"legId": 2, "quantity": 2, "price": 0.80}]],
        averagePrice=0.55,
    )
    snapshot["orderLegCollection"][1]["instruction"] = "HOLD"

    parsed = broker._parse_broker_order(snapshot, strategy_name="unit-test")

    assert parsed.order_class == Order.OrderClass.MULTILEG
    assert parsed.avg_fill_price == 0.55
    assert parsed.child_orders[1].side == Order.OrderSide.UNKNOWN
    assert parsed.broker_parse_degraded is True
    assert parsed.child_orders[1].broker_parse_degraded is True


def test_parsed_even_and_hand_built_zero_price_cannot_be_repriced():
    broker = _broker()
    even = _filled_credit_snapshot([], orderType="NET_ZERO", status="WORKING", filledQuantity=0, price="0.00")
    parsed = broker._parse_broker_order(even, strategy_name="unit-test")
    credit = broker._parse_broker_order(
        _filled_credit_snapshot([], orderType="NET_CREDIT", status="WORKING", filledQuantity=0),
        strategy_name="unit-test",
    )
    hand_built = _parent(_put_credit_children(), side=Order.OrderSide.BUY, limit_price=0)

    assert parsed._multileg_effect == "even"
    assert broker._prepare_multileg_option_order_spec(parsed, limit_price=0.10) is None
    assert parsed.limit_price == 0.0
    assert broker._prepare_multileg_option_order_spec(hand_built, limit_price=0.10) is None
    assert hand_built.limit_price == 0.0
    assert broker._prepare_multileg_option_order_spec(credit, limit_price=0) is None
    assert credit.limit_price == 1.25
    kept = broker._prepare_multileg_option_order_spec(credit, limit_price=1.10)
    assert kept["orderType"] == "NET_CREDIT"
    assert kept["price"] == "1.10"


def test_explicit_close_legs_cannot_over_close_the_same_contract(lifecycle):
    """Two explicit closes of one held contract are refused."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 1))

    over_close = _parent(
        [
            _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE),
            _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE),
        ],
        side=Order.OrderSide.SELL,
        limit_price=1.00,
    )
    over_close.strategy = "fixture"
    for leg in over_close.child_orders:
        leg.strategy = "fixture"

    assert broker.submit_order(over_close) is None
    assert transport.attempted_orders == []


def test_modify_even_package_to_a_price_is_refused(lifecycle):
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="even")[0]

    broker.modify_order(parent, limit_price=0.10)

    assert transport.replace_calls == []
    assert parent.limit_price == 0.0


def test_sub_tick_prices_are_judged_as_schwab_receives_them():
    broker = _broker()

    # 0.00004 is sent as "0.00": a credit or debit at zero is refused...
    assert broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="credit", price=0.00004) is None
    debit = _parent(_put_credit_children(), side=Order.OrderSide.BUY, limit_price=0.00004)
    debit._multileg_effect = "debit"
    assert broker._submit_order(debit) is None
    assert broker.client.place_calls == []

    # ...and an unhinted price that rounds to zero is an even package, not NET_CREDIT 0.00.
    spec = _spec(broker, _parent(_put_credit_children(), side=Order.OrderSide.SELL, limit_price=0.00004))
    assert (spec["orderType"], spec["price"]) == ("NET_ZERO", "0.00")

    # 0.00005 rounds half up to one sub-penny tick and stays a credit.
    spec = _spec(broker, _parent(_put_credit_children(), side=Order.OrderSide.SELL, limit_price=0.00005))
    assert (spec["orderType"], spec["price"]) == ("NET_CREDIT", "0.0001")


def test_non_finite_parent_or_replacement_price_is_refused():
    broker = _broker()

    assert broker._submit_order(_parent(_put_credit_children(), side=Order.OrderSide.SELL, limit_price=float("nan"))) is None
    credit = _parent(_put_credit_children(), side=Order.OrderSide.SELL, limit_price=1.25)
    assert broker._prepare_multileg_option_order_spec(credit, limit_price=float("inf")) is None
    assert credit.limit_price == 1.25
    assert broker.client.place_calls == []


def test_broken_package_warning_is_logged_once_per_order(mocker):
    """A package that cannot be read warns once and debugs the later polls."""
    # The broad poll re-reads the same history every 30 seconds.
    broker = _broker()
    logger = mocker.patch("lumibot.brokers.schwab.logger")
    snapshot = _filled_credit_snapshot([[{"legId": 1, "quantity": 2, "price": 2.05}]])
    snapshot["orderLegCollection"][1]["instrument"] = {"assetType": "OPTION"}

    for _ in range(3):
        assert broker._parse_broker_order(snapshot, strategy_name="unit-test") is None

    package_warnings = [c for c in logger.warning.call_args_list if "Skipping Schwab option package" in str(c)]
    package_debug = [c for c in logger.debug.call_args_list if "Skipping Schwab option package" in str(c)]
    assert len(package_warnings) == 1
    assert len(package_debug) == 2


def test_fractional_strike_keeps_occ_thousandths():
    broker = _broker()
    legs = [
        _option_leg(100.125, "CALL", Order.OrderSide.BUY_TO_OPEN),
        _option_leg(101, "CALL", Order.OrderSide.SELL_TO_OPEN),
    ]

    spec = _spec(broker, _parent(legs, limit_price=0.40))

    assert spec["orderLegCollection"][0]["instrument"]["symbol"] == "SPY   260619C00100125"


def test_stop_and_market_parents_are_not_sent_as_plain_net_limits():
    broker = _broker()
    stop_limit = _parent(
        _put_credit_children(),
        side=Order.OrderSide.SELL,
        limit_price=1.25,
        order_type=Order.OrderType.STOP_LIMIT,
        stop_price=1.00,
    )
    market = _parent(
        _put_credit_children(),
        side=Order.OrderSide.BUY,
        limit_price=1.25,
        order_type=Order.OrderType.MARKET,
    )
    smart = _parent(
        _put_credit_children(),
        side=Order.OrderSide.SELL,
        limit_price=1.25,
        order_type=Order.OrderType.SMART_LIMIT,
    )

    assert broker._submit_order(stop_limit) is None
    assert broker._submit_order(market) is None
    assert _spec(broker, smart)["orderType"] == "NET_CREDIT"
    assert broker.client.place_calls == []


def test_sub_tick_debit_is_not_sent_as_a_zero_price():
    broker = _broker()

    assert broker._submit_orders(
        _put_credit_children(), is_multileg=True, order_type="debit", price=0.00006,
    ) is None
    assert broker.client.place_calls == []

    broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="credit", price=0.00005)

    assert broker.client.place_calls[0][1]["orderType"] == "NET_CREDIT"
    assert broker.client.place_calls[0][1]["price"] == "0.0001"


def test_directional_package_prices_are_sent_without_weakening_the_limit():
    broker = _broker()

    broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="debit", price=1.255)
    broker._submit_orders(_put_credit_children(), is_multileg=True, order_type="credit", price=1.254)

    assert broker.client.place_calls[0][1]["price"] == "1.25"
    assert broker.client.place_calls[1][1]["price"] == "1.26"


def test_submitted_credit_cannot_become_net_zero(lifecycle):
    """A working credit package is not replaced at a zero price."""
    transport, broker, _queues = lifecycle
    parent = _parent(_fixture_put_credit_legs(), side=Order.OrderSide.SELL, limit_price=1.25)
    parent.strategy = "fixture"
    for leg in parent.child_orders:
        leg.strategy = "fixture"

    assert broker.submit_order(parent) is parent
    assert parent._multileg_effect == "credit"

    assert broker.modify_order(parent, limit_price=0) is False
    assert transport.replace_calls == []
    assert parent.limit_price == 1.25


def test_submitted_sub_tick_even_package_cannot_be_repriced(lifecycle):
    """An even package that formatted to zero cannot be given a new price."""
    transport, broker, _queues = lifecycle
    parent = _parent(_fixture_put_credit_legs(), side=Order.OrderSide.SELL, limit_price=0.00004)
    parent.strategy = "fixture"
    for leg in parent.child_orders:
        leg.strategy = "fixture"

    assert broker.submit_order(parent) is parent
    assert parent._multileg_effect == "even"

    assert broker.modify_order(parent, limit_price=0.10) is False
    assert transport.replace_calls == []
    assert parent.limit_price == 0.00004


def test_zero_net_fill_is_not_replaced_by_the_limit(lifecycle):
    """A reported package fill of zero stays zero instead of falling back to the limit."""
    _transport, broker, queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="debit", price=0.40)[0]
    _TRANSPORT.drain_stream_events(broker)
    parent.limit_price = 0.40

    transport_queue = {
        "status": "FILLED",
        "filledQuantity": 1,
        "averagePrice": 0,
        "price": 0.40,
        "orderActivityCollection": [],
    }
    _transport.queue_order_reads(parent.identifier, transport_queue)
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)

    assert parent.transactions[0].price == 0
    fills = [payload for name, payload in _priority_events(queues) if name == broker.FILLED_ORDER]
    assert fills[0]["price"] == 0


def test_modify_leaves_the_price_when_the_order_cannot_be_read(lifecycle):
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    broker._pull_broker_order = lambda _identifier: None

    assert broker.modify_order(parent, limit_price=1.05) is False
    assert transport.replace_calls == []
    assert parent.limit_price == 1.25


def test_unconfirmed_replace_does_not_adopt_the_new_price(lifecycle):
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    original_id = parent.identifier

    def replace_order(account_hash, identifier, order_spec):
        transport._check_account(account_hash)
        transport.replace_calls.append((identifier, order_spec))
        return type("Response", (), {"status_code": 201, "headers": {}, "text": ""})()

    transport.replace_order = replace_order

    assert broker.modify_order(parent, limit_price=1.05) is False
    assert parent.limit_price == 1.25
    assert parent.identifier == original_id


def test_explicit_close_without_a_position_is_rejected(lifecycle):
    """An explicit close with no matching position is refused."""
    transport, broker, _queues = lifecycle
    closing = _parent(
        [
            _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE),
            _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN),
        ],
        side=Order.OrderSide.BUY,
        limit_price=0.40,
    )
    closing.strategy = "fixture"
    for leg in closing.child_orders:
        leg.strategy = "fixture"

    assert broker.submit_order(closing) is None
    assert transport.attempted_orders == []


def test_working_package_reserves_its_close_leg(lifecycle):
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 1))

    def _closing(other_strike):
        """Close the held put and open the other strike."""
        order = _parent(
            [
                _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE),
                _option_leg(other_strike, "PUT", Order.OrderSide.BUY_TO_OPEN),
            ],
            side=Order.OrderSide.SELL,
            limit_price=0.40,
        )
        order.strategy = "fixture"
        for leg in order.child_orders:
            leg.strategy = "fixture"
        return order

    first = _closing(490)
    assert broker.submit_order(first) is first

    assert broker.submit_order(_closing(480)) is None
    assert len(transport.attempted_orders) == 1

    assert broker.modify_order(first, limit_price=0.35) is True
    assert transport.replace_calls[0][1]["price"] == "0.3500"


def _close_package(quantity, other_strike):
    """Close the held put and open the other strike at the given quantity."""
    order = _parent(
        [
            _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE, quantity=quantity),
            _option_leg(other_strike, "PUT", Order.OrderSide.BUY_TO_OPEN, quantity=quantity),
        ],
        side=Order.OrderSide.SELL,
        limit_price=0.40,
    )
    order.strategy = "fixture"
    for leg in order.child_orders:
        leg.strategy = "fixture"
    return order


def _package_execution(first_price, second_price=0.80, quantity=1):
    """One two-leg package execution."""
    return {
        "activityType": "EXECUTION",
        "executionLegs": [
            {"legId": 1, "quantity": quantity, "price": first_price},
            {"legId": 2, "quantity": quantity, "price": second_price},
        ],
    }


def _fill_one_package(transport, broker, parent):
    execution = _package_execution(2.10)
    transport.queue_order_reads(parent.identifier, {
        "status": "PARTIALLY_FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)


def test_partial_fill_keeps_filled_contracts_until_the_position_drops(lifecycle):
    """Filled close contracts stay reserved until the position quantity drops."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 3)
    broker._filled_positions.append(position)
    legs = [
        _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE, quantity=2),
        _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN, quantity=2),
    ]
    for leg in legs:
        leg.strategy = "fixture"
    parent = broker.submit_orders(legs, is_multileg=True, order_type="credit", price=1.25)[0]
    _fill_one_package(transport, broker, parent)

    # The book still shows the filled contract, and one contract remains working.
    assert broker.submit_order(_close_package(2, 480)) is None
    # Sync removes the filled contract and leaves the working one reserved.
    position.quantity = 2
    assert broker.submit_order(_close_package(2, 470)) is None
    assert broker.submit_order(_close_package(1, 460)) is not None
    assert len(transport.attempted_orders) == 2


def test_full_package_fill_stays_reserved_until_position_sync(lifecycle):
    """A fully filled close stays reserved until the position book goes flat."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 1)
    broker._filled_positions.append(position)
    legs = [
        _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE),
        _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN),
    ]
    for leg in legs:
        leg.strategy = "fixture"
    parent = broker.submit_orders(legs, is_multileg=True, order_type="credit", price=1.25)[0]
    execution = _package_execution(2.10)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert parent.is_filled()

    assert broker.submit_order(_close_package(1, 480)) is None
    position.quantity = 0
    assert broker._unreconciled_package_close_quantity(parent.child_orders[0]) == 0
    position.quantity = 1
    assert broker.submit_order(_close_package(1, 470)) is not None


def test_fill_already_reflected_in_the_book_is_not_reserved_again(lifecycle):
    """A fill noted after the position already dropped does not reserve those contracts twice."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 2)
    broker._filled_positions.append(position)
    parent = broker.submit_orders(_close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40)[0]
    position.quantity = 1

    execution = _package_execution(1.20)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)

    assert broker._unreconciled_package_close_quantity(parent.child_orders[0]) == 0
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_sync_before_the_fill_is_noted_does_not_reserve_it_again(lifecycle):
    """A sync that already removed the filled contract is not reserved a second time."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 2)
    broker._filled_positions.append(position)
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]

    reduced = Position("fixture", held, 1)
    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda _strategy: [reduced]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())

    execution = _package_execution(1.20)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)

    assert broker._unreconciled_package_close_quantity(parent.child_orders[0]) == 0
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_sync_during_submit_keeps_the_pre_submission_baseline(lifecycle):
    """A sync during place_order cannot make the reduced book the close baseline."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 2)
    broker._filled_positions.append(position)
    original_place = transport.place_order

    def place_order(account_hash, order_spec):
        position.quantity = 1
        broker._last_positions_refresh_degraded = False
        broker._pull_positions = lambda _strategy: [position]
        broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
        return original_place(account_hash, order_spec)

    transport.place_order = place_order
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    state = broker._unreconciled_package_closes[broker._package_close_key(parent.child_orders[0])]
    assert state["qty"] == 0.0
    assert state["position_abs"] + state["absorbed"] == 2.0

    execution = _package_execution(1.20)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)

    assert broker._unreconciled_package_close_quantity(parent.child_orders[0]) == 0
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_submit_baseline_is_kept_when_an_older_close_is_outstanding(lifecycle):
    """A sync during submit credits this package, not the older close."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 2)
    broker._filled_positions.append(position)
    older = broker.submit_orders(
        _close_package(1, 495).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    original_place = transport.place_order

    def place_order(account_hash, order_spec):
        position.quantity = 1
        broker._last_positions_refresh_degraded = False
        broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
        broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
        return original_place(account_hash, order_spec)

    transport.place_order = place_order
    newer = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    key = broker._package_close_key(newer.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["credits"].get(str(older.identifier), 0.0) == 0.0
    assert state["credits"][str(newer.identifier)] == 1.0
    assert state["qty"] == 0.0

    _fill_closed_package(transport, broker, newer)
    assert state["qty"] == 0.0
    assert state["credits"].get(str(newer.identifier), 0.0) == 0.0

    transport.queue_order_reads(older.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(older.identifier), older.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert older.is_canceled()
    assert state["qty"] == 0.0
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_submit_baseline_does_not_take_an_older_packages_existing_credit(lifecycle):
    """A drop this package caused is credited here without taking credit the older close already held."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 3))
    older = broker.submit_orders(
        _close_package(1, 495).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    _sync_held_quantity(broker, held, 2)
    key = broker._package_close_key(older.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["credits"][str(older.identifier)] == 1.0
    original_place = transport.place_order

    def place_order(account_hash, order_spec):
        broker._last_positions_refresh_degraded = False
        broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
        broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
        return original_place(account_hash, order_spec)

    transport.place_order = place_order
    newer = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]

    assert state["credits"][str(older.identifier)] == 1.0
    assert state["credits"][str(newer.identifier)] == 1.0
    assert state["qty"] == 0.0


def test_submit_baseline_does_not_undo_a_released_reservation(lifecycle):
    """A sync that already released a filled close does not also credit the package being sent."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    older = broker.submit_orders(
        _close_package(1, 495).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    _fill_closed_package(transport, broker, older)
    key = broker._package_close_key(older.child_orders[0])
    assert broker._unreconciled_package_closes[key]["qty"] == 1.0
    original_place = transport.place_order

    def place_order(account_hash, order_spec):
        broker._last_positions_refresh_degraded = False
        broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
        broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
        return original_place(account_hash, order_spec)

    transport.place_order = place_order
    newer = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    # The filled close had nothing left reserved, so sync drops its anchor and
    # acceptance creates a new one. Read that object.
    state = broker._unreconciled_package_closes[key]
    assert state["qty"] == 0.0
    assert state["credits"].get(str(newer.identifier), 0.0) == 0.0
    _fill_closed_package(transport, broker, newer)
    state = broker._unreconciled_package_closes[key]
    assert state["qty"] == 1.0
    assert broker.submit_order(_close_package(1, 480)) is None
    assert broker.submit_order(_close_package(1, 480)) is None


def _working_close_snapshot(**fields):
    """A working close package. Pass tag to mark it as this strategy's order."""
    snapshot = {
        "orderId": "spread-recovered",
        "orderType": "NET_CREDIT",
        "complexOrderStrategyType": "VERTICAL",
        "orderStrategyType": "SINGLE",
        "status": "WORKING",
        "quantity": 1,
        "filledQuantity": 0,
        "price": 0.40,
        "duration": "DAY",
        "enteredTime": "2026-06-01T14:30:00+0000",
        "orderLegCollection": [
            {
                "legId": 1,
                "instruction": "SELL_TO_CLOSE",
                "quantity": 1,
                "orderLegType": "OPTION",
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": "SPY   260619P00500000",
                    "putCall": "PUT",
                    "underlyingSymbol": "SPY",
                },
            },
            {
                "legId": 2,
                "instruction": "BUY_TO_OPEN",
                "quantity": 1,
                "orderLegType": "OPTION",
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": "SPY   260619P00490000",
                    "putCall": "PUT",
                    "underlyingSymbol": "SPY",
                },
            },
        ],
    }
    snapshot.update(fields)
    return snapshot


def test_untagged_or_filled_snapshots_are_not_recovered(lifecycle):
    """Recovery stays limited to a tagged working order."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    transport.queue_history_response([_working_close_snapshot()])
    broker.stream.dispatch(broker.POLL_EVENT, wait_until_complete=True)

    assert broker.get_tracked_order("spread-recovered") is None
    assert not getattr(broker, "_unreconciled_package_closes", None)

    transport.queue_history_response([_working_close_snapshot(status="FILLED", tag="fixture", filledQuantity=1)])
    broker.stream.dispatch(broker.POLL_EVENT, wait_until_complete=True)

    assert broker.get_tracked_order("spread-recovered") is None
    assert not getattr(broker, "_unreconciled_package_closes", None)


def _working_single_snapshot(**fields):
    """A working single-leg option. Pass tag to mark it as this strategy's order."""
    snapshot = {
        "orderId": "single-recovered",
        "orderType": "LIMIT",
        "orderStrategyType": "SINGLE",
        "status": "WORKING",
        "quantity": 1,
        "filledQuantity": 0,
        "price": 2.00,
        "duration": "DAY",
        "enteredTime": "2026-06-01T14:30:00+0000",
        "orderLegCollection": [
            {
                "legId": 1,
                "instruction": "BUY_TO_OPEN",
                "quantity": 1,
                "orderLegType": "OPTION",
                "instrument": {
                    "assetType": "OPTION",
                    "symbol": "SPY   260619C00500000",
                    "putCall": "CALL",
                    "underlyingSymbol": "SPY",
                },
            },
        ],
    }
    snapshot.update(fields)
    return snapshot


def test_tagged_working_single_order_keeps_the_broker_tag(lifecycle):
    """A single-leg working order keeps its broker tag and can be adopted."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    transport.queue_history_response([
        _working_single_snapshot(orderId="single-untagged"),
        _working_single_snapshot(orderId="single-blank", tag="   "),
        _working_single_snapshot(orderId="single-foreign", tag="other"),
        _working_single_snapshot(orderId="single-owned", tag="  fixture  "),
    ])

    broker.stream.dispatch(broker.POLL_EVENT, wait_until_complete=True)

    assert broker.get_tracked_order("single-untagged") is None
    assert broker.get_tracked_order("single-blank") is None
    assert broker.get_tracked_order("single-foreign") is None
    owned = broker.get_tracked_order("single-owned")
    assert owned is not None
    assert owned.order_class != Order.OrderClass.MULTILEG
    assert owned.tag == "fixture"


def test_recovered_single_leg_close_records_the_historical_fill_without_a_callback(lifecycle):
    """A recovered single-leg close keeps the filled contracts out of the reservation."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    working = _working_single_snapshot(tag="fixture", quantity=2, filledQuantity=1, averagePrice=1.10)
    working["orderLegCollection"][0]["instruction"] = "SELL_TO_CLOSE"
    working["orderLegCollection"][0]["quantity"] = 2

    broker._process_schwab_order_snapshot(broker._parse_broker_order(working, "fixture"))

    tracked = broker.get_tracked_order("single-recovered")
    assert tracked is not None
    assert [(item.price, item.quantity) for item in tracked.transactions] == [(1.10, 1)]
    assert not [
        payload for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]
    another = Order("fixture", held, 1, Order.OrderSide.SELL_TO_CLOSE, limit_price=1.10)
    assert broker.submit_order(another) is not None

    filled = _working_single_snapshot(
        status="FILLED", tag="fixture", quantity=2, filledQuantity=2, averagePrice=1.10,
    )
    filled["orderLegCollection"][0]["instruction"] = "SELL_TO_CLOSE"
    filled["orderLegCollection"][0]["quantity"] = 2
    broker._process_schwab_order_snapshot(broker._parse_broker_order(filled, "fixture"))

    fills = [
        payload for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]
    assert len(fills) == 1
    assert fills[0]["quantity"] == 1
    assert tracked.is_filled()


def test_recovered_working_package_is_anchored_before_the_poll_sync(lifecycle):
    """A sync in the recovery poll cannot make the reduced book the close baseline."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 2)
    broker._filled_positions.append(position)
    transport.queue_history_response([_working_close_snapshot(tag="fixture")])
    broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
    broker._last_positions_refresh_degraded = False

    broker.stream.dispatch(broker.POLL_EVENT, wait_until_complete=True)

    tracked = broker.get_tracked_order("spread-recovered")
    state = broker._unreconciled_package_closes[broker._package_close_key(tracked.child_orders[0])]
    assert state["qty"] == 0.0
    assert state["position_abs"] + state["absorbed"] == 2.0

    filled = _working_close_snapshot(
        status="FILLED",
        tag="fixture",
        filledQuantity=1,
        orderActivityCollection=[_package_execution(1.20)],
    )
    broker._process_schwab_order_snapshot(broker._parse_broker_order(filled, "fixture"))

    assert broker._unreconciled_package_close_quantity(tracked.child_orders[0]) == 0
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_recovered_package_callbacks_only_the_new_fill(lifecycle):
    """A recovered package reports the fill that arrives after adoption, not the history."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    working = _working_close_snapshot(tag="fixture", quantity=2, filledQuantity=1, averagePrice=1.20)
    for leg in working["orderLegCollection"]:
        leg["quantity"] = 2

    broker._process_schwab_order_snapshot(broker._parse_broker_order(working, "fixture"))

    tracked = broker.get_tracked_order("spread-recovered")
    assert tracked is not None
    assert [item.quantity for item in tracked.transactions] == [1]
    assert not [
        payload for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]

    filled = _working_close_snapshot(
        status="FILLED", tag="fixture", quantity=2, filledQuantity=2, averagePrice=1.20,
    )
    for leg in filled["orderLegCollection"]:
        leg["quantity"] = 2
    broker._process_schwab_order_snapshot(broker._parse_broker_order(filled, "fixture"))

    fills = [
        payload for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]
    assert len(fills) == 1
    assert fills[0]["quantity"] == 1
    assert tracked.is_filled()


def _quantity_close_snapshot(**fields):
    """A working close package whose legs use the same quantity as the parent."""
    quantity = fields.get("quantity", 1)
    snapshot = _working_close_snapshot(**fields)
    for leg in snapshot["orderLegCollection"]:
        leg["quantity"] = quantity
    return snapshot


def test_recovered_package_does_not_price_history_at_the_limit(lifecycle):
    """A missing execution price is not stored as the limit, so the next delta stays honest."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    working = _quantity_close_snapshot(tag="fixture", quantity=2, filledQuantity=1, price=1.20)

    broker._process_schwab_order_snapshot(broker._parse_broker_order(working, "fixture"))

    tracked = broker.get_tracked_order("spread-recovered")
    assert [(item.price, item.quantity) for item in tracked.transactions] == [(None, 1)]
    # The unfilled package is estimated from its legs, not the underlying's price:
    # sell the 500 put at 3.00 and buy the 490 put at 1.20 is a 1.80 credit, x100.
    leg_prices = {500: 3.00, 490: 1.20}
    strategy = type("Strategy", (), {"get_last_price": lambda _self, asset: leg_prices.get(asset.strike, 9.99)})()
    assert tracked.cash_pending(strategy) == pytest.approx(180.0)

    filled = _quantity_close_snapshot(
        status="FILLED", tag="fixture", quantity=2, filledQuantity=2, averagePrice=1.00, price=1.20,
    )
    broker._process_schwab_order_snapshot(broker._parse_broker_order(filled, "fixture"))

    fills = [
        payload for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]
    assert len(fills) == 1
    assert fills[0]["quantity"] == 1
    assert fills[0]["price"] == 1.00


def test_poll_parses_a_history_snapshot_once_and_keeps_a_failed_pull(lifecycle):
    """One reliable history parse feeds attribution. A later failed pull does not erase it."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    calls = {"n": 0}
    original = broker._parse_broker_order

    def _count_parse(payload, strategy_name, strategy_object=None):
        calls["n"] += 1
        return original(payload, strategy_name, strategy_object)

    broker._parse_broker_order = _count_parse
    transport.queue_history_response([
        _working_close_snapshot(status="FILLED", tag="fixture", filledQuantity=1),
    ])
    broker.stream.dispatch(broker.POLL_EVENT, wait_until_complete=True)

    assert calls["n"] == 1
    assert broker._schwab_seen_package_fills == {"spread-recovered": 1.0}
    assert broker.get_tracked_order("spread-recovered") is None

    def _denied(*_args, **_kwargs):
        return type("Response", (), {"status_code": 429, "headers": {}})()

    transport.get_orders_for_account = _denied
    del broker._last_position_sync_time
    broker.stream.dispatch(broker.POLL_EVENT, wait_until_complete=True)

    assert broker._schwab_seen_package_fills == {"spread-recovered": 1.0}
    assert broker._last_orders_refresh_degraded is True


def test_stream_startup_anchors_a_recovered_package_after_the_book_loads(lifecycle):
    """Startup loads positions before anchoring, so an empty book is not the baseline."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    transport.queue_history_response([_working_close_snapshot(tag="fixture")])
    broker._pull_positions = lambda _strategy: [Position("fixture", held, 2)]
    broker._last_positions_refresh_degraded = False

    broker._stream_established()

    assert broker.get_tracked_order("spread-recovered") is None
    state = next(iter(broker._unreconciled_package_closes.values()))
    assert state["qty"] == 0.0
    assert state["position_abs"] == 2.0
    assert state["absorbed"] == 0.0


def test_stream_startup_syncs_the_book_under_the_strategy_name(lifecycle):
    """Startup sync labels the book with the strategy, so the recovered close sees it."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    seen = {}

    def pull(strategy):
        name = strategy.name if strategy is not None else "Unknown"
        seen["name"] = name
        return [Position(name, held, 2)]

    transport.queue_history_response([_working_close_snapshot(tag="fixture")])
    broker._pull_positions = pull
    broker._last_positions_refresh_degraded = False
    broker._stream_established()

    assert seen["name"] == "fixture"
    state = next(iter(broker._unreconciled_package_closes.values()))
    assert state["position_abs"] == 2.0
    assert state["qty"] == 0.0

    broker._process_schwab_order_snapshot(
        broker._parse_broker_order(_working_close_snapshot(tag="fixture"), "fixture")
    )
    tracked = broker.get_tracked_order("spread-recovered")
    filled = _working_close_snapshot(
        status="FILLED",
        tag="fixture",
        filledQuantity=1,
        orderActivityCollection=[_package_execution(1.20)],
    )
    broker._process_schwab_order_snapshot(broker._parse_broker_order(filled, "fixture"))
    assert state["qty"] == 1.0

    broker._pull_positions = lambda strategy: [Position(
        strategy.name if strategy is not None else "Unknown", held, 1,
    )]
    broker._last_positions_refresh_degraded = False
    broker.sync_positions(broker._position_sync_strategy())
    assert state["qty"] == 0.0
    assert state["position_abs"] == 1.0
    assert broker.submit_order(_close_package(1, 480)) is not None
    assert tracked is not None


def test_unlabeled_book_counts_for_the_strategy_close(lifecycle):
    """A position labeled Unknown is this strategy's book for the recovered close."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("Unknown", held, 2)
    broker._filled_positions.append(position)
    broker._process_schwab_order_snapshot(
        broker._parse_broker_order(_working_close_snapshot(tag="fixture"), "fixture")
    )
    tracked = broker.get_tracked_order("spread-recovered")
    state = broker._unreconciled_package_closes[broker._package_close_key(tracked.child_orders[0])]
    assert state["position_abs"] == 2.0

    position.quantity = 1
    broker._reconcile_package_close_holds()
    assert state["position_abs"] == 1.0
    assert state["qty"] == 0.0
    assert state["absorbed"] == 1.0

    filled = _working_close_snapshot(
        status="FILLED",
        tag="fixture",
        filledQuantity=1,
        orderActivityCollection=[_package_execution(1.20)],
    )
    broker._process_schwab_order_snapshot(broker._parse_broker_order(filled, "fixture"))
    assert state["qty"] == 0.0
    assert broker._unreconciled_package_close_quantity(tracked.child_orders[0]) == 0

    broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
    broker._last_positions_refresh_degraded = False
    broker.sync_positions(broker._position_sync_strategy())
    assert state["qty"] == 0.0
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_unlabeled_row_satisfies_close_validation_and_keeps_the_reservation(lifecycle):
    """An unlabeled row is the held position, and a second close still stops at zero."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position(None, held, 1))

    assert broker.submit_order(_close_package(1, 480)) is not None
    assert broker.submit_order(_close_package(1, 470)) is None


def test_named_sync_folds_unlabeled_rows_without_dropping_other_strategies(lifecycle):
    """A named sync keeps one current row and leaves another strategy's row alone."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    other = _option_leg(510, "CALL", Order.OrderSide.BUY_TO_OPEN).asset
    foreign_asset = _option_leg(520, "CALL", Order.OrderSide.BUY_TO_OPEN).asset
    stale_named = Position("fixture", held, 9)
    unknown = Position("Unknown", held, 2)
    blank = Position("", other, 4)
    also_blank = Position(None, other, 8)
    foreign = Position("other-strategy", foreign_asset, 7)
    for position in (stale_named, unknown, blank, also_blank, foreign):
        broker._filled_positions.append(position)
    assert broker.get_tracked_positions("fixture") == [stale_named]

    broker._pull_positions = lambda strategy: [
        Position(strategy.name, held, 1),
        Position(strategy.name, other, 3),
    ]
    broker._last_positions_refresh_degraded = False
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())

    rows = [(row.strategy, row.asset.strike, row.quantity) for row in broker._filled_positions.get_list()]
    assert ("Unknown", 500, 2) not in rows
    assert ("fixture", 500, 1) in rows
    assert stale_named.quantity == 1
    assert ("", 510, 4) not in rows
    assert (None, 510, 8) not in rows
    assert [row for row in rows if row[0] == "fixture" and row[1] == 510] == [("fixture", 510, 3)]
    assert ("other-strategy", 520, 7) in rows
    assert len([row for row in rows if row[1] == 500 and row[0] == "fixture"]) == 1
    assert len([row for row in rows if row[1] == 510]) == 1


def test_degraded_sync_keeps_a_missing_unlabeled_row(lifecycle):
    """A partial parse updates the rows it saw and leaves a missing unlabeled row."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    missing_asset = _option_leg(510, "CALL", Order.OrderSide.BUY_TO_OPEN).asset
    seen = Position("Unknown", held, 2)
    missing = Position("Unknown", missing_asset, 4)
    broker._filled_positions.append(seen)
    broker._filled_positions.append(missing)
    broker._pull_positions = lambda strategy: [Position(strategy.name, held, 1)]
    broker._last_positions_refresh_degraded = True

    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())

    assert seen.strategy == "fixture"
    assert seen.quantity == 1
    assert missing.strategy == "Unknown"
    assert missing.quantity == 4
    assert broker.get_tracked_positions("fixture") == [seen]


def test_account_sync_does_not_relabel_an_unknown_row(lifecycle):
    """An account-wide sync updates quantity and leaves each strategy label alone."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    unknown = Position("Unknown", held, 2)
    named = Position("fixture", held, 2)
    broker._filled_positions.append(unknown)
    broker._filled_positions.append(named)
    broker._pull_positions = lambda _strategy: [Position("Unknown", held, 1)]
    broker._last_positions_refresh_degraded = False

    broker.sync_positions(None)

    assert unknown.strategy == "Unknown"
    assert unknown.quantity == 2
    assert named.strategy == "fixture"
    assert named.quantity == 1


def test_position_sync_keeps_surrounding_whitespace_in_the_strategy_name(lifecycle):
    """The registered strategy name is stored exactly, including surrounding spaces."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._strategy_name = " fixture "
    sync = broker._position_sync_strategy()
    assert sync is not None
    assert sync.name == " fixture "

    def pull(strategy):
        return [Position(strategy.name, held, 1)]

    broker._pull_positions = pull
    broker._last_positions_refresh_degraded = False
    broker.sync_positions(sync)

    assert broker.get_tracked_positions(" fixture ")[0].quantity == 1
    assert broker.get_tracked_positions("fixture") == []
    broker._strategy_name = "   "
    assert broker._position_sync_strategy() is None
    broker._strategy_name = None
    assert broker._position_sync_strategy() is None


def test_sync_growth_moves_the_anchor_and_keeps_the_reservation(lifecycle):
    """A larger book moves the close anchor without releasing the filled contracts."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 2)
    broker._filled_positions.append(position)
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    execution = _package_execution(1.20)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    state = broker._unreconciled_package_closes[broker._package_close_key(parent.child_orders[0])]
    assert state["qty"] == 1.0
    assert state["position_abs"] == 2.0

    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda _strategy: [Position("fixture", held, 3)]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    assert state["qty"] == 1.0
    assert state["position_abs"] == 3.0
    assert state["absorbed"] == 0.0
    assert broker.submit_order(_close_package(3, 480)) is None

    broker._pull_positions = lambda _strategy: [Position("fixture", held, 2)]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    assert broker._unreconciled_package_close_quantity(parent.child_orders[0]) == 0
    assert broker.submit_orders(
        _close_package(2, 470).child_orders, is_multileg=True, order_type="credit", price=0.40,
    ) is not None


def test_reconciled_fill_drops_the_close_anchor(lifecycle):
    """Once a filled close is reconciled, later syncs do not keep scanning it."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 1)
    broker._filled_positions.append(position)
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    execution = _package_execution(1.20)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    key = broker._package_close_key(parent.child_orders[0])
    assert key in broker._unreconciled_package_closes

    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda _strategy: []
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    assert key not in broker._unreconciled_package_closes

    broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    broker._pull_positions = lambda _strategy: []
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    assert not broker._unreconciled_package_closes

    broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    assert broker.submit_order(_close_package(1, 470)) is not None


def test_canceled_close_keeps_its_anchor_for_a_delayed_fill(lifecycle):
    """A canceled close keeps a zero credit so a later book drop can still apply to the fill."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 2)
    broker._filled_positions.append(position)
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    transport.queue_order_reads(parent.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    key = broker._package_close_key(parent.child_orders[0])
    assert parent.is_canceled()
    assert broker._unreconciled_package_closes[key]["absorbed"] == 0.0

    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda _strategy: [Position("fixture", held, 2)]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    assert key in broker._unreconciled_package_closes
    assert broker._unreconciled_package_closes[key]["position_abs"] == 2.0

    broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    assert broker._unreconciled_package_closes[key]["qty"] == 0.0
    assert broker._unreconciled_package_closes[key]["absorbed"] == 1.0

    execution = _package_execution(1.20)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert broker._unreconciled_package_close_quantity(parent.child_orders[0]) == 0

    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    assert key not in broker._unreconciled_package_closes


def _forget_tracked_order(broker, identifier):
    """Drop one order from every tracker, as retention cleanup does."""
    for bucket in (
        broker._unprocessed_orders,
        broker._new_orders,
        broker._partially_filled_orders,
        broker._filled_orders,
        broker._error_orders,
        broker._canceled_orders,
        broker._placeholder_orders,
    ):
        bucket.remove(identifier, key="identifier")


def test_canceled_close_without_credit_leaves_the_scan_with_its_order(lifecycle):
    """A zero-credit cancel anchor is removed once the order leaves tracking."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    transport.queue_order_reads(parent.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    key = broker._package_close_key(parent.child_orders[0])
    assert key in broker._unreconciled_package_closes
    assert broker._unreconciled_package_closes[key]["absorbed"] == 0.0

    _forget_tracked_order(broker, parent.identifier)
    broker._reconcile_package_close_holds()
    assert key not in broker._unreconciled_package_closes

    _sync_held_quantity(broker, held, 1)
    assert not broker._unreconciled_package_closes
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_close_anchor_leaves_the_scan_when_its_order_is_gone(lifecycle):
    """Retention removes the order, and the next sync stops scanning its anchor.

    The anchor stays through cancel while the order is tracked, including when
    it already holds drop credit. Once the order itself is gone, a delayed fill
    can no longer be noted, so the credit and the empty slot both leave.
    """
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    transport.queue_order_reads(parent.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    key = broker._package_close_key(parent.child_orders[0])
    assert parent.is_canceled()
    assert key in broker._unreconciled_package_closes

    _sync_held_quantity(broker, held, 1)
    assert broker._unreconciled_package_closes[key]["credits"][str(parent.identifier)] == 1.0

    _forget_tracked_order(broker, parent.identifier)
    broker._reconcile_package_close_holds()
    assert key not in broker._unreconciled_package_closes

    _sync_held_quantity(broker, held, 1)
    assert not broker._unreconciled_package_closes
    assert broker.submit_order(_close_package(1, 480)) is not None


def _sync_held_quantity(broker, held, quantity):
    """Replace the booked quantity and run one position sync."""
    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda _strategy: [Position("fixture", held, quantity)]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())


def _broker_snapshot(transport, identifier, **fields):
    """Copy the stored broker snapshot and overlay the caller's fields."""
    import copy

    snapshot = copy.deepcopy(transport._latest[str(identifier)])
    snapshot.update(fields)
    return snapshot


def _fill_closed_package(transport, broker, parent):
    """Deliver one filled close package without moving the position book."""
    execution = _package_execution(1.20)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)


def test_unrelated_position_drop_cannot_fund_a_later_package(lifecycle):
    """A drop larger than the outstanding close does not pay for a later fill."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 5))
    first = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    _sync_held_quantity(broker, held, 1)
    key = broker._package_close_key(first.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["qty"] == 0.0
    assert state["absorbed"] == 1.0
    assert state["credits"][str(first.identifier)] == 1.0

    _fill_closed_package(transport, broker, first)
    assert broker._unreconciled_package_close_quantity(first.child_orders[0]) == 0

    second = broker.submit_orders(
        _close_package(1, 480).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    _fill_closed_package(transport, broker, second)
    assert broker._unreconciled_package_close_quantity(second.child_orders[0]) == 1.0
    assert state["credits"].get(str(second.identifier), 0.0) == 0.0
    assert broker.submit_order(_close_package(1, 470)) is None


def test_canceled_package_credit_cannot_fund_a_different_package(lifecycle):
    """A canceled package keeps its drop credit, and a later package cannot spend it."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    first = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    transport.queue_order_reads(first.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(first.identifier), first.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert first.is_canceled()

    _sync_held_quantity(broker, held, 1)
    key = broker._package_close_key(first.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["absorbed"] == 1.0
    assert state["credits"][str(first.identifier)] == 1.0

    second = broker.submit_orders(
        _close_package(1, 480).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    _fill_closed_package(transport, broker, second)
    assert state["credits"][str(first.identifier)] == 1.0
    assert state["qty"] == 1.0
    assert broker._unreconciled_package_close_quantity(second.child_orders[0]) == 1.0
    assert broker.submit_order(_close_package(1, 470)) is None

    _fill_closed_package(transport, broker, first)
    assert state["credits"].get(str(first.identifier), 0.0) == 0.0
    assert state["qty"] == 1.0
    assert broker.submit_order(_close_package(1, 460)) is None


def test_position_drop_follows_the_package_execution(lifecycle):
    """A pulled fill is credited to that package, not an older canceled one."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    canceled = broker.submit_orders(
        _close_package(1, 495).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    newer = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    transport.queue_order_reads(canceled.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(canceled.identifier), canceled.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert canceled.is_canceled()

    broker._remember_schwab_package_fills([
        _broker_snapshot(transport, canceled.identifier, status="CANCELED", filledQuantity=0),
        _broker_snapshot(transport, newer.identifier, status="FILLED", filledQuantity=1, averagePrice=0.40),
    ])
    _sync_held_quantity(broker, held, 1)
    key = broker._package_close_key(newer.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["credits"].get(str(canceled.identifier), 0.0) == 0.0
    assert state["credits"][str(newer.identifier)] == 1.0

    _fill_closed_package(transport, broker, newer)
    assert broker._unreconciled_package_close_quantity(newer.child_orders[0]) == 0
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_two_executions_share_one_position_drop(lifecycle):
    """Each executed package is credited only up to the contracts it filled."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 4))
    larger = broker.submit_orders(
        _close_package(2, 495).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    smaller = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    broker._remember_schwab_package_fills([
        _broker_snapshot(transport, larger.identifier, status="FILLED", filledQuantity=1, averagePrice=0.40),
        _broker_snapshot(transport, smaller.identifier, status="FILLED", filledQuantity=1, averagePrice=0.40),
    ])

    _sync_held_quantity(broker, held, 2)

    key = broker._package_close_key(larger.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["limits"][str(larger.identifier)] == 2.0
    assert state["credits"][str(larger.identifier)] == 1.0
    assert state["credits"][str(smaller.identifier)] == 1.0

    _fill_closed_package(transport, broker, smaller)
    assert state["credits"].get(str(smaller.identifier), 0.0) == 0.0
    assert state["qty"] == 0.0
    assert state["credits"][str(larger.identifier)] == 1.0


def test_delayed_fill_keeps_credit_ahead_of_a_newer_working_package(lifecycle):
    """A pulled delayed fill stays on the canceled package when that package executed."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    canceled = broker.submit_orders(
        _close_package(1, 495).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    newer = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    transport.queue_order_reads(canceled.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(canceled.identifier), canceled.strategy)
    broker._process_schwab_order_snapshot(observed)

    broker._remember_schwab_package_fills([
        _broker_snapshot(transport, canceled.identifier, status="FILLED", filledQuantity=1, averagePrice=0.40),
        _broker_snapshot(transport, newer.identifier, status="WORKING", filledQuantity=0),
    ])
    _sync_held_quantity(broker, held, 1)
    key = broker._package_close_key(canceled.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["credits"][str(canceled.identifier)] == 1.0
    assert state["credits"].get(str(newer.identifier), 0.0) == 0.0

    _fill_closed_package(transport, broker, canceled)
    assert broker._unreconciled_package_close_quantity(canceled.child_orders[0]) == 0


def test_inflight_submit_does_not_take_an_older_execution(lifecycle):
    """A submit does not move credit off an older package whose pulled fill explains it."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    older = broker.submit_orders(
        _close_package(1, 495).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    broker._remember_schwab_package_fills([
        _broker_snapshot(transport, older.identifier, status="FILLED", filledQuantity=1, averagePrice=0.40),
    ])
    original_place = transport.place_order

    def place_order(account_hash, order_spec):
        broker._last_positions_refresh_degraded = False
        broker._pull_positions = lambda _strategy: [Position("fixture", held, 1)]
        broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
        return original_place(account_hash, order_spec)

    transport.place_order = place_order
    newer = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    key = broker._package_close_key(older.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["credits"].get(str(older.identifier), 0.0) == 1.0
    assert state["credits"].get(str(newer.identifier), 0.0) == 0.0

    _fill_closed_package(transport, broker, older)
    assert broker._unreconciled_package_close_quantity(older.child_orders[0]) == 0


def test_replaced_close_keeps_its_drop_credit_on_the_new_order_id(lifecycle):
    """A same-effect replace moves the close credit so the fill can spend it."""
    import copy

    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    old_id = str(parent.identifier)
    _sync_held_quantity(broker, held, 1)
    key = broker._package_close_key(parent.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    assert state["credits"][old_id] == 1.0
    assert state["limits"][old_id] == 1.0

    assert broker.modify_order(parent, limit_price=0.35) is True
    new_id = str(parent.identifier)
    assert new_id != old_id
    assert old_id not in state["credits"]
    assert old_id not in state["limits"]
    assert state["credits"][new_id] == 1.0
    assert state["limits"][new_id] == 1.0
    assert state["absorbed"] == 1.0

    snapshot = copy.deepcopy(transport._base[old_id])
    snapshot["orderId"] = new_id
    transport._base[new_id] = snapshot
    transport._latest[new_id] = copy.deepcopy(snapshot)
    _fill_closed_package(transport, broker, parent)

    assert state["qty"] == 0.0
    assert state["credits"].get(new_id, 0.0) == 0.0
    assert broker._unreconciled_package_close_quantity(parent.child_orders[0]) == 0
    assert broker.submit_order(_close_package(1, 480)) is not None


def test_replace_keeps_the_close_ahead_of_a_newer_package(lifecycle):
    """A same-effect replace keeps this package ahead of a newer close."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 3))
    older = broker.submit_orders(
        _close_package(1, 495).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    newer = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    key = broker._package_close_key(older.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    old_id = str(older.identifier)
    assert list(state["limits"]) == [old_id, str(newer.identifier)]

    assert broker.modify_order(older, limit_price=0.35) is True
    replaced_id = str(older.identifier)
    assert list(state["limits"]) == [replaced_id, str(newer.identifier)]
    assert old_id not in state["limits"]

    _sync_held_quantity(broker, held, 2)
    assert state["credits"].get(replaced_id, 0.0) == 1.0
    assert state["credits"].get(str(newer.identifier), 0.0) == 0.0


def test_replace_does_not_restore_room_a_fill_already_removed(lifecycle):
    """A recovery anchor of the new id cannot raise the limit a fill note reduced."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 4))
    legs = [
        _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE, quantity=2),
        _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN, quantity=2),
    ]
    for leg in legs:
        leg.strategy = "fixture"
    parent = broker.submit_orders(legs, is_multileg=True, order_type="credit", price=1.25)[0]
    _fill_one_package(transport, broker, parent)
    key = broker._package_close_key(parent.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    old_id = str(parent.identifier)
    assert state["limits"][old_id] == 1.0

    replacement_id = f"{old_id}-r"
    state["limits"][replacement_id] = 2.0
    assert broker.modify_order(parent, limit_price=0.35) is True
    assert str(parent.identifier) == replacement_id
    assert old_id not in state["limits"]
    assert state["limits"][replacement_id] == 1.0
    assert list(state["limits"]) == [replacement_id]


def test_replace_publishes_the_new_id_with_its_close_credit(lifecycle):
    """A fill cannot observe the replacement id before that id holds the credit."""
    import threading

    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    parent = broker.submit_orders(
        _close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40,
    )[0]
    _sync_held_quantity(broker, held, 1)
    key = broker._package_close_key(parent.child_orders[0])
    state = broker._unreconciled_package_closes[key]
    old_id = str(parent.identifier)
    assert state["credits"][old_id] == 1.0

    saw = threading.Event()
    observed = {}
    original_identifier = Order.identifier

    def note_fill():
        acquired = broker._lock.acquire(blocking=False)
        observed["unlocked"] = acquired
        if acquired:
            broker._lock.release()
        saw.set()
        broker._note_package_close_fill(parent, parent.quantity)

    def publish(order, value):
        original_identifier.fset(order, value)
        if order is not parent or str(value) == old_id or observed.get("filler") is not None:
            return
        filler = threading.Thread(target=note_fill)
        filler.start()
        observed["filler"] = filler
        assert saw.wait(2)

    Order.identifier = property(original_identifier.fget, publish)
    try:
        assert broker.modify_order(parent, limit_price=0.35) is True
        observed["filler"].join(timeout=2)
    finally:
        Order.identifier = original_identifier

    assert observed["unlocked"] is False
    assert not observed["filler"].is_alive()
    assert state["qty"] == 0.0
    assert state["credits"].get(str(parent.identifier), 0.0) == 0.0


def test_repeated_close_legs_share_one_summed_limit(lifecycle):
    """Close legs of one contract cap credit at their combined quantity."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    other = _option_leg(490, "PUT", Order.OrderSide.BUY_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 3))
    first = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE, quantity=1)
    second = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE, quantity=2)
    opening = _option_leg(500, "PUT", Order.OrderSide.BUY_TO_OPEN, quantity=9)
    distinct = _option_leg(490, "PUT", Order.OrderSide.BUY_TO_CLOSE, quantity=4)
    for leg in (first, second, opening, distinct):
        leg.strategy = "fixture"
    parent = Order(
        "fixture",
        Asset("SPY", asset_type=Asset.AssetType.MULTILEG),
        1,
        Order.OrderSide.SELL,
        limit_price=0.40,
        order_class=Order.OrderClass.MULTILEG,
        child_orders=[first, opening, second, distinct],
        identifier="same-contract",
    )
    same_key = broker._package_close_key(first)
    baselines = {same_key: {"position_abs": 6.0, "qty": 0.0, "credits": {}}}

    broker._anchor_package_close_positions(parent, baselines)
    broker._anchor_package_close_positions(parent, baselines)

    same = broker._unreconciled_package_closes[same_key]
    separate = broker._unreconciled_package_closes[broker._package_close_key(distinct)]
    assert same["limits"]["same-contract"] == 3.0
    assert same["credits"]["same-contract"] == 3.0
    assert separate["limits"]["same-contract"] == 4.0
    assert "same-contract" not in separate["credits"]


def test_position_sync_clears_a_hold_before_the_contract_reopens(lifecycle):
    """Removing the position during sync clears the hold, so a reopened contract can close."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    position = Position("fixture", held, 1)
    broker._filled_positions.append(position)
    parent = broker.submit_orders(_close_package(1, 490).child_orders, is_multileg=True, order_type="credit", price=0.40)[0]
    execution = _package_execution(1.20)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert broker.submit_order(_close_package(1, 480)) is None

    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda _strategy: []
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())
    reopened = Position("fixture", held, 1)
    broker._pull_positions = lambda _strategy: [reopened]
    broker.sync_positions(type("Strategy", (), {"name": "fixture"})())

    assert broker.submit_order(_close_package(1, 470)) is not None


def test_partial_fill_reopens_legs_closed_by_a_local_error(lifecycle):
    """A later partial fill reopens legs that a local error had closed."""
    transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    legs = [
        _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE, quantity=2),
        _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN, quantity=2),
    ]
    for leg in legs:
        leg.strategy = "fixture"
    parent = broker.submit_orders(legs, is_multileg=True, order_type="credit", price=1.25)[0]
    _TRANSPORT.drain_stream_events(broker)
    broker._process_error_order(parent, RuntimeError("replace failed"))
    assert all(not child.is_active() for child in parent.child_orders)

    _fill_one_package(transport, broker, parent)

    assert parent.is_active()
    assert not parent._closed_event.is_set()
    assert all(child.is_active() and not child._closed_event.is_set() for child in parent.child_orders)
    assert broker.submit_order(_close_package(1, 480)) is None


def test_confirmed_fill_after_cancel_marks_the_legs_filled(lifecycle):
    """A confirmed fill after a cancel marks the package legs filled without a child trade."""
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    _TRANSPORT.drain_stream_events(broker)
    transport.queue_order_reads(parent.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert all(child.is_canceled() for child in parent.child_orders)

    execution = _package_execution(2.10)
    transport.queue_order_reads(parent.identifier, {
        "status": "FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)

    assert parent.is_filled()
    assert all(child.is_filled() and child._closed_event.is_set() for child in parent.child_orders)
    assert all(not child.transactions for child in parent.child_orders)


def test_held_submit_error_closes_the_package_legs(lifecycle):
    """A submit error released from the held queue closes the package legs."""
    transport, broker, _queues = lifecycle
    parent = broker._multileg_parent_from_orders(
        _fixture_put_credit_legs(), order_type="credit", price=1.25,
    )
    broker._hold_trade_events = True
    transport.queue_submission_response(400, "Invalid request data")

    assert broker.submit_order(parent) is None
    _TRANSPORT.drain_stream_events(broker)
    assert broker._held_trades
    assert any(child.is_active() for child in parent.child_orders)

    broker._hold_trade_events = False
    broker.process_held_trades()

    assert parent.is_canceled()
    assert all(child.status == broker.ERROR_ORDER and child._closed_event.is_set() for child in parent.child_orders)
    assert not any(child.is_active() for child in parent.child_orders)


def _recovered_single_close(**fields):
    """A tagged single-leg close for two contracts, as Schwab reports it."""
    snapshot = _working_single_snapshot(tag="fixture", quantity=2, **fields)
    snapshot["orderLegCollection"][0]["instruction"] = "SELL_TO_CLOSE"
    snapshot["orderLegCollection"][0]["quantity"] = 2
    return snapshot


def test_recovered_single_leg_later_fill_is_priced_by_its_increment(lifecycle):
    """Schwab's average is cumulative: 1 at 1.10 then 2 averaging 1.20 is a new fill at 1.30."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(filledQuantity=1, averagePrice=1.10), "fixture",
    ))
    tracked = broker.get_tracked_order("single-recovered")
    _priority_events(queues)

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="FILLED", filledQuantity=2, averagePrice=1.20), "fixture",
    ))

    fills = [
        payload for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]
    assert [(fill["price"], fill["quantity"]) for fill in fills] == [(pytest.approx(1.30), 1)]
    # The cached average follows the broker, not the value seen at adoption.
    assert tracked.get_fill_price() == pytest.approx(1.20)


def test_recovered_package_with_incomplete_executions_is_not_priced_at_the_limit(lifecycle):
    """One leg's execution cannot price the package, and the limit fallback is not a fill."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    working = _quantity_close_snapshot(tag="fixture", quantity=2, filledQuantity=1, price=1.20)
    working["orderActivityCollection"] = [
        {"activityType": "EXECUTION", "executionLegs": [{"legId": 1, "quantity": 1, "price": 2.10}]},
    ]

    broker._process_schwab_order_snapshot(broker._parse_broker_order(working, "fixture"))

    tracked = broker.get_tracked_order("spread-recovered")
    assert [(item.price, item.quantity) for item in tracked.transactions] == [(None, 1)]
    assert tracked.get_fill_price() is None


def test_unreadable_package_snapshot_keeps_its_earlier_fill(lifecycle, monkeypatch):
    """A snapshot that cannot be read is not evidence that the package fill went away."""
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    filled = _broker_snapshot(transport, parent.identifier, status="FILLED", filledQuantity=1, averagePrice=1.25)
    broker._remember_schwab_package_fills([filled])
    assert broker._schwab_seen_package_fills == {str(parent.identifier): 1.0}

    broker._remember_schwab_package_fills([], unparsed_ids=[parent.identifier, "never-filled"])
    assert broker._schwab_seen_package_fills == {str(parent.identifier): 1.0}

    monkeypatch.setattr(broker, "_parse_broker_order", lambda *_args: None)
    broker._remember_schwab_package_fills([filled])
    assert broker._schwab_seen_package_fills == {str(parent.identifier): 1.0}

    # Gone from the pull altogether (aged out of history), it is dropped.
    broker._remember_schwab_package_fills([])
    assert broker._schwab_seen_package_fills == {}


def test_unknown_row_is_not_claimed_when_another_strategy_holds_the_asset(lifecycle):
    """An unlabeled account row may be another strategy's holding when that strategy has a row."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    close = Order("fixture", held, 1, Order.OrderSide.SELL_TO_CLOSE, limit_price=1.00)
    broker._filled_positions.append(Position("Unknown", held, 2))
    assert broker._position_for_order(close) is not None

    broker._filled_positions.append(Position("other-strategy", held, 2))

    assert broker._position_for_order(close) is None


def test_recovered_stop_limit_keeps_its_limit_and_tag_through_a_replace(lifecycle):
    """A recovered stock stop-limit can be repriced; the replacement keeps the limit and the tag."""
    _transport, broker, _queues = lifecycle
    snapshot = {
        "orderId": "stop-limit-recovered",
        "orderType": "STOP_LIMIT",
        "orderStrategyType": "SINGLE",
        "status": "WORKING",
        "quantity": 10,
        "filledQuantity": 0,
        "price": 95.0,
        "stopPrice": 96.0,
        "duration": "DAY",
        "enteredTime": "2026-06-01T14:30:00+0000",
        "tag": "fixture",
        "orderLegCollection": [{
            "legId": 1,
            "instruction": "SELL",
            "quantity": 10,
            "orderLegType": "EQUITY",
            "instrument": {"assetType": "EQUITY", "symbol": "SPY"},
        }],
    }
    order = broker._parse_broker_order(snapshot, "fixture")
    assert order.stop_limit_price == 95.0

    stop_only = broker._prepare_replacement_order_spec(order, snapshot, None, 97.0)
    assert (stop_only["price"], stop_only["stopPrice"], stop_only["tag"]) == ("95.00", "97.0", "fixture")

    new_limit = broker._prepare_replacement_order_spec(order, snapshot, 94.0, None)
    assert new_limit["price"] == "94.00"
    assert order.stop_limit_price == 95.0  # building a spec does not commit the change

    broker._set_schwab_limit_price(order, 94.0)
    assert (order.limit_price, order.stop_limit_price) == (94.0, 94.0)


def test_sync_importer_values_an_unpriced_recovered_fill_at_the_average(lifecycle):
    """An unpriced fill no longer breaks the shared importer with Decimal("None")."""
    _transport, broker, _queues = lifecycle
    tracked = Order("fixture", Asset("SPY"), 2, Order.OrderSide.BUY, limit_price=10, identifier="synced-1")
    tracked.add_transaction(None, 1)
    broker._process_new_order(tracked)
    incoming = Order("fixture", Asset("SPY"), 2, Order.OrderSide.BUY, limit_price=10, identifier="synced-1")
    incoming.status = Order.OrderStatus.FILLED
    incoming.filled_quantity = 2
    incoming.avg_fill_price = 10.5

    broker._process_broker_synced_order(incoming)

    assert [(item.price, float(item.quantity)) for item in tracked.transactions] == [(None, 1.0), (10.5, 1.0)]


def test_a_limit_fallback_prices_the_new_fill_directly(lifecycle):
    """With no executions or averagePrice the limit is not a cumulative average: 9 then a 10 limit is 10, not 11."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(filledQuantity=1, averagePrice=9.0), "fixture",
    ))
    _priority_events(queues)

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="FILLED", filledQuantity=2, price=10.0), "fixture",
    ))

    fills = [
        payload for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]
    assert [(fill["price"], fill["quantity"]) for fill in fills] == [(10.0, 1)]


def _sync_account_holding(broker, held, quantity):
    """Run a named position sync where Schwab reports one holding for the account."""
    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda strategy: [Position(strategy.name, held, quantity)]
    broker.sync_positions(broker._position_sync_strategy())


def test_named_sync_does_not_claim_an_unknown_row_another_strategy_may_own(lifecycle):
    """The sync keeps an Unknown row unclaimed while another strategy holds the asset."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    unknown = Position("Unknown", held, 2)
    broker._filled_positions.append(unknown)
    broker._filled_positions.append(Position("other-strategy", held, 2))

    _sync_account_holding(broker, held, 3)

    assert unknown.strategy == "Unknown"
    assert float(unknown.quantity) == 3
    assert not [row for row in broker._filled_positions.get_list() if row.strategy == "fixture"]
    close = Order("fixture", held, 1, Order.OrderSide.SELL_TO_CLOSE, limit_price=1.00)
    assert broker._position_for_order(close) is None


def test_named_sync_still_claims_an_unknown_row_nobody_else_holds(lifecycle):
    """Negative control: with no other owner the Unknown row becomes this strategy's."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    unknown = Position("Unknown", held, 2)
    broker._filled_positions.append(unknown)

    _sync_account_holding(broker, held, 3)

    assert unknown.strategy == "fixture"
    assert float(unknown.quantity) == 3


def _partial_recovered_snapshot():
    """A tagged two-contract order Schwab reports as PARTIALLY_FILLED, one filled at 2.00."""
    snapshot = _working_single_snapshot(
        orderId="partial-recovered", tag="fixture", quantity=2,
        status="PARTIALLY_FILLED", filledQuantity=1, averagePrice=2.00,
    )
    snapshot["orderLegCollection"][0]["quantity"] = 2
    return snapshot


def test_working_orders_are_recovered_before_initialize(lifecycle):
    """set_strategy_name runs right before initialize(), so recovery cannot wait for the first poll."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._broker_fully_ready = True
    broker._strategy_name = ""
    transport.queue_history_response([
        _recovered_single_close(filledQuantity=1, averagePrice=1.10),
        _partial_recovered_snapshot(),
        _working_single_snapshot(orderId="foreign-working", tag="other"),
    ])
    starts = []
    pull = broker.client.get_orders_for_account

    def recording_pull(account_hash, **kwargs):
        starts.append(kwargs.get("from_entered_datetime"))
        return pull(account_hash, **kwargs)

    broker.client.get_orders_for_account = recording_pull

    broker.set_strategy_name("fixture")

    # A GTC close can be older than the poll's 7 days; recovery reads 59 days,
    # a day inside Schwab's 60-day limit so the request is never out of range.
    from datetime import datetime, timezone
    assert len(starts) == 1
    assert 58.9 < (datetime.now(timezone.utc) - starts[0]).total_seconds() / 86400 < 59.1

    recovered = broker.get_tracked_order("single-recovered")
    assert recovered is not None
    assert [(item.price, item.quantity) for item in recovered.transactions] == [(1.10, 1)]
    partial = broker.get_tracked_order("partial-recovered")
    assert float(partial.quantity) == 2
    assert [(item.price, float(item.quantity)) for item in partial.transactions] == [(2.00, 1.0)]
    assert broker.get_tracked_order("foreign-working") is None


def test_a_stale_pull_does_not_lower_a_remembered_package_fill(lifecycle):
    """Fills only grow, so a successful pull reporting zero keeps the remembered fill."""
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    broker._remember_schwab_package_fills([
        _broker_snapshot(transport, parent.identifier, status="FILLED", filledQuantity=1, averagePrice=1.25),
    ])

    broker._remember_schwab_package_fills([
        _broker_snapshot(transport, parent.identifier, status="WORKING", filledQuantity=0),
    ])

    assert broker._schwab_seen_package_fills == {str(parent.identifier): 1.0}


def test_a_tracked_order_with_a_user_tag_is_not_skipped_by_the_poll(lifecycle):
    """A tag like "entry-42" is a user label; the order is still this strategy's by its id."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    order = Order("fixture", _option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN).asset, 1,
                  Order.OrderSide.BUY_TO_OPEN, limit_price=2.00)
    order.tag = "entry-42"
    submitted = broker.submit_order(order)
    _TRANSPORT.drain_stream_events(broker)
    filled = _broker_snapshot(transport, submitted.identifier, status="FILLED", filledQuantity=1, averagePrice=1.95)
    # Schwab's tag is the strategy, so recovery finds it after a restart; the
    # custom label stays on the local order.
    assert filled.get("tag") == "fixture"
    assert submitted.tag == "entry-42"
    transport.queue_history_response([filled])

    broker.stream.dispatch(broker.POLL_EVENT, wait_until_complete=True)

    assert submitted.is_filled()
    assert [(item.price, float(item.quantity)) for item in submitted.transactions] == [(1.95, 1.0)]
    # The strategy's own fill reaches on_filled_order despite the custom tag.
    fills = [payload for name, payload in _priority_events(_queues) if name == broker.FILLED_ORDER]
    assert [fill["order"].identifier for fill in fills] == [submitted.identifier]


def test_a_recovered_order_with_fills_is_partially_filled_without_a_callback(lifecycle):
    """Recovery restores the partial status and bucket; a later WORKING snapshot keeps them."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    snapshot = _recovered_single_close(filledQuantity=1, averagePrice=1.10)

    broker._process_schwab_order_snapshot(broker._parse_broker_order(snapshot, "fixture"))

    tracked = broker.get_tracked_order("single-recovered")
    assert tracked.status == Order.OrderStatus.PARTIALLY_FILLED
    assert tracked in broker._partially_filled_orders
    assert tracked not in broker._new_orders
    assert not [name for name, _payload in _priority_events(queues) if name in {
        broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER,
    }]

    broker._process_schwab_order_snapshot(broker._parse_broker_order(snapshot, "fixture"))
    assert tracked.status == Order.OrderStatus.PARTIALLY_FILLED


def test_first_iteration_sync_imports_only_this_strategys_working_orders(lifecycle):
    """sync_orders parses every account order under this strategy's name; only the tag proves ownership."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._first_iteration = True
    transport.queue_history_response([
        _working_single_snapshot(orderId="sync-untagged"),
        _working_single_snapshot(orderId="sync-foreign", tag="other"),
        _working_single_snapshot(orderId="sync-owned", tag="fixture"),
    ])

    broker.sync_orders(broker._position_sync_strategy())

    assert broker.get_tracked_order("sync-untagged") is None
    assert broker.get_tracked_order("sync-foreign") is None
    assert broker.get_tracked_order("sync-owned") is not None


def test_a_later_average_prices_an_unpriced_recovered_fill(lifecycle):
    """Same quantity, now with averagePrice: the average is filled in and no fill is added."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _quantity_close_snapshot(tag="fixture", quantity=2, filledQuantity=1, price=1.20), "fixture",
    ))
    tracked = broker.get_tracked_order("spread-recovered")
    assert tracked.get_fill_price() is None

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _quantity_close_snapshot(tag="fixture", quantity=2, filledQuantity=1, price=1.20, averagePrice=1.05),
        "fixture",
    ))

    assert tracked.get_fill_price() == pytest.approx(1.05)
    assert [(item.price, item.quantity) for item in tracked.transactions] == [(None, 1)]
    assert not [name for name, _payload in _priority_events(queues) if name in {
        broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER,
    }]


def test_a_fill_priced_at_the_limit_is_replaced_by_the_later_average(lifecycle):
    """An estimated fill is not an execution: 1 at the 10 limit, then 2 averaging 9.50, prices the second at 9.50."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(_recovered_single_close(), "fixture"))
    tracked = broker.get_tracked_order("single-recovered")

    for snapshot in (
        _recovered_single_close(status="PARTIALLY_FILLED", filledQuantity=1, price=10.0),
        _recovered_single_close(status="FILLED", filledQuantity=2, averagePrice=9.50),
    ):
        broker._process_schwab_order_snapshot(broker._parse_broker_order(snapshot, "fixture"))

    fills = [
        (payload["price"], payload["quantity"]) for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]
    assert fills == [(10.0, 1), (pytest.approx(9.50), 1)]
    assert tracked.get_fill_price() == pytest.approx(9.50)


def _recovered_single_close_of(quantity, **fields):
    """A tagged single-leg close for ``quantity`` contracts."""
    snapshot = _working_single_snapshot(tag="fixture", quantity=quantity, **fields)
    snapshot["orderLegCollection"][0]["instruction"] = "SELL_TO_CLOSE"
    snapshot["orderLegCollection"][0]["quantity"] = quantity
    return snapshot


def _fill_events(broker, queues):
    return [
        (payload["price"], payload["quantity"]) for name, payload in _priority_events(queues)
        if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}
    ]


def test_the_callback_sees_the_new_average(lifecycle):
    """The order's average is updated before the strategy hears about the fill."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(filledQuantity=1, averagePrice=1.10), "fixture",
    ))
    seen = []
    publish = broker._process_trade_event

    def recording(order, event, **kwargs):
        seen.append(order.avg_fill_price)
        return publish(order, event, **kwargs)

    broker._process_trade_event = recording

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="FILLED", filledQuantity=2, averagePrice=1.20), "fixture",
    ))

    assert seen == [pytest.approx(1.20)]


def test_a_same_quantity_average_replaces_a_limit_estimate(lifecycle):
    """A fill priced at the limit gets Schwab's average later without another fill."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(_recovered_single_close(), "fixture"))
    tracked = broker.get_tracked_order("single-recovered")
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="PARTIALLY_FILLED", filledQuantity=1, price=10.0), "fixture",
    ))
    assert _fill_events(broker, queues) == [(10.0, 1)]

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="PARTIALLY_FILLED", filledQuantity=1, averagePrice=9.0), "fixture",
    ))

    assert tracked.get_fill_price() == pytest.approx(9.0)
    # The estimate is rewritten to Schwab's confirmed average; no fill is added.
    assert [(item.price, float(item.quantity)) for item in tracked.transactions] == [(9.0, 1.0)]
    assert _fill_events(broker, queues) == []


def test_a_same_quantity_average_keeps_a_confirmed_fill(lifecycle):
    """1 confirmed at 10, then 1 at the 12 limit, then the same 2 averaging 9: the estimate becomes 8.

    The next unit, at 3 averaging 9, is 9 and the ledger is 27. Writing 9 onto
    the estimate would leave 10 + 9 and price that unit at 9 too, totaling 28.
    """
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 3))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(_recovered_single_close_of(3), "fixture"))

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close_of(3, status="PARTIALLY_FILLED", filledQuantity=1, averagePrice=10.0),
        "fixture",
    ))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close_of(3, status="PARTIALLY_FILLED", filledQuantity=2, price=12.0),
        "fixture",
    ))
    assert _fill_events(broker, queues) == [(10.0, 1), (12.0, 1)]

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close_of(3, status="PARTIALLY_FILLED", filledQuantity=2, averagePrice=9.0),
        "fixture",
    ))

    tracked = broker.get_tracked_order("single-recovered")
    assert [(item.price, float(item.quantity)) for item in tracked.transactions] == [(10.0, 1.0), (8.0, 1.0)]
    assert _fill_events(broker, queues) == []

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close_of(3, status="FILLED", filledQuantity=3, averagePrice=9.0),
        "fixture",
    ))

    assert _fill_events(broker, queues) == [(pytest.approx(9.0), 1)]
    assert sum(float(item.price) * float(item.quantity) for item in tracked.transactions) == pytest.approx(27.0)


def test_a_confirmed_cumulative_value_prices_later_fills(lifecycle):
    """1 at a 10 limit, 2 averaging 9.50 (19 confirmed), then 3 averaging 9.60: the third unit is 9.80."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 3))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(_recovered_single_close_of(3), "fixture"))

    for snapshot in (
        _recovered_single_close_of(3, status="PARTIALLY_FILLED", filledQuantity=1, price=10.0),
        _recovered_single_close_of(3, status="PARTIALLY_FILLED", filledQuantity=2, averagePrice=9.50),
        _recovered_single_close_of(3, status="FILLED", filledQuantity=3, averagePrice=9.60),
    ):
        broker._process_schwab_order_snapshot(broker._parse_broker_order(snapshot, "fixture"))

    assert _fill_events(broker, queues) == [(10.0, 1), (pytest.approx(9.50), 1), (pytest.approx(9.80), 1)]
    # The 10.00 estimate was rewritten when Schwab confirmed 9.50, so the ledger
    # totals Schwab's 3 x 9.60 = 28.80 rather than 29.30.
    tracked = broker.get_tracked_order("single-recovered")
    assert sum(float(item.price) * float(item.quantity) for item in tracked.transactions) == pytest.approx(28.80)


def test_first_iteration_sync_tracks_a_package_but_not_its_legs(lifecycle):
    """Legs copy the parent's tag; recovering one alone would track a synthetic leg id."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._first_iteration = True
    transport.queue_history_response([_working_close_snapshot(tag="fixture")])

    broker.sync_orders(broker._position_sync_strategy())

    assert broker.get_tracked_order("spread-recovered") is not None
    assert broker.get_tracked_order("spread-recovered-leg-1") is None
    assert broker.get_tracked_order("spread-recovered-leg-2") is None


def test_first_iteration_sync_imports_only_this_strategys_history(lifecycle):
    """Filled history needs this strategy's tag too; untagged and foreign history stays out."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._first_iteration = True
    filled = {"status": "FILLED", "filledQuantity": 1, "averagePrice": 2.00}
    transport.queue_history_response([
        _working_single_snapshot(orderId="history-untagged", **filled),
        _working_single_snapshot(orderId="history-foreign", tag="other", **filled),
        _working_single_snapshot(orderId="history-owned", tag="fixture", **filled),
    ])

    broker.sync_orders(broker._position_sync_strategy())

    assert broker.get_tracked_order("history-untagged") is None
    assert broker.get_tracked_order("history-foreign") is None
    assert broker.get_tracked_order("history-owned") is not None


def test_a_locally_submitted_order_belongs_only_to_its_own_strategy(lifecycle):
    """The custom-tag shortcut does not make strategy A's order strategy B's."""
    _transport, broker, _queues = lifecycle
    order = Order("strategy-a", Asset("SPY"), 1, Order.OrderSide.BUY, limit_price=1.00)
    order.tag = "entry-42"
    order._schwab_submitted_locally = True

    assert not broker.order_is_foreign_to_local_strategy(order, "strategy-a")
    assert broker.order_is_foreign_to_local_strategy(order, "strategy-b")
    # The positive check agrees: strategy A owns it, strategy B does not.
    assert broker.order_belongs_to_local_strategy(order, "strategy-a")
    assert not broker.order_belongs_to_local_strategy(order, "strategy-b")


def test_schwab_tag_is_the_strategy_even_with_a_custom_order_tag(lifecycle):
    """Recovery matches the strategy name, so a custom tag stays local and Schwab gets the strategy."""
    transport, broker, _queues = lifecycle
    order = Order("fixture", _option_leg(500, "CALL", Order.OrderSide.BUY_TO_OPEN).asset, 1,
                  Order.OrderSide.BUY_TO_OPEN, limit_price=2.00)
    order.tag = "entry-42"
    submitted = broker.submit_order(order)
    sent = _broker_snapshot(transport, submitted.identifier)

    replacement = broker._prepare_replacement_order_spec(submitted, sent, 2.10, None)

    assert (sent["tag"], replacement["tag"], submitted.tag) == ("fixture", "fixture", "entry-42")
    # After a restart, recovery recognizes it by that tag.
    restarted = broker._parse_broker_order(sent, "fixture")
    assert broker._schwab_order_owned_by_tag(restarted)


def test_imported_canceled_history_keeps_its_fill_and_does_not_replay_it(lifecycle):
    """A tagged canceled order with 1 of 2 filled is history; the next poll is not a new fill."""
    transport, broker, queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._first_iteration = True
    canceled = _recovered_single_close(status="CANCELED", filledQuantity=1, averagePrice=1.10)
    transport.queue_history_response([canceled])

    broker.sync_orders(broker._position_sync_strategy())

    imported = broker.get_tracked_order("single-recovered")
    assert [(item.price, float(item.quantity)) for item in imported.transactions] == [(1.10, 1.0)]
    _priority_events(queues)
    broker._process_schwab_order_snapshot(broker._parse_broker_order(canceled, "fixture"))
    assert _fill_events(broker, queues) == []


def test_imported_filled_history_without_executions_stays_unpriced(lifecycle):
    """With no executions or averagePrice the parser's average is the limit, not a fill price."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._first_iteration = True
    transport.queue_history_response([
        _working_single_snapshot(orderId="history-unpriced", tag="fixture", status="FILLED", filledQuantity=1),
    ])

    broker.sync_orders(broker._position_sync_strategy())

    imported = broker.get_tracked_order("history-unpriced")
    assert [(item.price, float(item.quantity)) for item in imported.transactions] == [(None, 1.0)]
    assert imported.get_fill_price() is None


def test_imported_canceled_history_keeps_a_full_unpriced_fill(lifecycle):
    """A 1-lot canceled at the limit, with no average, stays tracked with that unpriced fill.

    Recording the fill before the importer runs makes the quantity look fully
    executed, so the canceled order is marked filled and never tracked.
    """
    transport, broker, queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._first_iteration = True
    canceled = _working_single_snapshot(
        orderId="history-canceled-full", tag="fixture", status="CANCELED", filledQuantity=1,
    )
    transport.queue_history_response([canceled])

    broker.sync_orders(broker._position_sync_strategy())

    imported = broker.get_tracked_order("history-canceled-full")
    assert imported is not None
    assert imported.status == broker.CANCELED_ORDER
    assert [(item.price, float(item.quantity)) for item in imported.transactions] == [(None, 1.0)]
    _priority_events(queues)
    broker._process_schwab_order_snapshot(broker._parse_broker_order(canceled, "fixture"))
    # One drain sees both a replayed fill and a replayed cancellation.
    assert _priority_events(queues) == []


def test_a_filled_order_is_corrected_when_the_average_arrives(lifecycle):
    """Filled at a 10 limit estimate; a later FILLED snapshot averaging 9.00 corrects it without a new fill."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(_recovered_single_close(), "fixture"))
    tracked = broker.get_tracked_order("single-recovered")
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="FILLED", filledQuantity=2, price=10.0), "fixture",
    ))
    assert tracked.is_filled()
    assert _fill_events(broker, queues) == [(10.0, 2)]

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="FILLED", filledQuantity=2, averagePrice=9.0), "fixture",
    ))

    assert tracked.get_fill_price() == pytest.approx(9.0)
    assert [(item.price, float(item.quantity)) for item in tracked.transactions] == [(9.0, 2.0)]
    assert _fill_events(broker, queues) == []


def test_a_filled_average_without_a_quantity_still_corrects_the_estimate(lifecycle):
    """A later FILLED snapshot may bring only the average. The quantity is already on the order."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(_recovered_single_close(), "fixture"))
    tracked = broker.get_tracked_order("single-recovered")
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="FILLED", filledQuantity=2, price=10.0), "fixture",
    ))
    _fill_events(broker, queues)

    correction = _recovered_single_close(status="FILLED", averagePrice=9.0)
    correction.pop("filledQuantity", None)
    broker._process_schwab_order_snapshot(broker._parse_broker_order(correction, "fixture"))

    assert tracked.get_fill_price() == pytest.approx(9.0)
    assert [(item.price, float(item.quantity)) for item in tracked.transactions] == [(9.0, 2.0)]
    assert _fill_events(broker, queues) == []


def test_history_import_marks_the_fill_before_releasing_the_poll_lock(lifecycle):
    """The poll takes the transition lock before it can read the imported order."""
    transport, broker, _queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._first_iteration = True
    owned = []
    commit = broker._commit_schwab_history_import

    def recording(order):
        owned.append(broker._schwab_order_transition_lock._is_owned())
        return commit(order)

    broker._commit_schwab_history_import = recording
    transport.queue_history_response([
        _working_single_snapshot(
            orderId="history-canceled-full", tag="fixture", status="CANCELED", filledQuantity=1,
        ),
    ])

    broker.sync_orders(broker._position_sync_strategy())

    assert owned == [True]
    assert broker.get_tracked_order("history-canceled-full") is not None


def test_a_recovered_full_package_is_closed_by_the_filled_snapshot(lifecycle):
    """WORKING with every unit filled is recovered as history; FILLED at that quantity closes it and its legs."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 1))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _working_close_snapshot(tag="fixture", filledQuantity=1, averagePrice=0.40), "fixture",
    ))
    tracked = broker.get_tracked_order("spread-recovered")
    assert not tracked.is_filled()

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _working_close_snapshot(status="FILLED", tag="fixture", filledQuantity=1, averagePrice=0.40), "fixture",
    ))

    assert tracked.is_filled()
    assert tracked._closed_event.is_set()
    assert all(leg.is_filled() and leg._closed_event.is_set() for leg in tracked.child_orders)
    assert _fill_events(broker, queues) == []


def test_an_older_position_snapshot_does_not_overwrite_a_newer_one(lifecycle):
    """A sync whose pull started first but finished last is discarded."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._last_positions_refresh_degraded = False

    def older_pull(strategy):
        # A newer sync starts and finishes while this older pull is in flight.
        broker._pull_positions = lambda newer: [Position(newer.name, held, 1)]
        broker.sync_positions(broker._position_sync_strategy())
        return [Position(strategy.name, held, 3)]

    broker._pull_positions = older_pull
    broker.sync_positions(broker._position_sync_strategy())

    rows = [row for row in broker._filled_positions.get_list() if row.asset == held]
    assert [float(row.quantity) for row in rows] == [1.0]


def test_a_failed_newer_pull_does_not_discard_an_older_good_snapshot(lifecycle):
    """A newer sync that fails before returning positions is not a newer book."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 3))

    def failed_pull(_strategy):
        broker._set_positions_refresh_degraded(True)
        return []

    def older_pull(strategy):
        # A newer sync starts and fails while this older pull is in flight.
        broker._pull_positions = failed_pull
        broker.sync_positions(broker._position_sync_strategy())
        broker._set_positions_refresh_degraded(False)
        return [Position(strategy.name, held, 1)]

    broker._pull_positions = older_pull
    broker.sync_positions(broker._position_sync_strategy())

    rows = [row for row in broker._filled_positions.get_list() if row.asset == held]
    assert [float(row.quantity) for row in rows] == [1.0]


def test_an_older_account_wide_snapshot_does_not_overwrite_a_named_one(lifecycle):
    """Account-wide and named syncs write the same rows, so they share one order."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 3))
    broker._last_positions_refresh_degraded = False

    def older_account_wide_pull(_strategy):
        # A named sync starts and finishes while this account-wide pull is in flight.
        broker._pull_positions = lambda named: [Position(named.name, held, 1)]
        broker.sync_positions(broker._position_sync_strategy())
        return [Position("fixture", held, 3)]

    broker._pull_positions = older_account_wide_pull
    broker.sync_positions(None)

    rows = [row for row in broker._filled_positions.get_list() if row.asset == held]
    assert [float(row.quantity) for row in rows] == [1.0]


def test_initial_positions_after_recovery_do_not_duplicate_the_book(lifecycle):
    """Recovery can load the book before the strategy's constructor loads it again."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda strategy: [Position(strategy.name, held, 2)]

    broker.sync_positions(broker._position_sync_strategy())
    broker._set_initial_positions(broker._position_sync_strategy())

    rows = [row for row in broker._filled_positions.get_list() if row.asset == held]
    assert [float(row.quantity) for row in rows] == [2.0]


def test_imported_terminal_package_history_closes_its_legs(lifecycle):
    """A wait on a leg of a filled package from before startup returns."""
    transport, broker, _queues = lifecycle
    filled = _quantity_close_snapshot(
        status="FILLED", tag="fixture", quantity=2, filledQuantity=2, averagePrice=1.00, price=1.20,
    )
    transport.queue_history_response([filled])

    broker.sync_orders(broker._position_sync_strategy())

    imported = broker.get_tracked_order("spread-recovered")
    assert imported is not None
    assert imported.child_orders
    assert all(leg._closed_event.is_set() for leg in imported.child_orders)
    assert imported.is_filled()


def test_initial_positions_keep_the_cash_position_from_the_balance_refresh(lifecycle):
    """Schwab's position response omits cash; the starting sync must not prune it."""
    _transport, broker, _queues = lifecycle
    usd = Asset("USD", asset_type=Asset.AssetType.FOREX)
    broker.quote_assets = {usd}
    broker._filled_positions.append(Position("fixture", usd, 1000))
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._last_positions_refresh_degraded = False
    broker._pull_positions = lambda strategy: [Position(strategy.name, held, 2)]

    broker._set_initial_positions(broker._position_sync_strategy())

    assert [float(row.quantity) for row in broker._filled_positions.get_list() if row.asset == usd] == [1000.0]


def test_a_full_order_history_page_is_reported(lifecycle, mocker):
    """A page that is still full after every split may have dropped orders, so it is logged."""
    transport, broker, _queues = lifecycle
    log = mocker.patch("lumibot.brokers.schwab.logger")
    broker.SCHWAB_ORDER_HISTORY_PAGE = 2
    broker.SCHWAB_ORDER_HISTORY_SPLITS = 0
    transport.queue_history_response([
        _working_single_snapshot(orderId="page-1"),
        _working_single_snapshot(orderId="page-2"),
    ])

    assert len(broker._pull_broker_all_orders(days=59)) == 2

    assert any("page limit" in str(call.args[0]) for call in log.warning.call_args_list)


def test_an_unpriced_fill_does_not_inflate_an_estimate(lifecycle):
    """1 unpriced unit and 1 at a 10 limit, then 9.00 for both: the estimate is 9.00, not 18.00."""
    _transport, broker, _queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 3))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close_of(3, filledQuantity=1, price=10.0), "fixture",
    ))
    tracked = broker.get_tracked_order("single-recovered")
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close_of(3, status="PARTIALLY_FILLED", filledQuantity=2, price=10.0), "fixture",
    ))

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close_of(3, status="PARTIALLY_FILLED", filledQuantity=2, averagePrice=9.0), "fixture",
    ))

    assert [(item.price, float(item.quantity)) for item in tracked.transactions] == [(None, 1.0), (9.0, 1.0)]
    assert tracked.get_fill_price() == pytest.approx(9.0)


def test_imported_canceled_history_does_not_replay_its_cancel(lifecycle):
    """An order canceled before startup does not reach on_canceled_order on the next poll."""
    transport, broker, queues = lifecycle
    broker._pull_positions = lambda _strategy: []
    broker._last_positions_refresh_degraded = False
    broker._first_iteration = True
    canceled = _recovered_single_close(status="CANCELED", filledQuantity=0)
    transport.queue_history_response([canceled])
    broker.sync_orders(broker._position_sync_strategy())
    assert broker.get_tracked_order("single-recovered") is not None
    _priority_events(queues)

    broker._process_schwab_order_snapshot(broker._parse_broker_order(canceled, "fixture"))

    assert [name for name, _payload in _priority_events(queues)] == []


def test_a_full_order_history_page_is_split_into_complete_windows(lifecycle, mocker):
    """A full page may have cut off a working GTC close, so both halves are read."""
    transport, broker, _queues = lifecycle
    log = mocker.patch("lumibot.brokers.schwab.logger")
    broker.SCHWAB_ORDER_HISTORY_PAGE = 4
    broker.SCHWAB_ORDER_HISTORY_SPLITS = 1

    def page(*ids):
        return [_working_single_snapshot(orderId=identifier) for identifier in ids]

    transport.queue_history_response(page("e", "d", "c", "b"))  # full: "a" was cut off
    transport.queue_history_response(page("c", "b", "a"))  # older half
    transport.queue_history_response(page("e", "d", "c"))  # newer half; "c" is on the boundary

    pulled = broker._pull_broker_all_orders(days=59)

    assert [payload["orderId"] for payload in pulled] == ["e", "d", "c", "b", "a"]
    assert broker._last_orders_refresh_degraded is False
    assert not any("page limit" in str(call.args[0]) for call in log.warning.call_args_list)


def test_a_failed_order_pull_stays_failed_when_another_pull_succeeds(lifecycle):
    """A poll's failed empty pull is not read as history because another thread's pull worked."""
    import threading

    _transport, broker, _queues = lifecycle
    calls = []

    def window(_start, _end, _days, depth=0):
        calls.append(threading.current_thread().name)
        if len(calls) == 1:
            # Another thread's pull succeeds while this one is in flight, then this one fails.
            other = threading.Thread(target=broker._pull_broker_all_orders, name="other-sync")
            other.start()
            other.join()
            return None
        return []

    broker._pull_schwab_order_window = window

    assert broker._pull_broker_all_orders() == []

    assert calls[1] == "other-sync"
    assert broker._last_orders_refresh_degraded is False
    assert broker._orders_refresh_degraded() is True


def test_a_full_order_history_page_warns_once(lifecycle, mocker):
    """The poll repeats every 30 seconds; the page warning is logged once per window."""
    transport, broker, _queues = lifecycle
    log = mocker.patch("lumibot.brokers.schwab.logger")
    broker.SCHWAB_ORDER_HISTORY_PAGE = 1
    broker.SCHWAB_ORDER_HISTORY_SPLITS = 0
    for _ in range(3):
        transport.queue_history_response([_working_single_snapshot(orderId="page-1")])
        broker._pull_broker_all_orders()

    assert sum("page limit" in str(call.args[0]) for call in log.warning.call_args_list) == 1


def test_a_partial_snapshot_that_completes_the_order_is_the_filled_callback(lifecycle):
    """PARTIALLY_FILLED with every unit filled sends on_filled_order; the later FILLED adds nothing."""
    _transport, broker, queues = lifecycle
    held = _option_leg(500, "CALL", Order.OrderSide.SELL_TO_CLOSE).asset
    broker._filled_positions.append(Position("fixture", held, 2))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(_recovered_single_close(), "fixture"))
    tracked = broker.get_tracked_order("single-recovered")

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="PARTIALLY_FILLED", filledQuantity=1, averagePrice=1.10), "fixture",
    ))
    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="PARTIALLY_FILLED", filledQuantity=2, averagePrice=1.20), "fixture",
    ))

    events = [(name, payload["quantity"]) for name, payload in _priority_events(queues)
              if name in {broker.FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER}]
    assert events == [(broker.PARTIALLY_FILLED_ORDER, 1), (broker.FILLED_ORDER, 1)]
    assert tracked.is_filled()

    broker._process_schwab_order_snapshot(broker._parse_broker_order(
        _recovered_single_close(status="FILLED", filledQuantity=2, averagePrice=1.20), "fixture",
    ))
    assert _fill_events(broker, queues) == []


def test_a_throttled_replace_waits_out_retry_after_before_sending_again(lifecycle):
    """A 429 on replace sends nothing more, not even a read, until Retry-After ends."""
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    clock = [1000.0]
    broker._schwab_now = lambda: clock[0]
    scripted_replace = transport.replace_order

    def throttled(account_hash, identifier, order_spec):
        transport.replace_calls.append((identifier, order_spec))
        return type("Response", (), {"status_code": 429, "headers": {"Retry-After": "30"}, "text": ""})()

    transport.replace_order = throttled
    reads = []
    pull = broker._pull_broker_order
    broker._pull_broker_order = lambda identifier: reads.append(identifier) or pull(identifier)

    assert broker.modify_order(parent, limit_price=1.05) is False
    assert len(transport.replace_calls) == 1 and len(reads) == 1

    transport.replace_order = scripted_replace
    clock[0] += 10
    assert broker.modify_order(parent, limit_price=1.05) is False
    assert len(transport.replace_calls) == 1 and len(reads) == 1
    assert parent.limit_price == 1.25

    clock[0] += 21
    assert broker.modify_order(parent, limit_price=1.05) is not False
    assert len(transport.replace_calls) == 2
    assert parent.limit_price == 1.05


def test_a_strike_that_is_not_a_thousandth_has_no_occ_symbol():
    """100.1254 is not the 100.125 contract; float noise below a thousandth still is."""
    expiration = date(2026, 6, 19)

    assert Schwab._schwab_occ_symbol("SPY", expiration, "P", 100.1254) is None
    assert Schwab._schwab_occ_symbol("SPY", expiration, "P", 100.12500000001) == "SPY   260619P00100125"


def test_a_package_leg_with_an_unlisted_strike_is_refused():
    broker = _broker()
    children = [
        _option_leg(500.0004, "PUT", Order.OrderSide.SELL_TO_OPEN),
        _option_leg(490, "PUT", Order.OrderSide.BUY_TO_OPEN),
    ]

    assert broker._submit_orders(children, is_multileg=True, order_type="credit", price=1.25) is None
    assert broker.client.place_calls == []


def _close_reservation_broker(broker_class, position_quantity):
    """A bare broker holding one long option, with a working package that closes it."""
    from types import SimpleNamespace

    broker = broker_class.__new__(broker_class)
    broker.name = broker_class.__name__
    held = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE)
    package_leg = _option_leg(500, "PUT", Order.OrderSide.SELL_TO_CLOSE, quantity=position_quantity)
    package_leg.status = Order.OrderStatus.SUBMITTED
    package = SimpleNamespace(child_orders=[package_leg])
    broker._position_for_order = lambda _order: SimpleNamespace(quantity=position_quantity)
    broker.get_active_tracked_orders = lambda strategy=None, asset=None: [] if asset is not None else [package]
    broker.get_tracked_order = lambda _identifier: None
    broker._unreconciled_package_close_quantity = lambda _order: 0.0
    return broker, held


def test_a_working_package_close_reserves_its_contracts_on_schwab():
    broker, close = _close_reservation_broker(Schwab, position_quantity=2)

    with pytest.raises(ValueError):
        broker.resolve_option_order_intent(close)


def test_other_brokers_keep_their_close_reservation_lookup():
    """Package-leg reservation is Schwab's; the base lookup is unchanged on other brokers."""
    from lumibot.brokers.tradier import Tradier

    broker, close = _close_reservation_broker(Tradier, position_quantity=2)

    assert broker.resolve_option_order_intent(close) is close
