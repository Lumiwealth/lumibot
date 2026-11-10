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

    submitted = broker._submit_order(order)

    assert submitted is order
    assert len(broker.client.place_calls) == 1
    assert broker.client.place_calls[0][1]["orderType"] == "NET_CREDIT"
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
    events = []
    while True:
        try:
            events.append(queues.priority_queue.get_nowait())
        except Empty:
            return events


def _fixture_put_credit_legs():
    legs = _put_credit_children()
    for leg in legs:
        leg.strategy = "fixture"
    return legs


def test_package_fill_reaches_strategy_once_at_net_price_and_package_quantity(lifecycle):
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
    assert [leg["instrument"]["symbol"] for leg in spec["orderLegCollection"]] == [
        "SPY   260619P00500000",
        "SPY   260619P00490000",
    ]
    assert parent.identifier == f"{original_id}-r"
    assert parent.limit_price == 1.15
    assert all(child.parent_identifier == parent.identifier for child in parent.child_orders)


def test_modify_broker_parsed_multileg_keeps_credit_type(lifecycle):
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


def test_partial_package_fill_reserves_only_the_unfilled_remainder(lifecycle):
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
    execution = {"activityType": "EXECUTION", "executionLegs": [
        {"legId": 1, "quantity": 1, "price": 2.10},
        {"legId": 2, "quantity": 1, "price": 0.80},
    ]}
    transport.queue_order_reads(parent.identifier, {
        "status": "PARTIALLY_FILLED",
        "filledQuantity": 1,
        "orderActivityCollection": [execution],
    })
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)

    def _closing(quantity, other_strike):
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

    assert broker.submit_order(_closing(2, 480)) is None
    assert broker.submit_order(_closing(1, 470)) is not None
    assert len(transport.attempted_orders) == 2


def test_confirmed_fill_after_cancel_marks_the_legs_filled(lifecycle):
    transport, broker, _queues = lifecycle
    parent = broker.submit_orders(_fixture_put_credit_legs(), is_multileg=True, order_type="credit", price=1.25)[0]
    _TRANSPORT.drain_stream_events(broker)
    transport.queue_order_reads(parent.identifier, {"status": "CANCELED"})
    observed = broker._parse_broker_order(broker._pull_broker_order(parent.identifier), parent.strategy)
    broker._process_schwab_order_snapshot(observed)
    assert all(child.is_canceled() for child in parent.child_orders)

    execution = {"activityType": "EXECUTION", "executionLegs": [
        {"legId": 1, "quantity": 1, "price": 2.10},
        {"legId": 2, "quantity": 1, "price": 0.80},
    ]}
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
