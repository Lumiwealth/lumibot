"""Offline Kalshi order/account contracts through real LumiBot entities."""

import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.brokers import Kalshi
from lumibot.brokers.kalshi import KalshiStream
from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient

ASSET = Asset("TEST-MARKET", asset_type="prediction_contract")


class FakeClient:
    is_demo = True
    path_id = staticmethod(KalshiClient.path_id)

    def __init__(self):
        self.calls, self.orders, self.fills, self.positions = [], {}, [], []
        self.market = {
            "ticker": ASSET.symbol,
            "market_type": "binary",
            "status": "active",
            "last_price_dollars": "0.4321",
            "yes_bid_dollars": "0.42",
            "yes_ask_dollars": "0.44",
            "price_ranges": [{"start": "0.0000", "end": "1.0000", "step": "0.0001"}],
        }
        self.balance = {"balance": 10000, "balance_dollars": "100.1234", "portfolio_value": 500}

    def request(self, method, path, *, params=None, json=None, authenticated=True):
        self.calls.append((method, path, copy.deepcopy(params), copy.deepcopy(json)))
        if path == "/portfolio/balance":
            return copy.deepcopy(self.balance)
        if path == "/markets/TEST-MARKET":
            return {"market": copy.deepcopy(self.market)}
        if path == "/markets/candlesticks":
            return {"markets": [{"market_ticker": ASSET.symbol, "candlesticks": []}]}
        if method == "POST" and path == "/portfolio/events/orders":
            identifier = str(len(self.orders) + 1)
            self.orders[identifier] = {
                "ticker": json["ticker"],
                "order_id": identifier,
                "client_order_id": json["client_order_id"],
                "book_side": json["side"],
                "yes_price_dollars": json["price"],
                "type": "limit",
                "initial_count_fp": json["count"],
                "fill_count_fp": "0.00",
                "remaining_count_fp": json["count"],
                "status": "resting",
                "subaccount_number": json["subaccount"],
                "time_in_force": json["time_in_force"],
            }
            if json["time_in_force"] in ("immediate_or_cancel", "fill_or_kill"):
                self.orders[identifier].update(status="canceled", remaining_count_fp="0.00")
            return {"order_id": identifier, "fill_count": "0.00", "remaining_count": json["count"], "ts_ms": 1}
        if method == "GET" and path.startswith("/portfolio/orders/"):
            identifier = path.split("/")[-1]
            if identifier not in self.orders:
                raise KalshiAPIError("missing", status_code=404)
            return {"order": copy.deepcopy(self.orders[identifier])}
        if method == "DELETE":
            identifier = path.split("/")[-1]
            self.orders[identifier].update(status="canceled", remaining_count_fp="0.00")
            return {"order_id": identifier, "reduced_by": "1.00", "ts_ms": 1}
        if method == "POST" and path.endswith("/amend"):
            identifier = path.split("/")[-2]
            self.orders[identifier].update(
                yes_price_dollars=json["price"], client_order_id=json["updated_client_order_id"]
            )
            return {"order_id": identifier, "ts_ms": 1}
        raise AssertionError((method, path, params, json))

    def pages(self, path, key, *, params=None, authenticated=True):
        self.calls.append(("GET", path, copy.deepcopy(params), None))
        if path == "/portfolio/orders":
            return iter(copy.deepcopy(list(self.orders.values())))
        if path == "/portfolio/positions":
            return iter(copy.deepcopy(self.positions))
        if path == "/portfolio/fills":
            return iter(copy.deepcopy([f for f in self.fills if f["order_id"] == params["order_id"]]))
        if path.startswith("/historical/"):
            return iter([])
        raise AssertionError(path)

    def fill(self, order, quantity, price="0.4321", *, cancel=False):
        row = self.orders[order.identifier]
        filled = Decimal(row["fill_count_fp"]) + Decimal(str(quantity))
        remaining = Decimal(row["initial_count_fp"]) - filled
        row.update(
            fill_count_fp=str(filled),
            remaining_count_fp=str(remaining),
            status="executed" if remaining == 0 else "resting",
        )
        if cancel:
            row.update(status="canceled", remaining_count_fp="0.00")
        self.fills.append(
            {
                "fill_id": str(len(self.fills) + 1),
                "order_id": order.identifier,
                "count_fp": str(quantity),
                "yes_price_dollars": price,
                "fee_cost": "0.0001",
            }
        )
        signed = filled if row["book_side"] == "bid" else -filled
        self.positions = [{"ticker": ASSET.symbol, "position_fp": str(signed), "market_exposure_dollars": "0.5"}]


@pytest.fixture
def broker():
    broker = Kalshi(client=FakeClient(), connect_stream=False)
    broker.set_strategy_name("test")
    # A live strategy has an executor subscriber. Exercise that normal contract
    # rather than invoking the base broker's missing-subscriber error path.
    broker._add_subscriber(
        SimpleNamespace(
            name="test",
            add_event=Mock(),
            NEW_ORDER="new",
            CANCELED_ORDER="canceled",
            PARTIALLY_FILLED_ORDER="partial_fill",
            FILLED_ORDER="fill",
            ERROR_ORDER="error",
        )
    )
    yield broker
    broker.cleanup_streams()


def order(**kwargs):
    options = dict(
        strategy="test", asset=ASSET, quantity=2, side="buy", limit_price=0.4, order_type="limit", time_in_force="gtc"
    )
    options.update(kwargs)
    return Order(**options)


@pytest.mark.parametrize("side,book_side", [("buy", "bid"), ("sell", "ask")])
@pytest.mark.parametrize("tif", ["gtc", "ioc", "fok", "gtd"])
def test_supported_order_combinations(broker, side, book_side, tif):
    obj = order(
        side=side,
        time_in_force=tif,
        good_till_date=datetime.now(timezone.utc) + timedelta(days=1) if tif == "gtd" else None,
    )
    result = broker.submit_order(obj)
    assert result is obj
    submit = next(call for call in broker._client.calls if call[0] == "POST")
    assert submit[3]["side"] == book_side
    assert submit[3]["price"] == "0.4000"
    assert submit[3]["count"] == "2.00"
    assert ("expiration_time" in submit[3]) == (tif == "gtd")
    assert obj.was_transmitted()
    assert obj.status == (Order.OrderStatus.CANCELED if tif in ("ioc", "fok") else Order.OrderStatus.NEW)


def test_balance_units_and_total_equity(broker):
    assert broker._get_balances_at_broker(Asset("USD", asset_type="forex"), None) == (100.1234, 5, 105.1234)
    del broker._client.balance["balance_dollars"]
    assert broker._get_balances_at_broker(None, None) == (100, 5, 105)
    with pytest.raises(ValueError, match="USD"):
        broker._get_balances_at_broker(Asset("EUR", asset_type="forex"), None)
    assert broker.get_historical_account_value() == {}


@pytest.mark.parametrize("quantity", ["2.25", "-2.25", "0.00"])
def test_signed_positions_and_lookup(broker, quantity):
    broker._client.positions = [{"ticker": ASSET.symbol, "position_fp": quantity, "market_exposure_dollars": "1.125"}]
    positions = broker._pull_positions("test")
    if Decimal(quantity):
        assert Decimal(str(positions[0].quantity)) == Decimal(quantity)
        assert broker._pull_position("test", ASSET).avg_fill_price == 0.5
    else:
        assert positions == []
        assert broker._pull_position("test", ASSET) is None


@pytest.mark.parametrize("kind", ["market", "stop", "stop_limit", "trailing_stop", "smart_limit"])
def test_unsupported_order_types_send_no_requests(broker, kind):
    obj = order()
    obj.order_type = kind
    with pytest.raises(ValueError, match="limit orders only"):
        broker.submit_order(obj)
    assert broker._client.calls == []


@pytest.mark.parametrize("kind", ["bracket", "oco", "oto", "multileg"])
def test_unsupported_order_classes_send_no_requests(broker, kind):
    obj = order()
    obj.order_class = kind
    with pytest.raises(ValueError, match="simple orders"):
        broker.submit_order(obj)
    assert broker._client.calls == []


@pytest.mark.parametrize(
    "field,value,match",
    [
        ("time_in_force", "day", "time_in_force"),
        ("limit_price", 1.0, "limit price"),
        ("limit_price", 0.0, "limit price"),
        ("limit_price", 0.12345, "four decimal"),
        ("limit_price", float("nan"), "finite"),
        ("quantity", 0.001, "increments"),
        ("custom_params", {"post_only": True}, "custom"),
    ],
)
def test_invalid_order_parameters_fail_before_network(broker, field, value, match):
    obj = order()
    setattr(obj, field, value)
    with pytest.raises(ValueError, match=match):
        broker.submit_order(obj)
    assert broker._client.calls == []


def test_gtd_requires_future_aware_expiration(broker):
    for expiration in [None, datetime(2030, 1, 1), datetime(2000, 1, 1, tzinfo=timezone.utc)]:
        with pytest.raises(ValueError, match="Kalshi"):
            broker.submit_order(order(time_in_force="gtd", good_till_date=expiration))


@pytest.mark.parametrize(
    "change",
    [
        {"status": "closed"},
        {"market_type": "scalar"},
        {"mve_collection_ticker": "MULTI"},
        {"price_ranges": [{"start": "0", "end": "1", "step": "0.03"}]},
        {"price_ranges": []},
    ],
)
def test_market_rules_reject_without_submitting(broker, change):
    broker._client.market.update(change)
    with pytest.raises(ValueError, match="Kalshi"):
        broker.submit_order(order())
    assert all(call[0] != "POST" for call in broker._client.calls)


def test_modify_and_cancel_follow_provider_and_lifecycle(broker):
    obj = broker.submit_order(order())
    broker.modify_order(obj, limit_price=0.4123)
    assert obj.limit_price == 0.4123
    assert broker._pull_order(obj.identifier, "test").limit_price == 0.4123
    amend = next(c for c in broker._client.calls if c[1].endswith("/amend"))
    assert amend[3]["count"] == "2.00"
    with pytest.raises(ValueError, match="limit_price only"):
        broker.modify_order(obj, stop_price=0.2)
    obj.status = Order.OrderStatus.CANCELLING
    broker.cancel_order(obj)
    assert obj.status == Order.OrderStatus.CANCELED
    assert obj._closed_event.is_set()
    assert any(c[0] == "DELETE" for c in broker._client.calls)


def test_duplicate_and_out_of_order_fills_are_counted_once(broker):
    obj = broker.submit_order(order())
    broker._client.fill(obj, 0.75)
    broker._reconcile_order(obj.identifier)
    assert obj.status == Order.OrderStatus.PARTIALLY_FILLED
    assert obj.get_fill_price() == 0.4321
    stale = copy.deepcopy(broker._client.orders[obj.identifier])
    broker._client.fill(obj, 1.25, "0.4567")
    broker._client.fills.append(copy.deepcopy(broker._client.fills[0]))
    broker._reconcile_order(obj.identifier)
    broker._reconcile_order(obj.identifier)
    broker._apply_snapshot(stale, "test")
    assert obj.status == Order.OrderStatus.FILLED
    assert sum(Decimal(str(t.quantity)) for t in obj.transactions) == Decimal("2.00")
    assert obj.get_fill_price() == pytest.approx((0.75 * 0.4321 + 1.25 * 0.4567) / 2)
    assert obj.trade_cost == pytest.approx(0.0002)
    assert obj._closed_event.is_set()
    assert broker.get_tracked_position("test", ASSET).quantity == 2


def test_partial_fill_then_cancel_keeps_fill_and_terminal_callback(broker):
    obj = broker.submit_order(order())
    broker._client.fill(obj, 0.75, cancel=True)
    broker._reconcile_order(obj.identifier)
    assert obj.status == Order.OrderStatus.CANCELED
    assert len(obj.transactions) == 1
    assert obj.transactions[0].quantity == 0.75
    assert broker.get_tracked_position("test", ASSET).quantity == 0.75


def test_order_status_waits_for_authoritative_fills(broker):
    obj = broker.submit_order(order())
    row = broker._client.orders[obj.identifier]
    row.update(fill_count_fp="2.00", remaining_count_fp="0.00", status="executed")
    with pytest.raises(KalshiAPIError, match="not yet consistent"):
        broker._reconcile_order(obj.identifier)
    assert not obj.is_filled()
    assert not obj.transactions


def test_imported_history_does_not_emit_old_fill_callbacks(broker):
    obj = broker.submit_order(order())
    broker._client.fill(obj, 2)
    fresh = Kalshi(client=broker._client, connect_stream=False)
    try:
        fresh._on_filled_order = Mock()
        fresh.sync_orders("new-strategy")
        pulled = fresh.get_tracked_order(obj.identifier)
        assert pulled.status == Order.OrderStatus.FILLED
        assert sum(t.quantity for t in pulled.transactions) == 2
        fresh._on_filled_order.assert_not_called()
    finally:
        fresh.cleanup_streams()


@pytest.mark.parametrize("imported", [False, True])
def test_fills_ahead_of_order_snapshot_wait_for_consistent_state(broker, imported):
    obj = broker.submit_order(order())
    broker._client.fill(obj, 0.75)
    stale = copy.deepcopy(broker._client.orders[obj.identifier])
    broker._client.fill(obj, 1.25)
    target = Kalshi(client=broker._client, connect_stream=False) if imported else broker
    try:
        with pytest.raises(KalshiAPIError, match="not yet consistent"):
            target._apply_snapshot(stale, "test")
        assert not obj.transactions
        target._apply_snapshot(broker._client.orders[obj.identifier], "test")
        tracked = target.get_tracked_order(obj.identifier)
        assert tracked.is_filled()
        assert sum(t.quantity for t in tracked.transactions) == 2
        assert tracked._closed_event.is_set()
    finally:
        if imported:
            target.cleanup_streams()


def test_inherited_plural_submit_cancel_and_atomic_rejection(broker):
    orders = broker.submit_orders([order(), order(side="sell")])
    assert len(orders) == 2
    broker.cancel_orders(orders)
    assert all(obj.status == Order.OrderStatus.CANCELED for obj in orders)
    with pytest.raises(NotImplementedError, match="atomic"):
        broker.submit_orders([order()], is_multileg=True)


@pytest.mark.parametrize("status", [None, 409, 500])
def test_uncertain_submission_preserves_client_id(broker, status):
    original = broker._client.request
    ids = []

    def fail(method, path, **kwargs):
        if method == "POST":
            ids.append(kwargs["json"]["client_order_id"])
            raise KalshiAPIError("uncertain", status_code=status)
        return original(method, path, **kwargs)

    broker._client.request = fail
    obj = order()
    for _ in range(2):
        with pytest.raises(KalshiAPIError):
            broker.submit_order(obj)
    assert ids[0] == ids[1]
    assert obj.status == Order.OrderStatus.UNKNOWN


def test_rejected_submission_emits_error(broker):
    original = broker._client.request

    def reject(method, path, **kwargs):
        if method == "POST":
            raise KalshiAPIError("rejected", status_code=400)
        return original(method, path, **kwargs)

    broker._client.request = reject
    obj = order()
    with pytest.raises(KalshiAPIError):
        broker.submit_order(obj)
    assert obj.status == Order.OrderStatus.ERROR
    assert obj._closed_event.is_set()


def test_stream_dispatch_and_overflow_request_reconciliation(broker):
    broker.stream = KalshiStream(broker)
    broker._register_stream_events()
    obj = broker.submit_order(order())
    broker._client.fill(obj, 2)
    broker.stream._receive({"type": "fill", "msg": {"order_id": obj.identifier}})
    event, payload = broker.stream._queue.get_nowait()
    broker.stream._process_queue_event(event, payload)
    broker.stream._queue.task_done()
    assert obj.is_filled()
    broker.stream._repair.clear()
    for _ in range(101):
        broker.stream._receive({"type": "fill", "msg": {"order_id": obj.identifier}})
    assert broker.stream._repair.is_set()
    with pytest.raises(KalshiAPIError, match="subscription"):
        broker.stream._receive({"type": "error"})


class ContractStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        pass


def test_real_strategy_singular_plural_and_order_accessors(broker):
    strategy = ContractStrategy(broker=broker, analyze_backtest=False, synchronize_broker_on_start=False)
    assert strategy.update_broker_balances()
    assert strategy.get_cash() == 100.1234
    assert strategy.get_portfolio_value() == 105.1234
    first = strategy.create_order(ASSET, 1, "buy", limit_price=0.4, time_in_force="gtc")
    second = strategy.create_order(ASSET, 1, "sell", limit_price=0.4, time_in_force="gtc")
    strategy.submit_order([first, second])
    assert strategy.get_order(first.identifier).identifier == first.identifier
    assert len(strategy.get_orders()) == 2
    assert strategy.get_last_price(ASSET) == 0.4321
    assert strategy.get_last_prices([ASSET])[ASSET] == 0.4321
    assert strategy.get_quote(ASSET).bid == 0.42
    assert strategy.get_historical_prices(ASSET, 2).df.empty
    assert strategy.get_historical_prices_for_assets([ASSET], 2)[ASSET].df.empty
    strategy.modify_order(first, limit_price=0.41)
    strategy.cancel_orders([first, second])
    assert all(obj.status == Order.OrderStatus.CANCELED for obj in strategy.get_orders())
    assert strategy.get_position(ASSET) is None
    assert all(p.asset.symbol == "USD" for p in strategy.get_positions())


def test_lost_submit_response_recovers_original_order_and_fill(broker):
    original = broker._client.request

    def lose_response(method, path, **kwargs):
        response = original(method, path, **kwargs)
        if method == "POST":
            raise KalshiAPIError("response lost")
        return response

    broker._client.request = lose_response
    obj = order()
    with pytest.raises(KalshiAPIError):
        broker.submit_order(obj)
    broker._client.request = original
    broker.sync_orders("test")
    assert broker.get_tracked_order("1") is obj
    assert obj.identifier == "1"
    assert obj.was_transmitted()
    assert not broker._pending_submissions
    broker._client.fill(obj, 2)
    broker.sync_orders("test")
    assert obj.is_filled()
    assert len(obj.transactions) == 1
    with pytest.raises(ValueError, match="already submitted"):
        broker.submit_order(obj)


@pytest.mark.parametrize(
    "row,side",
    [
        ({"book_side": "bid"}, "buy"),
        ({"book_side": "ask"}, "sell"),
        ({"outcome_side": "yes"}, "buy"),
        ({"outcome_side": "no"}, "sell"),
        ({"action": "buy", "side": "yes"}, "buy"),
        ({"action": "sell", "side": "yes"}, "sell"),
        ({"action": "buy", "side": "no"}, "sell"),
        ({"action": "sell", "side": "no"}, "buy"),
    ],
)
def test_current_and_legacy_order_direction(broker, row, side):
    assert broker._side(row) == side


def test_unknown_direction_and_inconsistent_counts_rejected(broker):
    with pytest.raises(KalshiAPIError, match="direction"):
        broker._side({"side": "unknown"})
    for counts in [("2", "1", "2"), ("-1", "1", "1"), ("3", "0", "2")]:
        with pytest.raises(KalshiAPIError, match="quantities"):
            broker._counts(dict(zip(("fill_count_fp", "remaining_count_fp", "initial_count_fp"), counts)))


def test_get_order_404_searches_history_and_preserves_account_scope(broker):
    assert broker._pull_broker_order("absent") is None
    assert any(call[1] == "/historical/orders" for call in broker._client.calls)
    obj = broker.submit_order(order())
    broker._client.orders[obj.identifier]["subaccount_number"] = 3
    assert broker._pull_broker_order(obj.identifier) is None
    broker._client.request = Mock(side_effect=KalshiAPIError("unavailable", status_code=503))
    with pytest.raises(KalshiAPIError):
        broker._pull_broker_order("one")


def test_unknown_provider_status_is_not_assumed_filled(broker):
    obj = broker.submit_order(order())
    broker._client.orders[obj.identifier]["status"] = "future_state"
    broker._reconcile_order(obj.identifier)
    assert obj.status == Order.OrderStatus.UNKNOWN
    assert not obj.transactions


def test_read_failure_after_accepted_submit_does_not_raise_or_resubmit(broker):
    original = broker._client.request

    def get_fails(method, path, **kwargs):
        if path.startswith("/portfolio/orders/"):
            raise KalshiAPIError("temporary", status_code=503)
        return original(method, path, **kwargs)

    broker._client.request = get_fails
    obj = broker.submit_order(order())
    assert obj.was_transmitted()
    assert obj.status == Order.OrderStatus.NEW
    assert len(broker._client.orders) == 1


@pytest.mark.parametrize("subaccount", [-1, 64, True, 1.5, "bad"])
def test_bad_subaccount_rejected_before_connection(subaccount):
    with pytest.raises(ValueError, match="SUBACCOUNT"):
        Kalshi({"SUBACCOUNT": subaccount}, client=FakeClient(), connect_stream=False)


def test_cancel_events_held_during_sync_are_not_duplicated(broker):
    obj = broker.submit_order(order())
    broker._hold_trade_events = True
    broker.cancel_order(obj)
    broker._reconcile_order(obj.identifier)
    assert len(broker._held_trades) == 1
    broker._hold_trade_events = False
    broker.process_held_trades()
    assert obj.is_canceled()


def test_external_partial_order_is_seeded_then_only_new_fill_notifies(broker):
    obj = broker.submit_order(order())
    broker._client.fill(obj, 0.5)
    fresh = Kalshi(client=broker._client, connect_stream=False)
    fresh._add_subscriber(
        SimpleNamespace(
            name="test",
            add_event=Mock(),
            PARTIALLY_FILLED_ORDER="partial_fill",
            FILLED_ORDER="fill",
            NEW_ORDER="new",
            CANCELED_ORDER="cancel",
        )
    )
    try:
        fresh.sync_orders("test")
        tracked = fresh.get_tracked_order(obj.identifier)
        assert tracked.status == Order.OrderStatus.PARTIALLY_FILLED
        broker._client.fill(obj, 1.5)
        fresh.sync_orders("test")
        assert tracked.is_filled()
        assert [float(t.quantity) for t in tracked.transactions] == [0.5, 1.5]
    finally:
        fresh.cleanup_streams()
