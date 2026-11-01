"""Broker status observations must not replace processed fill receipts."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from alpaca.trading.client import TradingClient
from alpaca.trading.models import Order as ProviderOrder

from lumibot.brokers.alpaca import Alpaca
from lumibot.data_sources.alpaca_data import AlpacaData
from lumibot.entities import Asset, Order


def test_credit_package_read_normalizes_its_child_contracts_and_links():
    """SDK schema plus the signed mleg response demonstrated in synthetic Dev."""
    at = datetime(2027, 1, 4, tzinfo=timezone.utc)

    def leg(number, symbol, side, price):
        return ProviderOrder(
            id=UUID(int=number), client_order_id=f"neutral-leg-{number}", asset_id=UUID(int=number + 10),
            created_at=at, updated_at=at, submitted_at=at,
            symbol=symbol, asset_class="us_option", qty="1", filled_qty="1", filled_avg_price=price,
            side=side, order_class="simple", type="market", time_in_force="day", status="filled", extended_hours=False,
        )

    response = ProviderOrder(
        id=UUID(int=100), client_order_id="neutral-package", created_at=at, updated_at=at, submitted_at=at,
        qty="1", filled_qty="1", filled_avg_price="-0.8", side="buy", order_class="mleg", type="market",
        time_in_force="day", status="filled", extended_hours=False, legs=[
            leg(1, "SYN270115C00100000", "buy", "2"),
            leg(2, "SYN270115C00105000", "sell", "2.8"),
        ],
    )
    broker = object.__new__(Alpaca)
    parent = broker._parse_broker_order(response, "strategy")
    assert parent.is_parent()
    assert parent.asset.asset_type == Asset.AssetType.MULTILEG
    assert parent.asset.symbol == "SYN"
    from decimal import Decimal
    assert Decimal(str(parent.avg_fill_price)) == Decimal("-0.8")
    assert len(parent.child_orders) == 2
    assert [child.parent_identifier for child in parent.child_orders] == [parent.identifier] * 2
    assert [child.asset.strike for child in parent.child_orders] == [100, 105]
    assert all(child.asset.multiplier == 100 and child.asset.symbol == "SYN" for child in parent.child_orders)
    assert [child.filled_quantity for child in parent.child_orders] == [1, 1]


def test_direct_order_read_requests_the_complete_nested_provider_order():
    at = datetime(2027, 1, 4, tzinfo=timezone.utc)
    response = ProviderOrder(
        id=UUID(int=1), client_order_id="neutral-order", asset_id=UUID(int=2),
        created_at=at, submitted_at=at, updated_at=at,
        symbol="SYN", asset_class="us_equity", qty="1", filled_qty="0",
        side="buy", order_class="simple", type="market", time_in_force="day", status="new", extended_hours=False,
    )
    broker = object.__new__(Alpaca)
    broker.api = MagicMock(spec=TradingClient)
    broker.api.get_order_by_id.return_value = response
    assert broker._pull_broker_order(response.id) is response
    assert broker.api.get_order_by_id.call_args.kwargs["filter"].nested is True


@pytest.mark.parametrize("string_identifier", [False, True])
@pytest.mark.parametrize("prior_partial", [False, True])
@pytest.mark.parametrize("lookup_only", [False, True])
def test_polling_delivers_a_fill_after_status_was_already_synchronized(
    string_identifier, prior_partial, lookup_only,
):
    # The SDK model uses the provider's ordinary order-response schema. All
    # identifiers and values are synthetic; there is no network access.
    response = ProviderOrder(
        id=UUID(int=1), client_order_id="neutral-order", asset_id=UUID(int=2),
        created_at=datetime(2027, 1, 4, tzinfo=timezone.utc), symbol="SPY",
        updated_at=datetime(2027, 1, 4, tzinfo=timezone.utc),
        submitted_at=datetime(2027, 1, 4, tzinfo=timezone.utc),
        asset_class="us_equity", qty="2", filled_qty="2", filled_avg_price="101.25",
        order_class="simple", type="market", side="buy", time_in_force="day",
        status="filled", extended_hours=False,
    )
    client = MagicMock(spec=TradingClient)
    client.get_orders.return_value = [] if lookup_only else [response]
    client.get_order_by_id.return_value = response
    client.get_all_positions.return_value = []
    broker = Alpaca(
        {"PAPER": True}, trading_client=client,
        data_source=AlpacaData({"OAUTH_TOKEN": "synthetic-provider-token"}),
        connect_stream=False, start_orders_thread=False,
    )
    subscriber = SimpleNamespace(
        name="neutral", FILLED_ORDER=broker.FILLED_ORDER,
        PARTIALLY_FILLED_ORDER=broker.PARTIALLY_FILLED_ORDER, add_event=MagicMock(),
    )
    callback_lock_states = []
    subscriber.add_event.side_effect = lambda *_args: callback_lock_states.append(broker._lock._is_owned())
    broker._add_subscriber(subscriber)
    broker.stream = broker._get_stream_object()
    broker._register_stream_events()
    order = Order("neutral", Asset("SPY"), 2, "buy", identifier=str(response.id) if string_identifier else response.id)
    broker._process_new_order(order)
    if prior_partial:
        broker._process_trade_event(order, broker.PARTIALLY_FILLED_ORDER, price=100, filled_quantity=1)
        subscriber.add_event.reset_mock()
        callback_lock_states.clear()
    # sync_broker copies status before the polling thread sees the observation.
    # No transaction/callback has been processed merely by copying this string.
    order.status = "filled"
    broker.do_polling()
    while not broker.stream._queue.empty():
        event, payload = broker.stream._queue.get_nowait()
        broker.stream._process_queue_event(event, payload)
        broker.stream._queue.task_done()
    expected = [(1, 100), (1, 102.5)] if prior_partial else [(2, 101.25)]
    assert [(t.quantity, t.price) for t in order.transactions] == expected
    subscriber.add_event.assert_called_once()
    assert callback_lock_states == [False]
    assert subscriber.add_event.call_args.args[0] == broker.FILLED_ORDER
    broker.do_polling()
    assert len(order.transactions) == len(expected)
    subscriber.add_event.assert_called_once()


def test_cumulative_partial_observations_only_process_new_executions():
    broker = object.__new__(Alpaca)
    import threading
    broker._lock = threading.RLock()
    order = Order("neutral", Asset("SPY"), 10, "buy", identifier="synthetic")
    events = []
    def process(order, event, price, filled_quantity, multiplier):
        order.add_transaction(price, filled_quantity)
        order.status = "partial_fill" if event == broker.PARTIALLY_FILLED_ORDER else "fill"
        events.append(event)
    broker._process_trade_event = process
    for quantity, price in [(2, 100), (5, 102), (5, 102), (2, 100), (10, 103)]:
        observed = SimpleNamespace(id="synthetic", filled_qty=quantity, filled_avg_price=price,
                                   status="filled" if quantity == 10 else "partially_filled")
        assert broker._reconcile_confirmed_fill(order, observed)
    assert [t.quantity for t in order.transactions] == [2, 3, 5]
    assert sum(t.quantity * t.price for t in order.transactions) == 1030
    assert events == [broker.PARTIALLY_FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER, broker.FILLED_ORDER]


def test_order_reads_include_old_working_orders_and_paginate_them():
    broker = object.__new__(Alpaca)
    broker.api = MagicMock(spec=TradingClient)
    recent = SimpleNamespace(id='recent', submitted_at=datetime(2027, 1, 4, tzinfo=timezone.utc))
    first_page = [SimpleNamespace(id=f'open-{i}', submitted_at=datetime(2026, 1, 4, tzinfo=timezone.utc)) for i in range(500)]
    older = SimpleNamespace(id='older-open', submitted_at=datetime(2025, 1, 4, tzinfo=timezone.utc))
    broker.api.get_orders.side_effect = [[recent], first_page, [older]]
    rows = broker._pull_broker_all_orders()
    assert {str(row.id) for row in rows} == {'recent', 'older-open', *(f'open-{i}' for i in range(500))}
    calls = broker.api.get_orders.call_args_list
    assert all(call.kwargs["filter"].nested is True for call in calls)
    assert calls[1].kwargs['filter'].status.value == 'open'
    assert calls[2].kwargs['filter'].until == first_page[-1].submitted_at


@pytest.mark.parametrize("quantity", ["0.3", "0.123456789123456789"])
def test_alpaca_order_parser_preserves_provider_decimal_quantity(quantity):
    from decimal import Decimal
    response = ProviderOrder(
        id=UUID(int=3), client_order_id="fractional-order", asset_id=UUID(int=4),
        created_at=datetime(2027, 1, 4, tzinfo=timezone.utc), symbol="BTCUSD",
        updated_at=datetime(2027, 1, 4, tzinfo=timezone.utc),
        submitted_at=datetime(2027, 1, 4, tzinfo=timezone.utc),
        asset_class="crypto", qty=quantity, filled_qty=quantity, filled_avg_price="100",
        order_class="simple", type="market", side="buy", time_in_force="gtc",
        status="filled", extended_hours=False,
    )
    broker = object.__new__(Alpaca)
    parsed = broker._parse_broker_order(response, "neutral")
    assert isinstance(parsed.quantity, Decimal)
    assert parsed.quantity == Decimal(quantity)


@pytest.mark.parametrize("lookup_only", [False, True])
def test_polling_reconciles_same_status_partial_then_cancel(lookup_only):
    """Cumulative quantity changes even while the normalized status is unchanged."""
    from decimal import Decimal
    response = ProviderOrder(
        id=UUID(int=5), client_order_id="partial-order", asset_id=UUID(int=6),
        created_at=datetime(2027, 1, 4, tzinfo=timezone.utc), symbol="BTCUSD",
        updated_at=datetime(2027, 1, 4, tzinfo=timezone.utc),
        submitted_at=datetime(2027, 1, 4, tzinfo=timezone.utc),
        asset_class="crypto", qty="10", filled_qty="2", filled_avg_price="100",
        order_class="simple", type="limit", limit_price="110", side="buy", time_in_force="gtc",
        status="partially_filled", extended_hours=False,
    )
    client = MagicMock(spec=TradingClient)
    client.get_all_positions.return_value = []
    broker = Alpaca(
        {"PAPER": True}, trading_client=client,
        data_source=AlpacaData({"OAUTH_TOKEN": "synthetic-provider-token"}),
        connect_stream=False, start_orders_thread=False,
    )
    subscriber = SimpleNamespace(
        name="neutral", FILLED_ORDER=broker.FILLED_ORDER,
        CANCELED_ORDER=broker.CANCELED_ORDER, ERROR_ORDER=broker.ERROR_ORDER,
        PARTIALLY_FILLED_ORDER=broker.PARTIALLY_FILLED_ORDER, add_event=MagicMock(),
    )
    broker._add_subscriber(subscriber)
    broker.stream = broker._get_stream_object()
    broker._register_stream_events()
    order = broker._parse_broker_order(response, "neutral")
    broker._process_broker_synced_order(order)
    for quantity, status in [("5", "partially_filled"), ("5", "partially_filled"), ("6", "canceled"), ("6", "canceled")]:
        observation = response.model_copy(update={"filled_qty": quantity, "filled_avg_price": "100", "status": status})
        client.get_orders.return_value = [] if lookup_only else [observation]
        client.get_order_by_id.return_value = observation
        broker.do_polling()
        while not broker.stream._queue.empty():
            event, payload = broker.stream._queue.get_nowait()
            broker.stream._process_queue_event(event, payload)
            broker.stream._queue.task_done()
    assert [t.quantity for t in order.transactions] == [2, 3, 1]
    assert order.filled_quantity == Decimal("6")
    assert order.is_canceled()
    assert [call.args[0] for call in subscriber.add_event.call_args_list] == [
        broker.PARTIALLY_FILLED_ORDER, broker.PARTIALLY_FILLED_ORDER, broker.CANCELED_ORDER,
    ]


def test_polling_isolates_invalid_observation_and_still_verifies_missing_order():
    template = ProviderOrder(
        id=UUID(int=11), client_order_id="isolated-order", asset_id=UUID(int=12),
        updated_at=datetime(2027, 1, 4, tzinfo=timezone.utc),
        created_at=datetime(2027, 1, 4, tzinfo=timezone.utc), symbol="SPY",
        submitted_at=datetime(2027, 1, 4, tzinfo=timezone.utc),
        asset_class="us_equity", qty="1", filled_qty="0", filled_avg_price=None,
        order_class="simple", type="limit", limit_price="100", side="buy", time_in_force="day",
        status="new", extended_hours=False,
    )
    invalid = template.model_copy(update={"filled_qty": "2", "filled_avg_price": "100", "status": "partially_filled"})
    filled = template.model_copy(update={"id": UUID(int=13), "filled_qty": "1", "filled_avg_price": "100", "status": "filled"})
    missing = template.model_copy(update={"id": UUID(int=14), "status": "canceled"})
    client = MagicMock(spec=TradingClient)
    client.get_orders.return_value = [invalid, filled]
    client.get_order_by_id.return_value = missing
    client.get_all_positions.return_value = []
    broker = Alpaca({"PAPER": True}, trading_client=client,
                    data_source=AlpacaData({"OAUTH_TOKEN": "synthetic-provider-token"}),
                    connect_stream=False, start_orders_thread=False)
    tracked = []
    for response in [template, filled.model_copy(update={"status": "new", "filled_qty": "0"}),
                     missing.model_copy(update={"status": "new"})]:
        order = broker._parse_broker_order(response, "neutral")
        broker._process_new_order(order)
        tracked.append(order)
    broker.do_polling()
    assert tracked[0].transactions == []
    assert tracked[1].is_filled()
    assert sum(t.quantity for t in tracked[1].transactions) == 1
    assert tracked[2].is_canceled()
    client.get_order_by_id.assert_called_once_with(str(missing.id))
