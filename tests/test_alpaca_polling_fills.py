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
